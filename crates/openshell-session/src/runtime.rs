//! Public runtime handle.

use std::collections::HashMap;
use std::fmt;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};

use tokio::sync::{mpsc, oneshot};

use crate::actor::{Actor, Command, ReapOutcome};
use crate::config::RuntimeConfig;
use crate::error::Error;
use crate::gateway::Gateway;
use crate::ids::SessionKey;
use crate::session::Session;

/// Summary of one reap pass.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct ReapReport {
    /// Sandboxes that were stopped because they were idle.
    pub stopped: usize,
    /// Sandboxes that were deleted because retention expired.
    pub deleted: usize,
}

/// Lease-managed OpenShell sessions.
///
/// The runtime owns one lifecycle actor per [`SessionKey`]. It is safe to share
/// across tasks. Dropping the runtime shuts down its actors; retained sandboxes
/// are left for the next runtime or for operator cleanup.
pub struct SessionRuntime {
    inner: Arc<Inner>,
}

#[derive(Clone)]
struct ActorHandle {
    id: u64,
    tx: mpsc::UnboundedSender<Command>,
    alive: Arc<AtomicBool>,
    shutdown: Arc<AtomicBool>,
}

struct Inner {
    gateway: Arc<dyn Gateway>,
    config: RuntimeConfig,
    actors: Mutex<HashMap<SessionKey, ActorHandle>>,
    next_actor_id: AtomicU64,
    shutdown: AtomicBool,
}

impl SessionRuntime {
    /// Build a runtime over an existing gateway.
    ///
    /// # Errors
    ///
    /// Returns [`Error::InvalidConfig`] when `config` fails validation.
    pub fn new(gateway: Arc<dyn Gateway>, config: RuntimeConfig) -> Result<Self, Error> {
        config.validate()?;
        Ok(Self {
            inner: Arc::new(Inner {
                gateway,
                config,
                actors: Mutex::new(HashMap::new()),
                next_actor_id: AtomicU64::new(1),
                shutdown: AtomicBool::new(false),
            }),
        })
    }

    /// Connect to the configured OpenShell gateway and build a runtime.
    ///
    /// # Errors
    ///
    /// Returns a configuration, transport, or authentication error when the
    /// gateway cannot be reached.
    #[cfg(feature = "openshell")]
    pub async fn open(config: RuntimeConfig) -> Result<Self, Error> {
        let gateway = Arc::new(crate::openshell::OpenShellGateway::connect(&config).await?);
        Self::new(gateway, config)
    }

    /// Acquire a lease on the thread's sandbox, creating or starting it when
    /// necessary.
    ///
    /// # Errors
    ///
    /// Returns a gateway error when the sandbox cannot be made ready,
    /// [`Error::Conflict`] when the session limit is reached or the sandbox
    /// needs destructive recovery while another lease is held.
    pub async fn acquire(&self, key: SessionKey) -> Result<Session, Error> {
        if self.inner.shutdown.load(Ordering::SeqCst) {
            return Err(Error::Unavailable("runtime is shutting down".to_owned()));
        }
        let handle = self.actor_handle(&key)?;
        if handle.shutdown.load(Ordering::SeqCst) {
            return Err(Error::Unavailable("runtime is shutting down".to_owned()));
        }
        let (reply, receiver) = oneshot::channel();
        if handle.tx.send(Command::Acquire { reply }).is_err() {
            self.remove_if_current(&key, handle.id);
            return Err(Error::Unavailable(
                "session actor stopped before the request was queued".to_owned(),
            ));
        }
        let lease = receiver
            .await
            .map_err(|_| Error::Internal("session actor dropped its reply".to_owned()))??;
        Ok(Session::new(self.inner.gateway.clone(), lease))
    }

    /// Delete the thread's sandbox now. Safe to call repeatedly.
    ///
    /// A failed deletion keeps the actor so a later call can retry. When no
    /// actor exists, the call is a no-op.
    ///
    /// # Errors
    ///
    /// Returns a gateway error when deletion fails.
    pub async fn close(&self, key: &SessionKey) -> Result<(), Error> {
        let handle = {
            self.inner
                .actors
                .lock()
                .expect("bug: actor map poisoned")
                .get(key)
                .cloned()
        };
        let Some(handle) = handle else {
            return Ok(());
        };
        handle.alive.store(false, Ordering::SeqCst);
        let (reply, receiver) = oneshot::channel();
        if handle.tx.send(Command::Close { reply }).is_err() {
            self.remove_if_current(key, handle.id);
            return Ok(());
        }
        let result = receiver
            .await
            .map_err(|_| Error::Internal("session actor dropped close reply".to_owned()))?;
        if result.is_ok() {
            self.remove_if_current(key, handle.id);
        }
        result
    }

    /// Stop idle sandboxes and delete sandboxes whose retention window elapsed.
    ///
    /// # Errors
    ///
    /// Reaping is best effort and currently never returns an error. The result
    /// type leaves room for a durable coordinator later.
    pub async fn reap(&self) -> Result<ReapReport, Error> {
        let handles: Vec<_> = self
            .inner
            .actors
            .lock()
            .expect("bug: actor map poisoned")
            .iter()
            .map(|(key, handle)| (key.clone(), handle.clone()))
            .collect();

        let mut report = ReapReport::default();
        for (key, handle) in handles {
            let (reply, receiver) = oneshot::channel();
            if handle.tx.send(Command::Reap { reply }).is_err() {
                self.remove_if_current(&key, handle.id);
                continue;
            }
            if let Ok(ReapOutcome {
                stopped,
                deleted,
                empty,
            }) = receiver.await
            {
                report.stopped += stopped;
                report.deleted += deleted;
                if empty {
                    self.remove_if_current(&key, handle.id);
                }
            }
        }
        Ok(report)
    }

    /// Stop all lifecycle actors without deleting retained sandboxes.
    ///
    /// Sessions that outlive the call return [`Error::StaleSession`].
    pub fn shutdown(&self) {
        self.inner.shutdown.store(true, Ordering::SeqCst);
        let handles: Vec<_> = self
            .inner
            .actors
            .lock()
            .expect("bug: actor map poisoned")
            .drain()
            .map(|(_, handle)| handle)
            .collect();
        for handle in handles {
            handle.shutdown.store(true, Ordering::SeqCst);
            handle.alive.store(false, Ordering::SeqCst);
            let _ = handle.tx.send(Command::Shutdown);
        }
    }

    fn actor_handle(&self, key: &SessionKey) -> Result<ActorHandle, Error> {
        let mut actors = self.inner.actors.lock().expect("bug: actor map poisoned");
        if self.inner.shutdown.load(Ordering::SeqCst) {
            return Err(Error::Unavailable("runtime is shutting down".to_owned()));
        }
        if let Some(handle) = actors.get(key) {
            if !handle.tx.is_closed() {
                return Ok(handle.clone());
            }
            actors.remove(key);
        }
        if actors.len() >= self.inner.config.limits.max_sessions {
            return Err(Error::Conflict(
                "maximum number of sessions reached".to_owned(),
            ));
        }
        let id = self.inner.next_actor_id.fetch_add(1, Ordering::Relaxed);
        let alive = Arc::new(AtomicBool::new(false));
        let shutdown = Arc::new(AtomicBool::new(false));
        let (sender, receiver) = mpsc::unbounded_channel();
        let actor = Actor::new(
            self.inner.gateway.clone(),
            self.inner.config.clone(),
            key.clone(),
            receiver,
            sender.clone(),
            alive.clone(),
            shutdown.clone(),
        );
        tokio::spawn(actor.run());
        let handle = ActorHandle {
            id,
            tx: sender,
            alive,
            shutdown,
        };
        actors.insert(key.clone(), handle.clone());
        Ok(handle)
    }

    fn remove_if_current(&self, key: &SessionKey, id: u64) {
        let mut actors = self.inner.actors.lock().expect("bug: actor map poisoned");
        if actors.get(key).is_some_and(|handle| handle.id == id) {
            actors.remove(key);
        }
    }
}

impl Drop for SessionRuntime {
    fn drop(&mut self) {
        self.shutdown();
    }
}

impl fmt::Debug for SessionRuntime {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        let sessions = self.inner.actors.lock().map_or(0, |actors| actors.len());
        formatter
            .debug_struct("SessionRuntime")
            .field("sessions", &sessions)
            .finish_non_exhaustive()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::GatewayAuth;
    use crate::fake::FakeGateway;
    use crate::gateway::{ExecRequest, Phase};
    use crate::ids::{TenantId, ThreadId};
    use std::time::Duration;
    use url::Url;

    fn config() -> RuntimeConfig {
        let gateway = crate::config::GatewayConfig::new(
            Url::parse("https://gateway.example.com").unwrap(),
            "default",
            GatewayAuth::None,
        );
        let mut config = RuntimeConfig::new(gateway);
        config.lifecycle.idle_after = Duration::from_secs(60);
        config.lifecycle.retain_for = Duration::from_secs(24 * 60 * 60);
        config
    }

    fn key() -> SessionKey {
        SessionKey::new(
            TenantId::new("acme").unwrap(),
            ThreadId::new("thread-1").unwrap(),
        )
    }

    async fn settle() {
        for _ in 0..32 {
            tokio::task::yield_now().await;
        }
    }

    fn count(gateway: &FakeGateway, operation: &str) -> usize {
        gateway
            .calls()
            .iter()
            .filter(|call| call.as_str() == operation)
            .count()
    }

    #[tokio::test(start_paused = true)]
    async fn acquire_creates_then_idle_stops_and_restarts() {
        let gateway = Arc::new(FakeGateway::new());
        let runtime = SessionRuntime::new(gateway.clone(), config()).unwrap();
        let key = key();

        let session = runtime.acquire(key.clone()).await.unwrap();
        let name = session.name().clone();
        assert_eq!(count(&gateway, "create"), 1);
        drop(session);
        settle().await;

        tokio::time::advance(Duration::from_secs(61)).await;
        settle().await;
        assert_eq!(count(&gateway, "stop"), 1);

        let session = runtime.acquire(key.clone()).await.unwrap();
        assert_eq!(session.name(), &name);
        assert_eq!(count(&gateway, "start"), 1);
        drop(session);

        runtime.close(&key).await.unwrap();
        assert_eq!(count(&gateway, "delete"), 1);
    }

    #[tokio::test(start_paused = true)]
    async fn a_lease_prevents_the_idle_stop() {
        let gateway = Arc::new(FakeGateway::new());
        let runtime = SessionRuntime::new(gateway.clone(), config()).unwrap();
        let key = key();

        let first = runtime.acquire(key.clone()).await.unwrap();
        let second = runtime.acquire(key.clone()).await.unwrap();
        drop(first);
        settle().await;

        tokio::time::advance(Duration::from_secs(61)).await;
        settle().await;
        assert_eq!(count(&gateway, "stop"), 0);

        drop(second);
        settle().await;
        tokio::time::advance(Duration::from_secs(61)).await;
        settle().await;
        assert_eq!(count(&gateway, "stop"), 1);
    }

    #[tokio::test(start_paused = true)]
    async fn close_invalidates_existing_sessions() {
        let gateway = Arc::new(FakeGateway::new());
        let runtime = SessionRuntime::new(gateway.clone(), config()).unwrap();
        let key = key();

        let session = runtime.acquire(key.clone()).await.unwrap();
        runtime.close(&key).await.unwrap();

        let error = session
            .exec(ExecRequest::new(vec!["true".to_owned()]))
            .await
            .unwrap_err();
        assert!(matches!(error, Error::StaleSession));
    }

    #[tokio::test(start_paused = true)]
    async fn close_is_idempotent_without_an_actor() {
        let gateway = Arc::new(FakeGateway::new());
        let runtime = SessionRuntime::new(gateway, config()).unwrap();
        runtime.close(&key()).await.unwrap();
        runtime.close(&key()).await.unwrap();
    }

    #[tokio::test(start_paused = true)]
    async fn a_failed_close_keeps_the_actor_for_a_retry() {
        let gateway = Arc::new(FakeGateway::new());
        let runtime = SessionRuntime::new(gateway.clone(), config()).unwrap();
        let key = key();

        let session = runtime.acquire(key.clone()).await.unwrap();
        drop(session);
        gateway.fail_next("delete");

        assert!(runtime.close(&key).await.is_err());
        runtime.close(&key).await.unwrap();
        assert_eq!(count(&gateway, "delete"), 2);
    }

    #[tokio::test(start_paused = true)]
    async fn an_error_phase_is_recovered_by_stop_and_start() {
        let gateway = Arc::new(FakeGateway::new());
        let runtime = SessionRuntime::new(gateway.clone(), config()).unwrap();
        let key = key();

        let session = runtime.acquire(key.clone()).await.unwrap();
        let name = session.name().clone();
        drop(session);
        gateway.set_phase(&name, Phase::Error);

        let session = runtime.acquire(key.clone()).await.unwrap();
        assert!(count(&gateway, "stop") >= 1);
        assert!(count(&gateway, "start") >= 1);
        drop(session);
        runtime.close(&key).await.unwrap();
    }

    #[tokio::test(start_paused = true)]
    async fn recovery_is_refused_while_a_lease_is_held() {
        let gateway = Arc::new(FakeGateway::new());
        let runtime = SessionRuntime::new(gateway.clone(), config()).unwrap();
        let key = key();

        let session = runtime.acquire(key.clone()).await.unwrap();
        let name = session.name().clone();
        gateway.set_phase(&name, Phase::Error);

        let error = runtime.acquire(key.clone()).await.unwrap_err();
        assert!(matches!(error, Error::Conflict(_)));
        drop(session);
        runtime.close(&key).await.unwrap();
    }

    #[tokio::test(start_paused = true)]
    async fn a_deleted_sandbox_rotates_the_generation() {
        let gateway = Arc::new(FakeGateway::new());
        let runtime = SessionRuntime::new(gateway.clone(), config()).unwrap();
        let key = key();

        let session = runtime.acquire(key.clone()).await.unwrap();
        let first_name = session.name().clone();
        drop(session);
        assert!(
            gateway.delete(&first_name).await.is_ok(),
            "delete the fake sandbox directly"
        );

        let session = runtime.acquire(key.clone()).await.unwrap();
        assert_ne!(session.name(), &first_name);
        drop(session);
        runtime.close(&key).await.unwrap();
    }

    #[tokio::test(start_paused = true)]
    async fn reap_deletes_a_sandbox_after_retention() {
        let gateway = Arc::new(FakeGateway::new());
        let runtime = SessionRuntime::new(gateway.clone(), config()).unwrap();
        let key = key();

        let session = runtime.acquire(key.clone()).await.unwrap();
        drop(session);
        settle().await;

        tokio::time::advance(Duration::from_secs(61)).await;
        settle().await;
        assert_eq!(count(&gateway, "stop"), 1);

        tokio::time::advance(Duration::from_secs(24 * 60 * 60)).await;
        let report = runtime.reap().await.unwrap();
        assert_eq!(report.deleted, 1);
        assert_eq!(count(&gateway, "delete"), 1);
    }

    #[tokio::test(start_paused = true)]
    async fn reap_skips_a_leased_sandbox() {
        let gateway = Arc::new(FakeGateway::new());
        let runtime = SessionRuntime::new(gateway.clone(), config()).unwrap();
        let key = key();

        let _session = runtime.acquire(key.clone()).await.unwrap();
        tokio::time::advance(Duration::from_secs(24 * 60 * 60)).await;
        let report = runtime.reap().await.unwrap();
        assert_eq!(report.deleted, 0);
    }

    #[tokio::test(start_paused = true)]
    async fn a_failed_idle_stop_is_retried() {
        let gateway = Arc::new(FakeGateway::new());
        let runtime = SessionRuntime::new(gateway.clone(), config()).unwrap();
        let key = key();

        let session = runtime.acquire(key.clone()).await.unwrap();
        drop(session);
        settle().await;
        gateway.fail_next("stop");

        tokio::time::advance(Duration::from_secs(61)).await;
        settle().await;
        assert_eq!(count(&gateway, "stop"), 1);

        tokio::time::advance(Duration::from_secs(61)).await;
        settle().await;
        assert!(count(&gateway, "stop") >= 2);
    }

    #[tokio::test(start_paused = true)]
    async fn shutdown_invalidates_existing_sessions() {
        let gateway = Arc::new(FakeGateway::new());
        let runtime = SessionRuntime::new(gateway, config()).unwrap();
        let key = key();

        let session = runtime.acquire(key.clone()).await.unwrap();
        runtime.shutdown();

        let error = session
            .exec(ExecRequest::new(vec!["true".to_owned()]))
            .await
            .unwrap_err();
        assert!(matches!(error, Error::StaleSession));
    }

    #[tokio::test(start_paused = true)]
    async fn acquire_after_shutdown_is_refused() {
        let gateway = Arc::new(FakeGateway::new());
        let runtime = SessionRuntime::new(gateway, config()).unwrap();
        runtime.shutdown();

        let error = runtime.acquire(key()).await.unwrap_err();
        assert!(matches!(error, Error::Unavailable(_)));
    }

    #[tokio::test(start_paused = true)]
    async fn a_failed_acquire_is_cleaned_up_after_retention() {
        let gateway = Arc::new(FakeGateway::new());
        let mut config = config();
        config.lifecycle.idle_after = Duration::from_secs(1);
        config.lifecycle.retain_for = Duration::from_secs(2);
        let runtime = SessionRuntime::new(gateway.clone(), config).unwrap();
        let key = key();

        gateway.fail_next("wait_ready");
        assert!(runtime.acquire(key.clone()).await.is_err());

        tokio::time::advance(Duration::from_secs(2)).await;
        settle().await;
        tokio::time::advance(Duration::from_secs(3)).await;
        settle().await;
        let report = runtime.reap().await.unwrap();
        assert_eq!(report.deleted, 1);
    }

    #[tokio::test(start_paused = true)]
    async fn a_new_runtime_adopts_the_retained_sandbox() {
        let gateway = Arc::new(FakeGateway::new());
        let key = key();

        let first_runtime = SessionRuntime::new(gateway.clone(), config()).unwrap();
        let session = first_runtime.acquire(key.clone()).await.unwrap();
        let name = session.name().clone();
        drop(session);
        drop(first_runtime);

        let second_runtime = SessionRuntime::new(gateway.clone(), config()).unwrap();
        let session = second_runtime.acquire(key.clone()).await.unwrap();
        assert_eq!(session.name(), &name);
        assert_eq!(count(&gateway, "create"), 1);
        drop(session);
        second_runtime.close(&key).await.unwrap();
    }

    #[tokio::test(start_paused = true)]
    async fn exec_rejects_requests_over_the_limits() {
        let gateway = Arc::new(FakeGateway::new());
        let runtime = SessionRuntime::new(gateway, config()).unwrap();
        let key = key();
        let session = runtime.acquire(key.clone()).await.unwrap();

        let empty = session
            .exec(ExecRequest::new(Vec::new()))
            .await
            .unwrap_err();
        assert!(matches!(empty, Error::InvalidInput(_)));

        let mut traversal = ExecRequest::new(vec!["sh".to_owned()]);
        traversal.workdir = Some("../escape".to_owned());
        let traversal = session.exec(traversal).await.unwrap_err();
        assert!(matches!(traversal, Error::InvalidInput(_)));

        let mut oversized = ExecRequest::new(vec!["sh".to_owned()]);
        oversized.stdin = Some(bytes::Bytes::from(vec![0_u8; 9 * 1024 * 1024]));
        let oversized = session.exec(oversized).await.unwrap_err();
        assert!(matches!(oversized, Error::InvalidInput(_)));

        drop(session);
        runtime.close(&key).await.unwrap();
    }

    #[tokio::test(start_paused = true)]
    async fn a_new_generation_gets_a_new_name_after_close() {
        let gateway = Arc::new(FakeGateway::new());
        let runtime = SessionRuntime::new(gateway.clone(), config()).unwrap();
        let key = key();

        let first = runtime.acquire(key.clone()).await.unwrap();
        let first_name = first.name().clone();
        drop(first);
        runtime.close(&key).await.unwrap();

        let second = runtime.acquire(key.clone()).await.unwrap();
        assert_ne!(second.name(), &first_name);
        drop(second);
        runtime.close(&key).await.unwrap();
    }
}
