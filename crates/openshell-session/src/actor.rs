//! Per-session lifecycle actor.
//!
//! One actor owns all state for one [`SessionKey`] and serializes every
//! lifecycle transition. Callers talk to it through a command channel.

use std::collections::BTreeMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::time::Duration;

use tokio::sync::{Semaphore, mpsc, oneshot};
use tokio::time::{Instant, sleep};

use crate::config::{RuntimeConfig, WakeStrategy};
use crate::error::Error;
use crate::gateway::{CreateSpec, Gateway, Phase};
use crate::ids::{Generation, SandboxName, SessionKey};

/// A held lease on a ready sandbox.
pub(crate) struct Lease {
    pub(crate) name: SandboxName,
    pub(crate) generation: Generation,
    pub(crate) alive: Arc<AtomicBool>,
    pub(crate) max_output_bytes: usize,
    pub(crate) max_input_bytes: usize,
    pub(crate) exec_timeout: Duration,
    pub(crate) exec_permits: Arc<Semaphore>,
    pub(crate) current_generation: Arc<AtomicU64>,
    pub(crate) shutdown: Arc<AtomicBool>,
    release: mpsc::UnboundedSender<Command>,
}

impl Drop for Lease {
    fn drop(&mut self) {
        let _ = self.release.send(Command::Release {
            generation: self.generation,
        });
    }
}

/// Result of one reap pass.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub(crate) struct ReapOutcome {
    pub(crate) stopped: usize,
    pub(crate) deleted: usize,
    pub(crate) empty: bool,
}

/// Commands accepted by a session actor.
pub(crate) enum Command {
    Acquire {
        reply: oneshot::Sender<Result<Lease, Error>>,
    },
    Release {
        generation: Generation,
    },
    Close {
        reply: oneshot::Sender<Result<(), Error>>,
    },
    Reap {
        reply: oneshot::Sender<ReapOutcome>,
    },
    Idle {
        generation: Generation,
    },
    Shutdown,
}

/// Owns the lifecycle of one sandbox.
pub(crate) struct Actor {
    gateway: Arc<dyn Gateway>,
    config: RuntimeConfig,
    key: SessionKey,
    rx: mpsc::UnboundedReceiver<Command>,
    tx: mpsc::UnboundedSender<Command>,
    sandbox: Option<SandboxName>,
    generation: Option<Generation>,
    phase: Phase,
    leases: usize,
    last_release: Option<Instant>,
    stopped_at: Option<Instant>,
    alive: Arc<AtomicBool>,
    current_generation: Arc<AtomicU64>,
    shutdown: Arc<AtomicBool>,
    ever_created: bool,
    exec_permits: Arc<Semaphore>,
}

impl Actor {
    pub(crate) fn new(
        gateway: Arc<dyn Gateway>,
        config: RuntimeConfig,
        key: SessionKey,
        rx: mpsc::UnboundedReceiver<Command>,
        tx: mpsc::UnboundedSender<Command>,
        alive: Arc<AtomicBool>,
        shutdown: Arc<AtomicBool>,
    ) -> Self {
        let exec_permits = Arc::new(Semaphore::new(config.lifecycle.max_concurrent_execs));
        Self {
            gateway,
            config,
            key,
            rx,
            tx,
            sandbox: None,
            generation: None,
            phase: Phase::Absent,
            leases: 0,
            last_release: None,
            stopped_at: None,
            alive,
            current_generation: Arc::new(AtomicU64::new(0)),
            shutdown,
            ever_created: false,
            exec_permits,
        }
    }

    pub(crate) async fn run(mut self) {
        while let Some(command) = self.rx.recv().await {
            match command {
                Command::Acquire { reply } => {
                    let result = self.acquire().await;
                    let _ = reply.send(result);
                }
                Command::Release { generation } => self.release(generation),
                Command::Close { reply } => {
                    let result = self.close().await;
                    let failed = result.is_err();
                    let _ = reply.send(result);
                    if !failed {
                        break;
                    }
                }
                Command::Reap { reply } => {
                    let outcome = self.reap().await;
                    let _ = reply.send(outcome);
                    if outcome.empty {
                        break;
                    }
                }
                Command::Idle { generation } => self.idle(generation).await,
                Command::Shutdown => break,
            }
        }
        self.alive.store(false, Ordering::SeqCst);
    }

    async fn acquire(&mut self) -> Result<Lease, Error> {
        if let Err(error) = self.ensure_ready().await {
            if !self.ever_created && matches!(self.phase, Phase::Absent) {
                self.sandbox = None;
                self.generation = None;
                self.current_generation.store(0, Ordering::SeqCst);
            } else if self.leases == 0 && self.sandbox.is_some() {
                self.last_release = Some(Instant::now());
                if let Some(generation) = self.generation {
                    self.schedule_idle(generation);
                }
            }
            return Err(error);
        }
        self.leases += 1;
        let name = self
            .sandbox
            .clone()
            .ok_or_else(|| Error::Internal("ready sandbox has no name".to_owned()))?;
        let generation = self
            .generation
            .ok_or_else(|| Error::Internal("ready sandbox has no generation".to_owned()))?;
        let lease = Lease {
            name,
            generation,
            alive: self.alive.clone(),
            max_output_bytes: self.config.lifecycle.max_output_bytes,
            max_input_bytes: self.config.lifecycle.max_input_bytes,
            exec_timeout: self.config.lifecycle.exec_timeout,
            exec_permits: self.exec_permits.clone(),
            current_generation: self.current_generation.clone(),
            shutdown: self.shutdown.clone(),
            release: self.tx.clone(),
        };
        Ok(lease)
    }

    async fn ensure_ready(&mut self) -> Result<(), Error> {
        if self.shutdown.load(Ordering::SeqCst) {
            return Err(Error::Unavailable("runtime is shutting down".to_owned()));
        }
        if self.sandbox.is_none() {
            if let Some((name, generation)) = self.adopt().await? {
                self.set_generation(generation);
                self.sandbox = Some(name);
                self.ever_created = true;
            } else {
                let generation = Generation::new();
                self.set_generation(generation);
                self.sandbox = Some(SandboxName::derive(&self.key, generation));
            }
        }

        for _ in 0..4 {
            let name = self
                .sandbox
                .clone()
                .ok_or_else(|| Error::Internal("sandbox name disappeared".to_owned()))?;
            let phase = self
                .gateway
                .get(&name)
                .await?
                .map_or(Phase::Absent, |info| info.phase);
            self.phase = phase;

            match phase {
                Phase::Ready => {
                    if self.shutdown.load(Ordering::SeqCst) {
                        return Err(Error::Unavailable("runtime is shutting down".to_owned()));
                    }
                    self.stopped_at = None;
                    self.alive.store(true, Ordering::SeqCst);
                    return Ok(());
                }
                Phase::Absent => {
                    if self.ever_created {
                        if self.leases > 0 {
                            return Err(Error::Conflict(
                                "sandbox disappeared while a lease is held".to_owned(),
                            ));
                        }
                        self.recreate();
                        continue;
                    }
                    let created = self
                        .gateway
                        .create(CreateSpec {
                            name: name.clone(),
                            template: self.config.template.default.clone(),
                            labels: self.labels(),
                            command: Vec::new(),
                        })
                        .await?;
                    self.phase = created.phase;
                    self.ever_created = true;
                    self.gateway
                        .wait_ready(&name, self.config.lifecycle.ready_timeout)
                        .await?;
                }
                Phase::Stopped | Phase::Completed => {
                    self.gateway.start(&name).await?;
                    self.gateway
                        .wait_ready(&name, self.config.lifecycle.ready_timeout)
                        .await?;
                    self.ever_created = true;
                }
                Phase::Suspended => match self.gateway.resume(&name).await {
                    Ok(()) => {
                        self.gateway
                            .wait_ready(&name, self.config.lifecycle.ready_timeout)
                            .await?;
                        self.ever_created = true;
                    }
                    Err(Error::Unsupported(_)) => {
                        self.gateway.start(&name).await?;
                        self.gateway
                            .wait_ready(&name, self.config.lifecycle.ready_timeout)
                            .await?;
                        self.ever_created = true;
                    }
                    Err(error) => return Err(error),
                },
                Phase::Provisioning | Phase::Starting => {
                    self.gateway
                        .wait_ready(&name, self.config.lifecycle.ready_timeout)
                        .await?;
                    self.ever_created = true;
                }
                Phase::Stopping => {
                    self.wait_for(&name, Phase::Stopped, self.config.lifecycle.stop_timeout)
                        .await?;
                    self.gateway.start(&name).await?;
                    self.gateway
                        .wait_ready(&name, self.config.lifecycle.ready_timeout)
                        .await?;
                    self.ever_created = true;
                }
                Phase::Error => {
                    if self.leases > 0 {
                        return Err(Error::Conflict(
                            "sandbox needs recovery while a lease is held".to_owned(),
                        ));
                    }
                    match self.gateway.stop(&name).await {
                        Ok(()) => {}
                        Err(Error::NotFound(_)) => {
                            self.recreate();
                            continue;
                        }
                        Err(error) => return Err(error),
                    }
                    self.gateway.start(&name).await?;
                    self.gateway
                        .wait_ready(&name, self.config.lifecycle.ready_timeout)
                        .await?;
                    self.ever_created = true;
                    continue;
                }
                Phase::Deleting => {
                    if self.leases > 0 {
                        return Err(Error::Conflict(
                            "sandbox is being deleted while a lease is held".to_owned(),
                        ));
                    }
                    self.gateway
                        .wait_deleted(&name, self.config.lifecycle.delete_timeout)
                        .await?;
                    self.recreate();
                }
                Phase::Unknown => {
                    return Err(Error::Gateway("unknown sandbox phase".to_owned()));
                }
            }
        }

        Err(Error::Gateway(
            "sandbox did not become ready after recovery attempts".to_owned(),
        ))
    }

    fn release(&mut self, generation: Generation) {
        if self.generation != Some(generation) {
            return;
        }
        self.leases = self.leases.saturating_sub(1);
        if self.leases == 0 {
            self.last_release = Some(Instant::now());
            self.schedule_idle(generation);
        }
    }

    fn schedule_idle(&self, generation: Generation) {
        if matches!(self.config.lifecycle.wake, WakeStrategy::KeepWarm) {
            return;
        }
        let tx = self.tx.clone();
        let after = self.config.lifecycle.idle_after;
        tokio::spawn(async move {
            sleep(after).await;
            let _ = tx.send(Command::Idle { generation });
        });
    }

    async fn idle(&mut self, generation: Generation) {
        if self.generation != Some(generation) || self.leases != 0 {
            return;
        }
        let Some(last_release) = self.last_release else {
            return;
        };
        let idle_after = self.config.lifecycle.idle_after;
        let elapsed = last_release.elapsed();
        if elapsed < idle_after {
            let remaining = idle_after - elapsed;
            let tx = self.tx.clone();
            tokio::spawn(async move {
                sleep(remaining).await;
                let _ = tx.send(Command::Idle { generation });
            });
            return;
        }

        let Some(name) = self.sandbox.clone() else {
            return;
        };
        if self.phase != Phase::Ready {
            return;
        }

        let result = match self.config.lifecycle.wake {
            WakeStrategy::SuspendResume => match self.gateway.suspend(&name).await {
                Ok(()) => {
                    self.phase = Phase::Suspended;
                    Ok(())
                }
                Err(Error::Unsupported(_)) => match self.gateway.stop(&name).await {
                    Ok(()) => {
                        self.phase = Phase::Stopped;
                        Ok(())
                    }
                    Err(error) => Err(error),
                },
                Err(error) => Err(error),
            },
            WakeStrategy::StopStart | WakeStrategy::KeepWarm => {
                match self.gateway.stop(&name).await {
                    Ok(()) => {
                        self.phase = Phase::Stopped;
                        Ok(())
                    }
                    Err(error) => Err(error),
                }
            }
        };

        match result {
            Ok(()) => {
                self.stopped_at = Some(Instant::now());
                self.alive.store(false, Ordering::SeqCst);
                tracing::debug!(sandbox = %name, "sandbox released compute while idle");
            }
            Err(Error::NotFound(_)) => {
                self.sandbox = None;
                self.generation = None;
                self.phase = Phase::Absent;
                self.ever_created = false;
                self.alive.store(false, Ordering::SeqCst);
            }
            Err(error) => {
                tracing::warn!(sandbox = %name, %error, "failed to release idle sandbox; retrying");
                if let Some(generation) = self.generation {
                    let tx = self.tx.clone();
                    let backoff = self
                        .config
                        .lifecycle
                        .idle_after
                        .min(Duration::from_secs(30));
                    tokio::spawn(async move {
                        sleep(backoff).await;
                        let _ = tx.send(Command::Idle { generation });
                    });
                }
            }
        }
    }

    async fn reap(&mut self) -> ReapOutcome {
        let mut outcome = ReapOutcome::default();
        if self.leases != 0 {
            return outcome;
        }
        let Some(name) = self.sandbox.clone() else {
            outcome.empty = self.leases == 0;
            return outcome;
        };
        let Some(last_release) = self.last_release else {
            outcome.empty = self.sandbox.is_none() && self.leases == 0;
            return outcome;
        };
        let idle_since = self.stopped_at.unwrap_or(last_release);
        if idle_since.elapsed() < self.config.lifecycle.retain_for {
            return outcome;
        }

        if self.phase == Phase::Ready {
            match self.gateway.stop(&name).await {
                Ok(()) => {
                    self.phase = Phase::Stopped;
                    outcome.stopped += 1;
                }
                Err(error) => {
                    tracing::warn!(sandbox = %name, %error, "failed to stop expired sandbox");
                    return outcome;
                }
            }
        }

        if matches!(
            self.phase,
            Phase::Stopped
                | Phase::Suspended
                | Phase::Completed
                | Phase::Error
                | Phase::Provisioning
                | Phase::Starting
        ) {
            match self.gateway.delete(&name).await {
                Ok(()) => {
                    if self
                        .gateway
                        .wait_deleted(&name, self.config.lifecycle.delete_timeout)
                        .await
                        .is_ok()
                    {
                        outcome.deleted += 1;
                        self.sandbox = None;
                        self.generation = None;
                        self.phase = Phase::Absent;
                        self.alive.store(false, Ordering::SeqCst);
                    }
                }
                Err(error) => {
                    tracing::warn!(sandbox = %name, %error, "failed to delete expired sandbox");
                }
            }
        }

        outcome.empty = self.sandbox.is_none() && self.leases == 0;
        outcome
    }

    async fn close(&mut self) -> Result<(), Error> {
        self.alive.store(false, Ordering::SeqCst);
        if let Some(name) = self.sandbox.clone() {
            self.gateway.delete(&name).await?;
            self.gateway
                .wait_deleted(&name, self.config.lifecycle.delete_timeout)
                .await?;
        }
        self.sandbox = None;
        self.generation = None;
        self.phase = Phase::Absent;
        self.leases = 0;
        self.stopped_at = None;
        Ok(())
    }

    async fn wait_for(
        &self,
        name: &SandboxName,
        target: Phase,
        timeout: Duration,
    ) -> Result<(), Error> {
        let deadline = Instant::now() + timeout;
        loop {
            if let Some(info) = self.gateway.get(name).await?
                && info.phase == target
            {
                return Ok(());
            }
            if Instant::now() >= deadline {
                return Err(Error::Timeout(format!(
                    "waiting for sandbox to reach {target:?}"
                )));
            }
            sleep(Duration::from_millis(100)).await;
        }
    }

    async fn adopt(&self) -> Result<Option<(SandboxName, Generation)>, Error> {
        let mut labels = self.key_labels();
        labels.insert(
            "app.kubernetes.io/managed-by".to_owned(),
            "infinity-openshell".to_owned(),
        );
        let mut sandboxes = self.gateway.list_managed(&labels).await?;
        sandboxes.sort_by_key(|info| info.created_at_ms.unwrap_or(0));
        for info in sandboxes.into_iter().rev() {
            let Some(raw) = info.labels.get("infinity.hydro.run/gen") else {
                continue;
            };
            let Ok(value) = raw.parse::<u64>() else {
                continue;
            };
            return Ok(Some((info.name, Generation::from_raw(value))));
        }
        Ok(None)
    }

    fn set_generation(&mut self, generation: Generation) {
        self.generation = Some(generation);
        self.current_generation
            .store(generation.as_u64(), Ordering::SeqCst);
    }

    fn recreate(&mut self) {
        let generation = Generation::new();
        self.set_generation(generation);
        self.sandbox = Some(SandboxName::derive(&self.key, generation));
        self.phase = Phase::Absent;
        self.leases = 0;
        self.last_release = None;
        self.stopped_at = None;
        self.ever_created = false;
    }

    fn key_labels(&self) -> BTreeMap<String, String> {
        let mut labels = self.config.labels.clone();
        labels.insert("infinity.hydro.run/key".to_owned(), self.key.hash_hex());
        labels
    }

    fn labels(&self) -> BTreeMap<String, String> {
        let mut labels = self.key_labels();
        labels.insert(
            "app.kubernetes.io/managed-by".to_owned(),
            "infinity-openshell".to_owned(),
        );
        labels.insert(
            "infinity.hydro.run/runtime".to_owned(),
            env!("CARGO_PKG_VERSION").to_owned(),
        );
        labels.insert(
            "infinity.hydro.run/tenant".to_owned(),
            self.key.tenant_hash_hex(),
        );
        labels.insert(
            "infinity.hydro.run/thread".to_owned(),
            self.key.thread_hash_hex(),
        );
        if let Some(generation) = self.generation {
            labels.insert(
                "infinity.hydro.run/gen".to_owned(),
                generation.as_u64().to_string(),
            );
        }
        labels
    }
}
