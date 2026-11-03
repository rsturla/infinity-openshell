# Workspace archives

OpenShell keeps a stopped workspace for the retention window. After
`retain_for`, the runtime deletes the sandbox and the workspace with it. A
long-lived conversation can outlive that window.

`openshell-session` exposes a `SnapshotStore` seam. The runtime produces a tar
archive of the workspace and the embedding service stores it. The runtime
restores the archive into a replacement sandbox before the first command.

The library does not include an S3 client. The store is three methods and the
client chooses the backend.

## The seam

```rust
#[async_trait]
pub trait SnapshotStore: Send + Sync + std::fmt::Debug + 'static {
    async fn put(&self, key: &SnapshotKey, source: &Path) -> Result<SnapshotMeta, Error>;
    async fn get(&self, key: &SnapshotKey, destination: &Path) -> Result<bool, Error>;
    async fn delete(&self, key: &SnapshotKey) -> Result<(), Error>;
}
```

`SnapshotKey` is `tenant + thread`. It is stable across sandbox generations, so
a replacement sandbox restores the same workspace. Do not key archives by
sandbox name.

The runtime gives `put` a finished tar file and gives `get` an empty path to
fill. A store can copy, upload, encrypt, compress, or version the bytes. Return
`Ok(false)` from `get` when no archive exists.

## Configuration

```rust
config.snapshot.archive_on_delete = true;
config.snapshot.restore_on_create = true;
config.snapshot_store = Some(Arc::new(MyStore::new(...)));
```

| Field | Default | Meaning |
|---|---|---|
| `archive_on_delete` | `false` | Archive before an explicit close or a retention delete |
| `restore_on_create` | `false` | Restore the latest archive into a replacement sandbox |
| `temp_dir` | system temp | Local directory for the temporary tar file |
| `max_archive_bytes` | 1 GiB | Reject an archive above this size |
| `timeout` | 900s | Limit for one export or restore |

`RuntimeConfig::validate` rejects an enabled policy without a store.

## Lifecycle

- **Close**: archive, then delete. If the archive fails, the close fails and
  the sandbox stays. The caller can retry.
- **Retention delete**: archive, then delete. If the archive fails, the reaper
  leaves the sandbox in place and retries on the next pass.
- **Errored sandbox**: the reaper deletes it without archiving. A broken
  workspace is not a useful archive.
- **Create**: after the sandbox is ready and its policy is loaded, restore the
  archive before the first command. If the restore fails, the runtime deletes
  the replacement and returns the error. The next acquire retries.
- **Stop and start**: the workspace is still in place. The runtime does not
  restore.

Archiving may briefly start a stopped sandbox. Deletion is infrequent, so the
extra start is cheaper than archiving on every idle stop.

## Transfer details

The runtime exports with:

```shell
tar -C <workdir> --exclude=./.openshell-snapshot -cf - .
```

It writes the stream to `temp_dir`, enforces `max_archive_bytes`, and calls
`put`. The tar includes the workspace contents, not the image.

Restore uploads the archive in chunks of at most 4 MiB (also bounded by
`lifecycle.max_input_bytes`), then extracts with:

```shell
tar --no-same-owner --no-same-permissions --touch --no-overwrite-dir \
  -xf .openshell-snapshot/archive.tar -C <workdir>
```

The chunk upload uses the normal file-write path, so it works on every
compute driver. Large archives take one exec per chunk. Raise
`max_input_bytes` if 4 MiB chunks are too small for the workspace.

The image must contain `sh` and `tar`. The built-in Ubuntu image does.

## What is archived

The runtime archives `RuntimeConfig::workdir`. That path must match the sandbox
workspace. The command is:

```shell
tar -C <workdir> --exclude=./.openshell-snapshot -cf - .
```

It includes every file and directory under that root, including hidden files.
It excludes only the temporary restore directory.

It does not include:

- the image, installed packages, or anything outside `workdir`;
- `/tmp`, `/home`, `/var`, or another mount outside the workspace;
- process memory or environment variables;
- provider credentials.

OpenShell chooses the workspace in two ways:

- Docker and Podman use the image `WORKDIR`. An image with no `WORKDIR`, `/`, or
  `/sandbox` uses `/sandbox`.
- Kubernetes and MicroVM always use `/sandbox`.

`RuntimeConfig::workdir` is not derived from the image, template, or policy. It
defaults to `/sandbox` and is passed on every exec, read, write, and archive. Set
it to the workspace path for the driver and image. A smaller `workdir` archives a
smaller subtree.

The archive is a live view. A background writer can make it inconsistent. Stop
the workload or use a driver volume snapshot when the workspace contains a
database or another file that needs a consistent copy.

Restore extracts into the same `workdir`. The replacement image must provide a
compatible environment. Archive ownership and exact modes are not restored,
because the sandbox user cannot set arbitrary owners.

## Avoiding tar in the image

`sh` and `tar` are the portable default. Other options:

- **Driver volume.** Mount a per-thread PVC or named volume at the workspace and
  keep it across sandbox deletion. No copy and no in-image tool. This needs
  driver configuration and a volume lifecycle outside this crate.
- **SFTP.** `CreateSshSession` exposes the supervisor SFTP helper. A server-side
  SFTP client can list and transfer files without `tar`. The Rust SDK does not
  expose SFTP; the Go SDK does.
- **Injected helper.** Write a statically linked archiver into the workspace,
  make it executable, and run it. This removes the image dependency but ships a
  binary.
- **Custom command.** The archive and restore commands are currently fixed. A
  transfer seam would let an image use `busybox tar`, `bsdtar`, or a driver
  snapshot instead.

`tar` is the default because it is present in the base image, preserves
metadata, and streams without a server-side copy of each file.

## Security

- An archive contains everything in the workspace, including secrets the
  workload wrote. Treat the store as sensitive. Use encryption at rest, a
  per-tenant prefix, and a bucket policy that only the tool server can read.
- Keep the store credentials on the server. Do not give the workload an S3
  credential or an egress rule for the archive bucket.
- The runtime never prints archive contents. Errors include the exit status and
  a bounded stderr excerpt.

## Local filesystem example

`crates/infinity-openshell/examples/snapshot_local.rs` implements a local store
and runs the full cycle:

```shell
OPENSHELL_TEST_GATEWAY=demo \
  cargo run -p infinity-openshell --example snapshot_local --features bootstrap
```

It writes `work/state.txt`, closes the sandbox, acquires the same thread again,
and reads the file from the replacement sandbox.

## S3 sketch

An S3 store implements the same three methods:

- `put`: multipart upload `source` to
  `s3://<bucket>/<prefix>/<tenant>/<thread>/workspace.tar`. Write to a new
  object key or a version, then update a small `latest` pointer. A failed
  upload must not replace the previous archive.
- `get`: download the object for the key to `destination`. Return `Ok(false)`
  for `NoSuchKey`.
- `delete`: delete the object and the pointer. A missing object is not an
  error.

Use server-side encryption, versioning, and a lifecycle rule that keeps the
last N archives. The runtime does not call `delete` on its own; retention is
the store's or the operator's decision.

## Limits

- Archives are full tarballs. There are no incremental or block-level
  snapshots.
- The seam restores only when a new sandbox replaces a deleted one. It does not
  merge a restored archive into an existing workspace.
- Archiving runs at deletion time. A crash between the archive and the delete
  leaves the sandbox for the reaper, which archives again.
- Use the compute driver's volume snapshot when it provides one. The
  `SnapshotStore` trait can wrap that API instead of S3.
