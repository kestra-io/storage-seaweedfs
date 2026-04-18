# Kestra SeaweedFS Storage

## What

- Implements the storage backend under `io.kestra.storage.seaweedfs`.
- Includes classes such as `SeaweedFSStorage`, `SeaweedFSConfig`, `SeaweedFSFileAttributes`.

## Why

- This repository implements a Kestra storage backend for SeaweedFS Storage.
- It stores namespace files and internal execution artifacts outside local disk.

## How

### Architecture

Single-module plugin.

Infrastructure dependencies (Docker Compose services):

- `app`

### Project Structure

```
storage-seaweedfs/
├── src/main/java/io/kestra/storage/seaweedfs/
├── src/test/java/io/kestra/storage/seaweedfs/
├── build.gradle
└── README.md
```

## Local rules

- Keep the scope on Kestra internal storage behavior, not workflow task semantics.

## References

- https://kestra.io/docs/plugin-developer-guide
- https://kestra.io/docs/plugin-developer-guide/contribution-guidelines
