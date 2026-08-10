# tokio_kcp

[![Build And Test](https://github.com/yeqiaojun/tokio_kcp/actions/workflows/build-and-test.yml/badge.svg)](https://github.com/yeqiaojun/tokio_kcp/actions/workflows/build-and-test.yml)

A KCP implementation for Tokio 1.x.

## Toolchain

- The repository tracks the latest stable Rust toolchain through `rust-toolchain.toml`.
- The crate uses Rust 2024 edition and supports Rust 1.85 or newer.
- Tokio 1.53.1 is the minimum declared Tokio 1.x version.
- KCP 0.6.0 is used by the GitHub `v0.11.0` release.

## Build and test

```shell
cargo fmt --all -- --check
cargo clippy --all-targets --all-features -- -D warnings
cargo test --all-targets --all-features
```

See the [upgrade assessment](docs/research/2026-08-10-upgrade-assessment.md) for the Tokio, Rust, and KCP version decision.
