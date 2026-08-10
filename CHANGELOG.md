# Changelog

## 0.11.0 - 2026-08-10

- Upgrade the minimum Tokio version to 1.53.1.
- Upgrade KCP from 0.5.3 to 0.6.0.
- Move the crate to Rust 2024 edition with Rust 1.85 as the MSRV.
- Track the latest stable Rust toolchain for development and CI.
- Add formatting, Clippy, full-target tests, and an MSRV check to CI.

The KCP upgrade changes the version identity of `kcp::Error`, `kcp::KcpResult`,
and `kcp::Kcp` exposed by existing public signatures. Downstream users that
name those dependency types directly must update them to KCP 0.6.
