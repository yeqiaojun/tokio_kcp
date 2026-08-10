# Tokio / Rust / KCP 升级必要性评估

日期：2026-08-10
评估基线：`29c12dc54593a338acbe26f6b229a434202f6a0f`（工作区已有的升级改动不作为旧版基线）

## 结论先行

| 项目 | 建议 | 性能判断 | 安全 / 正确性判断 | 主要成本 |
| --- | --- | --- | --- | --- |
| Tokio `1.37` → `1.53.1` | **建议升级声明下限，但要知道无锁 fresh build 本来已解析到 1.53.1** | 上游有若干调度、waker 热路径优化；本地小样本显示 Tokio-only 升级有正向信号，但不是生产级结论 | 1.37.0 命中 broadcast soundness 公告；1.53.1 已修复。另有直接相关的 Linux connected UDP 错误唤醒修复 | Tokio MSRV 从 1.63 升至 1.71；若同时采用 edition 2024，此成本被 Rust 1.85 的 edition 下限覆盖 |
| Rust 工具链 → stable 1.97.1 | **建议**，主要为编译器正确性和构建工具安全维护 | 旧工具链没有被固定，无法定义可比较的性能基线；不能承诺业务吞吐提升 | 1.97.1 修复 LLVM 优化导致的误编译；1.96 还修复了两个 Cargo 第三方 registry 漏洞 | 新编译器可能带来 lint、符号改名和工具链兼容变化 |
| edition 2018 → 2024 | **可以升级，但不是性能或漏洞修复的必要条件** | edition 本身没有上游性能承诺 | 更严格的语言规则有利于长期维护，但不修复本项目已知漏洞 | MSRV 至少 1.85；resolver 和临时值析构顺序变化；迁移 lint 找到的 2 处 drop-order 已改成显式绑定 |
| `kcp` `0.5.3` → `0.6.0` | **已决定随 GitHub `tokio_kcp v0.11.0` 协同升级**；该决定以依赖现代化为目标，不宣称性能收益 | 0.6.0 的主要能力是可选 Tokio `AsyncWrite`/async flush/update；本仓库未启用且现有同步算法基本未变，本地混合升级也未显示额外收益 | 截止评估日，所查官方安全数据库没有 `kcp` 0.5.3 或 0.6.0 记录 | 本仓库的公开签名泄漏了 `kcp` 类型，升到 0.6 对部分下游调用者是类型级破坏；已验证 `deps-rust/xnet` 与 `xkk` 没有这类直接类型耦合 |

综合建议是：**先升级并测试 Tokio 和 Rust/CI；保留 KCP 0.5.3、rand 0.8、spin 0.9 的 major 系列，只提高兼容范围内的 patch 下限。** edition 2024 可以作为独立的源码现代化变更合入，不应和运行时性能收益绑定宣传。

## 1. 首先区分“清单下限”和“实际解析版本”

基线 [`Cargo.toml`](https://github.com/yeqiaojun/tokio_kcp/blob/29c12dc54593a338acbe26f6b229a434202f6a0f/Cargo.toml#L11-L35) 写的是：

```toml
edition = "2018"
kcp = "0.5.3"
tokio = { version = "1.37", ... }
```

Cargo 的普通版本字符串采用 caret requirement：`"1.37"` 等价于 `>=1.37.0, <2.0.0`，而 `"0.5.3"` 等价于 `>=0.5.3, <0.6.0`；这不是精确锁定。规则见 Cargo 官方的 [version requirement 语法](https://doc.rust-lang.org/cargo/reference/specifying-dependencies.html#version-requirement-syntax)。

这个仓库是 library，基线没有提交 `Cargo.lock`。对基线 commit 做一次隔离的 fresh `cargo metadata`（Rust/Cargo 1.97.1，2026-08-10 crates.io index）得到：

```text
tokio 1.53.1
kcp   0.5.3
```

随后 `cargo check --all-targets` 通过。因此：

- 把 `tokio = "1.37"` 改成 `tokio = "1.53.1"`，对今天的 fresh build **不改变实际 Tokio 代码**；它提高的是最低允许版本，能阻止下游已有 lockfile 继续选 1.37.x。
- `kcp = "0.5.3"` 不会自动跨到 0.6.0；KCP 的升级是真正的依赖版本变化。
- 如果要做可信性能 A/B，必须用 `=1.37.0` / `=1.53.1` 或各自 lockfile 固定图，不能只比较两个 caret 字符串。

## 2. 本仓库实际使用的 Tokio 面

生产路径集中在以下几类：

- `net/udp`：listener 用 `UdpSocket::recv_from`，client session 用 connected `UdpSocket::recv`，发送端用 `try_send_to`/`send_to`（[`listener.rs`](https://github.com/yeqiaojun/tokio_kcp/blob/29c12dc54593a338acbe26f6b229a434202f6a0f/src/listener.rs#L45-L167)、[`session.rs`](https://github.com/yeqiaojun/tokio_kcp/blob/29c12dc54593a338acbe26f6b229a434202f6a0f/src/session.rs#L95-L198)、[`skcp.rs`](https://github.com/yeqiaojun/tokio_kcp/blob/29c12dc54593a338acbe26f6b229a434202f6a0f/src/skcp.rs#L24-L93)）。
- `runtime/macros`：大量 `tokio::spawn` 和 `tokio::select!` 驱动 listener、session、UDP output task。
- `sync`：核心路径使用 bounded/unbounded `mpsc`；没有使用 Tokio `broadcast`、`RwLock`、`Notify`，Tokio `Mutex` 只在测试中出现。
- `time`：核心 listener 只在 UDP 接收错误后 `sleep(1s)`；KCP session 的固定 tick 已由 `std::thread` scheduler 驱动（[`scheduler.rs`](https://github.com/yeqiaojun/tokio_kcp/blob/29c12dc54593a338acbe26f6b229a434202f6a0f/src/scheduler.rs#L1-L80)）。`timeout`、`interval`、大量 `sleep` 主要在示例、测试和压测程序中。
- KCP output 仍是同步 `std::io::Write`：每个 datagram `to_owned()` 后 `try_send` 进 Tokio mpsc，再由后台 task 发送（[`skcp.rs`](https://github.com/yeqiaojun/tokio_kcp/blob/29c12dc54593a338acbe26f6b229a434202f6a0f/src/skcp.rs#L20-L79)）。

这个使用面决定了：UDP 正确性、runtime 调度和 mpsc 改动最相关；broadcast/RwLock、文件系统、TCP、signal、unstable timer 等变更不能算成本项目收益。

## 3. Tokio 1.37 → 1.53.1

### 3.1 与本仓库直接相关的修复

1. **Linux connected UDP 错误能够正确唤醒 `recv`。** Tokio 1.51.1 的 [#8001](https://github.com/tokio-rs/tokio/pull/8001) 让 `UdpSocket::recv` 等待 `READABLE | ERROR`，从而在只有 `SO_ERROR` readiness 时也能唤醒并返回错误。官方 [#8000](https://github.com/tokio-rs/tokio/issues/8000) 明确复现的是 Tokio 1.50；[1.37 源码仍只订阅 `READABLE`](https://github.com/tokio-rs/tokio/blob/tokio-1.37.0/tokio/src/net/udp.rs#L776-L781)，而 [1.53.1 已同时订阅 `READABLE | ERROR`](https://github.com/tokio-rs/tokio/blob/tokio-1.53.1/tokio/src/net/udp.rs#L790-L795)，所以“1.37 同样受影响”是基于相同代码路径的强推断，不是公告给出的版本范围。本仓库 client session 正在直接调用 connected UDP 的 `recv`，并在错误时关闭 session，因此该修复与业务路径直接相关。

2. **runtime weak-memory 死锁修复。** [#7622](https://github.com/tokio-rs/tokio/pull/7622) 修正 `wake_by_ref()` 的内存序问题；上游说明旧逻辑在弱内存模型竞态下可能死锁。所有 spawn task 都间接受益，但本仓库尚无复现证据，因此应归类为通用正确性加固，不是已证实的项目 bug。

3. **mpsc receiver 的遗留 waker 被及时释放。** Tokio 1.53.0 的 [#8095](https://github.com/tokio-rs/tokio/pull/8095) 修复 receiver 被 drop/任务被 abort、但 sender 仍存在时 waker 继续保活 RawTask 的问题。本仓库大量使用 mpsc 和可中止 task，模式相关；实际是否触发仍取决于 sender 生命周期。

4. **`select!` 从 1.44 起感知 cooperative budget。** [#7164](https://github.com/tokio-rs/tokio/pull/7164) 与本仓库的 session/listener select loop 有关，主要影响公平性和饥饿行为，不应直接换算成吞吐提升。

以下修复虽然重要，但对当前代码不可达：

- broadcast 的 soundness 修复 [#7232](https://github.com/tokio-rs/tokio/pull/7232)：本仓库不用 broadcast。
- `RwLock::with_max_readers(..., 0)` 可导致 UB 的修复 [#8076](https://github.com/tokio-rs/tokio/pull/8076)：本仓库不用 Tokio RwLock。
- 1.51.3/1.52.3 的 mpsc `len`、`OwnedPermit::release`、closed `try_recv` 修复（[#8062](https://github.com/tokio-rs/tokio/pull/8062)、[#8075](https://github.com/tokio-rs/tokio/pull/8075)、[#8074](https://github.com/tokio-rs/tokio/pull/8074)）：本仓库没有调用这些 API。
- 1.53.1 的稳定变更只修 Windows signal MSRV；另一项是 unstable alternative timer race。可见 [Tokio 1.53.1 changelog](https://github.com/tokio-rs/tokio/blob/tokio-1.53.1/tokio/CHANGELOG.md#1531-july-20th-2026)。本项目不启用 signal 或 unstable timer；选择 1.53.1 主要因为它是当前 patch，而非本项目专属修复。

### 3.2 性能：有可信的正向信号，但没有“升级必然大幅提速”的证据

上游可确认的相关微优化包括：

- Tokio 1.46 的 [#7340](https://github.com/tokio-rs/tokio/pull/7340) 从 multi-thread runtime local queue 热路径消除不必要的 `lfence`。
- Tokio 1.47 的 [#7450](https://github.com/tokio-rs/tokio/pull/7450) 优化 `AtomicWaker::wake` 的原子操作；mpsc 等异步组件会使用 waker，但上游没有给出 KCP/mpsc 端到端收益数字。
- Tokio 1.50 的 [#7834](https://github.com/tokio-rs/tokio/pull/7834) 避免 current-thread scheduler 的冗余 unpark；只与使用 current-thread runtime 的部署有关。

不能计入净收益的项目：

- 1.38 引入的 timer sharding 曾针对高并发 timeout/sleep 锁竞争（[#6534](https://github.com/tokio-rs/tokio/pull/6534)），但 1.45 因“可测性能回退/竞争增加”整体回滚（[#7226](https://github.com/tokio-rs/tokio/pull/7226)）。不能把早期 sharding benchmark 当成 1.53 默认 timer 的收益。
- 1.49 的高扩展 alternative timer 是 unstable opt-in（[#7467](https://github.com/tokio-rs/tokio/pull/7467)），本仓库没有启用。
- 1.51 的 LIFO slot stealing 与 1.52 的 sharded `spawn_blocking` queue 都因回退/挂起问题在后续 patch 被撤销（[#8100](https://github.com/tokio-rs/tokio/pull/8100)、[#8057](https://github.com/tokio-rs/tokio/pull/8057)），不应算在最终版本收益中。

#### 本地小样本（不是官方 benchmark）

环境是 MacBook Pro 18,3、Apple M1 Pro 8 核（6P+2E）、16 GB、rustc 1.97.1，均为 release build。命令为各变体的 `bench_5000 1000 250 0`；顺序交错为 `153/137/full`、`137/full/153`、`full/153/137`，两次运行间隔 3 秒。各组合跑 3 次，全部成功：

| 依赖组合 | elapsed 中位数 | 平均 latency | 相对旧组合 |
| --- | ---: | ---: | --- |
| Tokio 1.37.0 + KCP 0.5.3 | 42.48 ms | 7.83 ms | 基线 |
| Tokio 1.53.1 + KCP 0.5.3 | 36.79 ms | 7.26 ms | elapsed 约 -13.4%；latency 约 -7.3% |
| Tokio 1.53.1 + KCP 0.6.0 + rand 0.10 + spin 0.12 | 39.33 ms | 7.63 ms | 没显示 KCP/其他 major 的额外收益 |

这组数据只提供“Tokio-only 升级值得继续验证”的弱正向信号：样本只有 3 次，测试规模较小，而且没有生产网络丢包、RTT、CPU profile、长稳态和多平台数据。后续在相同 Tokio/KCP 依赖下做交错控制也观察到足以覆盖上述差值的运行间波动，因此不能把 13.4% 当作稳定收益或外推到生产。前两行使用原始源码，仅固定 Tokio 版本不同；第三行使用当前源码，并同时改变了 KCP/rand/spin，更不能把差异单独归因给 KCP 0.6.0。

### 3.3 安全公告

- `tokio 1.37.0` 受 [RUSTSEC-2025-0023](https://rustsec.org/advisories/RUSTSEC-2025-0023.html) / [GHSA-rr8g-9fpq-6wmg](https://github.com/advisories/GHSA-rr8g-9fpq-6wmg) 影响：broadcast channel 对 `Send + !Sync` 值并行调用 `clone`，可能触发 unsoundness。GitHub 将其列为 low severity；修复线包括 1.38.2、1.42.1、1.43.1 和 `>=1.44.2`。
- 本仓库不使用 broadcast，因此没有发现项目内可达利用路径。但提高 Tokio 最低版本仍可避免下游 lockfile 选择已知 unsound 版本。
- 本地 `cargo-audit 0.22.2`、RustSec DB 2026-08-10：fresh 基线（实际解析 Tokio 1.53.1）与升级树均为 `0 vulnerabilities`；强制 Tokio 1.37.0 时出现上述 `INFO unsound`。所有测试图还有 `instant 0.1.13` unmaintained warning，来源是 `reed-solomon-erasure 6 -> parking_lot 0.11`，它不是 Tokio/KCP 漏洞，也不会由这三项升级消除。

### 3.4 Tokio 兼容成本

[crates.io 1.37.0 元数据](https://crates.io/api/v1/crates/tokio/1.37.0) 声明 Rust 1.63，[1.53.1 元数据](https://crates.io/api/v1/crates/tokio/1.53.1) 声明 Rust 1.71。基线源码在 Rust 1.97.1 下、由 `1.37` caret fresh 解析到 Tokio 1.53.1 后已通过 `cargo check --all-targets`，说明本项目使用的稳定 API 没有迁移障碍。

若同时采用 edition 2024，项目本身最低 Rust 已是 1.85，因此 Tokio 的 1.71 MSRV 不增加额外成本。对 library 发布策略而言，有两种合理选择：

- 重视“已知安全最低线 + 更宽下游兼容”：最低写 `1.44.2`，CI 另测 fresh latest。
- 已决定只支持现代 Rust/edition 2024：最低直接写当前 `1.53.1`，简单明确。对本仓库当前方向，后者合理。

## 4. Rust stable 1.97.1 与 edition 2024 是两条独立轴

### 4.1 工具链升级：建议

edition 2018 的 crate 完全可以由 Rust 1.97.1 编译；edition 不是 compiler 版本。基线没有 `rust-toolchain.toml`，所以“旧 Rust”没有精确版本，无法对编译器升级给出百分比性能结论。

升级最新 stable 的明确理由是：

- [Rust 1.97.1 官方 release](https://github.com/rust-lang/rust/releases/tag/1.97.1) 回移 LLVM 修复，并回滚一个已知触发点，修复优化阶段误编译。若使用 1.97.0，升到 1.97.1 是必要的正确性修复；这不是 Rust 2018 代码特有的问题。
- Rust 1.96 的 Cargo 修复 [CVE-2026-5222](https://blog.rust-lang.org/2026/05/25/cve-2026-5222/)（特定第三方 sparse registry 凭据混淆，low）和 [CVE-2026-5223](https://blog.rust-lang.org/2026/05/25/cve-2026-5223/)（第三方 registry crate tarball symlink 覆盖，medium）。当前仓库只用 crates.io，官方说明 crates.io 不受 5223 影响，5222 也要求很特殊的第三方 registry 条件；这是构建链维护收益，不是 KCP 运行时漏洞修复。

`rust-toolchain.toml` 中 `channel = "stable"` 表示跟随移动的 stable channel，不等于永久精确固定 1.97.1；语义见 [rustup toolchain file](https://rust-lang.github.io/rustup/overrides.html#the-toolchain-file)。建议：

- 若目标是持续跟最新安全 stable：用 `stable`，CI 定期更新，并另设 MSRV job。
- 若目标是完全可复现：写精确 `1.97.1`，再由自动化更新。

### 4.2 edition 2024：现代化收益大于性能收益

Rust 2024 随 Rust 1.85 稳定；见 [Rust 1.85 官方发布说明](https://blog.rust-lang.org/2025/02/20/Rust-1.85.0/) 和 [Edition Guide](https://doc.rust-lang.org/edition-guide/rust-2024/index.html)。它不会自动让 UDP/KCP 更快，也不是修复 Tokio/KCP 公告的前提。

明确成本：

1. **MSRV**：edition 2024 至少要求 Rust 1.85。`rust-version = "1.85"` 是合理的包最低版本；开发工具链仍可用 1.97.1。`rust-version` 含义见 [Cargo manifest reference](https://doc.rust-lang.org/cargo/reference/manifest.html#the-rust-version-field)。该声明必须用真实 `cargo +1.85 check/test` 验证，而不是仅写字段。
2. **dependency resolver**：edition 2018 默认 resolver 1，2021 默认 resolver 2，2024 默认 resolver 3；resolver 3 还会利用 `rust-version` 选择兼容依赖。见 [Cargo resolver versions](https://doc.rust-lang.org/cargo/reference/resolver.html#resolver-versions)。隔离迁移时 Cargo 明确显示 normal build 不再继承 dev-dependency 的 Tokio `io-std`、`io-util`、`rt-multi-thread` 等 feature；这可减少无关 feature 面，但不是运行时吞吐承诺。
3. **析构顺序**：按官方 [temporary tail expression scope](https://doc.rust-lang.org/edition-guide/rust-2024/temporary-tail-expr-scope.html)，2024 会更早 drop tail expression 临时值。对基线逐步执行两次 `cargo fix --edition --all-targets`（2018→2021→2024）没有自动源码改写，但产生 2 个 `tail-expr-drop-order` 警告：`examples/bench_5000_echo40_server.rs` 的 `select!/accept` 分支，以及 `examples/serverbench.rs` 中持有 `OwnedSemaphorePermit` 后以 `warmup_one(...).await` 作 tail expression。迁移中已为两处结果增加显式局部绑定，使析构边界不再依赖 edition 的 tail-expression 规则；当前 2024 edition 的 lint、clippy 和测试均通过。
4. **其他规则变化**：unsafe attribute/extern、match ergonomics、`gen` 关键字、RPIT capture、never-type fallback 等都应由 [官方迁移流程](https://doc.rust-lang.org/edition-guide/editions/transitioning-an-existing-project-to-a-new-edition.html) 检查。本仓库基线没有触发对应自动修复，主要实际告警就是上述 drop order。

因此 edition 2024 建议作为独立提交：先迁移/检查语义，再升级依赖；这样回归原因可定位，也能保留“工具链升级但暂不改 edition”的退路。

## 5. KCP 0.5.3 → 0.6.0

### 5.1 上游实际变化

[crates.io 0.5.3](https://crates.io/api/v1/crates/kcp/0.5.3) 发布于 2023-05-28，仅有 `fastack-conserve` feature；[0.6.0](https://crates.io/api/v1/crates/kcp/0.6.0) 发布于 2025-06-11，新增可选 `tokio` feature，并把 `thiserror` 1 升到 2。两个 crate 都没有声明 `rust-version`。

对两个已发布 crate 内嵌的 VCS commit 做 [完整上游 compare](https://github.com/deepseeksss/kcp/compare/a47e494c52c64f4688a12648c58aa0c1b2d0fc69...8c0603ad352d6a5c6f61a4b606a4dc4e702d9e10)（也可见 [v0.5.3...v0.6.0 tag compare](https://github.com/deepseeksss/kcp/compare/v0.5.3...v0.6.0)），中间只有 4 个 commit，核心代码变化是：

- 去掉 `Kcp<Output>` 类型本身的 `Output: Write` 总约束；同步 `flush/update` 仍只在 `Output: Write` impl 中。
- 在 `tokio` feature 下为 `Output: AsyncWrite + Unpin` 新增 `async_flush_ack`、`async_flush`、`async_update`（[0.6.0 async 源码](https://github.com/deepseeksss/kcp/blob/v0.6.0/src/kcp.rs#L1309-L1542)）。
- 更新开发依赖、`thiserror` 和元数据；没有标记 KCP 拥塞控制、重传、窗口、分片等同步算法 bug fix，也没有上游 benchmark。

上游 [Releases](https://github.com/deepseeksss/kcp/releases) 没有 0.6.0 release note，仓库也没有 changelog；因此不能从缺失的发布说明推断出未记录的性能或安全修复。

这是从源码 diff 得出的结论：**0.6.0 是输出抽象/API 能力升级，而不是已有同步 KCP 算法的性能版。**

### 5.2 对本仓库的影响

- 本仓库没有为 `kcp` 开启 `tokio` feature，且 `UdpOutput` 仍实现 `std::io::Write`。仅把版本改为 0.6.0，不会走新的 async 路径。
- 隔离地只改 `kcp = "0.6.0"`，保持 edition 2018 和其他依赖不变，`cargo check --all-targets` 通过，无需修改本仓库源码；所以**仓库内部**的编译迁移成本低。
- 但这不是对下游的无损升级。`KcpStream`、`KcpListener` 的多个公开方法直接返回 `kcp::KcpResult`，`KcpConfig::apply_config` 还公开接收 `kcp::Kcp<W>`。Cargo 会把 `kcp 0.5` 和 `kcp 0.6` 当作不同 crate/type identity：下游若显式匹配或标注 `kcp 0.5::Error`，或把自己的 `kcp 0.5::Kcp` 传给 `apply_config`，升级后会编译失败。错误枚举的源码形状相似并不能消除这个类型级破坏。
- 只使用 `KcpStream`/`KcpListener`、`?`、`AsyncRead`/`AsyncWrite` 且不直接引用 `kcp` 类型的调用者，大概率无需改源码，但按严格的公开 API/semver 判断仍应视为破坏性变更。若推进，建议随 `tokio_kcp 0.11` 发布，并优先引入 crate 自有错误类型（或明确 re-export 固定版本的 KCP 类型），减少下一次依赖升级的泄漏面。
- 若未来启用 async KCP，理论上可以尝试减少当前 `buf.to_owned()` + 2048 深度 mpsc + background send task 的复制/排队；但 `UdpSocket` 是 datagram API且 server 共享 socket/目标地址，不能直接当普通 `AsyncWrite`。这需要自定义适配、重新设计 backpressure/FEC/锁持有范围，并用丢包和慢 socket 场景验证，不能视作 0.6.0 开箱即得的优化。

### 5.3 `deps-rust` / `xkk` 联动验证

在三个仓库的隔离副本中，将本仓库改为 `tokio_kcp 0.11.0 + kcp 0.6.0`，并仅让临时 `deps-rust/xnet` 通过 path dependency 指向该副本。结果如下：

- `tokio_kcp`：`cargo test --all-targets --all-features` 通过，包括 19 个库测试、2 个 kcp-go FEC 互操作测试和 2 个示例测试。
- `deps-rust/xnet`：不需要源码适配。它只使用 `KcpConfig`、`KcpListener`、`KcpStream`，并在边界立即把 KCP 错误转换为自己的 `Error::Kcp(String)`，没有引用 `kcp::Error`、`KcpResult` 或 `kcp::Kcp`。67 个库测试、7 个集成测试以及所有 benchmark 目标均成功。
- `xkk`：只通过本地 `xnet` 间接使用 KCP，没有直接 `tokio_kcp`/`kcp` 类型耦合。`cargo check --workspace --all-targets` 和 68 个 workspace 测试均通过；构建命令按仓库约定提供了 vendored `protoc`。

因此这两个下游仓库不存在源码迁移阻塞。实际落地应分三步提交：先推送 GitHub `tokio_kcp v0.11.0 + kcp 0.6`，再让 `deps-rust/xnet` 的 Git 依赖固定到该 tag 并更新锁文件，最后刷新 `xkk/Cargo.lock`。不应把临时 path dependency 提交进 `deps-rust`。

### 5.4 安全记录

2026-08-10 按 [OSV `/v1/query` 官方 API](https://google.github.io/osv.dev/post-v1-query/) 分别查询 `kcp` 0.5.3/0.6.0 均为空，GitHub Advisory 官方 API 的 [`affects=kcp`](https://api.github.com/advisories?ecosystem=rust&affects=kcp&per_page=100) 也返回空数组，RustSec advisory DB 没有 `kcp` package 条目。这只能表述为“所查数据库没有已发布记录”，不能证明代码不存在漏洞。

基于上游 diff、数据库结果和本地混合 benchmark，**没有性能或安全理由强制升级 0.6**；但联动验证证明升级可以安全落地到 `deps-rust/xnet` 和 `xkk`。本次以统一到最新依赖为目标，按 GitHub `tokio_kcp v0.11.0` 的破坏性版本流程推进；如要评估 async output，仍应另开原型和 benchmark，不与常规依赖维护混在一起。

## 6. 推荐落地顺序与验收门槛

1. **工具链/CI**：采用 Rust stable 1.97.1（或 moving `stable`），保留 `rust-version = "1.85"` 作为 edition 2024 MSRV，并增加真实 MSRV job。
2. **Tokio 单独升级**：把最低版本提高到 1.53.1；保持 KCP 0.5.3、rand 0.8、spin 0.9，仅提高各 major 内 patch 下限。这样能把本地已观察到的改善归因给 Tokio。
3. **edition 单独迁移**：已按 2018→2021→2024 检查迁移 lint并显式处理 2 处 drop-order；继续在 CI 运行 format、clippy、全测试和 Go interop。
4. **安全验收**：对最终 lock snapshot 运行 `cargo audit`；记录 `instant 0.1.13` unmaintained 为独立的 `reed-solomon-erasure` 技术债，不把它误报成 Tokio/KCP 升级失败。
5. **性能验收**：至少固定 lockfile、release profile、CPU affinity/worker 数、client 数、并发度、payload、RTT/丢包模型；交错运行并报告 p50/p95/p99、吞吐、CPU、RSS、丢包/重传和失败数。现有 3 次结果只能作为继续测试的信号。
6. **KCP 0.6 可选联动**：若目标是统一到最新依赖，按 `tokio_kcp 0.11` → `deps-rust` 锁文件 → `xkk` 锁文件的顺序升级；若还要启用 async output，则必须另做原型，证明能去掉队列/复制且没有 backpressure、FEC、共享 UDP socket 回归。

最终判断：**升级 Tokio/Rust 有维护与正确性必要性，并有本地性能正向信号；升级 edition 2024 是可取但独立的现代化工作；KCP 0.6.0 没有足够的性能、安全或 bug-fix 证据要求立即升级，但已证明可与 `deps-rust/xnet`、`xkk` 无源码适配地联动落地。**
