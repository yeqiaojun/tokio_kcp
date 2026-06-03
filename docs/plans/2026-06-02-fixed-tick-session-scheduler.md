# Fixed Tick Session Scheduler Implementation Plan

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Replace the per-session Tokio timer updater with a shared fixed-tick scheduler that matches the current `kcp-go` scheduling model closely enough to remove timer-driver lock contention under high session counts.

**Architecture:** Add a crate-local shared scheduler with a 10ms tick and a small number of shards. Each `KcpSession` registers once with the scheduler, stores its next due time atomically, and the scheduler scans due sessions on each tick to drive `KcpSocket::update()`. Session close/finalization stays in `session.rs`.

**Tech Stack:** Rust, Tokio, atomics, existing `spin::Mutex`, existing integration tests and 5000-session echo benchmark/profile flow.

---

### Task 1: Add scheduler regression coverage

**Files:**
- Create: `D:\tokio_kcp\src\scheduler.rs`

**Step 1: Write the failing test**

Add a scheduler unit test that registers due tasks and future tasks, verifies due tasks run on the next worker wake, and verifies future tasks do not run early.

**Step 2: Run test to verify it fails**

Run: `cargo test scheduler::test::runs_due_tasks_without_running_future_tasks --lib`

Expected: FAIL because the shared scheduler module does not exist yet.

### Task 2: Replace per-session updater tasks

**Files:**
- Modify: `D:\tokio_kcp\src\session.rs`
- Modify: `D:\tokio_kcp\src\skcp.rs`
- Modify: `D:\tokio_kcp\src\lib.rs`

**Step 1: Write minimal implementation**

Add a fixed-tick scheduler module, register each `KcpSession` once, move the update loop body into a scheduler-driven method, and finalize closed sessions from that path instead of a dedicated updater task.

**Step 2: Run targeted tests**

Run: `cargo test multi_echo --lib`
Run: `cargo test fec_echo --lib`

Expected: PASS

### Task 3: Verify the 5000-session echo benchmark/profile

**Files:**
- Use existing: `D:\tokio_kcp\examples\bench_5000_echo40_server.rs`
- Use existing: `D:\tokio_kcp\examples\bench_5000_echo40_client.rs`

**Step 1: Build and run benchmark/profile**

Run the existing 5000-session echo benchmark and regenerate the server profile artifacts.

**Step 2: Inspect output**

Confirm the new flamegraph base is no longer dominated by Tokio timer `reregister` / `clear_entry` mutex contention.
