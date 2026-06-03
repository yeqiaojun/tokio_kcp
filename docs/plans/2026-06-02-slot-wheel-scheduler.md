# Slot Wheel Scheduler Optimization Implementation Plan

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Replace the current retain-based fixed-tick scheduler scan with a 10ms slot-wheel scheduler so 5000 mostly-idle sessions do not spend most CPU on full-list scans.

**Architecture:** Keep the shared scheduler and 10ms cadence, but replace each shard's single `Vec<Weak<_>>` with a ring of buckets. Sessions are enqueued into the bucket matching their next due tick, and each tick only drains the current bucket plus immediate requeues, which removes the current `Vec::retain` full scan hotspot.

**Tech Stack:** Rust, Tokio, atomics, existing `spin::Mutex`, existing unit/integration tests, existing 5000-session echo benchmark/profile flow.

---

### Task 1: Add regression coverage for slot behavior

**Files:**
- Modify: `D:\tokio_kcp\src\scheduler.rs`

**Step 1: Write the failing test**

Add a scheduler unit test that registers a task due 200ms in the future and verifies the scheduler does not poll it during the first 60ms window.

**Step 2: Run test to verify it fails**

Run: `cargo test scheduler::test::does_not_poll_future_task_before_due_slot --lib`

Expected: FAIL because the current scheduler polls every task on every 10ms tick.

### Task 2: Replace the scheduler internals

**Files:**
- Modify: `D:\tokio_kcp\src\scheduler.rs`
- Modify: `D:\tokio_kcp\src\session.rs`

**Step 1: Write minimal implementation**

Replace the per-shard `Vec<Weak<_>>` plus `retain` scan with a ring of buckets keyed by due tick. Store scheduled entries in the correct future slot and only process the current slot each tick.

**Step 2: Run scheduler tests**

Run: `cargo test scheduler::test::runs_due_tasks_without_running_future_tasks --lib`
Run: `cargo test scheduler::test::does_not_poll_future_task_before_due_slot --lib`

Expected: PASS

### Task 3: Verify the workload hotspot moved

**Files:**
- Use existing: `D:\tokio_kcp\examples\bench_5000_echo40_server.rs`
- Use existing: `D:\tokio_kcp\examples\bench_5000_echo40_client.rs`

**Step 1: Re-run the 5000-session echo profile**

Run the same 5000-client echo benchmark and regenerate the server flamegraph/profile artifacts.

**Step 2: Inspect output**

Confirm the new flamegraph no longer shows `Vec<T,A>::retain` dominating the scheduler base.
