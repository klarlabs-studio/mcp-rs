// Derived from rmcp 3.5.1; maintained and modified by mcp-rs. See ../../../UPSTREAM.md.
//! Server-side runtime for the MCP Tasks extension (SEP-2663,
//! `io.modelcontextprotocol/tasks`).
//!
//! [`TaskManager`] owns the durable state for tasks a server has materialized
//! in response to task-eligible requests (currently `tools/call`). It:
//!
//! - spawns the underlying operation and tracks its lifecycle as a
//!   [`DetailedTask`] (`working` → terminal, optionally via `input_required`),
//! - answers `tasks/get` with the current state (including in-flight
//!   `inputRequests` and terminal `result`/`error` payloads),
//! - accepts `tasks/update` `inputResponses` and routes them to the running
//!   operation (ignoring unknown or already-answered keys per spec),
//! - handles cooperative `tasks/cancel`,
//! - enforces TTL-based expiry (`ttl_ms`), marking overdue tasks `failed`.
//!
//! Tasks are only durably observable once [`TaskManager::spawn`] returns,
//! satisfying the spec requirement that a server not return `CreateTaskResult`
//! before `tasks/get` for that id would resolve.

use std::{
    collections::{HashMap, HashSet, VecDeque},
    num::NonZeroUsize,
    pin::Pin,
    sync::{Arc, Mutex, Weak},
    time::Instant,
};

use futures::Future;
use tokio::sync::{mpsc, oneshot};
use tokio_util::sync::CancellationToken;

use crate::{
    error::ErrorData as McpError,
    model::{
        CallToolResult, DetailedTask, InputRequest, InputRequests, JsonObject, Task, TaskPayload,
        TaskStatus,
    },
};

/// Default TTL (5 minutes, in milliseconds) applied when none is specified.
pub const DEFAULT_TASK_TTL_MS: u64 = 300_000;

/// Default suggested polling interval, in milliseconds.
pub const DEFAULT_POLL_INTERVAL_MS: u64 = 1_000;

/// Helper to generate an ISO 8601 timestamp for task metadata.
pub fn current_timestamp() -> String {
    chrono::Utc::now().to_rfc3339()
}

/// Handle passed to a running task operation, allowing it to surface
/// server-to-client requests (elicitation, sampling, roots) mid-task and
/// await the client's `tasks/update` response.
#[derive(Clone)]
pub struct TaskContext {
    task_id: String,
    inner: Arc<Mutex<TaskManagerInner>>,
}

impl TaskContext {
    /// The id of the task this context belongs to.
    pub fn task_id(&self) -> &str {
        &self.task_id
    }

    /// Surface a server-to-client request under `key` and wait for the
    /// client's response delivered via `tasks/update`.
    ///
    /// While at least one request is outstanding the task reports
    /// `input_required` from `tasks/get`, with all outstanding requests in
    /// `inputRequests`. Keys must be unique over the lifetime of the task;
    /// reusing a key returns an error.
    pub async fn request_input(
        &self,
        key: impl Into<String>,
        request: InputRequest,
    ) -> Result<serde_json::Value, TaskExit> {
        let key = key.into();
        let (tx, rx) = oneshot::channel();
        {
            let mut inner = self.inner.lock().expect("task manager lock poisoned");
            let entry = inner.tasks.get_mut(&self.task_id).ok_or_else(|| {
                TaskExit::Error(McpError::internal_error(
                    "task no longer exists".to_string(),
                    None,
                ))
            })?;
            if !entry.used_input_keys.insert(key.clone()) {
                return Err(TaskExit::Error(McpError::internal_error(
                    format!("inputRequests key {key:?} was already used for this task"),
                    None,
                )));
            }
            entry.pending_inputs.insert(key.clone(), (request, tx));
            entry.touch();
            inner.publish(&self.task_id);
        }
        // The sender is dropped when `tasks/cancel` clears pending inputs.
        rx.await.map_err(|_| TaskExit::Cancelled)
    }

    /// Update the task's human-readable status message.
    pub fn set_status_message(&self, message: impl Into<String>) {
        let mut inner = self.inner.lock().expect("task manager lock poisoned");
        if let Some(entry) = inner.tasks.get_mut(&self.task_id) {
            entry.task.status_message = Some(message.into());
            entry.touch();
            inner.publish(&self.task_id);
        }
    }

    /// Returns `true` if `tasks/cancel` has been received for this task.
    /// Cooperative: operations should check this and stop when set.
    pub fn is_cancel_requested(&self) -> bool {
        let inner = self.inner.lock().expect("task manager lock poisoned");
        inner
            .tasks
            .get(&self.task_id)
            .is_some_and(|e| e.cancel_requested)
    }

    /// Resolves once `tasks/cancel` has been received for this task (or
    /// immediately, if it already has). Cooperative: pair with
    /// `tokio::select!` around long-running work to implement a cancellation
    /// exit path.
    ///
    /// An operation that stops in response should return
    /// [`TaskExit::Cancelled`] so the task settles as `cancelled`. Returning
    /// [`TaskExit::Error`] settles as `failed`, and finishing the work
    /// anyway settles as `completed` — per SEP-2663 cancellation is
    /// cooperative and a task may reach a non-`cancelled` terminal status.
    pub async fn cancelled(&self) {
        let mut rx = {
            let inner = self.inner.lock().expect("task manager lock poisoned");
            let Some(entry) = inner.tasks.get(&self.task_id) else {
                return;
            };
            if entry.cancel_requested {
                return;
            }
            entry.cancel_signal.subscribe()
        };
        // Wait until the watch flips to true; a closed channel means the
        // manager dropped the entry, which also unblocks the operation.
        while !*rx.borrow_and_update() {
            if rx.changed().await.is_err() {
                return;
            }
        }
    }
}

/// How a task operation finished without producing a result.
#[expect(
    clippy::exhaustive_enums,
    reason = "error variant for task exit may only be due to error or cancellation"
)]
#[derive(Debug)]
pub enum TaskExit {
    /// The operation is exiting in response to a cancellation request;
    /// the task settles as terminal `cancelled`.
    Cancelled,
    /// A real failure; the task settles as terminal `failed` with the
    /// error inlined, even after `tasks/cancel` was received.
    Error(McpError),
}

impl From<McpError> for TaskExit {
    fn from(error: McpError) -> Self {
        TaskExit::Error(error)
    }
}

/// Boxed future representing the async operation backing a task.
pub type TaskFuture = Pin<Box<dyn Future<Output = Result<CallToolResult, TaskExit>> + Send>>;

struct TaskEntry {
    task: Task,
    /// Terminal payload, if the task has finished.
    terminal: Option<TaskPayload>,
    /// When the task reached its terminal state; drives retention eviction.
    terminal_at: Option<Instant>,
    /// Outstanding input requests keyed by their unique identifier.
    pending_inputs: HashMap<String, (InputRequest, oneshot::Sender<serde_json::Value>)>,
    /// Every key ever used, to enforce uniqueness across the task lifetime.
    used_input_keys: std::collections::HashSet<String>,
    cancel_requested: bool,
    /// Signals the running operation that cancellation was requested
    /// (`true` once `tasks/cancel` arrives). Cooperative: the operation
    /// decides whether and how to stop.
    cancel_signal: tokio::sync::watch::Sender<bool>,
    created: Instant,
    join_handle: Option<tokio::task::JoinHandle<()>>,
}

impl TaskEntry {
    fn touch(&mut self) {
        self.task.last_updated_at = current_timestamp();
    }

    fn current_status(&self) -> TaskStatus {
        match &self.terminal {
            Some(payload) => payload.status(),
            None if !self.pending_inputs.is_empty() => TaskStatus::InputRequired,
            None => TaskStatus::Working,
        }
    }

    fn detailed(&self) -> DetailedTask {
        let payload = match &self.terminal {
            Some(p) => p.clone(),
            None if !self.pending_inputs.is_empty() => TaskPayload::InputRequired {
                input_requests: self
                    .pending_inputs
                    .iter()
                    .map(|(k, (req, _))| (k.clone(), req.clone()))
                    .collect::<InputRequests>(),
            },
            None => TaskPayload::Working,
        };
        DetailedTask::new(self.task.clone(), payload)
    }
}

#[derive(Default)]
struct TaskManagerInner {
    tasks: HashMap<String, TaskEntry>,
    listeners: HashMap<u64, TaskListener>,
    next_listener: u64,
}

struct TaskListener {
    ids: HashSet<String>,
    sender: mpsc::Sender<DetailedTask>,
    overflow: CancellationToken,
}

impl TaskManagerInner {
    fn publish(&self, id: &str) {
        let Some(entry) = self.tasks.get(id) else {
            return;
        };
        for listener in self.listeners.values() {
            if !listener.overflow.is_cancelled()
                && listener.ids.contains(id)
                && let Err(mpsc::error::TrySendError::Full(_)) =
                    listener.sender.try_send(entry.detailed())
            {
                listener.overflow.cancel();
            }
        }
    }
}

/// A bounded task-status receiver, released automatically when dropped.
/// Initial snapshots and subsequent changes are ordered atomically with task writes.
/// Unrelated task changes do not occupy the receiver's queue.
pub struct TaskSubscription {
    initial: VecDeque<DetailedTask>,
    receiver: mpsc::Receiver<DetailedTask>,
    overflow: CancellationToken,
    inner: Weak<Mutex<TaskManagerInner>>,
    id: u64,
}

/// A task receiver fell behind; reopen it to obtain fresh snapshots.
#[derive(Debug, thiserror::Error, PartialEq, Eq)]
#[error("Task subscription queue overflow; resynchronize")]
pub struct TaskSubscriptionOverflow;

impl TaskSubscription {
    /// Read the next snapshot or update. Manager shutdown ends the receiver.
    /// Overflow fails immediately rather than silently dropping state changes.
    pub async fn next(&mut self) -> Result<Option<DetailedTask>, TaskSubscriptionOverflow> {
        if self.overflow.is_cancelled() {
            return Err(TaskSubscriptionOverflow);
        }
        if let Some(task) = self.initial.pop_front() {
            return Ok(Some(task));
        }
        tokio::select! {
            biased;
            _ = self.overflow.cancelled() => Err(TaskSubscriptionOverflow),
            task = self.receiver.recv() => Ok(task),
        }
    }
}

impl Drop for TaskSubscription {
    fn drop(&mut self) {
        if let Some(inner) = self.inner.upgrade() {
            inner
                .lock()
                .expect("task manager lock poisoned")
                .listeners
                .remove(&self.id);
        }
    }
}

/// Options controlling a spawned task.
#[derive(Debug, Clone)]
#[non_exhaustive]
pub struct TaskOptions {
    /// TTL in milliseconds; `None` means unlimited retention.
    pub ttl_ms: Option<u64>,
    /// Suggested polling interval in milliseconds.
    pub poll_interval_ms: Option<u64>,
    /// Initial status message.
    pub status_message: Option<String>,
}

impl Default for TaskOptions {
    fn default() -> Self {
        Self {
            ttl_ms: Some(DEFAULT_TASK_TTL_MS),
            poll_interval_ms: Some(DEFAULT_POLL_INTERVAL_MS),
            status_message: None,
        }
    }
}

impl TaskOptions {
    pub fn new() -> Self {
        Self::default()
    }

    /// Set the TTL in milliseconds. `None` means unlimited retention.
    pub fn with_ttl_ms(mut self, ttl_ms: impl Into<Option<u64>>) -> Self {
        self.ttl_ms = ttl_ms.into();
        self
    }

    /// Set the suggested polling interval in milliseconds.
    pub fn with_poll_interval_ms(mut self, poll_interval_ms: u64) -> Self {
        self.poll_interval_ms = Some(poll_interval_ms);
        self
    }

    /// Set the initial status message.
    pub fn with_status_message(mut self, message: impl Into<String>) -> Self {
        self.status_message = Some(message.into());
        self
    }
}

/// Server-side task store and executor for the SEP-2663 Tasks extension.
///
/// Cheaply cloneable; all clones share the same state.
///
/// # Retention
///
/// Entries are swept opportunistically on every `spawn` / `get_task` /
/// `update_task` / `cancel_task` / `running_task_count` / `subscribe` call: non-terminal
/// tasks whose `ttl_ms` has elapsed are marked `failed` (their operation is
/// aborted), and terminal tasks are evicted after being retained for one
/// further `ttl_ms` window past their terminal transition so pollers can
/// observe the final state.
///
/// Note that the retention window intentionally extends past the
/// creation-based lifetime that `ttl_ms` advertises on the wire: a task that
/// runs to its TTL deadline is marked `failed` around `created + ttl_ms` and
/// stays observable until roughly `created + 2 × ttl_ms`. This is compliant —
/// SEP-2663 lets servers delete expired tasks *at any time* after the TTL,
/// so retaining them longer as an observation grace period is a server-side
/// policy choice, not a wire-contract change. Clients may treat the task as
/// unusable after `createdAt + ttlMs` regardless.
///
/// Tasks with `ttl_ms: None` are retained for the lifetime of the manager
/// (spec: unlimited retention) — bound task creation or call
/// [`Self::shutdown`] yourself if you spawn such tasks in a long-lived
/// server. There is no background sweeper; an idle manager holds its entries
/// until the next call.
#[derive(Clone, Default)]
pub struct TaskManager {
    inner: Arc<Mutex<TaskManagerInner>>,
}

impl TaskManager {
    pub fn new() -> Self {
        Self::default()
    }

    /// Subscribe to exact existing IDs, with one initial snapshot per distinct ID.
    /// The snapshots and registration share the task write lock, avoiding lost
    /// changes during admission. Unknown IDs fail the entire registration.
    /// Hosts must bound the number and length of IDs and active receivers.
    /// Queue capacities exceeding Tokio's limit are rejected.
    pub fn subscribe(
        &self,
        task_ids: &[String],
        capacity: NonZeroUsize,
    ) -> Result<TaskSubscription, McpError> {
        if capacity.get() > tokio::sync::Semaphore::MAX_PERMITS {
            return Err(McpError::invalid_params(
                "Task queue capacity exceeds Tokio limit",
                None,
            ));
        }
        let mut inner = self.inner.lock().expect("task manager lock poisoned");
        Self::sweep_expired(&mut inner);
        let ids: HashSet<_> = task_ids.iter().cloned().collect();
        let mut initial = VecDeque::new();
        // Preserve request order while deduplicating initial snapshots.
        let mut seen = HashSet::new();
        for id in task_ids {
            if seen.insert(id) {
                initial.push_back(
                    inner
                        .tasks
                        .get(id)
                        .ok_or_else(|| unknown_task(id))?
                        .detailed(),
                );
            }
        }
        let id = inner.next_listener;
        inner.next_listener = id.checked_add(1).ok_or_else(|| {
            McpError::internal_error("Task subscription ID space exhausted", None)
        })?;
        let (sender, receiver) = mpsc::channel(capacity.get());
        let overflow = CancellationToken::new();
        inner.listeners.insert(
            id,
            TaskListener {
                ids,
                sender,
                overflow: overflow.clone(),
            },
        );
        Ok(TaskSubscription {
            initial,
            receiver,
            overflow,
            inner: Arc::downgrade(&self.inner),
            id,
        })
    }

    /// Spawn an operation as a task and return its seed [`Task`] state for a
    /// `CreateTaskResult`. The task is durably observable via
    /// [`Self::get_task`] before this method returns.
    ///
    /// `make_future` receives a [`TaskContext`] for mid-task input requests,
    /// status messages, and cooperative cancellation checks.
    pub fn spawn<F>(&self, options: TaskOptions, make_future: F) -> Task
    where
        F: FnOnce(TaskContext) -> TaskFuture,
    {
        let task_id = uuid::Uuid::new_v4().to_string();
        let now = current_timestamp();
        let mut task = Task::new(task_id.clone(), TaskStatus::Working, now.clone(), now);
        task.ttl_ms = options.ttl_ms;
        task.poll_interval_ms = options.poll_interval_ms;
        task.status_message = options.status_message;

        let entry = TaskEntry {
            task: task.clone(),
            terminal: None,
            terminal_at: None,
            pending_inputs: HashMap::new(),
            used_input_keys: std::collections::HashSet::new(),
            cancel_requested: false,
            cancel_signal: tokio::sync::watch::channel(false).0,
            created: Instant::now(),
            join_handle: None,
        };
        {
            let mut inner = self.inner.lock().expect("task manager lock poisoned");
            // Opportunistic TTL sweep on every task creation, so terminal
            // entries are evicted even if clients never poll again.
            Self::sweep_expired(&mut inner);
            inner.tasks.insert(task_id.clone(), entry);
        }

        let context = TaskContext {
            task_id: task_id.clone(),
            inner: self.inner.clone(),
        };
        let future = make_future(context);
        let inner = self.inner.clone();
        let id_for_task = task_id.clone();
        let originating_request = crate::service::ORIGINATING_REQUEST
            .try_with(|id| id.clone())
            .ok();
        let handle = tokio::spawn(async move {
            let result = run_task_operation(originating_request, future).await;
            let mut inner = inner.lock().expect("task manager lock poisoned");
            let mut changed = false;
            if let Some(entry) = inner.tasks.get_mut(&id_for_task) {
                if entry.terminal.is_none() {
                    entry.terminal = Some(match result {
                        Ok(result) => TaskPayload::Completed {
                            result: result_to_object(&result),
                        },
                        Err(TaskExit::Cancelled) => TaskPayload::Cancelled,
                        Err(TaskExit::Error(error)) => TaskPayload::Failed {
                            error: error_to_object(&error),
                        },
                    });
                    entry.terminal_at = Some(Instant::now());
                    entry.pending_inputs.clear();
                    entry.touch();
                    entry.task.status = entry.current_status();
                    changed = true;
                }
                // The operation has finished; drop the JoinHandle so it is
                // not retained for the rest of the retention window.
                entry.join_handle = None;
            }
            if changed {
                inner.publish(&id_for_task);
            }
        });
        match self
            .inner
            .lock()
            .expect("task manager lock poisoned")
            .tasks
            .get_mut(&task_id)
        {
            // Only store the handle while the operation is still running: if
            // it already settled, the completion path above ran first and a
            // stored handle would never be cleared.
            Some(entry) => {
                if entry.terminal.is_none() {
                    entry.join_handle = Some(handle);
                }
            }
            // The entry is gone: shutdown() drained the map between the
            // insert and here. Abort rather than leak a detached operation.
            None => handle.abort(),
        }
        task
    }

    /// Handle `tasks/get`: return the current [`DetailedTask`] state.
    pub fn get_task(&self, task_id: &str) -> Result<DetailedTask, McpError> {
        let mut inner = self.inner.lock().expect("task manager lock poisoned");
        Self::sweep_expired(&mut inner);
        let entry = inner
            .tasks
            .get_mut(task_id)
            .ok_or_else(|| unknown_task(task_id))?;
        entry.task.status = entry.current_status();
        Ok(entry.detailed())
    }

    /// Handle `tasks/update`: deliver `inputResponses` to the running
    /// operation. Unknown, already-answered, or superseded keys are ignored
    /// per spec; a partial set of responses is accepted.
    pub fn update_task(
        &self,
        task_id: &str,
        input_responses: impl IntoIterator<Item = (String, serde_json::Value)>,
    ) -> Result<(), McpError> {
        let mut inner = self.inner.lock().expect("task manager lock poisoned");
        Self::sweep_expired(&mut inner);
        let entry = inner
            .tasks
            .get_mut(task_id)
            .ok_or_else(|| unknown_task(task_id))?;
        for (key, value) in input_responses {
            if let Some((_, tx)) = entry.pending_inputs.remove(&key) {
                // Receiver dropped means the operation moved on; ignore.
                let _ = tx.send(value);
            }
        }
        entry.touch();
        entry.task.status = entry.current_status();
        inner.publish(task_id);
        Ok(())
    }

    /// Handle `tasks/cancel`: cooperative cancellation (SEP-2663).
    ///
    /// Records the cancellation *intent* and acknowledges immediately, but
    /// does **not** abort the underlying future or force a terminal state.
    /// The operation observes cancellation via
    /// [`TaskContext::is_cancel_requested`] / [`TaskContext::cancelled`], or
    /// via the error returned from a pending [`TaskContext::request_input`]
    /// call (whose response channel is dropped here), and decides its own
    /// terminal status:
    ///
    /// - stops with [`TaskExit::Cancelled`] → recorded as `cancelled`,
    /// - stops with [`TaskExit::Error`] → recorded as `failed` with the
    ///   error inlined (a real failure after a cancel request is not masked),
    /// - finishes its work anyway → recorded as `completed` — per the spec,
    ///   "the task may still reach a non-`cancelled` terminal status".
    pub fn cancel_task(&self, task_id: &str) -> Result<(), McpError> {
        let mut inner = self.inner.lock().expect("task manager lock poisoned");
        Self::sweep_expired(&mut inner);
        let entry = inner
            .tasks
            .get_mut(task_id)
            .ok_or_else(|| unknown_task(task_id))?;
        entry.cancel_requested = true;
        let _ = entry.cancel_signal.send(true);
        if entry.terminal.is_none() {
            // Wake any operation parked on `request_input`: dropping the
            // response senders resolves those awaits with an error, giving
            // parked operations a cooperative exit path. The task leaves
            // `input_required` and reports `working` until it settles.
            entry.pending_inputs.clear();
            entry.touch();
            entry.task.status = entry.current_status();
            inner.publish(task_id);
        }
        Ok(())
    }

    /// Number of tasks currently in a non-terminal state.
    pub fn running_task_count(&self) -> usize {
        let mut inner = self.inner.lock().expect("task manager lock poisoned");
        Self::sweep_expired(&mut inner);
        inner
            .tasks
            .values()
            .filter(|e| e.terminal.is_none())
            .count()
    }

    /// Abort all running tasks and clear all task state.
    pub fn shutdown(&self) {
        let mut inner = self.inner.lock().expect("task manager lock poisoned");
        inner.listeners.clear();
        for (_, mut entry) in inner.tasks.drain() {
            if let Some(handle) = entry.join_handle.take() {
                handle.abort();
            }
        }
    }

    /// TTL sweep, run from every `TaskManager` entry point (SEP-2663: servers
    /// MAY mark a task `failed` any time after its TTL elapses, and
    /// subsequently delete it at any time; `ttl_ms: None` means unlimited
    /// retention).
    ///
    /// Two phases per entry:
    /// 1. A non-terminal task whose TTL has elapsed is marked `failed` (its
    ///    operation is aborted — the TTL is the SDK's hard-stop safety valve,
    ///    unlike cooperative `tasks/cancel`).
    /// 2. A *terminal* task is evicted once it has been retained for a full
    ///    TTL window after reaching its terminal state, so well-behaved
    ///    pollers get a chance to observe the final status before late
    ///    `tasks/get` calls start returning `-32602`.
    fn sweep_expired(inner: &mut TaskManagerInner) {
        let mut changed = Vec::new();
        // Phase 1: fail overdue non-terminal tasks.
        for entry in inner.tasks.values_mut() {
            if entry.terminal.is_none()
                && let Some(ttl_ms) = entry.task.ttl_ms
                && entry.created.elapsed().as_millis() >= u128::from(ttl_ms)
            {
                if let Some(handle) = entry.join_handle.take() {
                    handle.abort();
                }
                entry.terminal = Some(TaskPayload::Failed {
                    error: error_to_object(&McpError::internal_error(
                        "task expired: TTL elapsed before completion".to_string(),
                        None,
                    )),
                });
                entry.terminal_at = Some(Instant::now());
                entry.pending_inputs.clear();
                entry.touch();
                entry.task.status = TaskStatus::Failed;
                changed.push(entry.task.task_id.clone());
            }
        }
        for id in changed {
            inner.publish(&id);
        }
        // Phase 2: evict terminal tasks whose retention window has passed.
        inner.tasks.retain(|_, entry| {
            let (Some(ttl_ms), Some(terminal_at)) = (entry.task.ttl_ms, entry.terminal_at) else {
                return true;
            };
            terminal_at.elapsed().as_millis() < u128::from(ttl_ms)
        });
    }
}

fn unknown_task(task_id: &str) -> McpError {
    McpError::invalid_params(format!("unknown task: {task_id}"), None)
}

async fn run_task_operation(
    originating_request: Option<crate::model::RequestId>,
    future: TaskFuture,
) -> Result<CallToolResult, TaskExit> {
    match originating_request {
        Some(id) => crate::service::ORIGINATING_REQUEST.scope(id, future).await,
        None => future.await,
    }
}

fn result_to_object(result: &CallToolResult) -> JsonObject {
    match serde_json::to_value(result) {
        Ok(serde_json::Value::Object(map)) => map,
        _ => JsonObject::new(),
    }
}

fn error_to_object(error: &McpError) -> JsonObject {
    match serde_json::to_value(error) {
        Ok(serde_json::Value::Object(map)) => map,
        _ => JsonObject::new(),
    }
}

#[cfg(test)]
mod subscription_tests {
    use super::*;

    #[tokio::test]
    async fn dropping_receiver_releases_registration_and_failed_admission_retains_nothing() {
        let tasks = TaskManager::new();
        let task = tasks.spawn(TaskOptions::default(), |_| Box::pin(std::future::pending()));
        let capacity = NonZeroUsize::new(1).unwrap();
        assert!(
            tasks
                .subscribe(&[task.task_id.clone(), "missing".into()], capacity)
                .is_err()
        );
        assert!(tasks.inner.lock().unwrap().listeners.is_empty());
        let subscription = tasks
            .subscribe(std::slice::from_ref(&task.task_id), capacity)
            .unwrap();
        assert_eq!(tasks.inner.lock().unwrap().listeners.len(), 1);
        drop(subscription);
        assert!(tasks.inner.lock().unwrap().listeners.is_empty());
        assert!(
            tasks
                .subscribe(&[], NonZeroUsize::new(usize::MAX).unwrap())
                .is_err()
        );
        tasks.shutdown();
    }
}
