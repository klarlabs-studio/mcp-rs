//! Bounded, process-local modern notification subscriptions.
//! Each listener receives initial refresh hints, then only its accepted updates.
use crate::model::SubscriptionFilter;
use mcp_protocol::{ErrorData, model::ErrorCode, service::SubscriptionContext};
use std::{
    collections::{BTreeMap, BTreeSet},
    num::NonZeroUsize,
    sync::{
        Arc, Mutex, MutexGuard,
        atomic::{AtomicBool, Ordering},
    },
    time::Duration,
};
use tokio::sync::{Notify, mpsc};
use tokio_util::sync::CancellationToken;

/// Memory and delivery limits for one shared hub.
#[derive(Clone, Debug)]
pub struct SubscriptionConfig {
    /// Maximum active listen requests across all clients.
    pub max_streams: NonZeroUsize,
    /// Maximum requested resource entries in one listen request, including duplicates.
    pub max_resources_per_stream: NonZeroUsize,
    /// Maximum requested resource entries across active streams, including declined entries.
    pub max_resource_filters: NonZeroUsize,
    /// Maximum requested task entries per stream, including duplicates.
    pub max_tasks_per_stream: NonZeroUsize,
    /// Maximum requested task entries across active streams.
    pub max_task_filters: NonZeroUsize,
    /// Maximum bytes in a requested task ID.
    pub max_task_id_bytes: NonZeroUsize,
    /// Maximum bytes in a requested or published resource URI.
    pub max_uri_bytes: NonZeroUsize,
    /// Queued updates per stream. A full queue closes that stream for resynchronization.
    pub queue_capacity: NonZeroUsize,
    /// Deadline for sending one notification to the SDK peer.
    pub send_timeout: Duration,
}
impl Default for SubscriptionConfig {
    fn default() -> Self {
        Self {
            max_streams: NonZeroUsize::new(1024).unwrap_or(NonZeroUsize::MIN),
            max_resources_per_stream: NonZeroUsize::new(1000).unwrap_or(NonZeroUsize::MIN),
            max_resource_filters: NonZeroUsize::new(10_000).unwrap_or(NonZeroUsize::MIN),
            max_tasks_per_stream: NonZeroUsize::new(1000).unwrap_or(NonZeroUsize::MIN),
            max_task_filters: NonZeroUsize::new(10_000).unwrap_or(NonZeroUsize::MIN),
            max_task_id_bytes: NonZeroUsize::new(4096).unwrap_or(NonZeroUsize::MIN),
            max_uri_bytes: NonZeroUsize::new(4096).unwrap_or(NonZeroUsize::MIN),
            queue_capacity: NonZeroUsize::new(32).unwrap_or(NonZeroUsize::MIN),
            send_timeout: Duration::from_secs(5),
        }
    }
}
/// A hub configuration or publication failure.
#[derive(Debug, thiserror::Error, PartialEq, Eq)]
#[non_exhaustive]
pub enum SubscriptionError {
    /// The queue or deadline configuration is invalid.
    #[error("invalid subscription configuration: {0}")]
    InvalidConfig(&'static str),
    /// A URI is empty or exceeds the configured byte limit.
    #[error("invalid subscription resource URI")]
    InvalidUri,
    /// Shutdown permanently closed this hub.
    #[error("subscription hub closed")]
    Closed,
}
/// Queue admission counts, not delivery acknowledgments. Refresh hints are idempotent.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct DeliveryReport {
    /// Active filters matching the published change.
    pub matched: usize,
    /// Matching streams whose queues accepted the update.
    pub enqueued: usize,
    /// Matching streams closed because their queues were full.
    pub slow_streams: usize,
}
/// Current retained listen requests and resource/task filter allocation.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct SubscriptionStats {
    /// Active listen handlers, including those finishing cancellation.
    pub active_streams: usize,
    /// Requested URI entries retained by these handlers, including duplicates/declined entries.
    pub resource_filters: usize,
    /// Requested task entries retained, including duplicates/declined entries.
    pub task_filters: usize,
}
#[derive(Clone)]
enum Update {
    Resource(Arc<str>),
    Task(Box<crate::model::DetailedTask>),
    Tools,
    Resources,
    Prompts,
}
struct Entry {
    filter: SubscriptionFilter,
    resources: BTreeSet<String>,
    sender: mpsc::Sender<Update>,
    stop: CancellationToken,
    slow: Arc<AtomicBool>,
    charge: usize,
    task_charge: usize,
}
impl Entry {
    fn accepts(&self, update: &Update) -> bool {
        match update {
            Update::Task(_) => false, // Tasks use a separately bounded, ID-filtered receiver.
            Update::Resource(uri) => self.resources.contains(uri.as_ref()),
            Update::Tools => self.filter.tools_list_changed == Some(true),
            Update::Resources => self.filter.resources_list_changed == Some(true),
            Update::Prompts => self.filter.prompts_list_changed == Some(true),
        }
    }
}
#[derive(Default)]
struct State {
    entries: BTreeMap<u64, Entry>,
    next_id: u64,
    resources: usize,
    tasks: usize,
    closed: bool,
}
struct Inner {
    config: SubscriptionConfig,
    state: Mutex<State>,
    changed: Notify,
}
/// Shared subscription state, intentionally preserved across server clones/snapshots.
///
/// Publish methods never await peer I/O. Slow streams close rather than silently
/// losing invalidations. `shutdown` waits for listen registrations to unwind;
/// transport shutdown remains caller-owned. No background workers are spawned.
#[derive(Clone)]
#[must_use]
pub struct SubscriptionHub {
    inner: Arc<Inner>,
}
impl Default for SubscriptionHub {
    fn default() -> Self {
        Self::configured(SubscriptionConfig::default())
    }
}
impl std::fmt::Debug for SubscriptionHub {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SubscriptionHub")
            .field("config", &self.inner.config)
            .field("stats", &self.stats())
            .finish_non_exhaustive()
    }
}
impl SubscriptionHub {
    /// Construct a hub with explicit allocation and write limits.
    /// # Errors
    /// Rejects unsupported Tokio queue capacities and a zero write deadline.
    pub fn new(config: SubscriptionConfig) -> Result<Self, SubscriptionError> {
        if config.queue_capacity.get() > tokio::sync::Semaphore::MAX_PERMITS {
            return Err(SubscriptionError::InvalidConfig(
                "queue capacity exceeds Tokio limit",
            ));
        }
        if config.send_timeout.is_zero() {
            return Err(SubscriptionError::InvalidConfig(
                "send timeout must be nonzero",
            ));
        }
        Ok(Self::configured(config))
    }
    fn configured(config: SubscriptionConfig) -> Self {
        Self {
            inner: Arc::new(Inner {
                config,
                state: Mutex::new(State::default()),
                changed: Notify::new(),
            }),
        }
    }
    fn state(&self) -> MutexGuard<'_, State> {
        self.inner
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }
    /// Inspect retained allocation without exposing client identities.
    pub fn stats(&self) -> SubscriptionStats {
        let state = self.state();
        SubscriptionStats {
            active_streams: state.entries.len(),
            resource_filters: state.resources,
            task_filters: state.tasks,
        }
    }
    /// Queue an invalidation for an exact URI, including concrete template matches.
    /// # Errors
    /// Returns an invalid-URI or closed-hub error.
    pub fn notify_resource_updated(&self, uri: &str) -> Result<DeliveryReport, SubscriptionError> {
        if uri.is_empty() || uri.len() > self.inner.config.max_uri_bytes.get() {
            return Err(SubscriptionError::InvalidUri);
        }
        self.publish(Update::Resource(uri.into()))
    }
    /// Queue a tool-list invalidation for streams opting into that category.
    /// # Errors
    /// Returns an error after hub shutdown.
    pub fn notify_tools_changed(&self) -> Result<DeliveryReport, SubscriptionError> {
        self.publish(Update::Tools)
    }
    /// Queue a resource-list invalidation for streams opting into that category.
    /// # Errors
    /// Returns an error after hub shutdown.
    pub fn notify_resources_changed(&self) -> Result<DeliveryReport, SubscriptionError> {
        self.publish(Update::Resources)
    }
    /// Queue a prompt-list invalidation for streams opting into that category.
    /// # Errors
    /// Returns an error after hub shutdown.
    pub fn notify_prompts_changed(&self) -> Result<DeliveryReport, SubscriptionError> {
        self.publish(Update::Prompts)
    }
    fn publish(&self, update: Update) -> Result<DeliveryReport, SubscriptionError> {
        let state = self.state();
        if state.closed {
            return Err(SubscriptionError::Closed);
        }
        let mut report = DeliveryReport::default();
        for entry in state.entries.values() {
            if entry.stop.is_cancelled() || !entry.accepts(&update) {
                continue;
            }
            report.matched += 1;
            match entry.sender.try_send(update.clone()) {
                Ok(()) => report.enqueued += 1,
                Err(mpsc::error::TrySendError::Full(_)) => {
                    report.slow_streams += 1;
                    entry.slow.store(true, Ordering::Release);
                    entry.stop.cancel();
                }
                Err(mpsc::error::TrySendError::Closed(_)) => entry.stop.cancel(),
            }
        }
        Ok(report)
    }
    /// Permanently stop admission and request graceful closure of active streams.
    /// Initial hints or already queued transport frames may still be in flight.
    pub fn close(&self) {
        let mut state = self.state();
        state.closed = true;
        for entry in state.entries.values() {
            entry.stop.cancel();
        }
    }
    /// Close the hub and await all listen handlers' registration guards.
    /// Clients cancelling or disconnecting also release guards automatically.
    pub async fn shutdown(&self) {
        self.close();
        loop {
            let changed = self.inner.changed.notified();
            tokio::pin!(changed);
            changed.as_mut().enable();
            if self.stats().active_streams == 0 {
                return;
            }
            changed.await;
        }
    }
    pub(crate) fn valid_request(&self, filter: &SubscriptionFilter) -> bool {
        filter.task_ids.as_ref().is_none_or(|ids| {
            ids.len() <= self.inner.config.max_tasks_per_stream.get()
                && ids.iter().all(|id| {
                    !id.is_empty() && id.len() <= self.inner.config.max_task_id_bytes.get()
                })
        }) && filter.resource_subscriptions.as_ref().is_none_or(|uris| {
            uris.len() <= self.inner.config.max_resources_per_stream.get()
                && uris.iter().all(|uri| {
                    !uri.is_empty() && uri.len() <= self.inner.config.max_uri_bytes.get()
                })
        })
    }
    pub(crate) async fn listen(
        &self,
        context: SubscriptionContext,
        tasks: Option<&mcp_protocol::task_manager::TaskManager>,
    ) -> Result<(), ErrorData> {
        if !self.valid_request(context.requested()) {
            return Err(ErrorData::invalid_params(
                "Subscription filter exceeds limits",
                None,
            ));
        }
        let charge = context
            .requested()
            .resource_subscriptions
            .as_ref()
            .map_or(0, Vec::len);
        let task_charge = context.requested().task_ids.as_ref().map_or(0, Vec::len);
        let (sender, mut receiver) = mpsc::channel(self.inner.config.queue_capacity.get());
        let stop = context.request_context().ct.child_token();
        let slow = Arc::new(AtomicBool::new(false));
        let id = {
            let mut state = self.state();
            if state.closed {
                return Err(capacity_error("Subscription hub closed"));
            }
            if task_charge
                > self
                    .inner
                    .config
                    .max_task_filters
                    .get()
                    .saturating_sub(state.tasks)
                || state.entries.len() >= self.inner.config.max_streams.get()
                || charge
                    > self
                        .inner
                        .config
                        .max_resource_filters
                        .get()
                        .saturating_sub(state.resources)
            {
                return Err(capacity_error("Subscription capacity exceeded"));
            }
            let id = state.next_id;
            state.next_id = state
                .next_id
                .checked_add(1)
                .ok_or_else(|| capacity_error("Subscription ID space exhausted"))?;
            state.resources += charge;
            state.tasks += task_charge;
            state.entries.insert(
                id,
                Entry {
                    filter: context.accepted().clone(),
                    resources: context
                        .accepted()
                        .resource_subscriptions
                        .clone()
                        .unwrap_or_default()
                        .into_iter()
                        .collect(),
                    sender,
                    stop: stop.clone(),
                    slow: slow.clone(),
                    charge,
                    task_charge,
                },
            );
            id
        };
        let _registration = Registration {
            hub: self.clone(),
            id,
        };
        let task_ids = context.accepted().task_ids.as_deref().unwrap_or_default();
        let mut task_updates = if task_ids.is_empty() {
            None
        } else {
            Some(
                tasks
                    .ok_or_else(|| {
                        ErrorData::invalid_params("Task subscriptions unavailable", None)
                    })?
                    .subscribe(task_ids, self.inner.config.queue_capacity)?,
            )
        };
        // The SDK acknowledges before invoking listen. Initial invalidations cover
        // changes during admission and let clients read a current snapshot safely.
        let mut initial = Vec::new();
        let filter = context.accepted();
        if filter.tools_list_changed == Some(true) {
            initial.push(Update::Tools);
        }
        if filter.resources_list_changed == Some(true) {
            initial.push(Update::Resources);
        }
        if filter.prompts_list_changed == Some(true) {
            initial.push(Update::Prompts);
        }
        for uri in filter
            .resource_subscriptions
            .clone()
            .unwrap_or_default()
            .into_iter()
            .collect::<BTreeSet<_>>()
        {
            initial.push(Update::Resource(uri.into()));
        }
        for update in initial {
            if !self.send(&context, &stop, &slow, update).await? {
                return Ok(());
            }
        }
        loop {
            let update = tokio::select! {
                biased;
                _ = stop.cancelled() => return terminal(&slow),
                task = async {
                    match task_updates.as_mut() {
                        Some(updates) => updates.next().await,
                        None => std::future::pending().await,
                    }
                } => task.map_err(|_| capacity_error("Task subscription queue overflow; resynchronize"))?
                    .map(|task| Update::Task(Box::new(task))),
                update = receiver.recv() => update,
            };
            let Some(update) = update else {
                return Ok(());
            };
            if !self.send(&context, &stop, &slow, update).await? {
                return Ok(());
            }
        }
    }
    async fn send(
        &self,
        context: &SubscriptionContext,
        stop: &CancellationToken,
        slow: &AtomicBool,
        update: Update,
    ) -> Result<bool, ErrorData> {
        let send = async {
            match update {
                Update::Task(task) => {
                    context
                        .sink()
                        .send(crate::model::ServerNotification::TaskStatusNotification(
                            crate::model::TaskStatusNotification::new(
                                crate::model::TaskStatusNotificationParams::new(*task),
                            ),
                        ))
                        .await
                }
                Update::Resource(uri) => context.sink().notify_resource_updated(uri.as_ref()).await,
                Update::Tools => context.sink().notify_tool_list_changed().await,
                Update::Resources => context.sink().notify_resource_list_changed().await,
                Update::Prompts => context.sink().notify_prompt_list_changed().await,
            }
        };
        tokio::select! {
            biased;
            _=stop.cancelled()=> {terminal(slow)?;Ok(false)},
            result=tokio::time::timeout(self.inner.config.send_timeout,send)=>{
                result.map_err(|_|ErrorData::internal_error("Subscription delivery timed out",None))?.map_err(|_|ErrorData::internal_error("Subscription delivery failed",None))?;Ok(true)
            }
        }
    }
}
struct Registration {
    hub: SubscriptionHub,
    id: u64,
}
impl Drop for Registration {
    fn drop(&mut self) {
        {
            let mut state = self.hub.state();
            if let Some(entry) = state.entries.remove(&self.id) {
                state.resources -= entry.charge;
                state.tasks -= entry.task_charge;
            }
        }
        self.hub.inner.changed.notify_waiters();
    }
}
fn capacity_error(message: &'static str) -> ErrorData {
    ErrorData::new(ErrorCode(-32000), message, None)
}
fn terminal(slow: &AtomicBool) -> Result<(), ErrorData> {
    if slow.load(Ordering::Acquire) {
        Err(capacity_error("Subscription queue overflow; resynchronize"))
    } else {
        Ok(())
    }
}
