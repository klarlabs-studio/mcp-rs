use crate::model::*;
use crate::{
    Context, HandlerResponse, Middleware, Next, PromptBuilder, ResourceBuilder, Response,
    ToolBuilder, VisibilityTarget,
};
use crate::{
    prompt::PromptHandler,
    resource::{ResourceHandler, UriMatcher},
};
use futures_util::FutureExt;
use mcp_protocol::{ErrorData, ServerHandler};
use serde_json::{Value, json};
use std::{collections::BTreeMap, sync::Arc, time::Duration};

#[derive(Clone)]
pub(crate) struct RegisteredTool {
    pub(crate) definition: Tool,
    pub(crate) discovery: crate::discovery::DiscoveryOptions,
    pub(crate) handler: Next,
}
#[derive(Clone)]
pub(crate) struct RegisteredResource {
    pub(crate) definition: Resource,
    pub(crate) template: Option<ResourceTemplate>,
    pub(crate) matcher: Option<UriMatcher>,
    pub(crate) handler: ResourceHandler,
}
#[derive(Clone)]
pub(crate) struct RegisteredPrompt {
    pub(crate) definition: Prompt,
    pub(crate) handler: PromptHandler,
}

/// A cheaply cloneable application configuration.
///
/// Clones share configuration and handler state. Subsequent registrations use
/// copy-on-write, so existing clones retain their original configuration.
#[derive(Clone)]
#[must_use]
pub struct Server {
    pub(crate) inner: Arc<ServerState>,
}

#[derive(Clone)]
pub(crate) struct ServerState {
    identity: Implementation,
    build_info: Option<crate::BuildInfo>,
    instructions: Option<String>,
    pub(crate) tools: BTreeMap<String, RegisteredTool>,
    pub(crate) resources: BTreeMap<String, RegisteredResource>,
    pub(crate) prompts: BTreeMap<String, RegisteredPrompt>,
    middleware: Vec<Middleware>,
    timeout: Duration,
    completion: Option<Next>,
    pub(crate) tasks: Option<mcp_protocol::task_manager::TaskManager>,
    subscriptions: Option<crate::SubscriptionHub>,
    visibility: Option<crate::VisibilityPolicy>,
    result_cache: ResultCachePolicy,
    pub(crate) skills: BTreeMap<String, crate::skills::SkillIndexEntry>,
    pub(crate) skill_owners: BTreeMap<String, String>,
}

#[derive(Clone, Copy)]
struct ResultCachePolicy {
    ttl_ms: u64,
    scope: CacheScope,
}

impl Default for ResultCachePolicy {
    fn default() -> Self {
        Self {
            ttl_ms: 0,
            scope: CacheScope::Private,
        }
    }
}

impl ResultCachePolicy {
    fn fill_missing(&self, ttl_ms: &mut Option<u64>, cache_scope: &mut Option<CacheScope>) {
        ttl_ms.get_or_insert(self.ttl_ms);
        cache_scope.get_or_insert(self.scope);
    }
}

impl Server {
    /// Create a server configuration with a 30-second handler deadline.
    pub fn new(name: impl Into<String>, version: impl Into<String>) -> Self {
        Self {
            inner: Arc::new(ServerState {
                identity: Implementation::new(name, version),
                build_info: None,
                instructions: None,
                tools: BTreeMap::new(),
                resources: BTreeMap::new(),
                prompts: BTreeMap::new(),
                middleware: vec![],
                timeout: Duration::from_secs(30),
                completion: None,
                tasks: None,
                subscriptions: None,
                visibility: None,
                result_cache: ResultCachePolicy::default(),
                skills: BTreeMap::new(),
                skill_owners: BTreeMap::new(),
            }),
        }
    }
    /// Replace the native implementation identity, including name and version.
    /// Registered methods, derived capabilities, and instructions are preserved.
    /// Optional values in the supplied SDK type are retained as provided.
    pub fn with_server_info(mut self, identity: Implementation) -> Self {
        Arc::make_mut(&mut self.inner).identity = identity;
        self
    }
    /// Set the modern Go `serverInfo.buildInfo` extension; `None` clears it.
    /// This is independent of native identity replacement and is emitted in
    /// discovery metadata. Build details are supplied by the host, never inferred.
    pub fn with_build_info(mut self, build_info: impl Into<Option<crate::BuildInfo>>) -> Self {
        Arc::make_mut(&mut self.inner).build_info = build_info.into();
        self
    }
    /// Set cache hints on complete discovery, list, and resource-read results.
    ///
    /// The Rust SDK defaults to `ttlMs: 0` and `cacheScope: private`; this
    /// conservative default differs from the pinned Go implementation's
    /// `60000` millisecond private hint. Use [`CacheScope::Public`] only when
    /// responses are safe to share across authorization contexts: a host that
    /// honors a public hint may serve one caller's response to another caller.
    /// This sets protocol hints only; it does not cache responses in this server.
    pub fn with_result_cache(mut self, ttl_ms: u64, scope: CacheScope) -> Self {
        Arc::make_mut(&mut self.inner).result_cache = ResultCachePolicy { ttl_ms, scope };
        self
    }

    pub(crate) fn discovery(&self) -> Result<DiscoverResult, ErrorData> {
        let mut result =
            DiscoverResult::from_server_info(vec![ProtocolVersion::V_2026_07_28], self.get_info());
        result.ttl_ms = self.inner.result_cache.ttl_ms;
        result.cache_scope = self.inner.result_cache.scope;
        if let Some(build_info) = &self.inner.build_info {
            // The native SDK identity type is closed; its discovery metadata is not.
            // Extend the actual identity object rather than inventing a metadata key.
            if let Some(identity) = result
                .meta
                .as_mut()
                .and_then(|meta| meta.get_mut(crate::metadata::SERVER_INFO_KEY))
                .and_then(Value::as_object_mut)
            {
                identity.insert(
                    "buildInfo".into(),
                    serde_json::to_value(build_info).map_err(|_| {
                        ErrorData::internal_error("Unable to encode build metadata", None)
                    })?,
                );
            }
        }
        Ok(result)
    }
    /// Set a human-readable display title. An empty string clears the title.
    pub fn with_title(mut self, title: impl Into<String>) -> Self {
        Arc::make_mut(&mut self.inner).identity.title = nonempty(title.into());
        self
    }
    /// Describe the server. An empty string clears the description.
    pub fn with_description(mut self, description: impl Into<String>) -> Self {
        Arc::make_mut(&mut self.inner).identity.description = nonempty(description.into());
        self
    }
    /// Set a documentation/homepage URI. An empty string clears the URI.
    /// The value is advertised unchanged; this does not fetch or validate the URI.
    pub fn with_website_url(mut self, website_url: impl Into<String>) -> Self {
        Arc::make_mut(&mut self.inner).identity.website_url = nonempty(website_url.into());
        self
    }
    /// Replace icons using native SDK icon metadata. An empty collection clears them.
    /// Sources are advertised unchanged and are never fetched by this framework.
    /// The SDK uses `src` and an array of size strings; pinned Go icon wire formats differ.
    pub fn with_icons(mut self, icons: impl IntoIterator<Item = Icon>) -> Self {
        let icons: Vec<_> = icons.into_iter().collect();
        Arc::make_mut(&mut self.inner).identity.icons = (!icons.is_empty()).then_some(icons);
        self
    }
    /// Set guidance for clients/models. An empty string clears the instructions.
    /// Instructions accompany modern discovery.
    pub fn with_instructions(mut self, instructions: impl Into<String>) -> Self {
        Arc::make_mut(&mut self.inner).instructions = nonempty(instructions.into());
        self
    }
    /// Middleware runs in registration order around tools, resources, and prompts.
    pub fn with_middleware(mut self, middleware: Middleware) -> Self {
        Arc::make_mut(&mut self.inner).middleware.push(middleware);
        self
    }
    /// Replace request-aware visibility for tools, resources, and prompts.
    /// Lists are filtered before pagination; hidden objects reject direct access
    /// with the same error as missing objects. Completion references and resource subscription
    /// admission are checked too. Predicates run before application middleware;
    /// authenticated identity must already be present in request extensions.
    /// Keep callbacks fast/nonblocking. Existing subscriptions retain their grants;
    /// close their hub to revoke them. Capabilities and host `get_tool` lookups
    /// describe configured methods and remain independent of per-request visibility.
    pub fn with_visibility_policy<F>(mut self, policy: F) -> Self
    where
        F: Fn(&Context, VisibilityTarget<'_>) -> bool + Send + Sync + 'static,
    {
        Arc::make_mut(&mut self.inner).visibility = Some(Arc::new(policy));
        self
    }
    /// Async deadline for each handler. Blocking work must be moved off the runtime.
    pub fn with_timeout(mut self, timeout: Duration) -> Self {
        Arc::make_mut(&mut self.inner).timeout = timeout;
        self
    }
    /// Install a shared modern subscription hub. Clones/snapshots retain the same hub.
    /// Admission passes through application middleware; stream lifetime is governed
    /// by client cancellation, hub shutdown, and bounded delivery rather than handler timeouts.
    pub fn with_subscription_hub(mut self, hub: crate::SubscriptionHub) -> Self {
        Arc::make_mut(&mut self.inner).subscriptions = Some(hub);
        self
    }
    /// Begin registering a typed tool. Call `handler` to finish registration.
    pub fn tool(&mut self, name: impl Into<String>) -> ToolBuilder<'_> {
        ToolBuilder {
            server: self,
            name: name.into(),
            description: String::new(),
            meta: None,
            output_schema: None,
            annotations: None,
            title: None,
            skip_validation: false,
            discovery: Default::default(),
        }
    }
    /// Begin registering a static or URI-template text/binary resource.
    pub fn resource(&mut self, uri: impl Into<String>) -> ResourceBuilder<'_> {
        let uri = uri.into();
        ResourceBuilder {
            server: self,
            name: uri.clone(),
            uri,
            description: String::new(),
            mime_type: "text/plain".into(),
        }
    }
    /// Begin registering a parameterized prompt.
    pub fn prompt(&mut self, name: impl Into<String>) -> PromptBuilder<'_> {
        PromptBuilder {
            server: self,
            name: name.into(),
            description: String::new(),
            arguments: vec![],
        }
    }
    /// Install an SDK task manager and advertise the tasks extension.
    ///
    /// Native protocol handlers can return handles from this manager. Polling,
    /// updates, and cancellation delegate to the same manager. The application
    /// owns task admission, authorization, retention policy, and shutdown.
    pub fn with_task_manager(mut self, tasks: mcp_protocol::task_manager::TaskManager) -> Self {
        Arc::make_mut(&mut self.inner).tasks = Some(tasks);
        self
    }
    /// Configure parameter completion for prompt or resource-template references.
    ///
    /// The callback receives the native SDK request, including argument context.
    /// Completion execution passes through the same middleware and deadline guard.
    pub fn with_completion_handler<F, Fut>(mut self, handler: F) -> Self
    where
        F: Fn(Context, CompleteRequestParams) -> Fut + Send + Sync + 'static,
        Fut: std::future::Future<Output = Result<CompleteResult, ErrorData>> + Send + 'static,
    {
        let handler = Arc::new(handler);
        Arc::make_mut(&mut self.inner).completion = Some(Arc::new(move |ctx, args| {
            let handler = handler.clone();
            async move {
                let request = serde_json::from_value(args)
                    .map_err(|_| ErrorData::invalid_params("Invalid completion request", None))?;
                handler(ctx, request).await.map(Response::Completion)
            }
            .boxed()
        }));
        self
    }
    /// Atomically register an immutable, bounded in-memory skill bundle.
    /// Skills use ordinary resources and inherit middleware and visibility checks.
    /// # Errors
    /// Returns an error on namespace or resource conflicts; no changes are applied.
    pub fn register_skill(
        &mut self,
        bundle: crate::skills::SkillBundle,
    ) -> Result<(), crate::skills::SkillError> {
        use crate::skills::*;
        use base64::Engine;
        let mut candidate = self.clone();
        if self.inner.skills.contains_key(&bundle.path) {
            return Err(SkillError::DuplicateSkill(bundle.path));
        }
        candidate.ensure_skill_index()?;
        let entry = SkillIndexEntry {
            name: bundle.name.clone(),
            description: bundle.description.clone(),
            entry_type: SkillIndexType::SkillMd,
            url: format!("skill://{}/{}", bundle.path, SKILL_ENTRY_POINT),
        };
        for (path, file) in bundle.files {
            let uri = format!("skill://{}/{path}", bundle.path);
            let contents = match file.data {
                SkillData::Text(text) => ResourceContents::text(text, &uri),
                SkillData::Binary(bytes) => ResourceContents::blob(
                    base64::engine::general_purpose::STANDARD.encode(bytes),
                    &uri,
                ),
            }
            .with_mime_type(&file.mime_type);
            Arc::make_mut(&mut candidate.inner)
                .skill_owners
                .insert(uri.clone(), bundle.path.clone());
            candidate
                .resource(&uri)
                .name(if path == SKILL_ENTRY_POINT {
                    &bundle.name
                } else {
                    &path
                })
                .description(if path == SKILL_ENTRY_POINT {
                    &bundle.description
                } else {
                    ""
                })
                .mime_type(file.mime_type)
                .content_handler(move |_, _, _| {
                    let contents = contents.clone();
                    async move { Ok(vec![contents]) }
                })?;
        }
        Arc::make_mut(&mut candidate.inner)
            .skills
            .insert(bundle.path, entry);
        *self = candidate;
        Ok(())
    }
    pub(crate) fn ensure_skill_index(&mut self) -> Result<(), crate::skills::SkillError> {
        use crate::skills::SKILL_INDEX_URI;
        if self.inner.skills.is_empty() {
            self.resource(SKILL_INDEX_URI)
                .name("skills-index")
                .description("Agent Skills discovery index (SEP-2640)")
                .mime_type("application/json")
                .handler(|_, _| async {
                    Err(ErrorData::internal_error(
                        "Skill index dispatch unavailable",
                        None,
                    ))
                })?;
        }
        Ok(())
    }
    /// Atomically register a bounded, immutable packed skill snapshot.
    ///
    /// Archives are ordinary binary resources with an `archive` index entry.
    /// Their contents are not inspected or unpacked. Callers must supply a
    /// trusted archive containing a root `SKILL.md` with matching metadata.
    /// # Errors
    /// Returns an error on namespace or resource conflicts without changing this server.
    pub fn register_skill_archive(
        &mut self,
        archive: crate::skills::SkillArchive,
    ) -> Result<(), crate::skills::SkillError> {
        use crate::skills::{SkillError, SkillIndexEntry, SkillIndexType};
        use base64::Engine;
        if self.inner.skills.contains_key(&archive.path) {
            return Err(SkillError::DuplicateSkill(archive.path));
        }
        let mut candidate = self.clone();
        candidate.ensure_skill_index()?;
        let uri = format!("skill://{}{}", archive.path, archive.format.suffix());
        let name = archive.path.rsplit('/').next().unwrap_or_default();
        let entry = SkillIndexEntry {
            name: name.to_owned(),
            description: archive.description.clone(),
            entry_type: SkillIndexType::Archive,
            url: uri.clone(),
        };
        let contents = ResourceContents::blob(
            base64::engine::general_purpose::STANDARD.encode(archive.bytes),
            &uri,
        )
        .with_mime_type(archive.format.mime_type());
        candidate
            .resource(&uri)
            .name(name)
            .description(&archive.description)
            .mime_type(archive.format.mime_type())
            .content_handler(move |_, _, _| {
                let contents = contents.clone();
                async move { Ok(vec![contents]) }
            })?;
        let state = Arc::make_mut(&mut candidate.inner);
        state.skill_owners.insert(uri, archive.path.clone());
        state.skills.insert(archive.path, entry);
        *self = candidate;
        Ok(())
    }
    /// Remove a skill and all of its owned files or archive. Existing snapshots are preserved.
    pub fn remove_skill(&mut self, path: &str) -> bool {
        let path = path.trim_matches('/');
        if !self.inner.skills.contains_key(path) {
            return false;
        }
        let state = Arc::make_mut(&mut self.inner);
        state.skills.remove(path);
        let uris: Vec<_> = state
            .skill_owners
            .iter()
            .filter(|(_, owner)| owner.as_str() == path)
            .map(|(uri, _)| uri.clone())
            .collect();
        for uri in uris {
            state.resources.remove(&uri);
            state.skill_owners.remove(&uri);
        }
        if state.skills.is_empty() {
            state.resources.remove(crate::skills::SKILL_INDEX_URI);
        }
        true
    }
    fn skill_index_contents(&self, ctx: &Context) -> Result<Vec<ResourceContents>, ErrorData> {
        use crate::skills::*;
        let mut skills = Vec::new();
        for entry in self.inner.skills.values() {
            if let Some(resource) = self.inner.resources.get(&entry.url)
                && self.resource_visible(ctx, resource, &entry.url)?
            {
                skills.push(entry.clone());
            }
        }
        let index = SkillIndex {
            schema: SKILL_INDEX_SCHEMA.into(),
            skills,
        };
        let text = serde_json::to_string(&index)
            .map_err(|_| ErrorData::internal_error("Unable to encode skill index", None))?;
        Ok(vec![
            ResourceContents::text(text, SKILL_INDEX_URI).with_mime_type("application/json"),
        ])
    }
    /// Remove a configured tool. Collected client results remain caller-owned.
    pub fn remove_tool(&mut self, name: &str) -> bool {
        Arc::make_mut(&mut self.inner).tools.remove(name).is_some()
    }
    /// Remove a resource. Removing any skill resource removes its entire skill;
    /// removing the generated skills index removes all registered skills and archives.
    pub fn remove_resource(&mut self, uri: &str) -> bool {
        if uri == crate::skills::SKILL_INDEX_URI && !self.inner.skills.is_empty() {
            let paths: Vec<_> = self.inner.skills.keys().cloned().collect();
            for path in paths {
                self.remove_skill(&path);
            }
            return true;
        }
        if let Some(path) = self.inner.skill_owners.get(uri).cloned() {
            return self.remove_skill(&path);
        }
        Arc::make_mut(&mut self.inner)
            .resources
            .remove(uri)
            .is_some()
    }
    /// Remove a prompt by name.
    pub fn remove_prompt(&mut self, name: &str) -> bool {
        Arc::make_mut(&mut self.inner)
            .prompts
            .remove(name)
            .is_some()
    }
    /// Convert this configuration into an atomically replaceable live registry.
    pub fn into_live(self) -> crate::LiveServer {
        crate::LiveServer::new(self)
    }
    fn find_resource(
        &self,
        uri: &str,
    ) -> Option<(&RegisteredResource, crate::resource::UriParameters)> {
        if let Some(resource) = self
            .inner
            .resources
            .get(uri)
            .filter(|resource| resource.matcher.is_none())
        {
            return Some((resource, Default::default()));
        }
        self.inner.resources.values().find_map(|resource| {
            resource
                .matcher
                .as_ref()?
                .parameters(uri)
                .map(|params| (resource, params))
        })
    }
    pub(crate) fn visible(
        &self,
        context: &Context,
        target: VisibilityTarget<'_>,
    ) -> Result<bool, ErrorData> {
        crate::visibility::allows(self.inner.visibility.as_ref(), context, target)
    }
    fn resource_visible(
        &self,
        context: &Context,
        resource: &RegisteredResource,
        uri: &str,
    ) -> Result<bool, ErrorData> {
        self.visible(
            context,
            VisibilityTarget::Resource {
                uri,
                registered_uri: &resource.definition.uri,
                name: &resource.definition.name,
            },
        )
    }
    fn completion_visible(
        &self,
        context: &Context,
        reference: &Reference,
    ) -> Result<(), ErrorData> {
        if self.inner.visibility.is_none() {
            return Ok(());
        }
        match reference {
            Reference::Prompt(prompt) => {
                if !self.inner.prompts.contains_key(&prompt.name)
                    || !self.visible(context, VisibilityTarget::Prompt { name: &prompt.name })?
                {
                    return Err(ErrorData::invalid_params("Unknown prompt", None));
                }
            }
            Reference::Resource(reference) => {
                let resource = self
                    .inner
                    .resources
                    .get(&reference.uri)
                    .or_else(|| {
                        self.find_resource(&reference.uri)
                            .map(|(resource, _)| resource)
                    })
                    .ok_or_else(resource_not_found)?;
                if !self.resource_visible(context, resource, &reference.uri)? {
                    return Err(resource_not_found());
                }
            }
            _ => {
                return Err(ErrorData::invalid_params(
                    "Unknown completion reference",
                    None,
                ));
            }
        }
        Ok(())
    }
    async fn invoke(&self, context: Context, arguments: Value, mut next: Next) -> HandlerResponse {
        for middleware in self.inner.middleware.iter().rev() {
            let middleware = middleware.clone();
            let downstream = next;
            next = Arc::new(move |ctx, args| middleware(ctx, args, downstream.clone()));
        }
        // Construct the future inside the unwind boundary, including synchronous
        // middleware/handler panics. Panic details never enter protocol responses.
        let cancellation = context.ct.clone();
        let request =
            std::panic::AssertUnwindSafe(async { next(context, arguments).await }).catch_unwind();
        let outcome = tokio::select! {
            biased;
            _ = cancellation.cancelled() => return Err(ErrorData::internal_error("Request cancelled", None)),
            result = tokio::time::timeout(self.inner.timeout, request) => result,
        };
        match outcome {
            Ok(Ok(result)) => result,
            Ok(Err(_)) => Err(ErrorData::internal_error("Handler panicked", None)),
            Err(_) => Err(ErrorData::internal_error("Handler deadline exceeded", None)),
        }
    }
}

impl ServerHandler for Server {
    fn get_info(&self) -> ServerConfig {
        let mut capabilities = if self.inner.tasks.is_some() {
            ServerCapabilities::builder().enable_tasks().build()
        } else {
            ServerCapabilities::default()
        };
        if !self.inner.skills.is_empty() {
            capabilities
                .extensions
                .get_or_insert_with(Default::default)
                .insert(
                    crate::skills::SKILLS_EXTENSION_ID.into(),
                    Default::default(),
                );
        }
        if !self.inner.tools.is_empty() {
            capabilities.tools = Some(Default::default());
        }
        if !self.inner.resources.is_empty() {
            capabilities.resources = Some(Default::default());
        }
        if !self.inner.prompts.is_empty() {
            capabilities.prompts = Some(Default::default());
        }
        if self.inner.completion.is_some() {
            capabilities.completions = Some(Default::default());
        }
        if self.inner.subscriptions.is_some() {
            capabilities
                .resources
                .get_or_insert_with(Default::default)
                .subscribe = Some(true);
            capabilities
                .resources
                .get_or_insert_with(Default::default)
                .list_changed = Some(true);
            capabilities
                .tools
                .get_or_insert_with(Default::default)
                .list_changed = Some(true);
            capabilities
                .prompts
                .get_or_insert_with(Default::default)
                .list_changed = Some(true);
        }
        let mut info =
            ServerConfig::new(capabilities).with_server_info(self.inner.identity.clone());
        info.protocol_version = ProtocolVersion::V_2026_07_28;
        info.instructions = self.inner.instructions.clone();
        info
    }
    async fn discover(&self, _: Context) -> Result<DiscoverResult, ErrorData> {
        self.discovery()
    }
    fn supported_protocol_versions(&self) -> std::borrow::Cow<'static, [ProtocolVersion]> {
        std::borrow::Cow::Borrowed(&[ProtocolVersion::V_2026_07_28])
    }
    async fn initialize(
        &self,
        _: InitializeRequestParams,
        _: Context,
    ) -> Result<InitializeResult, ErrorData> {
        Err(ErrorData::invalid_request(
            "Use server/discover with MCP 2026-07-28",
            None,
        ))
    }
    fn accepted_subscription_filter(
        &self,
        requested: &SubscriptionFilter,
    ) -> Option<SubscriptionFilter> {
        let hub = self.inner.subscriptions.as_ref()?;
        if !hub.valid_request(requested) {
            return Some(SubscriptionFilter::new());
        }
        // This SDK callback has no request context. With a policy installed,
        // acknowledge valid requested URIs uniformly so known hidden resources
        // cannot be distinguished from missing ones before admission.
        if self.inner.visibility.is_some() {
            return Some(requested.clone());
        }
        let mut accepted = requested.clone();
        accepted.resource_subscriptions = requested.resource_subscriptions.as_ref().map(|uris| {
            uris.iter()
                .filter(|uri| self.find_resource(uri).is_some())
                .cloned()
                .collect()
        });
        Some(accepted)
    }
    async fn listen(
        &self,
        context: mcp_protocol::service::SubscriptionContext,
    ) -> Result<(), ErrorData> {
        let hub = self
            .inner
            .subscriptions
            .as_ref()
            .ok_or_else(ErrorData::method_not_found::<SubscriptionsListenRequestMethod>)?;
        if self.inner.visibility.is_none()
            && context
                .accepted()
                .resource_subscriptions
                .as_ref()
                .is_some_and(|uris| uris.iter().any(|uri| self.find_resource(uri).is_none()))
        {
            return Err(ErrorData::invalid_params(
                "Resource registry changed during subscription admission",
                None,
            ));
        }
        if !hub.valid_request(context.requested()) {
            return Err(ErrorData::invalid_params(
                "Subscription filter exceeds limits",
                None,
            ));
        }
        for uri in context
            .accepted()
            .resource_subscriptions
            .as_deref()
            .unwrap_or_default()
        {
            let (resource, _) = self.find_resource(uri).ok_or_else(resource_not_found)?;
            if !self.resource_visible(context.request_context(), resource, uri)? {
                return Err(resource_not_found());
            }
        }
        let mut request = context.request_context().clone();
        request
            .extensions
            .insert(SubscriptionsListenRequestParams::new(
                context.requested().clone(),
            ));
        let args = serde_json::to_value(context.accepted())
            .map_err(|_| ErrorData::invalid_params("Invalid subscription filter", None))?;
        let admission: Next =
            Arc::new(|_, _| async { Ok(Response::SubscriptionAdmission) }.boxed());
        if !matches!(
            self.invoke(request, args, admission).await?,
            Response::SubscriptionAdmission
        ) {
            return Err(invalid_middleware_response());
        }
        hub.listen(context, self.inner.tasks.as_ref()).await
    }
    async fn on_custom_request(
        &self,
        request: CustomRequest,
        _: Context,
    ) -> Result<CustomResult, ErrorData> {
        // The SDK falls back to CustomRequest when typed parameter decoding
        // fails. Keep malformed built-in requests distinct from unknown methods.
        if matches!(request.method.as_str(), "prompts/get" | "resources/read") {
            return Err(ErrorData::invalid_params(
                "Invalid prompt or resource request parameters",
                None,
            ));
        }
        // Task methods exist when tasks are installed, so report the
        // malformed parameters rather than treating the method as unknown.
        if self.inner.tasks.is_some()
            && matches!(
                request.method.as_str(),
                "tasks/get" | "tasks/update" | "tasks/cancel"
            )
        {
            return Err(ErrorData::invalid_params(
                "Invalid task request parameters",
                None,
            ));
        }
        Err(ErrorData::new(
            ErrorCode::METHOD_NOT_FOUND,
            request.method,
            None,
        ))
    }
    async fn get_task(
        &self,
        request: GetTaskParams,
        _: Context,
    ) -> Result<GetTaskResult, ErrorData> {
        let tasks = self
            .inner
            .tasks
            .as_ref()
            .ok_or_else(ErrorData::method_not_found::<GetTaskMethod>)?;
        Ok(GetTaskResult::new(tasks.get_task(&request.task_id)?))
    }
    async fn update_task(&self, request: UpdateTaskParams, _: Context) -> Result<(), ErrorData> {
        self.inner
            .tasks
            .as_ref()
            .ok_or_else(ErrorData::method_not_found::<UpdateTaskMethod>)?
            .update_task(&request.task_id, request.input_responses)
    }
    async fn cancel_task(&self, request: CancelTaskParams, _: Context) -> Result<(), ErrorData> {
        self.inner
            .tasks
            .as_ref()
            .ok_or_else(ErrorData::method_not_found::<CancelTaskMethod>)?
            .cancel_task(&request.task_id)
    }
    async fn complete(
        &self,
        request: CompleteRequestParams,
        context: Context,
    ) -> Result<CompleteResult, ErrorData> {
        let next = self
            .inner
            .completion
            .clone()
            .ok_or_else(ErrorData::method_not_found::<CompleteRequestMethod>)?;
        self.completion_visible(&context, &request.r#ref)?;
        let server = self.clone();
        let guarded: Next = Arc::new(move |ctx, args| {
            let server = server.clone();
            let next = next.clone();
            async move {
                let request: CompleteRequestParams = serde_json::from_value(args.clone())
                    .map_err(|_| ErrorData::invalid_params("Invalid completion request", None))?;
                server.completion_visible(&ctx, &request.r#ref)?;
                next(ctx, args).await
            }
            .boxed()
        });
        let args = serde_json::to_value(request)
            .map_err(|_| ErrorData::invalid_params("Invalid completion request", None))?;
        match self.invoke(context, args, guarded).await? {
            Response::Completion(result) => Ok(result),
            _ => Err(invalid_middleware_response()),
        }
    }
    fn get_tool(&self, name: &str) -> Option<Tool> {
        self.inner
            .tools
            .get(name)
            .map(|tool| tool.definition.clone())
    }
    async fn list_tools(
        &self,
        request: Option<ListToolsRequestParams>,
        context: Context,
    ) -> Result<ListToolsResult, ErrorData> {
        let params = request.unwrap_or_default();
        let page = self.discover_tools(
            &context,
            &crate::discovery::ToolDiscoveryParams {
                cursor: params.cursor,
                group: params.group,
                tags: params.tags,
                detail: params.detail,
            },
        )?;
        let mut result = ListToolsResult::with_all_items(page.tools);
        result.next_cursor = page.next_cursor;
        self.inner
            .result_cache
            .fill_missing(&mut result.ttl_ms, &mut result.cache_scope);
        Ok(result)
    }
    async fn call_tool(
        &self,
        request: CallToolRequestParams,
        mut ctx: Context,
    ) -> Result<CallToolResponse, ErrorData> {
        ctx.extensions.insert(request.clone());
        if let Some(tasks) = &self.inner.tasks {
            ctx.extensions.insert(tasks.clone());
        }
        let tool = self
            .inner
            .tools
            .get(request.name.as_ref())
            .ok_or_else(|| ErrorData::invalid_params("Unknown tool", None))?;
        if !self.visible(
            &ctx,
            VisibilityTarget::Tool {
                name: request.name.as_ref(),
            },
        )? {
            return Err(ErrorData::invalid_params("Unknown tool", None));
        }
        let value = self
            .invoke(
                ctx,
                Value::Object(request.arguments.unwrap_or_default()),
                tool.handler.clone(),
            )
            .await?;
        match value {
            Response::Tool(result) => Ok(result.into()),
            Response::Protocol(result) => Ok(*result),
            _ => Err(invalid_middleware_response()),
        }
    }
    async fn list_resources(
        &self,
        request: Option<PaginatedRequestParams>,
        context: Context,
    ) -> Result<ListResourcesResult, ErrorData> {
        let mut items = Vec::new();
        for resource in self
            .inner
            .resources
            .values()
            .filter(|resource| resource.template.is_none())
        {
            if self.resource_visible(&context, resource, &resource.definition.uri)? {
                items.push(resource.definition.clone());
            }
        }
        let (items, next) = paginate(items, request, |item| item.uri.as_str())?;
        let mut result = ListResourcesResult::with_all_items(items);
        result.next_cursor = next;
        self.inner
            .result_cache
            .fill_missing(&mut result.ttl_ms, &mut result.cache_scope);
        Ok(result)
    }
    async fn list_resource_templates(
        &self,
        request: Option<PaginatedRequestParams>,
        context: Context,
    ) -> Result<ListResourceTemplatesResult, ErrorData> {
        let mut items = Vec::new();
        for resource in self.inner.resources.values() {
            if let Some(template) = &resource.template
                && self.resource_visible(&context, resource, &resource.definition.uri)?
            {
                items.push(template.clone());
            }
        }
        let (items, next) = paginate(items, request, |item| item.uri_template.as_str())?;
        let mut result = ListResourceTemplatesResult::with_all_items(items);
        result.next_cursor = next;
        self.inner
            .result_cache
            .fill_missing(&mut result.ttl_ms, &mut result.cache_scope);
        Ok(result)
    }
    async fn read_resource(
        &self,
        request: ReadResourceRequestParams,
        mut ctx: Context,
    ) -> Result<ReadResourceResponse, ErrorData> {
        ctx.extensions.insert(request.clone());
        let (resource, _) = self
            .find_resource(&request.uri)
            .ok_or_else(resource_not_found)?;
        if !self.resource_visible(&ctx, resource, &request.uri)? {
            return Err(resource_not_found());
        }
        let server = self.clone();
        let definition = resource.definition.clone();
        let handler = resource.handler.clone();
        let matcher = resource.matcher.clone();
        let exact_uri = resource.definition.uri.clone();
        let next: Next = Arc::new(move |ctx, args| {
            let handler = handler.clone();
            let matcher = matcher.clone();
            let exact_uri = exact_uri.clone();
            let definition = definition.clone();
            let server = server.clone();
            async move {
                let uri = args["uri"]
                    .as_str()
                    .ok_or_else(|| ErrorData::invalid_params("Missing URI", None))?
                    .to_owned();
                let parameters = match matcher {
                    Some(matcher) => matcher.parameters(&uri).ok_or_else(|| {
                        ErrorData::invalid_params("URI no longer matches resource", None)
                    })?,
                    None if uri == exact_uri => Default::default(),
                    None => {
                        return Err(ErrorData::invalid_params(
                            "URI no longer matches resource",
                            None,
                        ));
                    }
                };
                if !server.visible(
                    &ctx,
                    VisibilityTarget::Resource {
                        uri: &uri,
                        registered_uri: &definition.uri,
                        name: &definition.name,
                    },
                )? {
                    return Err(resource_not_found());
                }
                let contents =
                    if uri == crate::skills::SKILL_INDEX_URI && !server.inner.skills.is_empty() {
                        ReadResourceResult::new(server.skill_index_contents(&ctx)?).into()
                    } else {
                        handler(ctx, uri, parameters).await?
                    };
                Ok(match contents {
                    ReadResourceResponse::Complete(result) => Response::Resource(result),
                    response => Response::ResourceProtocol(Box::new(response)),
                })
            }
            .boxed()
        });
        let result = self.invoke(ctx, json!({"uri":request.uri}), next).await?;
        let mut response = match result {
            Response::Resource(result) => result.into(),
            Response::ResourceProtocol(response) => *response,
            _ => return Err(invalid_middleware_response()),
        };
        if let ReadResourceResponse::Complete(result) = &mut response
            && result
                .result_type
                .as_ref()
                .is_none_or(ResultType::is_complete)
        {
            self.inner
                .result_cache
                .fill_missing(&mut result.ttl_ms, &mut result.cache_scope);
        }
        Ok(response)
    }

    async fn list_prompts(
        &self,
        request: Option<PaginatedRequestParams>,
        context: Context,
    ) -> Result<ListPromptsResult, ErrorData> {
        let mut items = Vec::new();
        for prompt in self.inner.prompts.values() {
            if self.visible(
                &context,
                VisibilityTarget::Prompt {
                    name: &prompt.definition.name,
                },
            )? {
                items.push(prompt.definition.clone());
            }
        }
        let (items, next) = paginate(items, request, |item| item.name.as_str())?;
        let mut result = ListPromptsResult::with_all_items(items);
        result.next_cursor = next;
        self.inner
            .result_cache
            .fill_missing(&mut result.ttl_ms, &mut result.cache_scope);
        Ok(result)
    }
    async fn get_prompt(
        &self,
        request: GetPromptRequestParams,
        mut ctx: Context,
    ) -> Result<GetPromptResponse, ErrorData> {
        ctx.extensions.insert(request.clone());
        let prompt = self
            .inner
            .prompts
            .get(&request.name)
            .ok_or_else(|| ErrorData::invalid_params("Unknown prompt", None))?;
        if !self.visible(
            &ctx,
            VisibilityTarget::Prompt {
                name: &request.name,
            },
        )? {
            return Err(ErrorData::invalid_params("Unknown prompt", None));
        }
        let args: BTreeMap<String, String> =
            serde_json::from_value(Value::Object(request.arguments.unwrap_or_default()))
                .map_err(|_| ErrorData::invalid_params("Prompt arguments must be strings", None))?;
        for arg in prompt.definition.arguments.as_deref().unwrap_or_default() {
            if arg.required == Some(true) && !args.contains_key(&arg.name) {
                return Err(ErrorData::invalid_params(
                    format!("Missing prompt argument: {}", arg.name),
                    None,
                ));
            }
        }
        let handler = prompt.handler.clone();
        let next: Next = Arc::new(move |ctx, args| {
            let handler = handler.clone();
            async move {
                let args = serde_json::from_value(args)
                    .map_err(|_| ErrorData::invalid_params("Invalid prompt arguments", None))?;
                handler(ctx, args).await.map(|response| match response {
                    GetPromptResponse::Complete(result) => Response::Prompt(result),
                    response => Response::PromptProtocol(Box::new(response)),
                })
            }
            .boxed()
        });
        let result = self.invoke(ctx, json!(args), next).await?;
        match result {
            Response::Prompt(result) => Ok(result.into()),
            Response::PromptProtocol(response) => Ok(*response),
            _ => Err(invalid_middleware_response()),
        }
    }
}

fn invalid_middleware_response() -> ErrorData {
    ErrorData::internal_error("Middleware returned the wrong response variant", None)
}

impl std::fmt::Debug for Server {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Server")
            .field("name", &self.inner.identity.name)
            .field("version", &self.inner.identity.version)
            .field("tools", &self.inner.tools.len())
            .field("resources", &self.inner.resources.len())
            .field("prompts", &self.inner.prompts.len())
            .field("timeout", &self.inner.timeout)
            .finish_non_exhaustive()
    }
}

pub(crate) fn paginate<T>(
    items: Vec<T>,
    request: Option<PaginatedRequestParams>,
    key: impl Fn(&T) -> &str,
) -> Result<(Vec<T>, Option<String>), ErrorData> {
    let cursor = request
        .and_then(|request| request.cursor)
        .filter(|cursor| !cursor.is_empty());
    let start = match cursor {
        Some(cursor) => items
            .iter()
            .position(|item| key(item) == cursor)
            .map(|index| index + 1)
            .ok_or_else(|| ErrorData::invalid_params("Invalid cursor", None))?,
        None => 0,
    };
    let remaining = items.len().saturating_sub(start);
    let page = items.into_iter().skip(start).take(100).collect::<Vec<_>>();
    let next = if remaining > 100 {
        page.last().map(|item| key(item).to_owned())
    } else {
        None
    };
    Ok((page, next))
}

fn resource_not_found() -> ErrorData {
    ErrorData::invalid_params("Resource not found", None)
}

fn nonempty(value: String) -> Option<String> {
    (!value.is_empty()).then_some(value)
}
