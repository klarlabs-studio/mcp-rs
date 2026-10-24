//! Progressive tool discovery shared by host catalogs and standard `tools/list`.

use crate::{Context, LiveServer, Server, VisibilityTarget, model::*, server::paginate};
use mcp_protocol::ErrorData;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Default)]
pub(crate) struct DiscoveryOptions {
    pub(crate) group: String,
    pub(crate) tags: Vec<String>,
    pub(crate) defer_schema: bool,
}

pub use crate::model::{ListedTool as DiscoveredTool, ToolListDetail as ToolDiscoveryDetail};

/// Selection for a host-owned catalog; filters apply before pagination.
#[derive(Debug, Clone, Default, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct ToolDiscoveryParams {
    /// Last tool name from the previous page in the same filtered catalog.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub cursor: Option<String>,
    /// Exact group to select. An empty string selects all groups.
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub group: String,
    /// Every requested tag must be present; matching is case-sensitive.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub tags: Vec<String>,
    /// Schema selection; omit to respect per-tool deferral.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub detail: Option<ToolDiscoveryDetail>,
}

/// A stable, name-ordered catalog page of at most 100 tools.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ToolDiscoveryResult {
    /// Matching, visible tools.
    pub tools: Vec<DiscoveredTool>,
    /// Pass with the same filters to retrieve another page.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub next_cursor: Option<String>,
}

impl Server {
    /// Select a host-owned tool catalog from this configuration snapshot.
    ///
    /// Applies the ordinary visibility policy using the supplied request context.
    /// Groups/tags are discovery hints, never authorization. Calling tools still
    /// uses their original schemas. The same selection logic serves standard `tools/list`.
    ///
    /// # Errors
    /// Returns an error if visibility evaluation fails or the cursor is not in
    /// the currently visible, filtered catalog. Keep filters and snapshot stable
    /// when requesting subsequent pages.
    pub fn discover_tools(
        &self,
        context: &Context,
        params: &ToolDiscoveryParams,
    ) -> Result<ToolDiscoveryResult, ErrorData> {
        let mut tools = Vec::new();
        for registered in self.inner.tools.values() {
            let options = &registered.discovery;
            if !self.visible(
                context,
                VisibilityTarget::Tool {
                    name: &registered.definition.name,
                },
            )? || (!params.group.is_empty() && options.group != params.group)
                || !params.tags.iter().all(|tag| options.tags.contains(tag))
            {
                continue;
            }
            let tool = registered.definition.clone();
            let schemas = match params.detail {
                Some(ToolDiscoveryDetail::Full) => true,
                Some(ToolDiscoveryDetail::Names) => false,
                None => !options.defer_schema,
            };
            tools.push(DiscoveredTool {
                name: tool.name,
                title: tool.title,
                description: tool.description,
                input_schema: schemas.then_some(tool.input_schema),
                output_schema: if schemas { tool.output_schema } else { None },
                annotations: tool.annotations,
                icons: tool.icons,
                meta: tool.meta,
                group: options.group.clone(),
                tags: options.tags.clone(),
            });
        }
        let request = PaginatedRequestParams::default().with_cursor(params.cursor.clone());
        let (tools, next_cursor) = paginate(tools, Some(request), |tool| tool.name.as_ref())?;
        Ok(ToolDiscoveryResult { tools, next_cursor })
    }
}

impl LiveServer {
    /// Select a host-owned tool catalog from the current runtime configuration.
    ///
    /// Each call uses one atomic snapshot, including its visibility policy. Registry
    /// changes are visible on the next call; retain [`Self::snapshot`] and call
    /// [`Server::discover_tools`] on it when paging through a stable configuration.
    /// The same selection logic serves standard `tools/list`.
    ///
    /// # Errors
    /// Returns the same visibility and cursor errors as [`Server::discover_tools`].
    pub fn discover_tools(
        &self,
        context: &Context,
        params: &ToolDiscoveryParams,
    ) -> Result<ToolDiscoveryResult, ErrorData> {
        self.snapshot().discover_tools(context, params)
    }
}
