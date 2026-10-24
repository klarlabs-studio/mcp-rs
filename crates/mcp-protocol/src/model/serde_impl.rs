// Derived from rmcp 3.5.1; maintained and modified by mcp-rs. See ../../../UPSTREAM.md.
use std::borrow::Cow;

use serde::{Deserialize, Serialize};

use super::{
    CustomNotification, CustomRequest, Extensions, JsonObject, MetaObject, Notification,
    NotificationMetaObject, NotificationNoParam, Request, RequestMetaObject, RequestNoParam,
    RequestOptionalParam,
};

/// Float deserializers that also accept the number map serde_json's
/// `arbitrary_precision` feature produces for buffered (untagged/flattened) values.
/// Like JSON itself, they reject NaN and infinities.
pub(crate) mod json_float {
    use serde::{Deserialize, Deserializer, de::Error};
    use serde_json::Number;

    fn to_f64<E: Error>(number: Number) -> Result<f64, E> {
        // `None` only for a value beyond `f64`, which serde_json also rejects
        // without the feature.
        number
            .as_f64()
            .ok_or_else(|| E::custom(format_args!("number out of range: {number}")))
    }

    pub(crate) fn f64<'de, D: Deserializer<'de>>(deserializer: D) -> Result<f64, D::Error> {
        to_f64(Number::deserialize(deserializer)?)
    }

    pub(crate) fn option_f64<'de, D: Deserializer<'de>>(
        deserializer: D,
    ) -> Result<Option<f64>, D::Error> {
        Option::<Number>::deserialize(deserializer)?
            .map(to_f64)
            .transpose()
    }

    pub(crate) fn option_f32<'de, D: Deserializer<'de>>(
        deserializer: D,
    ) -> Result<Option<f32>, D::Error> {
        // serde_json also reads an `f32` field as `f64` and narrows it with `as`.
        Ok(option_f64(deserializer)?.map(|value| value as f32))
    }
}

/// Wire-side view of `params`: the `_meta` map plus the remaining fields.
///
/// All metadata types are transparent wrappers over [`JsonObject`], so the
/// serde plumbing works on the raw map; call sites wrap/unwrap the typed
/// metadata ([`RequestMetaObject`] / [`NotificationMetaObject`]).
#[derive(Deserialize)]
struct WithMeta<'a, P> {
    _meta: Option<Cow<'a, JsonObject>>,
    #[serde(flatten)]
    _rest: P,
}

impl<P: Serialize> Serialize for WithMeta<'_, P> {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::SerializeMap;

        // Serialize _rest to a Value so we can inspect and strip any duplicate _meta
        let mut rest_value =
            serde_json::to_value(&self._rest).map_err(serde::ser::Error::custom)?;

        // Extract _meta from the serialized params (if it's an object containing one)
        let params_meta: Option<JsonObject> = rest_value
            .as_object_mut()
            .and_then(|obj| obj.remove("_meta"))
            .and_then(|v| serde_json::from_value(v).ok());

        // Merge: params-level _meta as base, extensions-level _meta overwrites on conflict
        let merged_meta = match (self._meta.as_deref(), params_meta) {
            (Some(ext_meta), Some(mut params_meta)) => {
                params_meta.extend(ext_meta.clone());
                Some(params_meta)
            }
            (Some(ext_meta), None) => Some(ext_meta.clone()),
            (None, Some(params_meta)) => Some(params_meta),
            (None, None) => None,
        };

        // Serialize as a flat map: single _meta + remaining params fields
        let rest_obj = match rest_value {
            serde_json::Value::Object(map) => map,
            _ => serde_json::Map::new(),
        };
        let meta_count = usize::from(merged_meta.is_some());
        let mut map = serializer.serialize_map(Some(rest_obj.len() + meta_count))?;

        if let Some(meta) = &merged_meta {
            map.serialize_entry("_meta", meta)?;
        }

        for (k, v) in &rest_obj {
            map.serialize_entry(k, v)?;
        }

        map.end()
    }
}

#[derive(Serialize, Deserialize)]
struct Proxy<'a, M, P> {
    method: M,
    params: WithMeta<'a, P>,
}

#[derive(Serialize, Deserialize)]
struct ProxyOptionalParam<'a, M, P> {
    method: M,
    params: Option<WithMeta<'a, P>>,
}

#[derive(Serialize, Deserialize)]
struct ProxyNoParam<M> {
    method: M,
}

/// Combine the message-specific `_meta` map with a legacy [`MetaObject`]
/// extension so metadata stored in [`Extensions`] is not lost on the wire.
/// On key conflicts the message-specific map wins.
fn merge_legacy_meta<'a>(
    typed: Option<&'a JsonObject>,
    extensions: &'a Extensions,
) -> Option<Cow<'a, JsonObject>> {
    let legacy = extensions.get::<MetaObject>().map(|meta| &meta.0);
    match (typed, legacy) {
        (Some(typed), None) => Some(Cow::Borrowed(typed)),
        (None, Some(legacy)) => Some(Cow::Borrowed(legacy)),
        (Some(typed), Some(legacy)) => {
            let mut merged = legacy.clone();
            merged.extend(
                typed
                    .iter()
                    .map(|(key, value)| (key.clone(), value.clone())),
            );
            Some(Cow::Owned(merged))
        }
        (None, None) => None,
    }
}

/// Borrow the request `_meta` map from extensions, if any.
fn request_meta(extensions: &Extensions) -> Option<Cow<'_, JsonObject>> {
    let typed = extensions.get::<RequestMetaObject>().map(|meta| &meta.0.0);
    merge_legacy_meta(typed, extensions)
}

/// Borrow the notification `_meta` map from extensions, if any.
fn notification_meta(extensions: &Extensions) -> Option<Cow<'_, JsonObject>> {
    let typed = extensions
        .get::<NotificationMetaObject>()
        .map(|meta| &meta.0.0);
    merge_legacy_meta(typed, extensions)
}

/// Build extensions holding a typed metadata map deserialized from `params._meta`.
fn extensions_with_meta<T>(meta: Option<Cow<'_, JsonObject>>) -> Extensions
where
    T: From<JsonObject> + Clone + Send + Sync + 'static,
{
    let mut extensions = Extensions::new();
    if let Some(meta) = meta {
        extensions.insert(T::from(meta.into_owned()));
    }
    extensions
}

impl<M, R> Serialize for Request<M, R>
where
    M: Serialize,
    R: Serialize,
{
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        Proxy::serialize(
            &Proxy {
                method: &self.method,
                params: WithMeta {
                    _rest: &self.params,
                    _meta: request_meta(&self.extensions),
                },
            },
            serializer,
        )
    }
}

impl<'de, M, R> Deserialize<'de> for Request<M, R>
where
    M: Deserialize<'de>,
    R: Deserialize<'de>,
{
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        let body = Proxy::deserialize(deserializer)?;
        Ok(Request {
            extensions: extensions_with_meta::<RequestMetaObject>(body.params._meta),
            method: body.method,
            params: body.params._rest,
        })
    }
}

impl<M, R> Serialize for RequestOptionalParam<M, R>
where
    M: Serialize,
    R: Serialize,
{
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        Proxy::serialize(
            &Proxy {
                method: &self.method,
                params: WithMeta {
                    _rest: &self.params,
                    _meta: request_meta(&self.extensions),
                },
            },
            serializer,
        )
    }
}

impl<'de, M, R> Deserialize<'de> for RequestOptionalParam<M, R>
where
    M: Deserialize<'de>,
    R: Deserialize<'de>,
{
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        // Decode the flattened remainder as a map first: flattening Option<R>
        // swallows malformed objects, and flattening R bypasses its
        // deny_unknown_fields validation. Deserialize R normally after extracting
        // metadata so both invalid values and unknown keys are rejected.
        let body = ProxyOptionalParam::<'_, _, JsonObject>::deserialize(deserializer)?;
        let mut params = None;
        let mut _meta = None;
        if let Some(body_params) = body.params {
            params = Some(
                R::deserialize(serde_json::Value::Object(body_params._rest))
                    .map_err(serde::de::Error::custom)?,
            );
            _meta = body_params._meta;
        }
        Ok(RequestOptionalParam {
            extensions: extensions_with_meta::<RequestMetaObject>(_meta),
            method: body.method,
            params,
        })
    }
}

impl<M> Serialize for RequestNoParam<M>
where
    M: Serialize,
{
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        // Emit `params` only when metadata is present, so the wire shape of
        // meta-less requests stays `{"method": ...}`.
        match request_meta(&self.extensions) {
            Some(_meta) => Proxy::serialize(
                &Proxy {
                    method: &self.method,
                    params: WithMeta {
                        _meta: Some(_meta),
                        _rest: (),
                    },
                },
                serializer,
            ),
            None => ProxyNoParam::serialize(
                &ProxyNoParam {
                    method: &self.method,
                },
                serializer,
            ),
        }
    }
}

impl<'de, M> Deserialize<'de> for RequestNoParam<M>
where
    M: Deserialize<'de>,
{
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        let body = ProxyOptionalParam::<'_, _, Option<JsonObject>>::deserialize(deserializer)?;
        let _meta = body.params.and_then(|params| params._meta);
        Ok(RequestNoParam {
            extensions: extensions_with_meta::<RequestMetaObject>(_meta),
            method: body.method,
        })
    }
}

impl<M, R> Serialize for Notification<M, R>
where
    M: Serialize,
    R: Serialize,
{
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        Proxy::serialize(
            &Proxy {
                method: &self.method,
                params: WithMeta {
                    _rest: &self.params,
                    _meta: notification_meta(&self.extensions),
                },
            },
            serializer,
        )
    }
}

impl<'de, M, R> Deserialize<'de> for Notification<M, R>
where
    M: Deserialize<'de>,
    R: Deserialize<'de>,
{
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        let body = ProxyOptionalParam::<'_, _, R>::deserialize(deserializer)?;
        let (_meta, params) = match body.params {
            Some(with_meta) => (with_meta._meta, with_meta._rest),
            None => {
                // JSON-RPC 2.0: params is optional. Treat absent params as {}.
                let empty = serde_json::Value::Object(serde_json::Map::new());
                let r = R::deserialize(empty).map_err(serde::de::Error::custom)?;
                (None, r)
            }
        };
        Ok(Notification {
            extensions: extensions_with_meta::<NotificationMetaObject>(_meta),
            method: body.method,
            params,
        })
    }
}

impl<M> Serialize for NotificationNoParam<M>
where
    M: Serialize,
{
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        // Emit `params` only when metadata is present, so the wire shape of
        // meta-less notifications stays `{"method": ...}`.
        match notification_meta(&self.extensions) {
            Some(_meta) => Proxy::serialize(
                &Proxy {
                    method: &self.method,
                    params: WithMeta {
                        _meta: Some(_meta),
                        _rest: (),
                    },
                },
                serializer,
            ),
            None => ProxyNoParam::serialize(
                &ProxyNoParam {
                    method: &self.method,
                },
                serializer,
            ),
        }
    }
}

impl<'de, M> Deserialize<'de> for NotificationNoParam<M>
where
    M: Deserialize<'de>,
{
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        let body = ProxyOptionalParam::<'_, _, Option<JsonObject>>::deserialize(deserializer)?;
        let _meta = body.params.and_then(|params| params._meta);
        Ok(NotificationNoParam {
            extensions: extensions_with_meta::<NotificationMetaObject>(_meta),
            method: body.method,
        })
    }
}

impl Serialize for CustomRequest {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        let _meta = request_meta(&self.extensions);
        let params = self.params.as_ref();

        let params = if _meta.is_some() || params.is_some() {
            Some(WithMeta {
                _meta,
                _rest: &self.params,
            })
        } else {
            None
        };

        ProxyOptionalParam::serialize(
            &ProxyOptionalParam {
                method: &self.method,
                params,
            },
            serializer,
        )
    }
}

impl<'de> Deserialize<'de> for CustomRequest {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        let body =
            ProxyOptionalParam::<'_, _, Option<serde_json::Value>>::deserialize(deserializer)?;
        let mut params = None;
        let mut _meta = None;
        if let Some(body_params) = body.params {
            params = body_params._rest;
            _meta = body_params._meta;
        }
        Ok(CustomRequest {
            extensions: extensions_with_meta::<RequestMetaObject>(_meta),
            method: body.method,
            params,
        })
    }
}

impl Serialize for CustomNotification {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        let _meta = notification_meta(&self.extensions);
        let params = self.params.as_ref();

        let params = if _meta.is_some() || params.is_some() {
            Some(WithMeta {
                _meta,
                _rest: &self.params,
            })
        } else {
            None
        };

        ProxyOptionalParam::serialize(
            &ProxyOptionalParam {
                method: &self.method,
                params,
            },
            serializer,
        )
    }
}

impl<'de> Deserialize<'de> for CustomNotification {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        let body =
            ProxyOptionalParam::<'_, _, Option<serde_json::Value>>::deserialize(deserializer)?;
        let mut params = None;
        let mut _meta = None;
        if let Some(body_params) = body.params {
            params = body_params._rest;
            _meta = body_params._meta;
        }
        Ok(CustomNotification {
            extensions: extensions_with_meta::<NotificationMetaObject>(_meta),
            method: body.method,
            params,
        })
    }
}

#[cfg(test)]
mod progressive_discovery_tests {
    use crate::model::{ClientRequest, ListToolsRequest, ToolListDetail};
    use serde_json::json;

    #[test]
    fn optional_tool_parameters_preserve_filters_and_reject_malformed_objects() {
        let request: ListToolsRequest = serde_json::from_value(json!({
            "method":"tools/list", "params":{
                "_meta":{"trace":"test"}, "group":"research", "tags":["read"], "detail":"names"
            }
        }))
        .expect("valid query");
        let params = request.params.as_ref().expect("present parameters");
        assert_eq!(params.group, "research");
        assert_eq!(params.tags, ["read"]);
        assert_eq!(params.detail, Some(ToolListDetail::Names));
        let encoded = serde_json::to_value(request).expect("encode query");
        assert_eq!(encoded["params"]["_meta"]["trace"], "test");
        for params in [
            json!({"detail":"bad"}),
            json!({"tags":"read"}),
            json!({"unknown":true}),
            json!({"group":7}),
            json!(["read"]),
        ] {
            let wire = json!({"method":"tools/list", "params":params});
            assert!(
                serde_json::from_value::<ListToolsRequest>(wire.clone()).is_err(),
                "{wire}"
            );
            if params.is_object() {
                assert!(matches!(
                    serde_json::from_value::<ClientRequest>(wire).expect("custom fallback"),
                    ClientRequest::CustomRequest(_)
                ));
            }
        }
        let absent: ListToolsRequest =
            serde_json::from_value(json!({"method":"tools/list"})).expect("absent parameters");
        assert!(absent.params.is_none());
        let empty: ListToolsRequest =
            serde_json::from_value(json!({"method":"tools/list", "params":{}}))
                .expect("empty parameters");
        assert!(empty.params.is_some());
    }
}
