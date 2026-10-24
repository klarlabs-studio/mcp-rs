// Includes validation derived from rmcp 3.5.1; see the repository UPSTREAM.md.
//! Feature-independent validation of tool routing annotations.
use std::collections::HashSet;

use serde_json::Value;

use super::JsonObject;

/// Validate `x-mcp-header` annotations in a tool input schema.
///
/// Annotations are supported only on direct top-level properties with an explicit
/// `string`, `integer` or `boolean` type. Names must be nonempty HTTP tokens and
/// case-insensitively unique. Annotations in nested schemas, definitions and
/// combinators are rejected, rather than silently omitted from routing headers.
/// Schema-valued keywords are inspected without following references; arbitrary
/// data in examples, defaults, constants and extension metadata is not inspected.
/// This does not validate the rest of the JSON Schema.
///
/// # Errors
/// Returns the schema location and reason for an unsupported annotation.
pub fn validate_tool_header_annotations(input_schema: &JsonObject) -> Result<(), String> {
    let mut pending = vec![(input_schema, "$".to_owned(), false)];
    let mut seen = HashSet::new();
    while let Some((schema, path, direct_property)) = pending.pop() {
        if let Some(raw) = schema.get("x-mcp-header") {
            if !direct_property {
                return Err(format!(
                    "{path}: x-mcp-header is supported only on direct top-level properties"
                ));
            }
            let Value::String(header) = raw else {
                return Err(format!("{path}: x-mcp-header must be a string"));
            };
            if header.is_empty() || !header.chars().all(is_tchar) {
                return Err(format!(
                    "{path}: x-mcp-header must be a nonempty HTTP token"
                ));
            }
            if !seen.insert(header.to_ascii_lowercase()) {
                return Err(format!(
                    "{path}: duplicate x-mcp-header `{header}` (case-insensitive)"
                ));
            }
            if !matches!(
                schema.get("type").and_then(Value::as_str),
                Some("string" | "integer" | "boolean")
            ) {
                return Err(format!(
                    "{path}: x-mcp-header requires an explicit string/integer/boolean type"
                ));
            }
        }
        // Walk schema positions only. Never interpret application data as schema.
        for keyword in [
            "properties",
            "patternProperties",
            "$defs",
            "definitions",
            "dependentSchemas",
        ] {
            if let Some(Value::Object(children)) = schema.get(keyword) {
                for (key, child) in children {
                    if let Value::Object(child) = child {
                        let key = key.replace('~', "~0").replace('/', "~1");
                        pending.push((
                            child,
                            format!("{path}/{keyword}/{key}"),
                            path == "$" && keyword == "properties",
                        ));
                    }
                }
            }
        }
        for keyword in ["allOf", "anyOf", "oneOf", "prefixItems"] {
            if let Some(Value::Array(children)) = schema.get(keyword) {
                for (index, child) in children.iter().enumerate() {
                    if let Value::Object(child) = child {
                        pending.push((child, format!("{path}/{keyword}/{index}"), false));
                    }
                }
            }
        }
        for keyword in [
            "items",
            "contains",
            "additionalProperties",
            "unevaluatedProperties",
            "propertyNames",
            "unevaluatedItems",
            "not",
            "if",
            "then",
            "else",
            "contentSchema",
        ] {
            if let Some(Value::Object(child)) = schema.get(keyword) {
                pending.push((child, format!("{path}/{keyword}"), false));
            }
        }
    }
    Ok(())
}

// RFC 9110 §5.6.2 token characters.
fn is_tchar(c: char) -> bool {
    c.is_ascii_alphanumeric()
        || matches!(
            c,
            '!' | '#'
                | '$'
                | '%'
                | '&'
                | '\''
                | '*'
                | '+'
                | '-'
                | '.'
                | '^'
                | '_'
                | '`'
                | '|'
                | '~'
        )
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn check(schema: Value) -> Result<(), String> {
        validate_tool_header_annotations(schema.as_object().unwrap())
    }
    fn property(schema: Value) -> Value {
        json!({"type":"object", "properties":{"value":schema}})
    }

    #[test]
    fn accepts_all_http_token_characters_and_supported_primitive_types() {
        assert!(
            check(json!({"type":"object", "properties":{
                "s":{"type":"string", "x-mcp-header":"Az09!#$%&'*+-.^_`|~"},
                "i":{"type":"integer", "x-mcp-header":"Count"},
                "b":{"type":"boolean", "x-mcp-header":"Enabled"}
            }}))
            .is_ok()
        );
    }

    #[test]
    fn rejects_invalid_annotation_names_values_and_types() {
        for name in [
            json!(""),
            json!("bad:name"),
            json!("bad name"),
            json!("café"),
            json!(4),
            Value::Null,
        ] {
            assert!(
                check(property(json!({"type":"string", "x-mcp-header":name}))).is_err(),
                "{name}"
            );
        }
        for kind in [
            json!("number"),
            json!("object"),
            json!("array"),
            json!(["string", "null"]),
            Value::Null,
        ] {
            assert!(
                check(property(json!({"type":kind, "x-mcp-header":"Value"}))).is_err(),
                "{kind}"
            );
        }
        assert!(check(property(json!({"x-mcp-header":"Value"}))).is_err());
    }

    #[test]
    fn detects_case_insensitive_collisions() {
        assert!(
            check(json!({"properties":{
                "first":{"type":"string", "x-mcp-header":"Region"},
                "second":{"type":"string", "x-mcp-header":"rEgIoN"}
            }}))
            .is_err()
        );
    }

    #[test]
    fn rejects_annotations_in_schema_positions_without_following_references() {
        let leaf = json!({"type":"string", "x-mcp-header":"Value"});
        assert!(check(leaf.clone()).is_err());
        for keyword in [
            "properties",
            "patternProperties",
            "$defs",
            "definitions",
            "dependentSchemas",
        ] {
            let mut child = json!({"type":"object"});
            child[keyword] = json!({"child":leaf});
            assert!(check(property(child)).is_err(), "{keyword}");
        }
        for keyword in ["allOf", "anyOf", "oneOf", "prefixItems"] {
            let mut schema = json!({"type":"object"});
            schema[keyword] = json!([leaf]);
            assert!(check(schema).is_err(), "{keyword}");
        }
        for keyword in [
            "items",
            "contains",
            "additionalProperties",
            "unevaluatedProperties",
            "propertyNames",
            "unevaluatedItems",
            "not",
            "if",
            "then",
            "else",
            "contentSchema",
        ] {
            let mut schema = json!({"type":"object"});
            schema[keyword] = leaf.clone();
            assert!(check(schema).is_err(), "{keyword}");
        }
        assert!(
            check(json!({"$defs":{"value":leaf}, "properties":{"value":{"$ref":"#/$defs/value"}}}))
                .is_err()
        );
    }

    #[test]
    fn ignores_application_data_and_unannotated_recursive_schemas() {
        let data = json!({"x-mcp-header":false,"properties":{"x":{"x-mcp-header":"ignored"}}});
        assert!(
            check(json!({"type":"object", "examples":[data], "default":data,
                "const":data, "enum":[data], "x-custom":data,
                "properties":{"value":{"type":"object", "default":data}},
                "$defs":{"node":{"type":"object", "properties":{"next":{"$ref":"#/$defs/node"}}}}
            }))
            .is_ok()
        );
    }

    #[test]
    fn error_locations_escape_json_pointer_property_names() {
        let error =
            check(json!({"properties":{"a~/b":{"type":"string", "x-mcp-header":""}}})).unwrap_err();
        assert!(error.starts_with("$/properties/a~0~1b:"), "{error}");
    }
}
