// Derived from rmcp 3.5.1; maintained and modified by mcp-rs. See ../../../UPSTREAM.md.
//! Type-safe schema definitions for MCP elicitation requests.
//!
//! This module provides strongly-typed schema definitions for elicitation requests
//! that comply with the MCP 2025-06-18 specification. Elicitation schemas must be
//! objects with primitive-typed properties.
//!
//! # Example
//!
//! ```rust
//! use mcp_protocol::model::*;
//!
//! let schema = ElicitationSchema::builder()
//!     .required_email("email")
//!     .required_integer("age", 0, 150)
//!     .optional_bool("newsletter", false)
//!     .build();
//! ```

use std::{borrow::Cow, collections::BTreeMap, marker::PhantomData};

use indexmap::IndexMap;
use serde::{Deserialize, Deserializer, Serialize};

use crate::{const_string, model::ConstString};

// =============================================================================
// CONST TYPES FOR JSON SCHEMA TYPE FIELD
// =============================================================================

const_string!(ObjectTypeConst = "object");
const_string!(StringTypeConst = "string");
const_string!(NumberTypeConst = "number");
const_string!(IntegerTypeConst = "integer");
const_string!(BooleanTypeConst = "boolean");
const_string!(EnumTypeConst = "string");
const_string!(ArrayTypeConst = "array");

// =============================================================================
// PRIMITIVE SCHEMA DEFINITIONS
// =============================================================================

/// Primitive schema definition for elicitation properties.
///
/// According to MCP 2025-06-18 specification, elicitation schemas must have
/// properties of primitive types only (string, number, integer, boolean, enum).
///
/// Note: Put Enum as the first variant to avoid ambiguity during deserialization.
/// This is due to the fact that EnumSchema can contain StringSchema internally and serde
/// uses first match wins strategy when deserializing untagged enums.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "schemars", derive(schemars::JsonSchema))]
#[serde(untagged)]
#[non_exhaustive]
pub enum PrimitiveSchemaDefinition {
    /// Enum property (explicit enum schema)
    Enum(EnumSchema),
    /// String property (with optional enum constraint)
    String(StringSchema),
    /// Number property (with optional enum constraint)
    Number(NumberSchema),
    /// Integer property (with optional enum constraint)
    Integer(IntegerSchema),
    /// Boolean property
    Boolean(BooleanSchema),
}

// =============================================================================
// STRING SCHEMA
// =============================================================================

/// String format types allowed by the MCP specification.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "schemars", derive(schemars::JsonSchema))]
#[serde(rename_all = "kebab-case")]
#[non_exhaustive]
pub enum StringFormat {
    /// Email address format
    Email,
    /// URI format
    Uri,
    /// Date format (YYYY-MM-DD)
    Date,
    /// Date-time format (ISO 8601)
    DateTime,
}

/// Schema definition for string properties.
///
/// Compliant with MCP 2025-06-18 specification for elicitation schemas.
/// Supports only the fields allowed by the MCP spec:
/// - format limited to: "email", "uri", "date", "date-time"
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "schemars", derive(schemars::JsonSchema))]
#[serde(rename_all = "camelCase")]
#[non_exhaustive]
pub struct StringSchema {
    /// Type discriminator
    #[serde(rename = "type")]
    pub type_: StringTypeConst,

    /// Optional title for the schema
    #[serde(skip_serializing_if = "Option::is_none")]
    pub title: Option<Cow<'static, str>>,

    /// Human-readable description
    #[serde(skip_serializing_if = "Option::is_none")]
    pub description: Option<Cow<'static, str>>,

    /// Minimum string length
    #[serde(skip_serializing_if = "Option::is_none")]
    pub min_length: Option<u32>,

    /// Maximum string length
    #[serde(skip_serializing_if = "Option::is_none")]
    pub max_length: Option<u32>,

    /// String format - limited to: "email", "uri", "date", "date-time"
    #[serde(skip_serializing_if = "Option::is_none")]
    pub format: Option<StringFormat>,

    /// Default value
    #[serde(skip_serializing_if = "Option::is_none")]
    pub default: Option<String>,
}

impl Default for StringSchema {
    fn default() -> Self {
        Self {
            type_: StringTypeConst,
            title: None,
            description: None,
            min_length: None,
            max_length: None,
            format: None,
            default: None,
        }
    }
}

impl StringSchema {
    /// Create a new string schema
    pub fn new() -> Self {
        Self::default()
    }

    /// Create an email string schema
    pub fn email() -> Self {
        Self {
            format: Some(StringFormat::Email),
            ..Default::default()
        }
    }

    /// Create a URI string schema
    pub fn uri() -> Self {
        Self {
            format: Some(StringFormat::Uri),
            ..Default::default()
        }
    }

    /// Create a date string schema
    pub fn date() -> Self {
        Self {
            format: Some(StringFormat::Date),
            ..Default::default()
        }
    }

    /// Create a date-time string schema
    pub fn date_time() -> Self {
        Self {
            format: Some(StringFormat::DateTime),
            ..Default::default()
        }
    }

    /// Set title
    pub fn title(mut self, title: impl Into<Cow<'static, str>>) -> Self {
        self.title = Some(title.into());
        self
    }

    /// Set description
    pub fn description(mut self, description: impl Into<Cow<'static, str>>) -> Self {
        self.description = Some(description.into());
        self
    }

    /// Set minimum and maximum length
    pub fn with_length(mut self, min: u32, max: u32) -> Result<Self, &'static str> {
        if min > max {
            return Err("min_length must be <= max_length");
        }
        self.min_length = Some(min);
        self.max_length = Some(max);
        Ok(self)
    }

    /// Set minimum and maximum length (panics on invalid input)
    pub fn length(mut self, min: u32, max: u32) -> Self {
        assert!(min <= max, "min_length must be <= max_length");
        self.min_length = Some(min);
        self.max_length = Some(max);
        self
    }

    /// Set minimum length
    pub fn min_length(mut self, min: u32) -> Self {
        self.min_length = Some(min);
        self
    }

    /// Set maximum length
    pub fn max_length(mut self, max: u32) -> Self {
        self.max_length = Some(max);
        self
    }

    /// Set format (limited to: "email", "uri", "date", "date-time")
    pub fn format(mut self, format: StringFormat) -> Self {
        self.format = Some(format);
        self
    }

    /// Set default value
    pub fn with_default(mut self, default: impl Into<String>) -> Self {
        self.default = Some(default.into());
        self
    }
}

// =============================================================================
// NUMBER SCHEMA
// =============================================================================

/// Schema definition for number properties (floating-point).
///
/// Compliant with MCP 2025-06-18 specification for elicitation schemas.
/// Supports only the fields allowed by the MCP spec.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "schemars", derive(schemars::JsonSchema))]
#[serde(rename_all = "camelCase")]
#[non_exhaustive]
pub struct NumberSchema {
    /// Type discriminator
    #[serde(rename = "type")]
    pub type_: NumberTypeConst,

    /// Optional title for the schema
    #[serde(skip_serializing_if = "Option::is_none")]
    pub title: Option<Cow<'static, str>>,

    /// Human-readable description
    #[serde(skip_serializing_if = "Option::is_none")]
    pub description: Option<Cow<'static, str>>,

    /// Minimum value (inclusive)
    #[serde(
        default,
        deserialize_with = "super::serde_impl::json_float::option_f64",
        skip_serializing_if = "Option::is_none"
    )]
    pub minimum: Option<f64>,

    /// Maximum value (inclusive)
    #[serde(
        default,
        deserialize_with = "super::serde_impl::json_float::option_f64",
        skip_serializing_if = "Option::is_none"
    )]
    pub maximum: Option<f64>,

    /// Default value
    #[serde(
        default,
        deserialize_with = "super::serde_impl::json_float::option_f64",
        skip_serializing_if = "Option::is_none"
    )]
    pub default: Option<f64>,
}

impl Default for NumberSchema {
    fn default() -> Self {
        Self {
            type_: NumberTypeConst,
            title: None,
            description: None,
            minimum: None,
            maximum: None,
            default: None,
        }
    }
}

impl NumberSchema {
    /// Create a new number schema
    pub fn new() -> Self {
        Self::default()
    }

    /// Set minimum and maximum (inclusive)
    pub fn with_range(mut self, min: f64, max: f64) -> Result<Self, &'static str> {
        if min > max {
            return Err("minimum must be <= maximum");
        }
        self.minimum = Some(min);
        self.maximum = Some(max);
        Ok(self)
    }

    /// Set minimum and maximum (panics on invalid input)
    pub fn range(mut self, min: f64, max: f64) -> Self {
        assert!(min <= max, "minimum must be <= maximum");
        self.minimum = Some(min);
        self.maximum = Some(max);
        self
    }

    /// Set minimum (inclusive)
    pub fn minimum(mut self, min: f64) -> Self {
        self.minimum = Some(min);
        self
    }

    /// Set maximum (inclusive)
    pub fn maximum(mut self, max: f64) -> Self {
        self.maximum = Some(max);
        self
    }

    /// Set title
    pub fn title(mut self, title: impl Into<Cow<'static, str>>) -> Self {
        self.title = Some(title.into());
        self
    }

    /// Set description
    pub fn description(mut self, description: impl Into<Cow<'static, str>>) -> Self {
        self.description = Some(description.into());
        self
    }

    /// Set default value
    pub fn with_default(mut self, default: f64) -> Self {
        self.default = Some(default);
        self
    }
}

// =============================================================================
// INTEGER SCHEMA
// =============================================================================

/// Schema definition for integer properties.
///
/// Compliant with MCP 2025-06-18 specification for elicitation schemas.
/// Supports only the fields allowed by the MCP spec.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "schemars", derive(schemars::JsonSchema))]
#[serde(rename_all = "camelCase")]
#[non_exhaustive]
pub struct IntegerSchema {
    /// Type discriminator
    #[serde(rename = "type")]
    pub type_: IntegerTypeConst,

    /// Optional title for the schema
    #[serde(skip_serializing_if = "Option::is_none")]
    pub title: Option<Cow<'static, str>>,

    /// Human-readable description
    #[serde(skip_serializing_if = "Option::is_none")]
    pub description: Option<Cow<'static, str>>,

    /// Minimum value (inclusive)
    #[serde(skip_serializing_if = "Option::is_none")]
    pub minimum: Option<i64>,

    /// Maximum value (inclusive)
    #[serde(skip_serializing_if = "Option::is_none")]
    pub maximum: Option<i64>,

    /// Default value
    #[serde(skip_serializing_if = "Option::is_none")]
    pub default: Option<i64>,
}

impl Default for IntegerSchema {
    fn default() -> Self {
        Self {
            type_: IntegerTypeConst,
            title: None,
            description: None,
            minimum: None,
            maximum: None,
            default: None,
        }
    }
}

impl IntegerSchema {
    /// Create a new integer schema
    pub fn new() -> Self {
        Self::default()
    }

    /// Set minimum and maximum (inclusive)
    pub fn with_range(mut self, min: i64, max: i64) -> Result<Self, &'static str> {
        if min > max {
            return Err("minimum must be <= maximum");
        }
        self.minimum = Some(min);
        self.maximum = Some(max);
        Ok(self)
    }

    /// Set minimum and maximum (panics on invalid input)
    pub fn range(mut self, min: i64, max: i64) -> Self {
        assert!(min <= max, "minimum must be <= maximum");
        self.minimum = Some(min);
        self.maximum = Some(max);
        self
    }

    /// Set minimum (inclusive)
    pub fn minimum(mut self, min: i64) -> Self {
        self.minimum = Some(min);
        self
    }

    /// Set maximum (inclusive)
    pub fn maximum(mut self, max: i64) -> Self {
        self.maximum = Some(max);
        self
    }

    /// Set title
    pub fn title(mut self, title: impl Into<Cow<'static, str>>) -> Self {
        self.title = Some(title.into());
        self
    }

    /// Set description
    pub fn description(mut self, description: impl Into<Cow<'static, str>>) -> Self {
        self.description = Some(description.into());
        self
    }

    /// Set default value
    pub fn with_default(mut self, default: i64) -> Self {
        self.default = Some(default);
        self
    }
}

// =============================================================================
// BOOLEAN SCHEMA
// =============================================================================

/// Schema definition for boolean properties.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "schemars", derive(schemars::JsonSchema))]
#[serde(rename_all = "camelCase")]
#[non_exhaustive]
pub struct BooleanSchema {
    /// Type discriminator
    #[serde(rename = "type")]
    pub type_: BooleanTypeConst,

    /// Optional title for the schema
    #[serde(skip_serializing_if = "Option::is_none")]
    pub title: Option<Cow<'static, str>>,

    /// Human-readable description
    #[serde(skip_serializing_if = "Option::is_none")]
    pub description: Option<Cow<'static, str>>,

    /// Default value
    #[serde(skip_serializing_if = "Option::is_none")]
    pub default: Option<bool>,
}

impl Default for BooleanSchema {
    fn default() -> Self {
        Self {
            type_: BooleanTypeConst,
            title: None,
            description: None,
            default: None,
        }
    }
}

impl BooleanSchema {
    /// Create a new boolean schema
    pub fn new() -> Self {
        Self::default()
    }

    /// Set title
    pub fn title(mut self, title: impl Into<Cow<'static, str>>) -> Self {
        self.title = Some(title.into());
        self
    }

    /// Set description
    pub fn description(mut self, description: impl Into<Cow<'static, str>>) -> Self {
        self.description = Some(description.into());
        self
    }

    /// Set default value
    pub fn with_default(mut self, default: bool) -> Self {
        self.default = Some(default);
        self
    }
}

// =============================================================================
// ENUM SCHEMA
// =============================================================================

/// Schema definition for enum properties.
///
/// Represent single entry for titled item
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "schemars", derive(schemars::JsonSchema))]
#[non_exhaustive]
pub struct ConstTitle {
    #[serde(rename = "const")]
    pub const_: String,
    pub title: String,
}

impl ConstTitle {
    /// Create a new ConstTitle.
    pub fn new(const_: impl Into<String>, title: impl Into<String>) -> Self {
        Self {
            const_: const_.into(),
            title: title.into(),
        }
    }
}

/// Legacy enum schema, keep for backward compatibility
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
#[cfg_attr(feature = "schemars", derive(schemars::JsonSchema))]
#[non_exhaustive]
pub struct LegacyEnumSchema {
    #[serde(rename = "type")]
    pub type_: StringTypeConst,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub title: Option<Cow<'static, str>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub description: Option<Cow<'static, str>>,
    #[serde(rename = "enum")]
    pub enum_: Vec<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub enum_names: Option<Vec<String>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub default: Option<String>,
}

impl LegacyEnumSchema {
    pub fn new(enum_values: Vec<String>) -> Self {
        Self {
            type_: StringTypeConst,
            title: None,
            description: None,
            enum_: enum_values,
            enum_names: None,
            default: None,
        }
    }
}

/// Untitled single-select
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
#[cfg_attr(feature = "schemars", derive(schemars::JsonSchema))]
#[non_exhaustive]
pub struct UntitledSingleSelectEnumSchema {
    #[serde(rename = "type")]
    pub type_: StringTypeConst,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub title: Option<Cow<'static, str>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub description: Option<Cow<'static, str>>,
    #[serde(rename = "enum")]
    pub enum_: Vec<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub default: Option<String>,
}

/// Titled single-select
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
#[cfg_attr(feature = "schemars", derive(schemars::JsonSchema))]
#[non_exhaustive]
pub struct TitledSingleSelectEnumSchema {
    #[serde(rename = "type")]
    pub type_: StringTypeConst,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub title: Option<Cow<'static, str>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub description: Option<Cow<'static, str>>,
    #[serde(rename = "oneOf")]
    pub one_of: Vec<ConstTitle>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub default: Option<String>,
}

impl TitledSingleSelectEnumSchema {
    /// Create a new TitledSingleSelectEnumSchema.
    pub fn new(one_of: Vec<ConstTitle>) -> Self {
        Self {
            type_: StringTypeConst,
            title: None,
            description: None,
            one_of,
            default: None,
        }
    }
}

/// Combined single-select
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "schemars", derive(schemars::JsonSchema))]
#[serde(untagged)]
#[non_exhaustive]
pub enum SingleSelectEnumSchema {
    Untitled(UntitledSingleSelectEnumSchema),
    Titled(TitledSingleSelectEnumSchema),
}

/// Items for untitled multi-select options
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "schemars", derive(schemars::JsonSchema))]
#[non_exhaustive]
pub struct UntitledItems {
    #[serde(rename = "type")]
    pub type_: StringTypeConst,
    #[serde(rename = "enum")]
    pub enum_: Vec<String>,
}

impl UntitledItems {
    pub fn new(enum_values: Vec<String>) -> Self {
        Self {
            type_: StringTypeConst,
            enum_: enum_values,
        }
    }
}

/// Items for titled multi-select options
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "schemars", derive(schemars::JsonSchema))]
#[non_exhaustive]
pub struct TitledItems {
    // MCP spec requires "anyOf" for multi-select enums (allows any combination)
    // Alias "oneOf" for compatibility with schemars
    #[serde(rename = "anyOf", alias = "oneOf")]
    pub any_of: Vec<ConstTitle>,
}

impl TitledItems {
    /// Create a new TitledItems.
    pub fn new(any_of: Vec<ConstTitle>) -> Self {
        Self { any_of }
    }
}

/// Multi-select untitled options
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "schemars", derive(schemars::JsonSchema))]
#[serde(rename_all = "camelCase")]
#[non_exhaustive]
pub struct UntitledMultiSelectEnumSchema {
    #[serde(rename = "type")]
    pub type_: ArrayTypeConst,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub title: Option<Cow<'static, str>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub description: Option<Cow<'static, str>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub min_items: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub max_items: Option<u64>,
    pub items: UntitledItems,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub default: Option<Vec<String>>,
}

/// Multi-select titled options
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "schemars", derive(schemars::JsonSchema))]
#[serde(rename_all = "camelCase")]
#[non_exhaustive]
pub struct TitledMultiSelectEnumSchema {
    #[serde(rename = "type")]
    pub type_: ArrayTypeConst,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub title: Option<Cow<'static, str>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub description: Option<Cow<'static, str>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub min_items: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub max_items: Option<u64>,
    pub items: TitledItems,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub default: Option<Vec<String>>,
}

impl TitledMultiSelectEnumSchema {
    /// Create a new TitledMultiSelectEnumSchema.
    pub fn new(items: TitledItems) -> Self {
        Self {
            type_: ArrayTypeConst,
            title: None,
            description: None,
            min_items: None,
            max_items: None,
            items,
            default: None,
        }
    }

    /// Set the title.
    pub fn with_title(mut self, title: impl Into<Cow<'static, str>>) -> Self {
        self.title = Some(title.into());
        self
    }

    /// Set the description.
    pub fn with_description(mut self, description: impl Into<Cow<'static, str>>) -> Self {
        self.description = Some(description.into());
        self
    }

    /// Set the minimum number of items.
    pub fn with_min_items(mut self, min_items: u64) -> Self {
        self.min_items = Some(min_items);
        self
    }

    /// Set the maximum number of items.
    pub fn with_max_items(mut self, max_items: u64) -> Self {
        self.max_items = Some(max_items);
        self
    }

    /// Set the default values.
    pub fn with_default(mut self, default: Vec<String>) -> Self {
        self.default = Some(default);
        self
    }
}

/// Multi-select enum options
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "schemars", derive(schemars::JsonSchema))]
#[serde(untagged)]
#[non_exhaustive]
pub enum MultiSelectEnumSchema {
    Untitled(UntitledMultiSelectEnumSchema),
    Titled(TitledMultiSelectEnumSchema),
}

/// Compliant with MCP 2025-06-18 specification for elicitation schemas.
/// Enums must have string type for values and can optionally include human-readable names.
///
/// # Example
///
/// ```rust
/// use mcp_protocol::model::*;
///
/// let enum_schema = EnumSchema::builder(vec!["US".to_string(), "UK".to_string()])
///    .multiselect()
///    .min_items(1u64).expect("Min items should be correct value")
///    .max_items(4u64).expect("Max items should be correct value")
///    .description("Country code")
///    .build();
/// ```
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "schemars", derive(schemars::JsonSchema))]
#[serde(untagged)]
#[non_exhaustive]
pub enum EnumSchema {
    Single(SingleSelectEnumSchema),
    Multi(MultiSelectEnumSchema),
    Legacy(LegacyEnumSchema),
}

/// Marker type for single-select enum builder
#[derive(Debug)]
#[expect(clippy::exhaustive_structs, reason = "intentionally exhaustive")]
pub struct SingleSelect;
/// Marker type for multi-select enum builder
#[derive(Debug)]
#[expect(clippy::exhaustive_structs, reason = "intentionally exhaustive")]
pub struct MultiSelect;
/// Builder for EnumSchema
/// Allows to create various enum schema types (single/multi select, titled/untitled)
/// with validation of provided parameters
///
/// # Example
/// ```rust
/// use mcp_protocol::model::*;
/// let enum_schema = EnumSchema::builder(vec!["Red".to_string(), "Green".to_string(), "Blue".to_string()])
///  .multiselect()
///  .enum_titles(vec!["Red Color".to_string(), "Green Color".to_string(), "Blue Color".to_string()])
///  .expect("Number of titles should match number of values")
///  .min_items(1u64).expect("Min items should be correct value")
///  .max_items(3u64).expect("Max items should be correct value")
///  .description("Select your favorite colors")
///  .build();
/// ```
#[derive(Debug)]
pub struct EnumSchemaBuilder<T> {
    /// Enum values
    enum_values: Vec<String>,
    /// If true generate Titled EnumSchema, UnTitled otherwise
    titled: bool,
    /// Title of EnumSchema
    title: Option<Cow<'static, str>>,
    /// Description of EnumSchema
    description: Option<Cow<'static, str>>,
    /// Titles of given enum values
    enum_titles: Vec<String>,
    /// Minimum number of items to choose for MultiSelect
    min_items: Option<u64>,
    /// Maximum number of items to choose for MultiSelect
    max_items: Option<u64>,
    /// Default values for enum
    default: Vec<String>,
    select_type: PhantomData<T>,
}

/// Default implementation for single-select enum builder
impl Default for EnumSchemaBuilder<SingleSelect> {
    fn default() -> Self {
        Self {
            title: None,
            description: None,
            titled: false,
            enum_titles: Vec::new(),
            enum_values: Vec::new(),
            min_items: None,
            max_items: None,
            default: Vec::new(),
            select_type: PhantomData,
        }
    }
}

/// Common enum schema builder methods
impl<T> EnumSchemaBuilder<T> {
    /// Set title of enum schema
    pub fn title(mut self, value: impl Into<Cow<'static, str>>) -> Self {
        self.title = Some(value.into());
        self
    }

    /// Set description of enum schema
    pub fn description(mut self, value: impl Into<Cow<'static, str>>) -> Self {
        self.description = Some(value.into());
        self
    }

    /// Set enum as untitled
    /// Clears any previously set titles
    pub fn untitled(mut self) -> Self {
        self.enum_titles = Vec::new();
        self.titled = false;
        self
    }

    /// Set titles to enum values. Also, implicitly set this enum schema as titled
    pub fn enum_titles(mut self, titles: Vec<String>) -> Result<EnumSchemaBuilder<T>, String> {
        if titles.len() != self.enum_values.len() {
            return Err(format!(
                "Provided number of titles do not match number of values: expected {}, but got {}",
                self.enum_values.len(),
                titles.len()
            ));
        }
        self.titled = true;
        self.enum_titles = titles;
        Ok(self)
    }
}

/// Enum selection builder for single-select enums
impl EnumSchemaBuilder<SingleSelect> {
    pub fn new(values: Vec<String>) -> EnumSchemaBuilder<SingleSelect> {
        EnumSchemaBuilder {
            enum_values: values,
            ..Default::default()
        }
    }

    /// Transition to multi-select enum builder.
    ///
    /// Clears any previously set default values and resets min/max items.
    /// After this transition, you can use `min_items()`, `max_items()`, and
    /// `with_default()` for multi-select semantics.
    pub fn multiselect(self) -> EnumSchemaBuilder<MultiSelect> {
        EnumSchemaBuilder {
            enum_values: self.enum_values,
            titled: self.titled,
            title: self.title,
            description: self.description,
            enum_titles: self.enum_titles,
            min_items: None,
            max_items: None,
            default: Vec::new(), // Clear default for multi-select
            select_type: PhantomData,
        }
    }

    /// Set default value
    pub fn with_default(
        mut self,
        default_value: impl Into<String>,
    ) -> Result<EnumSchemaBuilder<SingleSelect>, String> {
        let value: String = default_value.into();
        if !self.enum_values.contains(&value) {
            return Err("Provided default value is not in enum values".to_string());
        }
        self.default = vec![value];
        Ok(self)
    }

    /// Build enum schema
    pub fn build(mut self) -> EnumSchema {
        match self.titled {
            false => EnumSchema::Single(SingleSelectEnumSchema::Untitled(
                UntitledSingleSelectEnumSchema {
                    type_: StringTypeConst,
                    title: self.title,
                    description: self.description,
                    enum_: self.enum_values,
                    default: self.default.pop(),
                },
            )),
            true => EnumSchema::Single(SingleSelectEnumSchema::Titled(
                TitledSingleSelectEnumSchema {
                    type_: StringTypeConst,
                    title: self.title,
                    description: self.description,
                    one_of: self
                        .enum_titles
                        .into_iter()
                        .zip(self.enum_values)
                        .map(|(title, const_)| ConstTitle { const_, title })
                        .collect(),
                    default: self.default.pop(),
                },
            )),
        }
    }
}

/// Enum selection builder for multi-select enums
impl EnumSchemaBuilder<MultiSelect> {
    /// Set enum as single-select
    /// If it was multi-select, clear default values
    pub fn single_select(self) -> EnumSchemaBuilder<SingleSelect> {
        EnumSchemaBuilder {
            enum_values: self.enum_values,
            titled: self.titled,
            title: self.title,
            description: self.description,
            enum_titles: self.enum_titles,
            min_items: None,
            max_items: None,
            default: Vec::new(), // Clear default for single-select
            select_type: PhantomData,
        }
    }

    /// Set default values
    pub fn with_default(
        mut self,
        default_values: Vec<String>,
    ) -> Result<EnumSchemaBuilder<MultiSelect>, String> {
        for value in &default_values {
            if !self.enum_values.contains(value) {
                return Err("One of the provided default values is not in enum values".to_string());
            }
        }
        if let Some(min) = self.min_items
            && (default_values.len() as u64) < min
        {
            return Err("Number of provided default values is less than min_items".to_string());
        }
        if let Some(max) = self.max_items
            && (default_values.len() as u64) > max
        {
            return Err("Number of provided default values is greater than max_items".to_string());
        }
        self.default = default_values;
        Ok(self)
    }

    /// Set minimal number of items for multi-select enum options
    pub fn min_items(mut self, value: u64) -> Result<EnumSchemaBuilder<MultiSelect>, String> {
        if let Some(max) = self.max_items
            && value > max
        {
            return Err("Provided value is greater than max_items".to_string());
        }
        self.min_items = Some(value);
        Ok(self)
    }

    /// Set maximal number of items for multi-select enum options
    pub fn max_items(mut self, value: u64) -> Result<EnumSchemaBuilder<MultiSelect>, String> {
        if let Some(min) = self.min_items
            && value < min
        {
            return Err("Provided value is less than min_items".to_string());
        }
        self.max_items = Some(value);
        Ok(self)
    }

    /// Build enum schema
    pub fn build(self) -> EnumSchema {
        match self.titled {
            false => EnumSchema::Multi(MultiSelectEnumSchema::Untitled(
                UntitledMultiSelectEnumSchema {
                    type_: ArrayTypeConst,
                    title: self.title,
                    description: self.description,
                    min_items: self.min_items,
                    max_items: self.max_items,
                    items: UntitledItems {
                        type_: StringTypeConst,
                        enum_: self.enum_values,
                    },
                    default: if self.default.is_empty() {
                        None
                    } else {
                        Some(self.default)
                    },
                },
            )),
            true => EnumSchema::Multi(MultiSelectEnumSchema::Titled(TitledMultiSelectEnumSchema {
                type_: ArrayTypeConst,
                title: self.title,
                description: self.description,
                min_items: self.min_items,
                max_items: self.max_items,
                items: TitledItems {
                    any_of: self
                        .enum_titles
                        .into_iter()
                        .zip(self.enum_values)
                        .map(|(title, const_)| ConstTitle { const_, title })
                        .collect(),
                },
                default: if self.default.is_empty() {
                    None
                } else {
                    Some(self.default)
                },
            })),
        }
    }
}

impl EnumSchema {
    /// Creates a new `EnumSchemaBuilder` with the given enum values.
    ///
    /// This convenience method allows you to construct an enum schema by specifying
    /// the possible string values for the enum. Use the returned builder to further
    /// configure the schema before building it.
    ///
    /// # Arguments
    ///
    /// * `values` - A vector of strings representing the allowed enum values.
    ///
    /// # Example
    ///
    /// ```
    /// use mcp_protocol::model::*;
    ///
    /// let enum_schema = EnumSchema::builder(vec!["A".to_string(), "B".to_string()]).
    ///     with_default("A").
    ///     expect("Default value should be valid").
    ///     enum_titles(vec!["Option A".to_string(), "Option B".to_string()]).
    ///     expect("Number of titles should match number of values").
    ///     build();
    /// ```
    pub fn builder(values: Vec<String>) -> EnumSchemaBuilder<SingleSelect> {
        EnumSchemaBuilder::new(values)
    }
}

// =============================================================================
// ELICITATION SCHEMA
// =============================================================================

/// Type-safe elicitation schema for requesting structured user input.
///
/// This enforces the MCP 2025-06-18 specification that elicitation schemas
/// must be objects with primitive-typed properties.
///
/// # Example
///
/// ```rust
/// use mcp_protocol::model::*;
///
/// let schema = ElicitationSchema::builder()
///     .required_email("email")
///     .required_integer("age", 0, 150)
///     .optional_bool("newsletter", false)
///     .build();
/// ```
#[derive(Debug, Clone, PartialEq, Serialize)]
#[cfg_attr(feature = "schemars", derive(schemars::JsonSchema))]
#[cfg_attr(feature = "schemars", schemars(!into))]
#[serde(rename_all = "camelCase", into = "ElicitationSchemaWire")]
#[non_exhaustive]
pub struct ElicitationSchema {
    /// Optional JSON Schema dialect identifier (the `$schema` keyword).
    ///
    /// The 2025-11-25 protocol revision allows a `requestedSchema` to declare its
    /// dialect. It is preserved verbatim so a declared dialect survives a
    /// decode/re-encode round-trip instead of being silently dropped.
    #[serde(rename = "$schema", default, skip_serializing_if = "Option::is_none")]
    #[cfg_attr(feature = "schemars", schemars(with = "String"))]
    pub schema: Option<Cow<'static, str>>,

    /// Always "object" for elicitation schemas
    #[serde(rename = "type")]
    pub type_: ObjectTypeConst,

    /// Optional title for the schema
    #[serde(skip_serializing_if = "Option::is_none")]
    pub title: Option<Cow<'static, str>>,

    /// Property definitions (must be primitive types)
    pub properties: BTreeMap<String, PrimitiveSchemaDefinition>,

    /// Property names in wire order. Schemas constructed from a `BTreeMap`
    /// use the map's sorted key order.
    #[serde(skip)]
    pub property_order: Option<Vec<String>>,

    /// List of required property names
    #[serde(skip_serializing_if = "Option::is_none")]
    pub required: Option<Vec<String>>,

    /// Optional description of what this schema represents
    #[serde(skip_serializing_if = "Option::is_none")]
    pub description: Option<Cow<'static, str>>,
}

#[derive(Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
struct ElicitationSchemaWire {
    #[serde(rename = "$schema", default, skip_serializing_if = "Option::is_none")]
    schema: Option<Cow<'static, str>>,
    #[serde(rename = "type")]
    type_: ObjectTypeConst,
    #[serde(skip_serializing_if = "Option::is_none")]
    title: Option<Cow<'static, str>>,
    properties: IndexMap<String, PrimitiveSchemaDefinition>,
    #[serde(skip_serializing_if = "Option::is_none")]
    required: Option<Vec<String>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    description: Option<Cow<'static, str>>,
}

impl From<ElicitationSchemaWire> for ElicitationSchema {
    fn from(schema: ElicitationSchemaWire) -> Self {
        Self {
            schema: schema.schema,
            type_: schema.type_,
            title: schema.title,
            property_order: Some(schema.properties.keys().cloned().collect()),
            properties: schema.properties.into_iter().collect(),
            required: schema.required,
            description: schema.description,
        }
    }
}

impl From<ElicitationSchema> for ElicitationSchemaWire {
    fn from(schema: ElicitationSchema) -> Self {
        let mut remaining = schema.properties;
        let mut properties = IndexMap::with_capacity(remaining.len());

        if let Some(property_order) = schema.property_order {
            for name in property_order {
                if let Some(definition) = remaining.remove(&name) {
                    properties.insert(name, definition);
                }
            }
        }
        properties.extend(remaining);

        Self {
            schema: schema.schema,
            type_: schema.type_,
            title: schema.title,
            properties,
            required: schema.required,
            description: schema.description,
        }
    }
}

impl<'de> Deserialize<'de> for ElicitationSchema {
    fn deserialize<__D>(__deserializer: __D) -> Result<Self, __D::Error>
    where
        __D: Deserializer<'de>,
    {
        ElicitationSchemaWire::deserialize(__deserializer).map(Into::into)
    }
}

impl ElicitationSchema {
    /// Create a new elicitation schema with the given properties
    pub fn new(properties: BTreeMap<String, PrimitiveSchemaDefinition>) -> Self {
        let property_order = Some(properties.keys().cloned().collect());
        Self {
            schema: None,
            type_: ObjectTypeConst,
            title: None,
            properties,
            property_order,
            required: None,
            description: None,
        }
    }

    /// Convert from a JSON Schema object (typically generated by schemars)
    ///
    /// This allows converting from JsonObject to ElicitationSchema, which is useful
    /// when working with automatically generated schemas from types.
    ///
    /// # Example
    ///
    /// ```rust,ignore
    /// use mcp_protocol::model::*;
    ///
    /// let json_schema = schema_for_type::<MyType>();
    /// let elicitation_schema = ElicitationSchema::from_json_schema(json_schema)?;
    /// ```
    ///
    /// # Errors
    ///
    /// Returns a [`serde_json::Error`] if the JSON object cannot be deserialized
    /// into a valid ElicitationSchema.
    pub fn from_json_schema(schema: crate::model::JsonObject) -> Result<Self, serde_json::Error> {
        serde_json::from_value(serde_json::Value::Object(schema))
    }

    /// Generate an ElicitationSchema from a Rust type that implements JsonSchema
    ///
    /// This is a convenience method that combines schema generation and conversion.
    /// It uses the same schema generation settings as the rest of the MCP SDK.
    ///
    /// # Example
    ///
    /// ```rust,ignore
    /// use mcp_protocol::model::*;
    /// use schemars::JsonSchema;
    /// use serde::{Deserialize, Serialize};
    ///
    /// #[derive(JsonSchema, Serialize, Deserialize)]
    /// struct UserInput {
    ///     name: String,
    ///     age: u32,
    /// }
    ///
    /// let schema = ElicitationSchema::from_type::<UserInput>()?;
    /// ```
    ///
    /// # Errors
    ///
    /// Returns a [`serde_json::Error`] if the generated schema cannot be converted
    /// to a valid ElicitationSchema.
    #[cfg(feature = "schemars")]
    pub fn from_type<T>() -> Result<Self, serde_json::Error>
    where
        T: schemars::JsonSchema,
    {
        use crate::schemars::generate::SchemaSettings;

        let mut settings = SchemaSettings::draft07();
        settings.transforms = vec![Box::new(schemars::transform::AddNullable::default())];
        let generator = settings.into_generator();
        let schema = generator.into_root_schema_for::<T>();
        let object = serde_json::to_value(schema).expect("failed to serialize schema");
        match object {
            serde_json::Value::Object(object) => Self::from_json_schema(object),
            _ => panic!(
                "Schema serialization produced non-object value: expected JSON object but got {:?}",
                object
            ),
        }
    }

    /// Set the required fields
    pub fn with_required(mut self, required: Vec<String>) -> Self {
        self.required = Some(required);
        self
    }

    /// Set the title
    pub fn with_title(mut self, title: impl Into<Cow<'static, str>>) -> Self {
        self.title = Some(title.into());
        self
    }

    /// Set the description
    pub fn with_description(mut self, description: impl Into<Cow<'static, str>>) -> Self {
        self.description = Some(description.into());
        self
    }

    /// Set the JSON Schema dialect identifier (the `$schema` keyword).
    pub fn with_schema(mut self, schema: impl Into<Cow<'static, str>>) -> Self {
        self.schema = Some(schema.into());
        self
    }

    /// Create a builder for constructing elicitation schemas fluently
    pub fn builder() -> ElicitationSchemaBuilder {
        ElicitationSchemaBuilder::new()
    }
}

// =============================================================================
// BUILDER
// =============================================================================

/// Fluent builder for constructing elicitation schemas.
///
/// # Example
///
/// ```rust
/// use mcp_protocol::model::*;
///
/// let schema = ElicitationSchema::builder()
///     .required_email("email")
///     .required_integer("age", 0, 150)
///     .optional_bool("newsletter", false)
///     .description("User registration")
///     .build();
/// ```
#[derive(Debug, Default)]
#[expect(clippy::exhaustive_structs, reason = "intentionally exhaustive")]
pub struct ElicitationSchemaBuilder {
    pub properties: BTreeMap<String, PrimitiveSchemaDefinition>,
    pub required: Vec<String>,
    pub title: Option<Cow<'static, str>>,
    pub description: Option<Cow<'static, str>>,
}

impl ElicitationSchemaBuilder {
    /// Create a new builder
    pub fn new() -> Self {
        Self::default()
    }

    /// Add a property to the schema
    pub fn property(mut self, name: impl Into<String>, schema: PrimitiveSchemaDefinition) -> Self {
        self.properties.insert(name.into(), schema);
        self
    }

    /// Add a required property to the schema
    pub fn required_property(
        mut self,
        name: impl Into<String>,
        schema: PrimitiveSchemaDefinition,
    ) -> Self {
        let name_str = name.into();
        self.required.push(name_str.clone());
        self.properties.insert(name_str, schema);
        self
    }

    // ===========================================================================
    // TYPED PROPERTY METHODS - Cleaner API without PrimitiveSchemaDefinition wrapper
    // ===========================================================================

    /// Add a string property with custom builder (required)
    pub fn string_property(
        mut self,
        name: impl Into<String>,
        f: impl FnOnce(StringSchema) -> StringSchema,
    ) -> Self {
        self.properties.insert(
            name.into(),
            PrimitiveSchemaDefinition::String(f(StringSchema::new())),
        );
        self
    }

    /// Add a required string property with custom builder
    pub fn required_string_property(
        mut self,
        name: impl Into<String>,
        f: impl FnOnce(StringSchema) -> StringSchema,
    ) -> Self {
        let name_str = name.into();
        self.required.push(name_str.clone());
        self.properties.insert(
            name_str,
            PrimitiveSchemaDefinition::String(f(StringSchema::new())),
        );
        self
    }

    /// Add a number property with custom builder
    pub fn number_property(
        mut self,
        name: impl Into<String>,
        f: impl FnOnce(NumberSchema) -> NumberSchema,
    ) -> Self {
        self.properties.insert(
            name.into(),
            PrimitiveSchemaDefinition::Number(f(NumberSchema::new())),
        );
        self
    }

    /// Add a required number property with custom builder
    pub fn required_number_property(
        mut self,
        name: impl Into<String>,
        f: impl FnOnce(NumberSchema) -> NumberSchema,
    ) -> Self {
        let name_str = name.into();
        self.required.push(name_str.clone());
        self.properties.insert(
            name_str,
            PrimitiveSchemaDefinition::Number(f(NumberSchema::new())),
        );
        self
    }

    /// Add an integer property with custom builder
    pub fn integer_property(
        mut self,
        name: impl Into<String>,
        f: impl FnOnce(IntegerSchema) -> IntegerSchema,
    ) -> Self {
        self.properties.insert(
            name.into(),
            PrimitiveSchemaDefinition::Integer(f(IntegerSchema::new())),
        );
        self
    }

    /// Add a required integer property with custom builder
    pub fn required_integer_property(
        mut self,
        name: impl Into<String>,
        f: impl FnOnce(IntegerSchema) -> IntegerSchema,
    ) -> Self {
        let name_str = name.into();
        self.required.push(name_str.clone());
        self.properties.insert(
            name_str,
            PrimitiveSchemaDefinition::Integer(f(IntegerSchema::new())),
        );
        self
    }

    /// Add a boolean property with custom builder
    pub fn bool_property(
        mut self,
        name: impl Into<String>,
        f: impl FnOnce(BooleanSchema) -> BooleanSchema,
    ) -> Self {
        self.properties.insert(
            name.into(),
            PrimitiveSchemaDefinition::Boolean(f(BooleanSchema::new())),
        );
        self
    }

    /// Add a required boolean property with custom builder
    pub fn required_bool_property(
        mut self,
        name: impl Into<String>,
        f: impl FnOnce(BooleanSchema) -> BooleanSchema,
    ) -> Self {
        let name_str = name.into();
        self.required.push(name_str.clone());
        self.properties.insert(
            name_str,
            PrimitiveSchemaDefinition::Boolean(f(BooleanSchema::new())),
        );
        self
    }

    // ===========================================================================
    // CONVENIENCE METHODS - Simple common cases
    // ===========================================================================

    /// Add a required string property
    pub fn required_string(self, name: impl Into<String>) -> Self {
        self.required_property(name, PrimitiveSchemaDefinition::String(StringSchema::new()))
    }

    /// Add an optional string property
    pub fn optional_string(self, name: impl Into<String>) -> Self {
        self.property(name, PrimitiveSchemaDefinition::String(StringSchema::new()))
    }

    /// Add a required email property
    pub fn required_email(self, name: impl Into<String>) -> Self {
        self.required_property(
            name,
            PrimitiveSchemaDefinition::String(StringSchema::email()),
        )
    }

    /// Add an optional email property
    pub fn optional_email(self, name: impl Into<String>) -> Self {
        self.property(
            name,
            PrimitiveSchemaDefinition::String(StringSchema::email()),
        )
    }

    /// Add a required string property with custom builder
    pub fn required_string_with(
        self,
        name: impl Into<String>,
        f: impl FnOnce(StringSchema) -> StringSchema,
    ) -> Self {
        self.required_property(
            name,
            PrimitiveSchemaDefinition::String(f(StringSchema::new())),
        )
    }

    /// Add an optional string property with custom builder
    pub fn optional_string_with(
        self,
        name: impl Into<String>,
        f: impl FnOnce(StringSchema) -> StringSchema,
    ) -> Self {
        self.property(
            name,
            PrimitiveSchemaDefinition::String(f(StringSchema::new())),
        )
    }

    // Convenience methods for numbers

    /// Add a required number property with range
    pub fn required_number(self, name: impl Into<String>, min: f64, max: f64) -> Self {
        self.required_property(
            name,
            PrimitiveSchemaDefinition::Number(NumberSchema::new().range(min, max)),
        )
    }

    /// Add an optional number property with range
    pub fn optional_number(self, name: impl Into<String>, min: f64, max: f64) -> Self {
        self.property(
            name,
            PrimitiveSchemaDefinition::Number(NumberSchema::new().range(min, max)),
        )
    }

    /// Add a required number property with custom builder
    pub fn required_number_with(
        self,
        name: impl Into<String>,
        f: impl FnOnce(NumberSchema) -> NumberSchema,
    ) -> Self {
        self.required_property(
            name,
            PrimitiveSchemaDefinition::Number(f(NumberSchema::new())),
        )
    }

    /// Add an optional number property with custom builder
    pub fn optional_number_with(
        self,
        name: impl Into<String>,
        f: impl FnOnce(NumberSchema) -> NumberSchema,
    ) -> Self {
        self.property(
            name,
            PrimitiveSchemaDefinition::Number(f(NumberSchema::new())),
        )
    }

    // Convenience methods for integers

    /// Add a required integer property with range
    pub fn required_integer(self, name: impl Into<String>, min: i64, max: i64) -> Self {
        self.required_property(
            name,
            PrimitiveSchemaDefinition::Integer(IntegerSchema::new().range(min, max)),
        )
    }

    /// Add an optional integer property with range
    pub fn optional_integer(self, name: impl Into<String>, min: i64, max: i64) -> Self {
        self.property(
            name,
            PrimitiveSchemaDefinition::Integer(IntegerSchema::new().range(min, max)),
        )
    }

    /// Add a required integer property with custom builder
    pub fn required_integer_with(
        self,
        name: impl Into<String>,
        f: impl FnOnce(IntegerSchema) -> IntegerSchema,
    ) -> Self {
        self.required_property(
            name,
            PrimitiveSchemaDefinition::Integer(f(IntegerSchema::new())),
        )
    }

    /// Add an optional integer property with custom builder
    pub fn optional_integer_with(
        self,
        name: impl Into<String>,
        f: impl FnOnce(IntegerSchema) -> IntegerSchema,
    ) -> Self {
        self.property(
            name,
            PrimitiveSchemaDefinition::Integer(f(IntegerSchema::new())),
        )
    }

    // Convenience methods for booleans

    /// Add a required boolean property
    pub fn required_bool(self, name: impl Into<String>) -> Self {
        self.required_property(
            name,
            PrimitiveSchemaDefinition::Boolean(BooleanSchema::new()),
        )
    }

    /// Add an optional boolean property with default value
    pub fn optional_bool(self, name: impl Into<String>, default: bool) -> Self {
        self.property(
            name,
            PrimitiveSchemaDefinition::Boolean(BooleanSchema::new().with_default(default)),
        )
    }

    /// Add a required boolean property with custom builder
    pub fn required_bool_with(
        self,
        name: impl Into<String>,
        f: impl FnOnce(BooleanSchema) -> BooleanSchema,
    ) -> Self {
        self.required_property(
            name,
            PrimitiveSchemaDefinition::Boolean(f(BooleanSchema::new())),
        )
    }

    /// Add an optional boolean property with custom builder
    pub fn optional_bool_with(
        self,
        name: impl Into<String>,
        f: impl FnOnce(BooleanSchema) -> BooleanSchema,
    ) -> Self {
        self.property(
            name,
            PrimitiveSchemaDefinition::Boolean(f(BooleanSchema::new())),
        )
    }

    // Enum convenience methods

    /// Add a required enum property using EnumSchema
    pub fn required_enum_schema(self, name: impl Into<String>, enum_schema: EnumSchema) -> Self {
        self.required_property(name, PrimitiveSchemaDefinition::Enum(enum_schema))
    }

    /// Add an optional enum property using EnumSchema
    pub fn optional_enum_schema(self, name: impl Into<String>, enum_schema: EnumSchema) -> Self {
        self.property(name, PrimitiveSchemaDefinition::Enum(enum_schema))
    }

    /// Mark an existing property as required
    pub fn mark_required(mut self, name: impl Into<String>) -> Self {
        self.required.push(name.into());
        self
    }

    /// Set the schema title
    pub fn title(mut self, title: impl Into<Cow<'static, str>>) -> Self {
        self.title = Some(title.into());
        self
    }

    /// Set the schema description
    pub fn description(mut self, description: impl Into<Cow<'static, str>>) -> Self {
        self.description = Some(description.into());
        self
    }

    /// Build the elicitation schema with validation
    ///
    /// # Errors
    ///
    /// Returns an error if:
    /// - Required fields reference non-existent properties
    /// - No properties are defined (empty schema)
    pub fn build(self) -> Result<ElicitationSchema, &'static str> {
        // Validate that all required fields exist in properties
        if !self.required.is_empty() {
            for field_name in &self.required {
                if !self.properties.contains_key(field_name) {
                    return Err("Required field does not exist in properties");
                }
            }
        }

        let property_order = Some(self.properties.keys().cloned().collect());
        Ok(ElicitationSchema {
            schema: None,
            type_: ObjectTypeConst,
            title: self.title,
            properties: self.properties,
            property_order,
            required: if self.required.is_empty() {
                None
            } else {
                Some(self.required)
            },
            description: self.description,
        })
    }

    /// Build the elicitation schema without validation (panics on invalid schema)
    ///
    /// # Panics
    ///
    /// Panics if required fields reference non-existent properties
    pub fn build_unchecked(self) -> ElicitationSchema {
        self.build().expect("Invalid elicitation schema")
    }
}
