use std::fmt;

use serde::{Deserialize, Serialize};
use serde_json::{Value, json};

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Hash)]
#[serde(untagged)]
pub enum RequestId {
    Number(i64),
    String(String),
}

impl RequestId {
    pub const fn number(value: i64) -> Self {
        Self::Number(value)
    }
}

impl From<u64> for RequestId {
    fn from(value: u64) -> Self {
        Self::Number(i64::try_from(value).unwrap_or(i64::MAX))
    }
}

impl fmt::Display for RequestId {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Number(value) => value.fmt(formatter),
            Self::String(value) => value.fmt(formatter),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct RpcRequest {
    pub method: String,
    pub id: RequestId,
    #[serde(default)]
    pub params: Value,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct RpcNotification {
    pub method: String,
    #[serde(default)]
    pub params: Value,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct RpcResponse {
    pub id: RequestId,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub result: Option<Value>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub error: Option<RpcError>,
}

/// App-server omits the JSON-RPC 2.0 header. Request and notification variants
/// must be attempted before responses because a response's result/error fields
/// are optional in the wire schema.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(untagged)]
pub enum RpcEnvelope {
    Request(RpcRequest),
    Notification(RpcNotification),
    Response(RpcResponse),
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct RpcError {
    pub code: i64,
    pub message: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub data: Option<Value>,
}

impl RpcEnvelope {
    pub fn request(id: RequestId, method: impl Into<String>, params: Value) -> Self {
        Self::Request(RpcRequest {
            method: method.into(),
            id,
            params,
        })
    }

    pub fn notification(method: impl Into<String>, params: Value) -> Self {
        Self::Notification(RpcNotification {
            method: method.into(),
            params,
        })
    }

    pub fn success(id: RequestId, result: Value) -> Self {
        Self::Response(RpcResponse {
            id,
            result: Some(result),
            error: None,
        })
    }
}

pub fn initialize_request(id: RequestId) -> RpcEnvelope {
    RpcEnvelope::request(
        id,
        "initialize",
        json!({
            "clientInfo": {
                "name": "codex_native_arch",
                "title": "Codex Native for Arch Linux",
                "version": env!("CARGO_PKG_VERSION")
            },
            "capabilities": {
                "experimentalApi": true,
                "mcpServerOpenaiFormElicitation": true
            }
        }),
    )
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct PairingInfo {
    #[serde(default)]
    pub pairing_code: Option<String>,
    #[serde(default)]
    pub manual_pairing_code: Option<String>,
    #[serde(default)]
    pub environment_id: Option<String>,
    #[serde(default)]
    pub expires_at: Option<Value>,
    #[serde(default)]
    pub pairing_url: Option<String>,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn response_request_and_notification_are_unambiguous() {
        let response: RpcEnvelope =
            serde_json::from_str(r#"{"id":7,"result":{"ok":true}}"#).unwrap();
        assert!(matches!(
            response,
            RpcEnvelope::Response(RpcResponse {
                id: RequestId::Number(7),
                ..
            })
        ));

        let request: RpcEnvelope = serde_json::from_str(
            r#"{"id":"approval-7","method":"item/commandExecution/requestApproval","params":{}}"#,
        )
        .unwrap();
        assert!(matches!(
            request,
            RpcEnvelope::Request(RpcRequest {
                id: RequestId::String(_),
                ..
            })
        ));

        let notification: RpcEnvelope =
            serde_json::from_str(r#"{"method":"turn/started","params":{}}"#).unwrap();
        assert!(matches!(notification, RpcEnvelope::Notification(_)));
    }

    #[test]
    fn remote_pairing_tolerates_unknown_fields_and_numeric_expiry() {
        let info: PairingInfo = serde_json::from_str(
            r#"{"manualPairingCode":"ABCD-EFGH","expiresAt":1784310000,"newField":9}"#,
        )
        .unwrap();
        assert_eq!(info.manual_pairing_code.as_deref(), Some("ABCD-EFGH"));
        assert_eq!(info.expires_at, Some(json!(1784310000_i64)));
    }
}
