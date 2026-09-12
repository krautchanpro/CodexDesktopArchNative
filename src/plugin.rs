use std::path::PathBuf;

use serde::Deserialize;
use serde_json::{Map, Value};

#[derive(Debug, Clone, Deserialize, Default)]
#[serde(rename_all = "camelCase")]
pub struct PluginCatalog {
    #[serde(default)]
    pub marketplaces: Vec<PluginMarketplace>,
    #[serde(default)]
    pub marketplace_load_errors: Vec<MarketplaceLoadError>,
}

#[derive(Debug, Clone, Deserialize, Default)]
#[serde(rename_all = "camelCase")]
pub struct MarketplaceLoadError {
    #[serde(default)]
    pub marketplace_path: String,
    #[serde(default)]
    pub message: String,
}

#[derive(Debug, Clone, Deserialize, Default)]
#[serde(rename_all = "camelCase")]
pub struct PluginMarketplace {
    pub name: String,
    #[serde(default)]
    pub path: Option<PathBuf>,
    #[serde(default)]
    pub plugins: Vec<PluginSummary>,
}

#[derive(Debug, Clone, Deserialize, Default)]
#[serde(rename_all = "camelCase")]
pub struct PluginSummary {
    pub id: String,
    pub name: String,
    #[serde(default)]
    pub installed: bool,
    #[serde(default)]
    pub enabled: bool,
    #[serde(default)]
    pub install_policy: String,
    #[serde(default)]
    pub auth_policy: String,
    #[serde(default)]
    pub availability: String,
    #[serde(default)]
    pub local_version: Option<String>,
    #[serde(default)]
    pub version: Option<String>,
    #[serde(default)]
    pub interface: Option<PluginInterface>,
    #[serde(default)]
    pub source: Value,
}

#[derive(Debug, Clone, Deserialize, Default)]
#[serde(rename_all = "camelCase")]
pub struct PluginInterface {
    #[serde(default)]
    pub display_name: Option<String>,
    #[serde(default)]
    pub short_description: Option<String>,
}

#[derive(Debug, Clone)]
pub struct PluginEntry {
    pub marketplace_name: String,
    pub marketplace_path: Option<PathBuf>,
    pub summary: PluginSummary,
}

impl PluginCatalog {
    pub fn parse(value: Value) -> serde_json::Result<Self> {
        serde_json::from_value(value)
    }

    #[cfg(test)]
    pub fn entries(&self) -> Vec<PluginEntry> {
        let mut entries = self
            .marketplaces
            .iter()
            .flat_map(|marketplace| {
                marketplace
                    .plugins
                    .iter()
                    .cloned()
                    .map(|summary| PluginEntry {
                        marketplace_name: marketplace.name.clone(),
                        marketplace_path: marketplace.path.clone(),
                        summary,
                    })
            })
            .collect::<Vec<_>>();
        entries.sort_by_key(|entry| {
            (
                !entry.summary.installed,
                entry.display_name().to_ascii_lowercase(),
            )
        });
        entries
    }

    pub fn find_entry(&self, predicate: impl Fn(&PluginEntry) -> bool) -> Option<PluginEntry> {
        self.marketplaces.iter().find_map(|marketplace| {
            marketplace.plugins.iter().find_map(|summary| {
                let entry = PluginEntry {
                    marketplace_name: marketplace.name.clone(),
                    marketplace_path: marketplace.path.clone(),
                    summary: summary.clone(),
                };
                predicate(&entry).then_some(entry)
            })
        })
    }

    pub fn filtered_entries(
        &self,
        installed_only: bool,
        query: &str,
        limit: usize,
    ) -> (Vec<PluginEntry>, usize) {
        let mut matches = self
            .marketplaces
            .iter()
            .flat_map(|marketplace| {
                marketplace.plugins.iter().filter_map(move |summary| {
                    if installed_only && !summary.installed {
                        return None;
                    }
                    let display_name = summary
                        .interface
                        .as_ref()
                        .and_then(|interface| interface.display_name.as_deref())
                        .unwrap_or(&summary.name);
                    let description = summary
                        .interface
                        .as_ref()
                        .and_then(|interface| interface.short_description.as_deref())
                        .unwrap_or_default();
                    if !query.is_empty()
                        && ![
                            display_name,
                            summary.name.as_str(),
                            marketplace.name.as_str(),
                            description,
                        ]
                        .iter()
                        .any(|value| value.to_ascii_lowercase().contains(query))
                    {
                        return None;
                    }
                    Some((display_name.to_ascii_lowercase(), marketplace, summary))
                })
            })
            .collect::<Vec<_>>();
        matches.sort_by(|left, right| left.0.cmp(&right.0));
        let total = matches.len();
        let entries = matches
            .into_iter()
            .take(limit)
            .map(|(_, marketplace, summary)| PluginEntry {
                marketplace_name: marketplace.name.clone(),
                marketplace_path: marketplace.path.clone(),
                summary: summary.clone(),
            })
            .collect();
        (entries, total)
    }
}

impl PluginEntry {
    pub fn display_name(&self) -> &str {
        self.summary
            .interface
            .as_ref()
            .and_then(|interface| interface.display_name.as_deref())
            .unwrap_or(&self.summary.name)
    }

    pub fn description(&self) -> &str {
        self.summary
            .interface
            .as_ref()
            .and_then(|interface| interface.short_description.as_deref())
            .unwrap_or_default()
    }

    pub fn version(&self) -> &str {
        self.summary
            .local_version
            .as_deref()
            .or(self.summary.version.as_deref())
            .unwrap_or_default()
    }

    pub fn locator_params(&self) -> Value {
        let mut params = Map::new();
        params.insert(
            "pluginName".into(),
            Value::String(self.summary.name.clone()),
        );
        if let Some(path) = &self.marketplace_path {
            params.insert(
                "marketplacePath".into(),
                Value::String(path.to_string_lossy().into_owned()),
            );
        } else {
            params.insert(
                "remoteMarketplaceName".into(),
                Value::String(self.marketplace_name.clone()),
            );
        }
        Value::Object(params)
    }

    pub fn local_root(&self) -> Option<PathBuf> {
        (self.summary.source.get("type").and_then(Value::as_str) == Some("local"))
            .then(|| self.summary.source.get("path").and_then(Value::as_str))
            .flatten()
            .map(PathBuf::from)
    }

    pub fn is_computer_use(&self) -> bool {
        self.summary.name == "computer-use" || self.summary.id.starts_with("computer-use@")
    }

    pub fn is_qwen_buddy(&self) -> bool {
        self.summary.name == "local-qwen-delegate"
            || self.summary.id.starts_with("local-qwen-delegate@")
    }
}

pub fn plugin_enabled_key(plugin_id: &str) -> String {
    format!("plugins.{}.enabled", quote_key(plugin_id))
}

pub fn plugin_mcp_key(plugin_id: &str, server: &str, suffix: &str) -> String {
    format!(
        "plugins.{}.mcp_servers.{}.{}",
        quote_key(plugin_id),
        quote_key(server),
        suffix
    )
}

pub fn app_enabled_key(app_id: &str) -> String {
    format!("apps.{}.enabled", quote_key(app_id))
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct AppToggleState {
    pub connected: bool,
    pub active: bool,
}

/// `isEnabled` is the catalog default even for apps the account has not
/// connected. Only an accessible app has a real, user-controllable enabled
/// state.
pub fn app_toggle_state(app: &Value) -> AppToggleState {
    let connected = app
        .get("isAccessible")
        .and_then(Value::as_bool)
        .unwrap_or(false);
    let active = connected
        && app
            .get("isEnabled")
            .and_then(Value::as_bool)
            .unwrap_or(false);
    AppToggleState { connected, active }
}

fn quote_key(value: &str) -> String {
    format!("\"{}\"", value.replace('\\', "\\\\").replace('"', "\\\""))
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::*;

    #[test]
    fn catalog_flattens_and_prioritizes_installed_plugins() {
        let catalog = PluginCatalog::parse(json!({
            "marketplaces": [{
                "name": "openai-bundled",
                "path": "/tmp/marketplace.json",
                "plugins": [
                    {"id":"later@openai-bundled","name":"later","installed":false,"enabled":false,"source":{"type":"remote"}},
                    {"id":"computer-use@openai-bundled","name":"computer-use","installed":true,"enabled":true,"source":{"type":"local","path":"/tmp/computer-use"},"interface":{"displayName":"Computer Use"}}
                ]
            }]
        }))
        .unwrap();

        let entries = catalog.entries();
        assert_eq!(entries[0].display_name(), "Computer Use");
        assert!(entries[0].is_computer_use());
        assert_eq!(
            entries[0].local_root(),
            Some(PathBuf::from("/tmp/computer-use"))
        );
        assert_eq!(
            entries[0].locator_params(),
            json!({"pluginName":"computer-use","marketplacePath":"/tmp/marketplace.json"})
        );
        assert!(catalog.find_entry(PluginEntry::is_computer_use).is_some());
        let (filtered, total) = catalog.filtered_entries(true, "computer", 1);
        assert_eq!(total, 1);
        assert_eq!(filtered.len(), 1);
        assert_eq!(filtered[0].display_name(), "Computer Use");
    }

    #[test]
    fn remote_catalog_rendering_is_bounded_before_entries_are_cloned() {
        let plugins = (0..500)
            .map(|index| {
                json!({
                    "id": format!("plugin-{index}@remote"),
                    "name": format!("plugin-{index}"),
                    "installed": false,
                    "source": {"type": "remote"}
                })
            })
            .collect::<Vec<_>>();
        let catalog = PluginCatalog::parse(json!({
            "marketplaces": [{"name": "remote", "plugins": plugins}]
        }))
        .unwrap();
        let (visible, total) = catalog.filtered_entries(false, "", 120);
        assert_eq!(total, 500);
        assert_eq!(visible.len(), 120);
    }

    #[test]
    fn config_keys_quote_plugin_and_server_names() {
        assert_eq!(
            plugin_enabled_key("computer-use@openai-bundled"),
            "plugins.\"computer-use@openai-bundled\".enabled"
        );
        assert_eq!(app_enabled_key("my.app"), "apps.\"my.app\".enabled");
        assert_eq!(
            plugin_mcp_key("computer-use@openai-bundled", "computer-use", "enabled"),
            "plugins.\"computer-use@openai-bundled\".mcp_servers.\"computer-use\".enabled"
        );
    }

    #[test]
    fn app_toggle_requires_account_access_before_catalog_default_counts() {
        assert_eq!(
            app_toggle_state(&json!({
                "isAccessible": false,
                "isEnabled": true
            })),
            AppToggleState {
                connected: false,
                active: false
            }
        );
        assert_eq!(
            app_toggle_state(&json!({
                "isAccessible": true,
                "isEnabled": true
            })),
            AppToggleState {
                connected: true,
                active: true
            }
        );
        assert_eq!(
            app_toggle_state(&json!({
                "isAccessible": true,
                "isEnabled": false
            })),
            AppToggleState {
                connected: true,
                active: false
            }
        );
        assert_eq!(
            app_toggle_state(&json!({ "isEnabled": true })),
            AppToggleState {
                connected: false,
                active: false
            }
        );
    }
}
