use serde::{Deserialize, Serialize};

pub const MODE_MANUAL: &str = "manual";
pub const MODE_QWEN_ASSIST: &str = "qwen-assist";
pub const MODE_QWEN: &str = "qwen";
pub const MODE_GEMINI: &str = "gemini";
pub const MODE_OPENROUTER: &str = "openrouter";
pub const MODE_MISTRAL: &str = "mistral";
const LEGACY_MODE_AUTO_BALANCED: &str = "auto-balanced";
const LEGACY_MODE_AUTO_MAX_SAVINGS: &str = "auto-max-savings";
const LEGACY_MODE_LOCKED_SOL_MAX: &str = "locked-sol-max";
const LEGACY_MODE_AUTO_SAVER: &str = "auto-saver";
const LEGACY_MODE_LOCAL: &str = concat!("g", "rm");

const LUNA_MODEL: &str = "gpt-5.6-luna";
const TERRA_MODEL: &str = "gpt-5.6-terra";
const SOL_MODEL: &str = "gpt-5.6-sol";
const MAX_RECORDED_TURN_ROUTES: usize = 100;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct RouteDecision {
    pub model: String,
    pub effort: String,
    #[serde(default = "standard_service_tier")]
    pub service_tier: String,
    pub reason: String,
    #[serde(default)]
    pub automatic: bool,
    /// A local Qwen lane selected for this turn. The canonical turn still
    /// runs through Codex so desktop and iOS retain one thread identity.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub local_route: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct TokenSavingsReceipt {
    /// Conservative client-side estimate versus the Sol Max baseline.
    pub potential_tokens_saved: u64,
    /// Portion attributed to choosing a cheaper ChatGPT model/effort.
    #[serde(default)]
    pub automatic_routing_tokens_saved: u64,
    /// Portion attributed to successful non-GPT Buddy inference events.
    #[serde(default)]
    pub qwen_tokens_saved: u64,
    #[serde(default)]
    pub qwen_local_tokens: u64,
    #[serde(default)]
    pub qwen_successful_uses: u32,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub qwen_routes: Vec<String>,
    /// Measured turn usage when cumulative or per-turn app-server telemetry
    /// made it available. Prompts and response text are never retained.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub observed_turn_tokens: Option<u64>,
    pub basis: String,
}

impl TokenSavingsReceipt {
    pub fn display_label(&self) -> String {
        if self.qwen_successful_uses > 0 {
            format!(
                "Potential GPT tokens saved: ~{} · Buddy ~{} · local estimate",
                grouped_number(self.potential_tokens_saved),
                grouped_number(self.qwen_tokens_saved),
            )
        } else {
            format!(
                "Potential GPT tokens saved: ~{} · local estimate",
                grouped_number(self.potential_tokens_saved)
            )
        }
    }

    pub fn tooltip(&self) -> String {
        let observed = self
            .observed_turn_tokens
            .map(|tokens| format!("Observed turn usage: {} tokens. ", grouped_number(tokens)))
            .unwrap_or_else(|| "Observed turn usage was unavailable. ".into());
        format!(
            "{observed}{} Generated locally after completion; this receipt is never added to the prompt, conversation, or model context.",
            self.basis
        )
    }
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct QwenSavingsEvidence {
    pub observed_uses: u32,
    pub successful_uses: u32,
    pub failed_or_rejected_uses: u32,
    pub model_calls: u32,
    pub local_prompt_tokens: u64,
    pub local_output_tokens: u64,
    pub potential_tokens_saved: u64,
    pub routes: Vec<String>,
}

impl QwenSavingsEvidence {
    pub fn record(
        &mut self,
        route: &str,
        successful: bool,
        model_calls: u32,
        local_prompt_tokens: u64,
        local_output_tokens: u64,
        potential_tokens_saved: u64,
    ) {
        self.observed_uses = self.observed_uses.saturating_add(1);
        self.model_calls = self.model_calls.saturating_add(model_calls);
        self.local_prompt_tokens = self.local_prompt_tokens.saturating_add(local_prompt_tokens);
        self.local_output_tokens = self.local_output_tokens.saturating_add(local_output_tokens);
        if successful {
            self.successful_uses = self.successful_uses.saturating_add(1);
            self.potential_tokens_saved = self
                .potential_tokens_saved
                .saturating_add(potential_tokens_saved);
            if !self.routes.iter().any(|value| value == route) {
                self.routes.push(route.to_owned());
            }
        } else {
            self.failed_or_rejected_uses = self.failed_or_rejected_uses.saturating_add(1);
        }
    }

    pub fn local_tokens(&self) -> u64 {
        self.local_prompt_tokens
            .saturating_add(self.local_output_tokens)
    }
}

impl RouteDecision {
    pub fn display_label(&self) -> String {
        let prefix = if self.automatic {
            "Qwen Assist"
        } else {
            "Manual"
        };
        let mut label = format!(
            "{prefix} → {} · {} · {}",
            model_label(&self.model),
            effort_label(&self.effort),
            if self.service_tier == "priority" {
                "Fast"
            } else {
                "Standard"
            }
        );
        if let Some(local_route) = self.local_route.as_deref() {
            label.push_str(" · ");
            label.push_str(local_route);
        }
        label
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct TurnRouteRecord {
    pub turn_id: String,
    pub route: RouteDecision,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub token_savings: Option<TokenSavingsReceipt>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct TaskRoutingState {
    #[serde(default = "manual_mode")]
    pub mode: String,
    /// Whether this task may use any app-managed local or cloud subagent.
    /// Missing values stay enabled for backwards-compatible task state.
    #[serde(default = "subagents_enabled_by_default")]
    pub allow_subagents: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub delegate_effort: Option<String>,
    #[serde(default)]
    pub last_route: Option<RouteDecision>,
    #[serde(default)]
    pub turn_routes: Vec<TurnRouteRecord>,
}

impl Default for TaskRoutingState {
    fn default() -> Self {
        Self::new(MODE_MANUAL)
    }
}

impl TaskRoutingState {
    pub fn new(mode: &str) -> Self {
        Self {
            mode: normalize_mode(mode).to_owned(),
            allow_subagents: true,
            delegate_effort: None,
            last_route: None,
            turn_routes: Vec::new(),
        }
    }

    pub fn set_mode(&mut self, mode: &str) {
        self.mode = normalize_mode(mode).to_owned();
    }

    pub fn record_turn(&mut self, turn_id: &str, route: RouteDecision) {
        self.turn_routes.retain(|record| record.turn_id != turn_id);
        self.turn_routes.push(TurnRouteRecord {
            turn_id: turn_id.to_owned(),
            route: route.clone(),
            token_savings: None,
        });
        if self.turn_routes.len() > MAX_RECORDED_TURN_ROUTES {
            let excess = self.turn_routes.len() - MAX_RECORDED_TURN_ROUTES;
            self.turn_routes.drain(..excess);
        }
        self.last_route = Some(route);
    }

    pub fn route_for_turn(&self, turn_id: &str) -> Option<&RouteDecision> {
        self.turn_routes
            .iter()
            .rev()
            .find(|record| record.turn_id == turn_id)
            .map(|record| &record.route)
    }

    pub fn record_token_savings(&mut self, turn_id: &str, receipt: TokenSavingsReceipt) -> bool {
        let Some(record) = self
            .turn_routes
            .iter_mut()
            .rev()
            .find(|record| record.turn_id == turn_id)
        else {
            return false;
        };
        record.token_savings = Some(receipt);
        true
    }

    pub fn token_savings_for_turn(&self, turn_id: &str) -> Option<&TokenSavingsReceipt> {
        self.turn_routes
            .iter()
            .rev()
            .find(|record| record.turn_id == turn_id)
            .and_then(|record| record.token_savings.as_ref())
    }
}

/// Apply the task-level subagent policy to a route immediately before it is
/// recorded or sent. This is intentionally independent of the UI state so a
/// hidden or stale control cannot re-enable a subagent route.
pub fn enforce_subagent_policy(mut route: RouteDecision, allow_subagents: bool) -> RouteDecision {
    if allow_subagents {
        return route;
    }
    route.local_route = None;
    route.automatic = false;
    route.reason = "Subagents are disabled for this task; Codex handles the turn directly.".into();
    route
}

pub fn normalize_mode(mode: &str) -> &'static str {
    match mode {
        MODE_QWEN | LEGACY_MODE_LOCAL => MODE_QWEN,
        MODE_GEMINI => MODE_GEMINI,
        MODE_OPENROUTER => MODE_OPENROUTER,
        MODE_MISTRAL => MODE_MISTRAL,
        MODE_QWEN_ASSIST
        | LEGACY_MODE_AUTO_BALANCED
        | LEGACY_MODE_AUTO_MAX_SAVINGS
        | LEGACY_MODE_AUTO_SAVER => MODE_MANUAL,
        LEGACY_MODE_LOCKED_SOL_MAX => MODE_MANUAL,
        _ => MODE_MANUAL,
    }
}

pub fn is_auto_mode(mode: &str) -> bool {
    normalize_mode(mode) == MODE_QWEN_ASSIST
}

pub fn is_delegate_mode(mode: &str) -> bool {
    matches!(
        normalize_mode(mode),
        MODE_QWEN | MODE_GEMINI | MODE_OPENROUTER | MODE_MISTRAL
    )
}

pub fn is_direct_delegate_mode(mode: &str) -> bool {
    matches!(
        normalize_mode(mode),
        MODE_GEMINI | MODE_OPENROUTER | MODE_MISTRAL
    )
}

pub fn qwen_assist_route(model: &str, effort: &str, service_tier: Option<&str>) -> RouteDecision {
    RouteDecision {
        model: model.to_owned(),
        effort: effort.to_owned(),
        service_tier: service_tier.unwrap_or("standard").to_owned(),
        reason: "Codex keeps this task's selected model, reasoning, and speed; Qwen Buddy may handle eligible low-level work as a bounded local subagent.".into(),
        automatic: true,
        local_route: None,
    }
}

pub fn manual_route(model: &str, effort: &str, service_tier: Option<&str>) -> RouteDecision {
    RouteDecision {
        model: model.to_owned(),
        effort: effort.to_owned(),
        service_tier: service_tier.unwrap_or("standard").to_owned(),
        reason: "Selected manually for this task.".into(),
        automatic: false,
        local_route: None,
    }
}

pub fn model_label(model: &str) -> String {
    match model {
        LUNA_MODEL => "Luna".into(),
        TERRA_MODEL => "Terra".into(),
        SOL_MODEL => "Sol".into(),
        "" => "Default model".into(),
        other => other.to_owned(),
    }
}

pub fn effort_label(effort: &str) -> &'static str {
    match effort {
        "low" => "Light",
        "medium" => "Medium",
        "high" => "High",
        "xhigh" => "Extra High",
        "max" => "Max",
        "ultra" => "Ultra",
        _ => "Default",
    }
}

fn standard_service_tier() -> String {
    "standard".into()
}

fn manual_mode() -> String {
    MODE_MANUAL.into()
}

fn subagents_enabled_by_default() -> bool {
    true
}

#[cfg(test)]
pub fn estimate_token_savings(
    route: &RouteDecision,
    observed_turn_tokens: Option<u64>,
    measurement: &str,
) -> TokenSavingsReceipt {
    estimate_token_savings_with_qwen(
        route,
        observed_turn_tokens,
        measurement,
        &QwenSavingsEvidence::default(),
    )
}

pub fn estimate_token_savings_with_qwen(
    route: &RouteDecision,
    observed_turn_tokens: Option<u64>,
    measurement: &str,
    qwen: &QwenSavingsEvidence,
) -> TokenSavingsReceipt {
    let automatic_routing_tokens_saved = 0_u64;
    let mut basis = format!(
        "Codex kept the task's selected model, reasoning, and speed, so no model-routing savings were claimed ({measurement})."
    );
    if qwen.successful_uses > 0 {
        let routes = if qwen.routes.is_empty() {
            "Qwen Buddy".to_owned()
        } else {
            qwen.routes.join(", ")
        };
        basis.push_str(&format!(
            " Buddy evidence: {} successful use(s) across {routes}, {} non-GPT model call(s), and {} measured non-GPT tokens. The estimate counts successful prompt and output tokens handled outside GPT; condenser context avoidance uses the larger non-duplicated total.",
            qwen.successful_uses,
            qwen.model_calls,
            grouped_number(qwen.local_tokens()),
        ));
    } else if route.local_route.is_some() {
        basis.push_str(
            " A Buddy model was selected for this turn, but no successful hidden usage event was observed, so no non-GPT savings were claimed.",
        );
    }
    if qwen.failed_or_rejected_uses > 0 {
        basis.push_str(&format!(
            " {} rejected or failed Buddy use(s) contributed zero savings.",
            qwen.failed_or_rejected_uses
        ));
    }
    let qwen_tokens_saved = qwen.potential_tokens_saved;
    let potential_tokens_saved = automatic_routing_tokens_saved.saturating_add(qwen_tokens_saved);
    TokenSavingsReceipt {
        potential_tokens_saved,
        automatic_routing_tokens_saved,
        qwen_tokens_saved,
        qwen_local_tokens: qwen.local_tokens(),
        qwen_successful_uses: qwen.successful_uses,
        qwen_routes: qwen.routes.clone(),
        observed_turn_tokens,
        basis,
    }
}

fn grouped_number(value: u64) -> String {
    let digits = value.to_string();
    let mut grouped = String::with_capacity(digits.len() + digits.len() / 3);
    for (index, character) in digits.chars().enumerate() {
        if index > 0 && (digits.len() - index).is_multiple_of(3) {
            grouped.push(',');
        }
        grouped.push(character);
    }
    grouped
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn legacy_model_router_modes_migrate_to_manual() {
        for mode in [
            LEGACY_MODE_AUTO_BALANCED,
            LEGACY_MODE_AUTO_MAX_SAVINGS,
            LEGACY_MODE_AUTO_SAVER,
        ] {
            assert_eq!(normalize_mode(mode), MODE_MANUAL);
        }
        assert_eq!(normalize_mode(LEGACY_MODE_LOCKED_SOL_MAX), MODE_MANUAL);
        assert_eq!(normalize_mode(LEGACY_MODE_LOCAL), MODE_QWEN);
    }

    #[test]
    fn qwen_assist_keeps_selected_codex_settings() {
        let route = qwen_assist_route(SOL_MODEL, "max", Some("priority"));
        assert_eq!(route.model, SOL_MODEL);
        assert_eq!(route.effort, "max");
        assert_eq!(route.service_tier, "priority");
        assert!(route.automatic);
        assert!(route.display_label().starts_with("Qwen Assist"));
    }

    #[test]
    fn qwen_runs_through_sol_while_cloud_buddies_remain_direct() {
        assert!(is_delegate_mode(MODE_QWEN));
        assert!(!is_direct_delegate_mode(MODE_QWEN));
        assert!(is_direct_delegate_mode(MODE_GEMINI));
        assert!(is_direct_delegate_mode(MODE_OPENROUTER));
        assert!(is_direct_delegate_mode(MODE_MISTRAL));
    }

    #[test]
    fn turn_route_history_is_bounded_and_replaceable() {
        let mut state = TaskRoutingState::new(MODE_QWEN_ASSIST);
        for index in 0..105 {
            state.record_turn(
                &format!("turn-{index}"),
                qwen_assist_route(SOL_MODEL, "max", None),
            );
        }
        assert_eq!(state.turn_routes.len(), MAX_RECORDED_TURN_ROUTES);
        assert!(state.route_for_turn("turn-0").is_none());
        assert!(state.route_for_turn("turn-104").is_some());
    }

    #[test]
    fn task_subagents_default_on_and_support_legacy_state() {
        assert!(TaskRoutingState::default().allow_subagents);
        let state: TaskRoutingState = serde_json::from_value(serde_json::json!({
            "mode": MODE_MANUAL
        }))
        .unwrap();
        assert!(state.allow_subagents);
    }

    #[test]
    fn disabled_subagent_policy_strips_local_route() {
        let route = RouteDecision {
            model: SOL_MODEL.into(),
            effort: "high".into(),
            service_tier: "standard".into(),
            reason: "automatic test route".into(),
            automatic: true,
            local_route: Some("Qwen Sol subagent".into()),
        };
        let route = enforce_subagent_policy(route, false);
        assert!(!route.automatic);
        assert!(route.local_route.is_none());
        assert!(route.reason.contains("disabled"));
    }

    #[test]
    fn savings_receipts_are_bounded_local_metadata() {
        let route = qwen_assist_route(SOL_MODEL, "max", None);
        let receipt = estimate_token_savings(&route, Some(10_000), "cumulative delta");
        assert_eq!(receipt.potential_tokens_saved, 0);
        assert_eq!(receipt.automatic_routing_tokens_saved, 0);
        assert_eq!(receipt.qwen_tokens_saved, 0);
        assert!(receipt.tooltip().contains("never added to the prompt"));

        let mut state = TaskRoutingState::new(MODE_QWEN_ASSIST);
        state.record_turn("turn", route.clone());
        assert!(state.record_token_savings("turn", receipt.clone()));
        assert_eq!(state.token_savings_for_turn("turn"), Some(&receipt));

        let wire_route = serde_json::to_value(route).unwrap();
        assert!(
            wire_route.get("tokenSavings").is_none(),
            "a local savings receipt must never enter turn/start route metadata"
        );
    }

    #[test]
    fn successful_qwen_evidence_adds_only_conservative_measured_savings() {
        let route = RouteDecision {
            model: TERRA_MODEL.into(),
            effort: "high".into(),
            service_tier: "standard".into(),
            reason: "test".into(),
            automatic: true,
            local_route: Some("Qwen Sol subagent".into()),
        };
        let mut qwen = QwenSavingsEvidence::default();
        qwen.record("Qwen Sol subagent", true, 4, 3_000, 700, 700);
        qwen.record("Qwen Condenser", true, 2, 1_500, 200, 8_000);
        qwen.record("Qwen Sol subagent", false, 0, 0, 0, 9_999);

        let receipt =
            estimate_token_savings_with_qwen(&route, Some(10_000), "test telemetry", &qwen);
        assert_eq!(receipt.automatic_routing_tokens_saved, 0);
        assert_eq!(receipt.qwen_tokens_saved, 8_700);
        assert_eq!(receipt.potential_tokens_saved, 8_700);
        assert_eq!(receipt.qwen_local_tokens, 5_400);
        assert_eq!(receipt.qwen_successful_uses, 2);
        assert_eq!(
            receipt.qwen_routes,
            vec!["Qwen Sol subagent", "Qwen Condenser"]
        );
        assert!(receipt.display_label().contains("Buddy ~8,700"));
        assert!(receipt.basis.contains("prompt and output tokens"));
        assert!(receipt.basis.contains("contributed zero savings"));
    }

    #[test]
    fn manual_routes_never_claim_automatic_savings() {
        let route = manual_route(SOL_MODEL, "max", None);
        let receipt = estimate_token_savings(&route, Some(25_000), "cumulative delta");
        assert_eq!(receipt.potential_tokens_saved, 0);
        assert!(receipt.basis.contains("no model-routing savings"));
    }

    #[test]
    fn manual_routes_still_count_observed_qwen_savings() {
        let route = manual_route(SOL_MODEL, "max", None);
        let mut qwen = QwenSavingsEvidence::default();
        qwen.record("Qwen Luna", true, 1, 500, 120, 120);
        let receipt =
            estimate_token_savings_with_qwen(&route, Some(25_000), "test telemetry", &qwen);
        assert_eq!(receipt.automatic_routing_tokens_saved, 0);
        assert_eq!(receipt.qwen_tokens_saved, 120);
        assert_eq!(receipt.potential_tokens_saved, 120);
    }
}
