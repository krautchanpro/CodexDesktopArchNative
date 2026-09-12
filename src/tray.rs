use std::sync::mpsc::{self, Receiver, Sender};

use ksni::blocking::{Handle, TrayMethods};

const APP_ID: &str = "io.codexnative.Arch";

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum TrayCommand {
    Show,
    Quit,
}

struct CodexTray {
    commands: Sender<TrayCommand>,
}

impl CodexTray {
    fn send(&self, command: TrayCommand) {
        let _ = self.commands.send(command);
    }
}

impl ksni::Tray for CodexTray {
    fn id(&self) -> String {
        APP_ID.into()
    }

    fn category(&self) -> ksni::Category {
        ksni::Category::ApplicationStatus
    }

    fn title(&self) -> String {
        "Codex Native".into()
    }

    fn icon_name(&self) -> String {
        APP_ID.into()
    }

    fn activate(&mut self, _x: i32, _y: i32) {
        self.send(TrayCommand::Show);
    }

    fn menu(&self) -> Vec<ksni::MenuItem<Self>> {
        use ksni::menu::StandardItem;

        vec![
            StandardItem {
                label: "Show Codex Native".into(),
                icon_name: "window-new".into(),
                activate: Box::new(|tray: &mut Self| tray.send(TrayCommand::Show)),
                ..Default::default()
            }
            .into(),
            ksni::MenuItem::Separator,
            StandardItem {
                label: "Quit".into(),
                icon_name: "application-exit".into(),
                activate: Box::new(|tray: &mut Self| tray.send(TrayCommand::Quit)),
                ..Default::default()
            }
            .into(),
        ]
    }

    fn watcher_offline(&self, reason: ksni::OfflineReason) -> bool {
        tracing::warn!(?reason, "system tray watcher is temporarily offline");
        true
    }
}

pub struct TrayService {
    commands: Receiver<TrayCommand>,
    handle: Option<Handle<CodexTray>>,
}

impl TrayService {
    pub fn start() -> Self {
        let (sender, commands) = mpsc::channel();
        let tray = CodexTray { commands: sender };
        let handle = match tray.spawn() {
            Ok(handle) => {
                tracing::info!("registered native StatusNotifier system tray item");
                Some(handle)
            }
            Err(error) => {
                tracing::warn!(%error, "system tray is unavailable; close will exit normally");
                None
            }
        };

        Self { commands, handle }
    }

    pub fn is_available(&self) -> bool {
        self.handle
            .as_ref()
            .is_some_and(|handle| !handle.is_closed())
    }

    pub fn drain_commands(&self) -> Vec<TrayCommand> {
        self.commands.try_iter().collect()
    }

    pub fn shutdown(&self) {
        if let Some(handle) = &self.handle {
            handle.shutdown().wait();
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use ksni::Tray;

    fn test_tray() -> (CodexTray, Receiver<TrayCommand>) {
        let (commands, receiver) = mpsc::channel();
        (CodexTray { commands }, receiver)
    }

    #[test]
    fn metadata_uses_the_installed_desktop_identity() {
        let (tray, _) = test_tray();
        assert_eq!(tray.id(), APP_ID);
        assert_eq!(tray.icon_name(), APP_ID);
        assert_eq!(tray.title(), "Codex Native");
    }

    #[test]
    fn primary_activation_requests_the_main_window() {
        let (mut tray, receiver) = test_tray();
        tray.activate(0, 0);
        assert_eq!(receiver.try_recv(), Ok(TrayCommand::Show));
    }

    #[test]
    fn menu_exposes_show_and_explicit_quit() {
        let (tray, _) = test_tray();
        let labels = tray
            .menu()
            .into_iter()
            .filter_map(|item| match item {
                ksni::MenuItem::Standard(item) => Some(item.label),
                _ => None,
            })
            .collect::<Vec<_>>();
        assert_eq!(labels, ["Show Codex Native", "Quit"]);
    }
}
