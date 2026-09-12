mod activity;
mod app;
mod automation;
mod backend;
mod chatgpt;
mod context;
mod history;
mod host;
mod markdown;
mod model;
mod persistence;
mod plugin;
mod protocol;
mod qwen;
mod routing;
mod tray;
mod ui;

use std::{cell::Cell, env, rc::Rc, sync::Arc, time::Duration};

use adw::prelude::*;
use anyhow::Context;
use backend::AccountHub;
use tracing_subscriber::EnvFilter;

const APP_ID: &str = "io.codexnative.Arch";

fn main() -> glib::ExitCode {
    if let Some(result) = run_cli_mode() {
        return match result {
            Ok(()) => glib::ExitCode::SUCCESS,
            Err(error) => {
                eprintln!("codex-native: {error:#}");
                glib::ExitCode::FAILURE
            }
        };
    }
    if let Err(error) = run() {
        eprintln!("codex-native: {error:#}");
        return glib::ExitCode::FAILURE;
    }
    glib::ExitCode::SUCCESS
}

fn run_cli_mode() -> Option<anyhow::Result<()>> {
    let mut args = env::args().skip(1);
    match args.next().as_deref() {
        Some("--run-automation") => Some((|| {
            let id = args
                .next()
                .context("--run-automation requires an automation UUID")?;
            let id = uuid::Uuid::parse_str(&id).context("invalid automation UUID")?;
            automation::run_automation(id)
        })()),
        Some("--run-automation-now") => Some((|| {
            let id = args
                .next()
                .context("--run-automation-now requires an automation UUID")?;
            let id = uuid::Uuid::parse_str(&id).context("invalid automation UUID")?;
            automation::run_automation_now(id)
        })()),
        Some("--qwen-report") => Some((|| {
            let report = qwen::inspect()?;
            println!("{}", serde_json::to_string_pretty(&report)?);
            Ok(())
        })()),
        Some("--watch-one-time-reset") => Some((|| {
            let threshold = args
                .next()
                .context("--watch-one-time-reset requires a remaining-percent threshold")?
                .parse::<u8>()
                .context("reset threshold must be an integer")?;
            let idempotency_key = args
                .next()
                .context("--watch-one-time-reset requires an authorization UUID")?;
            ui::run_one_time_reset_watch(threshold, idempotency_key)
        })()),
        Some("--version") | Some("-V") => {
            println!("codex-native {}", env!("CARGO_PKG_VERSION"));
            Some(Ok(()))
        }
        Some("--help") | Some("-h") => {
            println!(
                "Codex Native for Arch Linux\n\nUsage: codex-native [--run-automation UUID | --run-automation-now UUID | --qwen-report | --watch-one-time-reset PERCENT UUID]\n"
            );
            Some(Ok(()))
        }
        Some(_) | None => None,
    }
}

fn run() -> anyhow::Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| EnvFilter::new("codex_native=info,warn")),
        )
        .compact()
        .init();

    let runtime = Arc::new(
        tokio::runtime::Builder::new_multi_thread()
            .enable_all()
            .thread_name("codex-native-io")
            .build()
            .context("failed to create Tokio runtime")?,
    );
    let stored_state = persistence::load_state();
    let hub = AccountHub::spawn(
        runtime.clone(),
        stored_state.preferences.codex_binary.clone(),
    );

    let smoke_fixtures = env::var_os("CODEX_NATIVE_SMOKE_FIXTURES").is_some();
    let application_id = if smoke_fixtures {
        env::var("CODEX_NATIVE_SMOKE_APPLICATION_ID").unwrap_or_else(|_| APP_ID.into())
    } else {
        APP_ID.into()
    };
    if smoke_fixtures && env::var_os("CODEX_NATIVE_SMOKE_APPLICATION_ID").is_some() {
        let smoke_name =
            env::var("CODEX_NATIVE_SMOKE_APP_NAME").unwrap_or_else(|_| "codex-native-smoke".into());
        glib::set_prgname(Some(&smoke_name));
    }
    let application = adw::Application::builder()
        .application_id(&application_id)
        .build();
    application
        .register(None::<&gio::Cancellable>)
        .context("failed to register the Codex Native application")?;
    if application.is_remote() {
        application.run();
        return Ok(());
    }

    let tray = Rc::new(tray::TrayService::start());
    let tray_available = tray.is_available();
    let _tray_hold = tray_available.then(|| application.hold());
    let tray_restore_in_progress = Rc::new(Cell::new(false));

    let hub_activate = hub.clone();
    let tray_restore_activate = tray_restore_in_progress.clone();
    application.connect_activate(move |app| {
        app::activate(
            app,
            hub_activate.clone(),
            tray_available,
            tray_restore_activate.clone(),
        );
    });

    let weak_application = application.downgrade();
    let tray_commands = tray.clone();
    let tray_restore_commands = tray_restore_in_progress.clone();
    glib::timeout_add_local(Duration::from_millis(100), move || {
        let Some(application) = weak_application.upgrade() else {
            return glib::ControlFlow::Break;
        };
        for command in tray_commands.drain_commands() {
            tracing::debug!(?command, "handling system tray command");
            match command {
                tray::TrayCommand::Show => {
                    app::present_or_activate(&application, &tray_restore_commands)
                }
                tray::TrayCommand::Quit => {
                    for window in application.windows() {
                        window.close();
                    }
                    application.quit();
                }
            }
        }
        glib::ControlFlow::Continue
    });

    let hub_shutdown = hub.clone();
    let tray_shutdown = tray.clone();
    application.connect_shutdown(move |_| {
        tray_shutdown.shutdown();
        hub_shutdown.shutdown();
    });

    application.run();
    drop(hub);
    drop(runtime);
    Ok(())
}
