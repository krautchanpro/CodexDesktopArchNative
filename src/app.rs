use std::{cell::Cell, rc::Rc, time::Duration};

use adw::prelude::*;
use gtk::gdk;

use crate::{backend::AccountHub, ui::MainWindow};

pub fn activate(
    application: &adw::Application,
    hub: AccountHub,
    tray_available: bool,
    tray_restore_in_progress: Rc<Cell<bool>>,
) {
    if present_existing(application, &tray_restore_in_progress) {
        return;
    }

    load_css();
    let window = MainWindow::build(application, hub, tray_available, tray_restore_in_progress);
    window.present();
}

pub fn present_or_activate(
    application: &adw::Application,
    tray_restore_in_progress: &Rc<Cell<bool>>,
) {
    if !present_existing(application, tray_restore_in_progress) {
        application.activate();
    }
}

fn present_existing(
    application: &adw::Application,
    tray_restore_in_progress: &Rc<Cell<bool>>,
) -> bool {
    let windows = application.windows();
    let Some(window) = windows.into_iter().next() else {
        tracing::debug!("application activation found no existing main window");
        return false;
    };
    tracing::debug!("presenting the existing main window from the tray");
    tray_restore_in_progress.set(true);
    window.set_visible(true);
    window.unminimize();
    window.present();
    let weak_window = window.downgrade();
    let tray_restore_in_progress = tray_restore_in_progress.clone();
    glib::timeout_add_local_once(Duration::from_millis(350), move || {
        if let Some(window) = weak_window.upgrade() {
            window.unminimize();
            window.present();
            if let Some(toplevel) = window
                .surface()
                .and_then(|surface| surface.downcast::<gdk::Toplevel>().ok())
            {
                toplevel.focus(gdk::CURRENT_TIME);
            }
            tracing::debug!(
                visible = window.is_visible(),
                mapped = window.is_mapped(),
                realized = window.is_realized(),
                active = window.is_active(),
                "tray window restore settled"
            );
        }
        tray_restore_in_progress.set(false);
    });
    true
}

fn load_css() {
    let provider = gtk::CssProvider::new();
    provider.load_from_string(include_str!("../data/style.css"));
    if let Some(display) = gdk::Display::default() {
        gtk::style_context_add_provider_for_display(
            &display,
            &provider,
            gtk::STYLE_PROVIDER_PRIORITY_APPLICATION,
        );
    }
}
