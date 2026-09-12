use std::{
    cell::{Cell, RefCell},
    fs,
    path::{Path, PathBuf},
    rc::Rc,
    time::Duration,
};

use adw::prelude::*;
use gtk::{gio, glib};
use webkit::prelude::*;

pub const CHATGPT_URL: &str = "https://chatgpt.com/";
const IDLE_UNLOAD_DELAY: Duration = Duration::from_secs(90);

/// Owns the one optional ChatGPT WebKit process used by the ChatGPT page.
///
/// Codex tasks, Markdown, diffs, settings, and every other page remain native GTK widgets.
/// The web process is created only when this page is opened and is destroyed after it has
/// remained in the background, or immediately when the user selects Unload.
pub struct ChatgptSurface {
    host: gtk::Box,
    window: adw::ApplicationWindow,
    toast_overlay: adw::ToastOverlay,
    status: gtk::Label,
    progress: gtk::ProgressBar,
    back: gtk::Button,
    forward: gtk::Button,
    reload: gtk::Button,
    data_directory: PathBuf,
    cache_directory: PathBuf,
    network_session: RefCell<Option<webkit::NetworkSession>>,
    web_context: RefCell<Option<webkit::WebContext>>,
    view: RefCell<Option<webkit::WebView>>,
    last_uri: RefCell<String>,
    idle_unload: RefCell<Option<glib::SourceId>>,
    active: Cell<bool>,
}

impl ChatgptSurface {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        host: &gtk::Box,
        window: &adw::ApplicationWindow,
        toast_overlay: &adw::ToastOverlay,
        status: &gtk::Label,
        progress: &gtk::ProgressBar,
        back: &gtk::Button,
        forward: &gtk::Button,
        reload: &gtk::Button,
    ) -> Rc<Self> {
        let (data_directory, cache_directory) = chatgpt_directories();
        for directory in [&data_directory, &cache_directory] {
            if let Err(error) = fs::create_dir_all(directory) {
                tracing::warn!(path = %directory.display(), %error, "could not create ChatGPT data directory");
            }
        }
        let surface = Rc::new(Self {
            host: host.clone(),
            window: window.clone(),
            toast_overlay: toast_overlay.clone(),
            status: status.clone(),
            progress: progress.clone(),
            back: back.clone(),
            forward: forward.clone(),
            reload: reload.clone(),
            data_directory,
            cache_directory,
            network_session: RefCell::new(None),
            web_context: RefCell::new(None),
            view: RefCell::new(None),
            last_uri: RefCell::new(CHATGPT_URL.to_owned()),
            idle_unload: RefCell::new(None),
            active: Cell::new(false),
        });
        surface.update_controls();
        surface
    }

    pub fn activate(self: &Rc<Self>) {
        self.active.set(true);
        self.cancel_idle_unload();
        self.ensure_loaded();
    }

    pub fn deactivate(self: &Rc<Self>) {
        self.active.set(false);
        self.schedule_idle_unload();
    }

    pub fn go_back(&self) {
        if let Some(view) = self.view.borrow().as_ref()
            && view.can_go_back()
        {
            view.go_back();
        }
    }

    pub fn go_forward(&self) {
        if let Some(view) = self.view.borrow().as_ref()
            && view.can_go_forward()
        {
            view.go_forward();
        }
    }

    pub fn go_home(self: &Rc<Self>) {
        self.activate();
        if let Some(view) = self.view.borrow().as_ref() {
            view.load_uri(CHATGPT_URL);
        }
    }

    pub fn reload(self: &Rc<Self>) {
        let was_loaded = self.view.borrow().is_some();
        self.activate();
        if was_loaded && let Some(view) = self.view.borrow().as_ref() {
            view.reload();
        }
    }

    pub fn unload(&self) {
        self.cancel_idle_unload();
        let Some(view) = self.view.borrow_mut().take() else {
            self.update_controls();
            return;
        };
        if let Some(uri) = view.uri() {
            *self.last_uri.borrow_mut() = uri.to_string();
        }
        view.stop_loading();
        self.host.remove(&view);
        view.terminate_web_process();
        drop(view);
        self.web_context.borrow_mut().take();
        self.network_session.borrow_mut().take();
        self.progress.set_visible(false);
        self.status.set_label(
            "ChatGPT unloaded; your sign-in and conversations remain in its private app profile.",
        );
        self.update_controls();
        schedule_allocator_trim();
    }

    fn ensure_loaded(self: &Rc<Self>) {
        if self.view.borrow().is_some() {
            self.update_controls();
            return;
        }

        let settings = webkit::Settings::builder()
            .enable_javascript(true)
            .enable_html5_database(true)
            .enable_html5_local_storage(true)
            .enable_media(true)
            .enable_media_capabilities(true)
            .enable_media_stream(true)
            .enable_mediasource(true)
            .enable_webaudio(true)
            .enable_webrtc(true)
            .enable_webgl(true)
            .enable_site_specific_quirks(true)
            .enable_back_forward_navigation_gestures(true)
            .javascript_can_access_clipboard(true)
            .media_playback_allows_inline(true)
            .media_playback_requires_user_gesture(false)
            .build();
        let network_session = webkit::NetworkSession::new(
            Some(self.data_directory.to_string_lossy().as_ref()),
            Some(self.cache_directory.to_string_lossy().as_ref()),
        );
        network_session.set_persistent_credential_storage_enabled(true);
        self.connect_downloads(&network_session);
        let mut memory_pressure = webkit::MemoryPressureSettings::new();
        memory_pressure.set_memory_limit(1024);
        memory_pressure.set_kill_threshold(0.90);
        memory_pressure.set_strict_threshold(0.75);
        memory_pressure.set_conservative_threshold(0.50);
        let web_context = webkit::WebContext::builder()
            .memory_pressure_settings(&memory_pressure)
            .build();
        web_context.set_cache_model(webkit::CacheModel::DocumentBrowser);
        let view = webkit::WebView::builder()
            .web_context(&web_context)
            .network_session(&network_session)
            .settings(&settings)
            .hexpand(true)
            .vexpand(true)
            .can_focus(true)
            .build();
        view.set_tooltip_text(Some("ChatGPT Pro and Voice"));

        self.connect_view(&view);
        self.host.append(&view);
        *self.web_context.borrow_mut() = Some(web_context);
        *self.network_session.borrow_mut() = Some(network_session);
        *self.view.borrow_mut() = Some(view.clone());
        self.status
            .set_label("Loading ChatGPT in the native window…");
        self.progress.set_fraction(0.0);
        self.progress.set_visible(true);
        view.load_uri(self.last_uri.borrow().as_str());
        self.update_controls();
    }

    fn connect_view(self: &Rc<Self>, view: &webkit::WebView) {
        let weak = Rc::downgrade(self);
        view.connect_load_changed(move |view, event| {
            let Some(surface) = weak.upgrade() else {
                return;
            };
            if !surface.is_current_view(view) {
                return;
            }
            match event {
                webkit::LoadEvent::Started => {
                    surface.status.set_label("Loading ChatGPT…");
                    surface.progress.set_visible(true);
                }
                webkit::LoadEvent::Finished => {
                    if let Some(uri) = view.uri() {
                        *surface.last_uri.borrow_mut() = uri.to_string();
                    }
                    surface.progress.set_visible(false);
                    surface.status.set_label(
                        "ChatGPT is running in-app. Select Pro in ChatGPT’s model picker or use its Voice button.",
                    );
                    surface.update_controls();
                }
                _ => {}
            }
        });

        let weak = Rc::downgrade(self);
        view.connect_estimated_load_progress_notify(move |view| {
            let Some(surface) = weak.upgrade() else {
                return;
            };
            if surface.is_current_view(view) {
                surface
                    .progress
                    .set_fraction(view.estimated_load_progress());
            }
        });

        let weak = Rc::downgrade(self);
        view.connect_load_failed(move |view, _, uri, error| {
            let Some(surface) = weak.upgrade() else {
                return false;
            };
            if surface.is_current_view(view) {
                surface.progress.set_visible(false);
                surface
                    .status
                    .set_label(&format!("Could not load {uri}: {error}"));
            }
            false
        });

        let weak = Rc::downgrade(self);
        view.connect_web_process_terminated(move |view, reason| {
            let Some(surface) = weak.upgrade() else {
                return;
            };
            if surface.is_current_view(view) {
                surface.progress.set_visible(false);
                surface.status.set_label(&format!(
                    "ChatGPT’s isolated web process stopped ({reason:?}). Select Reload to restart it."
                ));
            }
        });

        let weak = Rc::downgrade(self);
        view.connect_decide_policy(move |_, decision, kind| {
            if !matches!(
                kind,
                webkit::PolicyDecisionType::NavigationAction
                    | webkit::PolicyDecisionType::NewWindowAction
            ) {
                return false;
            }
            let Ok(navigation) = decision
                .clone()
                .downcast::<webkit::NavigationPolicyDecision>()
            else {
                return false;
            };
            let Some(mut action) = navigation.navigation_action() else {
                return false;
            };
            let Some(uri) = action.request().and_then(|request| request.uri()) else {
                return false;
            };
            if is_in_app_navigation(&uri) {
                return false;
            }
            decision.ignore();
            if let Some(surface) = weak.upgrade() {
                surface.open_external(&uri);
            }
            true
        });

        let weak = Rc::downgrade(self);
        view.connect_permission_request(move |view, request| {
            let Some(surface) = weak.upgrade() else {
                request.deny();
                return true;
            };
            let Ok(media) = request
                .clone()
                .downcast::<webkit::UserMediaPermissionRequest>()
            else {
                return false;
            };
            if !view.uri().as_deref().is_some_and(is_in_app_navigation) {
                request.deny();
                return true;
            }
            surface.confirm_media_permission(request.clone(), &media);
            true
        });

        let weak = Rc::downgrade(self);
        view.connect_run_file_chooser(move |_, request| {
            let Some(surface) = weak.upgrade() else {
                request.cancel();
                return true;
            };
            surface.choose_upload(request);
            true
        });
    }

    fn confirm_media_permission(
        &self,
        request: webkit::PermissionRequest,
        media: &webkit::UserMediaPermissionRequest,
    ) {
        let audio = media.is_for_audio_device();
        let video = media.is_for_video_device();
        if !audio && !video {
            request.deny();
            return;
        }
        let device = match (audio, video) {
            (true, true) => "microphone and camera",
            (true, false) => "microphone",
            (false, true) => "camera",
            (false, false) => unreachable!(),
        };
        let dialog = adw::AlertDialog::new(
            Some(&format!("Allow ChatGPT to use your {device}?")),
            Some(
                "Access is granted only to the in-app ChatGPT session and ends when that session is unloaded.",
            ),
        );
        dialog.add_responses(&[("deny", "Don’t allow"), ("allow", "Allow")]);
        dialog.set_default_response(Some("deny"));
        dialog.set_close_response("deny");
        dialog.choose(
            Some(&self.window),
            None::<&gio::Cancellable>,
            move |response| {
                if response.as_str() == "allow" {
                    request.allow();
                } else {
                    request.deny();
                }
            },
        );
    }

    fn choose_upload(&self, request: &webkit::FileChooserRequest) {
        let dialog = gtk::FileDialog::builder()
            .title("Attach files to ChatGPT")
            .modal(true)
            .build();
        if let Some(filter) = request.mime_types_filter() {
            dialog.set_default_filter(Some(&filter));
        }
        let request = request.clone();
        if request.selects_multiple() {
            dialog.open_multiple(
                Some(&self.window),
                None::<&gio::Cancellable>,
                move |result| match result {
                    Ok(files) => {
                        let selected = (0..files.n_items())
                            .filter_map(|index| files.item(index).and_downcast::<gio::File>())
                            .filter_map(|file| file.path())
                            .map(|path| path.to_string_lossy().into_owned())
                            .collect::<Vec<_>>();
                        let selected = selected.iter().map(String::as_str).collect::<Vec<_>>();
                        request.select_files(&selected);
                    }
                    Err(_) => request.cancel(),
                },
            );
        } else {
            dialog.open(
                Some(&self.window),
                None::<&gio::Cancellable>,
                move |result| match result {
                    Ok(file) => {
                        if let Some(path) = file.path() {
                            let path = path.to_string_lossy();
                            request.select_files(&[path.as_ref()]);
                        } else {
                            request.cancel();
                        }
                    }
                    Err(_) => request.cancel(),
                },
            );
        }
    }

    fn connect_downloads(self: &Rc<Self>, network_session: &webkit::NetworkSession) {
        let weak = Rc::downgrade(self);
        network_session.connect_download_started(move |_, download| {
            let weak_destination = weak.clone();
            download.connect_decide_destination(move |download, suggested| {
                let Some(surface) = weak_destination.upgrade() else {
                    return false;
                };
                let Some(destination) = unique_download_destination(suggested) else {
                    surface.toast("Could not prepare a ChatGPT download destination");
                    return false;
                };
                let uri = gio::File::for_path(&destination).uri();
                download.set_allow_overwrite(false);
                download.set_destination(&uri);
                true
            });
            let weak_finished = weak.clone();
            download.connect_finished(move |download| {
                if let Some(surface) = weak_finished.upgrade() {
                    let destination = download
                        .destination()
                        .and_then(|uri| gio::File::for_uri(&uri).path())
                        .map(|path| path.display().to_string())
                        .unwrap_or_else(|| "Downloads".to_owned());
                    surface.toast(&format!("ChatGPT download saved to {destination}"));
                }
            });
            let weak_failed = weak.clone();
            download.connect_failed(move |_, error| {
                if let Some(surface) = weak_failed.upgrade() {
                    surface.toast(&format!("ChatGPT download failed: {error}"));
                }
            });
        });
    }

    fn schedule_idle_unload(self: &Rc<Self>) {
        self.cancel_idle_unload();
        if self.view.borrow().is_none() {
            return;
        }
        let weak = Rc::downgrade(self);
        let source = glib::timeout_add_local_once(IDLE_UNLOAD_DELAY, move || {
            let Some(surface) = weak.upgrade() else {
                return;
            };
            surface.idle_unload.borrow_mut().take();
            if !surface.active.get() {
                surface.unload();
            }
        });
        *self.idle_unload.borrow_mut() = Some(source);
    }

    fn cancel_idle_unload(&self) {
        if let Some(source) = self.idle_unload.borrow_mut().take() {
            source.remove();
        }
    }

    fn update_controls(&self) {
        let view = self.view.borrow();
        self.back
            .set_sensitive(view.as_ref().is_some_and(|view| view.can_go_back()));
        self.forward
            .set_sensitive(view.as_ref().is_some_and(|view| view.can_go_forward()));
        self.reload.set_sensitive(true);
    }

    fn is_current_view(&self, candidate: &webkit::WebView) -> bool {
        self.view
            .borrow()
            .as_ref()
            .is_some_and(|current| current == candidate)
    }

    fn open_external(&self, uri: &str) {
        let launcher = gtk::UriLauncher::new(uri);
        let overlay = self.toast_overlay.clone();
        launcher.launch(
            Some(&self.window),
            None::<&gio::Cancellable>,
            move |result| {
                if let Err(error) = result {
                    overlay.add_toast(adw::Toast::new(&format!(
                        "Could not open external link: {error}"
                    )));
                }
            },
        );
    }

    fn toast(&self, message: &str) {
        self.toast_overlay.add_toast(adw::Toast::new(message));
    }
}

#[cfg(all(target_os = "linux", target_env = "gnu"))]
fn schedule_allocator_trim() {
    glib::timeout_add_local_once(Duration::from_secs(2), || {
        // WebKit releases large GTK-side allocations asynchronously after its view is removed.
        // Returning free arenas to glibc keeps the optional surface from inflating idle RSS.
        unsafe {
            libc::malloc_trim(0);
        }
    });
}

#[cfg(not(all(target_os = "linux", target_env = "gnu")))]
fn schedule_allocator_trim() {}

fn chatgpt_directories() -> (PathBuf, PathBuf) {
    let data = dirs::data_dir()
        .unwrap_or_else(std::env::temp_dir)
        .join("codex-native/chatgpt");
    let cache = dirs::cache_dir()
        .unwrap_or_else(std::env::temp_dir)
        .join("codex-native/chatgpt");
    (data, cache)
}

fn unique_download_destination(suggested: &str) -> Option<PathBuf> {
    let directory = dirs::download_dir()?;
    fs::create_dir_all(&directory).ok()?;
    let filename = Path::new(suggested)
        .file_name()
        .and_then(|name| name.to_str())
        .filter(|name| !name.is_empty())
        .unwrap_or("chatgpt-download");
    let candidate = directory.join(filename);
    if !candidate.exists() {
        return Some(candidate);
    }
    let path = Path::new(filename);
    let stem = path
        .file_stem()
        .and_then(|value| value.to_str())
        .unwrap_or("download");
    let extension = path.extension().and_then(|value| value.to_str());
    (1..10_000)
        .map(|index| match extension {
            Some(extension) => directory.join(format!("{stem} ({index}).{extension}")),
            None => directory.join(format!("{stem} ({index})")),
        })
        .find(|path| !path.exists())
}

fn is_in_app_navigation(uri: &str) -> bool {
    if uri.starts_with("about:") || uri.starts_with("blob:") || uri.starts_with("data:") {
        return true;
    }
    let Some(authority) = uri
        .strip_prefix("https://")
        .or_else(|| uri.strip_prefix("http://"))
        .and_then(|rest| rest.split('/').next())
    else {
        return false;
    };
    let host = authority
        .rsplit('@')
        .next()
        .unwrap_or(authority)
        .split(':')
        .next()
        .unwrap_or(authority)
        .trim_end_matches('.')
        .to_ascii_lowercase();
    [
        "chatgpt.com",
        "openai.com",
        "auth0.com",
        "google.com",
        "microsoftonline.com",
        "live.com",
        "apple.com",
        "cloudflare.com",
    ]
    .iter()
    .any(|domain| host == *domain || host.ends_with(&format!(".{domain}")))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn only_chatgpt_and_login_hosts_stay_in_app() {
        assert!(is_in_app_navigation("https://chatgpt.com/"));
        assert!(is_in_app_navigation("https://auth.openai.com/log-in"));
        assert!(is_in_app_navigation(
            "https://accounts.google.com/o/oauth2/v2/auth"
        ));
        assert!(is_in_app_navigation("about:srcdoc"));
        assert!(is_in_app_navigation("about:blank"));
        assert!(!is_in_app_navigation("https://example.com/openai.com"));
        assert!(!is_in_app_navigation("https://chatgpt.com@evil.example/"));
        assert!(!is_in_app_navigation("file:///etc/passwd"));
        assert!(!is_in_app_navigation("javascript:alert(1)"));
    }

    #[test]
    fn chatgpt_surface_is_lazy_and_has_a_bounded_idle_lifetime() {
        assert_eq!(CHATGPT_URL, "https://chatgpt.com/");
        assert_eq!(IDLE_UNLOAD_DELAY, Duration::from_secs(90));
    }
}
