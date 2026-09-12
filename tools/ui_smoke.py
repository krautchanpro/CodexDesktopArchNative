#!/usr/bin/env python3
"""AT-SPI smoke driver for a running Codex Native GTK process."""

from __future__ import annotations

import argparse
import json
import os
import sqlite3
import struct
import sys
import time
import warnings
import zlib
from collections.abc import Callable, Iterable
from pathlib import Path

import gi

gi.require_version("Atspi", "2.0")
gi.require_version("Gdk", "4.0")
gi.require_version("Gtk", "4.0")
from gi.repository import Atspi, Gdk, GLib, Gtk  # noqa: E402


APP_NAME = os.environ.get("CODEX_NATIVE_SMOKE_APP_NAME", "codex-native")
_CLIPBOARD_FIXTURE: tuple[object, ...] | None = None
warnings.filterwarnings("ignore", category=DeprecationWarning, module=__name__)


def children(node: Atspi.Accessible) -> Iterable[Atspi.Accessible]:
    for index in range(node.get_child_count()):
        try:
            child = node.get_child_at_index(index)
        except GLib.Error:
            continue
        if child is not None:
            yield child


def descendants(node: Atspi.Accessible) -> Iterable[Atspi.Accessible]:
    yield node
    for child in children(node):
        yield from descendants(child)


def is_showing(node: Atspi.Accessible) -> bool:
    try:
        return node.get_state_set().contains(Atspi.StateType.SHOWING)
    except GLib.Error:
        return False


def node_name(node: Atspi.Accessible) -> str:
    try:
        return node.get_name() or ""
    except GLib.Error:
        return ""


def accessible_text(node: Atspi.Accessible) -> str:
    """Return both an accessible name and text-interface content."""
    parts = [node_name(node)]
    if node.get_role_name() in {"document text", "entry", "text"}:
        try:
            count = Atspi.Text.get_character_count(node)
            if count > 0:
                parts.append(Atspi.Text.get_text(node, 0, count))
        except (GLib.Error, TypeError):
            pass
    return "\n".join(part for part in parts if part)


def actions(node: Atspi.Accessible) -> list[str]:
    try:
        return [node.get_action_name(index) for index in range(node.get_n_actions())]
    except GLib.Error:
        return []


def has_activation_action(node: Atspi.Accessible) -> bool:
    return any(name in {"click", "press", "activate"} for name in actions(node))


def find_app() -> Atspi.Accessible:
    desktop = Atspi.get_desktop(0)
    # New smoke instances are normally last. Inspect them first so an unrelated
    # hung accessibility peer cannot block discovery of the target application.
    for index in reversed(range(desktop.get_child_count())):
        try:
            app = desktop.get_child_at_index(index)
        except GLib.Error:
            continue
        if app is None:
            continue
        if node_name(app) == APP_NAME:
            return app
    raise RuntimeError("codex-native is not exposed through AT-SPI")


def wait_until(
    predicate: Callable[[], object], timeout: float = 10.0, interval: float = 0.1
) -> object:
    deadline = time.monotonic() + timeout
    last_error: Exception | None = None
    while time.monotonic() < deadline:
        context = GLib.MainContext.default()
        while context.pending():
            context.iteration(False)
        try:
            value = predicate()
        except (GLib.Error, RuntimeError) as error:
            last_error = error
        else:
            if value:
                return value
        time.sleep(interval)
    if last_error is not None:
        raise RuntimeError(f"timed out: {last_error}") from last_error
    raise RuntimeError("timed out waiting for UI state")


def install_clipboard_image_fixture() -> None:
    global _CLIPBOARD_FIXTURE
    Gtk.init()
    display = Gdk.Display.get_default()
    if display is None:
        raise RuntimeError("GTK smoke process could not connect to the display")
    def png_chunk(kind: bytes, payload: bytes) -> bytes:
        checksum = zlib.crc32(kind + payload) & 0xFFFFFFFF
        return struct.pack(">I", len(payload)) + kind + payload + struct.pack(">I", checksum)

    png = b"\x89PNG\r\n\x1a\n"
    png += png_chunk(b"IHDR", struct.pack(">IIBBBBB", 1, 1, 8, 6, 0, 0, 0))
    png += png_chunk(b"IDAT", zlib.compress(b"\x00\x34\x85\xf0\xff"))
    png += png_chunk(b"IEND", b"")
    clipboard = display.get_clipboard()
    provider = Gdk.ContentProvider.new_for_bytes("image/png", GLib.Bytes.new(png))
    if not clipboard.set_content(provider):
        raise RuntimeError("could not install the image clipboard fixture")
    _CLIPBOARD_FIXTURE = (clipboard, provider, png)


def send_ctrl_v() -> None:
    Atspi.generate_keyboard_event(0xFFE3, None, Atspi.KeySynthType.PRESS)
    try:
        Atspi.generate_keyboard_event(ord("v"), None, Atspi.KeySynthType.PRESSRELEASE)
    finally:
        Atspi.generate_keyboard_event(0xFFE3, None, Atspi.KeySynthType.RELEASE)


def send_enter() -> None:
    Atspi.generate_keyboard_event(0xFF0D, None, Atspi.KeySynthType.PRESSRELEASE)


def find_named(
    root: Atspi.Accessible,
    name: str,
    *,
    role: str | None = None,
    showing: bool = True,
) -> Atspi.Accessible | None:
    for node in descendants(root):
        if node_name(node) != name:
            continue
        if role is not None and node.get_role_name() != role:
            continue
        if showing and not is_showing(node):
            continue
        return node
    return None


def find_matching(
    root: Atspi.Accessible,
    predicate: Callable[[Atspi.Accessible], bool],
    *,
    showing: bool = True,
) -> Atspi.Accessible | None:
    for node in descendants(root):
        if showing and not is_showing(node):
            continue
        if predicate(node):
            return node
    return None


def press(node: Atspi.Accessible) -> None:
    available = actions(node)
    if not available:
        raise RuntimeError(
            f"{node.get_role_name()} {node_name(node)!r} has no accessible action"
        )
    preferred = next(
        (
            index
            for index, name in enumerate(available)
            if name in {"click", "press", "activate"}
        ),
        0,
    )
    if not node.do_action(preferred):
        raise RuntimeError(f"action failed for {node_name(node)!r}: {available}")


def press_named(root: Atspi.Accessible, name: str, timeout: float = 10.0) -> None:
    node = wait_until(
        lambda: find_matching(
            root,
            lambda candidate: node_name(candidate) == name
            and has_activation_action(candidate),
        ),
        timeout=timeout,
    )
    press(node)


def assert_responsive(app: Atspi.Accessible, timeout: float = 2.0) -> None:
    started = time.monotonic()
    windows = list(children(app))
    elapsed = time.monotonic() - started
    if elapsed > timeout or not windows or node_name(windows[0]) != "Codex Native":
        raise RuntimeError(f"application did not answer AT-SPI within {timeout:.1f}s")


def is_checked(node: Atspi.Accessible) -> bool:
    states = node.get_state_set()
    return states.contains(Atspi.StateType.CHECKED) or states.contains(
        Atspi.StateType.PRESSED
    )


def is_enabled(node: Atspi.Accessible) -> bool:
    states = node.get_state_set()
    return states.contains(Atspi.StateType.ENABLED) and states.contains(
        Atspi.StateType.SENSITIVE
    )


def is_sensitive(node: Atspi.Accessible) -> bool:
    """GTK exposes SENSITIVE reliably even when AT-SPI omits ENABLED."""
    try:
        return node.get_state_set().contains(Atspi.StateType.SENSITIVE)
    except GLib.Error:
        return False


def press_sensitive_named(
    root: Atspi.Accessible, name: str, timeout: float = 10.0
) -> None:
    node = wait_until(
        lambda: find_matching(
            root,
            lambda candidate: node_name(candidate) == name
            and has_activation_action(candidate)
            and is_sensitive(candidate),
        ),
        timeout=timeout,
    )
    press(node)


def select_combo_option(
    root: Atspi.Accessible, combo_name: str, option_name: str
) -> None:
    combo = wait_until(lambda: find_named(root, combo_name, role="combo box"))
    if not actions(combo):
        # GTK4 ComboBoxText exposes its active row through AT-SPI Selection,
        # while the collapsed combo itself intentionally has no click action.
        mistral_selected = (
            find_named(root, "Model: Mistral AI", role="combo box") is not None
        )
        if mistral_selected and option_name in {"High", "Non-thinking"}:
            option_index = {"High": 0, "Non-thinking": 1}[option_name]
        else:
            option_index = {
                "Standard": 0,
                "Fast": 1,
                "Default · GPT-5.6 Terra": 0,
                "OpenCode": 1,
                "Gemini": 2,
                "OpenRouter Free": 3,
                "Mistral AI": 4,
                "GPT-5.5": 5,
                "GPT-5.6 Luna": 6,
                "GPT-5.6 Terra": 7,
                "GPT-5.6 Sol": 8,
                "Non-thinking": 0,
                "Minimal": 0,
                "Low": 1,
                "Medium": 2,
                "High": 3,
            }.get(option_name)
        if option_index is None:
            raise RuntimeError(
                f"could not select {option_name!r} in combo {combo_name!r}"
            )
        selected = False
        # A freshly hydrated GTK4 ComboBoxText can acknowledge the first
        # AT-SPI selection before its model-change signal is connected. Repeat
        # the same idempotent selection briefly to remove startup flakiness.
        for _ in range(3):
            selected = Atspi.Selection.select_child(combo, option_index) or selected
            context = GLib.MainContext.default()
            while context.pending():
                context.iteration(False)
            time.sleep(0.1)
        if not selected:
            raise RuntimeError(
                f"could not select {option_name!r} in combo {combo_name!r}"
            )
        return
    press(combo)
    option = wait_until(
        lambda: find_matching(
            root,
            lambda node: node_name(node) == option_name
            and node is not combo
            and has_activation_action(node),
        )
    )
    press(option)


def normalized_thread_name(name: str) -> str:
    return (
        name.removeprefix("Currently open task ")
        .removeprefix("★ ")
        .removeprefix("☐ ")
        .removeprefix("☑ ")
    )


def thread_identity(name: str) -> str:
    return normalized_thread_name(name).rsplit(" · ", 1)[0]


def thread_is_pinned(name: str) -> bool:
    return name.removeprefix("Currently open task ").startswith("★ ")


def associated_control(
    root: Atspi.Accessible, label_name: str, role: str
) -> Atspi.Accessible:
    label = wait_until(
        lambda: find_named(root, label_name, role="label", showing=False)
    )
    parent = label.get_parent()
    start = label.get_index_in_parent() + 1
    for index in range(start, parent.get_child_count()):
        candidate = parent.get_child_at_index(index)
        if candidate.get_role_name() == role:
            return candidate
        nested = find_matching(
            candidate,
            lambda node: node.get_role_name() == role,
            showing=False,
        )
        if nested is not None:
            return nested
    raise RuntimeError(f"{role} control not found after {label_name}")


def dump_tree(app: Atspi.Accessible, showing_only: bool) -> None:
    def walk(node: Atspi.Accessible, depth: int) -> None:
        if not showing_only or is_showing(node):
            name = node_name(node)
            role = node.get_role_name()
            available = actions(node)
            suffix = f" actions={available}" if available else ""
            if name or available:
                print(f"{'  ' * depth}{role}: {name!r}{suffix}")
        for child in children(node):
            walk(child, depth + 1)

    walk(app, 0)


def smoke_navigation(app: Atspi.Accessible) -> None:
    chat_button = wait_until(
        lambda: find_named(app, "Chat", role="button", showing=True)
    )
    nav_scroller = chat_button
    for _ in range(5):
        if nav_scroller.get_role_name() == "scroll pane":
            break
        nav_scroller = nav_scroller.get_parent()
    else:
        raise RuntimeError("workspace navigation is not inside a scroll pane")
    wait_until(
        lambda: find_matching(
            nav_scroller,
            lambda node: node.get_role_name() == "scroll bar",
            showing=True,
        )
    )
    press_named(app, "Collapse workspace navigation")
    wait_until(
        lambda: find_named(
            app, "Expand workspace navigation", role="button", showing=True
        )
    )
    wait_until(
        lambda: find_named(app, "Chat", role="button", showing=False) is None
    )
    assert_responsive(app)
    press_named(app, "Expand workspace navigation")
    wait_until(
        lambda: find_named(
            app, "Collapse workspace navigation", role="button", showing=True
        )
    )
    wait_until(lambda: find_named(app, "Chat", role="button", showing=True))
    if find_named(app, "Settings", role="button", showing=False) is None:
        raise RuntimeError("scrollable workspace navigation is missing Settings")
    print("PASS collapsible and scrollable workspace navigation")
    pages = [
        "Chat",
        "ChatGPT",
        "Projects",
        "Sites",
        "Scheduled",
        "Terminal",
        "Agent workspace",
        "Context",
        "Extensions",
        "OpenCode",
        "Computer use",
        "Remote access",
        "Diagnostics",
        "Settings",
    ]
    for page in pages:
        node = wait_until(
            lambda page=page: find_named(app, page, role="button", showing=False)
        )
        press(node)
        time.sleep(0.2)
        assert_responsive(app)
        if page == "ChatGPT":
            wait_until(lambda: chatgpt_is_loaded(app), timeout=45.0)
            press_named(app, "Unload ChatGPT")
            wait_until(lambda: chatgpt_is_unloaded(app))
        print(f"PASS page: {page}")
    for removed_page in ["Pull requests", "Review", "Source control"]:
        if find_named(app, removed_page, role="button", showing=False) is not None:
            raise RuntimeError(f"retired native Git page is still visible: {removed_page}")
    print("PASS native Git pages are absent")


def smoke_memoria_jobs(app: Atspi.Accessible) -> None:
    wait_until(lambda: find_named(app, "Memoria jobs: 5", role="label"), timeout=10.0)
    print("PASS Memoria jobs: live header count shows pending and active work")


def smoke_reset_dialog(app: Atspi.Accessible) -> None:
    reset = wait_until(
        lambda: find_matching(
            app,
            lambda node: node.get_role_name() == "button"
            and node_name(node).startswith("Resets "),
        )
    )
    original = node_name(reset)
    press(reset)
    wait_until(lambda: find_named(app, "Use one Codex reset credit?", role="label"))
    cancel = wait_until(lambda: find_named(app, "Cancel", role="button"))
    if find_named(app, "Use one reset", role="button") is None:
        raise RuntimeError("reset confirmation did not expose its guarded action")
    press(cancel)
    wait_until(lambda: find_named(app, "Cancel", role="button") is None)
    reset_after = wait_until(lambda: find_named(app, original, role="button"))
    if node_name(reset_after) != original:
        raise RuntimeError("reset credit count changed during cancel-only smoke")
    assert_responsive(app)
    print(f"PASS reset dialog: canceled with {original} unchanged")


def smoke_settings(app: Atspi.Accessible) -> None:
    press_named(app, "Settings")
    time.sleep(0.2)
    labels = [
        "Desktop notifications",
        "Show reasoning summaries",
        "Start remote host on login",
        "Collect local resource diagnostics",
        "Experimental native macro execution",
    ]
    for label in labels:
        control = associated_control(app, label, "switch")
        original = is_checked(control)
        press(control)
        wait_until(lambda control=control: is_checked(control) != original)
        press(control)
        wait_until(lambda control=control: is_checked(control) == original)
        assert_responsive(app)
        print(f"PASS reversible setting: {label}")

    for label in ["Codex CLI path", "External browser", "Editor"]:
        control = associated_control(app, label, "text")
        if not control.get_state_set().contains(Atspi.StateType.EDITABLE):
            raise RuntimeError(f"{label} is not editable")
        print(f"PASS editable setting: {label}")

    for label in [
        "Macro parallel steps",
        "Macro command memory (MiB)",
        "Macro command timeout (seconds)",
    ]:
        associated_control(app, label, "spin button")
        print(f"PASS bounded macro setting: {label}")

    if find_matching(
        app,
        lambda node: node.get_role_name() == "label"
        and "Macro execution is off" in node_name(node),
    ) is None:
        raise RuntimeError("Settings is missing the prompt-free macro A/B status")

    press_named(app, "Refresh account usage")
    assert_responsive(app)
    print("PASS setting action: Refresh account usage")

    save = wait_until(lambda: find_named(app, "Save settings", role="button"))
    started = time.monotonic()
    press(save)
    time.sleep(0.05)
    assert_responsive(app)
    wait_until(
        lambda: find_named(app, "Save settings", role="button"),
        timeout=30.0,
    )
    elapsed = time.monotonic() - started
    print(f"PASS non-blocking settings save: {elapsed:.2f}s")


def smoke_task_pin_context_menu(app: Atspi.Accessible) -> None:
    press_named(app, "Chat")
    time.sleep(0.2)

    def thread_row() -> Atspi.Accessible | None:
        return find_matching(
            app,
            lambda node: node.get_role_name() == "button"
            and node_name(node).endswith((" · notLoaded", " · idle")),
        )

    row = wait_until(thread_row)
    row_identity = thread_identity(node_name(row))

    def current_row() -> Atspi.Accessible | None:
        return find_matching(
            app,
            lambda node: node.get_role_name() == "button"
            and thread_identity(node_name(node)) == row_identity,
        )

    press(row)
    wait_until(
        lambda: (active := current_row()) is not None
        and active.get_state_set().contains(Atspi.StateType.SELECTED),
        timeout=20.0,
    )
    wait_until(
        lambda: (button := find_named(app, "Pin or unpin task", role="button"))
        is not None
        and is_sensitive(button),
        timeout=20.0,
    )
    time.sleep(0.3)
    was_pinned = thread_is_pinned(node_name(wait_until(current_row)))
    press_sensitive_named(app, "Pin or unpin task")
    wait_until(
        lambda: (active := current_row()) is not None
        and thread_is_pinned(node_name(active)) != was_pinned
    )
    press_sensitive_named(app, "Pin or unpin task")
    wait_until(
        lambda: (active := current_row()) is not None
        and thread_is_pinned(node_name(active)) == was_pinned
    )
    assert_responsive(app)
    print("PASS task pin state: toggle and restore (secondary-click binding unit-tested)")


def smoke_safe_actions(app: Atspi.Accessible) -> None:
    """Exercise host/UI actions that do not infer, publish, install, or mutate data."""

    def page_action(page: str, action: str, *, wait_ready: bool = False) -> None:
        press_named(app, page)
        time.sleep(0.2)
        press_sensitive_named(app, action, timeout=30.0)
        time.sleep(0.25)
        assert_responsive(app)
        if wait_ready:
            wait_until(
                lambda: (button := find_named(app, action, role="button"))
                is not None
                and is_sensitive(button),
                timeout=45.0,
            )
        print(f"PASS safe action: {page} / {action}")

    press_named(app, "Terminal")
    time.sleep(0.2)
    start = find_named(app, "Start shell", role="button")
    if start is not None:
        press(start)
        wait_until(
            lambda: find_named(app, "Focus shell", role="button")
            or find_named(app, "Retry", role="button"),
            timeout=20.0,
        )
    assert_responsive(app)
    print("PASS safe action: Terminal / start or retain shell")

    page_action("Agent workspace", "Check isolation", wait_ready=True)

    press_named(app, "Projects")
    for action in ["Add project", "Open in editor", "Reveal folder"]:
        wait_until(lambda action=action: find_named(app, action, role="button"))
    assert_responsive(app)
    print("PASS safe state: Projects / native project controls")

    press_named(app, "Sites")
    time.sleep(0.2)
    sites_start = wait_until(
        lambda: find_named(app, "Start a Sites task", role="button")
    )
    if is_sensitive(sites_start):
        press(sites_start)
        composer = wait_until(
            lambda: find_matching(
                app,
                lambda node: node.get_role_name() == "text"
                and node.get_state_set().contains(Atspi.StateType.EDITABLE)
                and "build and deploy a website with Sites" in accessible_text(node),
            )
        )
        set_text(composer, "")
        press_named(app, "Sites")
    elif not find_matching(
        app,
        lambda node: node.get_role_name() == "label"
        and (
            "Sites is not installed" in node_name(node)
            or "Sites is installed but disabled" in node_name(node)
        ),
    ):
        raise RuntimeError("Sites task action is disabled without an explanation")
    press_sensitive_named(app, "Manage Sites plugin")
    wait_until(lambda: find_named(app, "Refresh extensions", role="button"))
    assert_responsive(app)
    print("PASS safe actions: Sites / guarded task state and open plugin manager")

    press_named(app, "Extensions")
    time.sleep(0.2)
    installed_only = wait_until(
        lambda: find_named(app, "Installed only", role="check box")
    )
    if is_checked(installed_only):
        press_sensitive_named(app, "Refresh extensions")
        time.sleep(0.4)
    elif not find_matching(
        app,
        lambda node: node.get_role_name() == "label"
        and "Remote catalog" in node_name(node),
    ):
        raise RuntimeError(
            "opening a missing Sites plugin did not expose the remote catalog"
        )
    assert_responsive(app)
    print("PASS safe actions: Extensions / expected catalog state remains responsive")

    page_action("OpenCode", "Refresh health and usage", wait_ready=True)
    press_named(app, "Computer use")
    time.sleep(0.2)
    computer_check = wait_until(
        lambda: find_named(app, "Check readiness", role="button")
    )
    if is_sensitive(computer_check):
        press(computer_check)
        time.sleep(0.25)
        assert_responsive(app)
        print("PASS safe action: Computer use / Check readiness")
    elif find_named(app, "Computer Use is unavailable", role="label") is not None:
        print("PASS safe state: Computer Use unavailable action is disabled")
    else:
        raise RuntimeError("Computer Use readiness is disabled without an explanation")

    press_named(app, "Scheduled")
    time.sleep(0.2)
    press_sensitive_named(app, "New scheduled task")
    wait_until(lambda: find_named(app, "New scheduled task", role="alert"))
    press_sensitive_named(app, "Cancel")
    wait_until(lambda: find_named(app, "New scheduled task", role="alert") is None)
    assert_responsive(app)
    print("PASS safe action: Scheduled / open and cancel editor")

    page_action("Remote access", "Refresh remote status")
    page_action("Diagnostics", "Run diagnostics")
    page_action("Diagnostics", "Check package update")
    page_action("Diagnostics", "Check remote runtime")


def smoke_stop_turn(app: Atspi.Accessible) -> None:
    """Interrupt the active local fixture and verify the UI does not RefCell-panic."""
    log_path = os.environ.get("CODEX_NATIVE_FAKE_LOG")
    if not log_path:
        raise RuntimeError("CODEX_NATIVE_FAKE_LOG is required for the stop-turn smoke")

    def requests() -> list[dict[str, object]]:
        try:
            with open(log_path, encoding="utf-8") as handle:
                return [json.loads(line) for line in handle if line.strip()]
        except FileNotFoundError:
            return []

    baseline = len(requests())
    press_named(app, "Chat")
    wait_until(
        lambda: find_matching(
            app,
            lambda node: node.get_role_name() == "label"
            and node_name(node) == "Codex Native rich transcript smoke",
        ),
        timeout=10.0,
    )
    press_sensitive_named(app, "Stop current turn")
    interrupt = wait_until(
        lambda: next(
            (
                request
                for request in requests()[baseline:]
                if request.get("method") == "turn/interrupt"
            ),
            None,
        ),
        timeout=10.0,
    )
    params = interrupt.get("params") or {}
    if params.get("threadId") != "codex-native-smoke-task":
        raise RuntimeError("Stop targeted the wrong task")
    if params.get("turnId") != "codex-native-smoke-turn":
        raise RuntimeError("Stop targeted the wrong active turn")
    assert_responsive(app)
    print("PASS Stop current turn: request queued without a RefCell borrow crash")


def set_text(node: Atspi.Accessible, value: str) -> None:
    try:
        editable = node.get_editable_text_iface()
    except GLib.Error as error:
        raise RuntimeError(f"{node_name(node)!r} is not editable: {error}") from error
    if editable is None or not editable.set_text_contents(value):
        raise RuntimeError(f"could not set text for {node_name(node)!r}")


def goal_button(app: Atspi.Accessible) -> Atspi.Accessible | None:
    return find_matching(
        app,
        lambda node: node_name(node) == "Goal selector" and bool(actions(node)),
    )


def open_goal_selector(app: Atspi.Accessible) -> None:
    def activate() -> bool:
        control = goal_button(app)
        if control is None:
            return False
        press(control)
        return True

    wait_until(activate, timeout=20.0)


def wait_goal_status(app: Atspi.Accessible, status: str) -> None:
    wait_until(
        lambda: find_matching(
            app,
            lambda node: node.get_role_name() == "label"
            and node_name(node).startswith(f"Goal {status} ·"),
        ),
        timeout=20.0,
    )


def smoke_goal_lifecycle(app: Atspi.Accessible) -> None:
    """Exercise goal RPCs without starting a turn or consuming model usage."""
    print("RUN goal: select reversible test task", flush=True)
    press_named(app, "Chat")
    time.sleep(0.2)

    row = wait_until(
        lambda: find_matching(
            app,
            lambda node: node.get_role_name() == "button"
            and thread_identity(node_name(node)).startswith(
                "Existing task send fixture "
            ),
        )
    )
    press(row)
    time.sleep(0.8)
    wait_until(
        lambda: goal_button(app),
        timeout=20.0,
    )
    print("RUN goal: open selector", flush=True)
    open_goal_selector(app)
    wait_until(lambda: find_named(app, "No goal for this task", role="label"))

    print("RUN goal: open create dialog", flush=True)
    press_named(app, "Create goal…")
    wait_until(lambda: find_named(app, "Task goal", role="label"))
    objective = associated_control(app, "Objective", "text")
    set_text(objective, "Codex Native reversible UI smoke")
    print("RUN goal: save active goal", flush=True)
    press_named(app, "Save")
    wait_until(lambda: find_named(app, "Task goal", role="label") is None)
    wait_goal_status(app, "Active")
    open_goal_selector(app)
    wait_until(lambda: find_named(app, "Active goal", role="label"))
    print("PASS goal action: create")

    print("RUN goal: stop", flush=True)
    press_named(app, "Stop goal")
    wait_goal_status(app, "Stopped")
    open_goal_selector(app)
    wait_until(lambda: find_named(app, "Stopped goal", role="label"))
    if find_named(app, "Stopped manually.", role="label") is None:
        raise RuntimeError("paused goal did not explain why it stopped")
    print("RUN goal: resume", flush=True)
    press_named(app, "Resume goal")
    wait_goal_status(app, "Active")
    open_goal_selector(app)
    wait_until(lambda: find_named(app, "Active goal", role="label"))
    print("PASS goal actions: stop reason and resume")

    print("RUN goal: clear", flush=True)
    press_named(app, "Clear goal…")
    wait_until(lambda: find_named(app, "Clear this task’s goal?", role="label"))
    press_named(app, "Clear goal")
    wait_until(
        lambda: find_matching(
            app,
            lambda node: node.get_role_name() == "label"
            and node_name(node).startswith("Goal ")
            and " · " in node_name(node),
        )
        is None,
        timeout=20.0,
    )
    open_goal_selector(app)
    wait_until(lambda: find_named(app, "No goal for this task", role="label"), timeout=20.0)
    assert_responsive(app)
    print("PASS goal action: clear and restore no-goal state")


def smoke_chatgpt(app: Atspi.Accessible) -> None:
    press_named(app, "ChatGPT")
    wait_until(lambda: find_named(app, "Unload ChatGPT", role="button"))
    if chatgpt_is_unloaded(app):
        press_named(app, "Reload ChatGPT")
    wait_until(lambda: chatgpt_is_loaded(app), timeout=45.0)
    assert_responsive(app)
    press_named(app, "Unload ChatGPT")
    wait_until(lambda: chatgpt_is_unloaded(app))
    assert_responsive(app)
    print("PASS ChatGPT: same-window load and full unload")


def smoke_active_task_highlight(app: Atspi.Accessible) -> None:
    press_named(app, "Chat")
    row = wait_until(
        lambda: find_matching(
            app,
            lambda node: node.get_role_name() == "button"
            and thread_identity(node_name(node)).startswith(
                "Second existing task fixture "
            ),
        )
    )
    identity = thread_identity(node_name(row))
    press(row)

    def selected_row() -> Atspi.Accessible | None:
        return find_matching(
            app,
            lambda node: node.get_role_name() == "button"
            and thread_identity(node_name(node)) == identity
            and node.get_state_set().contains(Atspi.StateType.SELECTED),
        )

    wait_until(selected_row, timeout=20.0)
    selected = [
        node
        for node in descendants(app)
        if is_showing(node)
        and node.get_role_name() == "button"
        and node.get_state_set().contains(Atspi.StateType.SELECTED)
        and " · " in node_name(node)
    ]
    if len(selected) != 1:
        raise RuntimeError(f"expected one highlighted task, found {len(selected)}")
    assert_responsive(app)
    print(f"PASS active task highlight: {node_name(selected[0])}")


def smoke_bulk_task_actions(app: Atspi.Accessible) -> None:
    log_path = os.environ.get("CODEX_NATIVE_FAKE_LOG")
    if not log_path:
        raise RuntimeError("CODEX_NATIVE_FAKE_LOG is required for bulk task smoke")

    def matching_bulk_rows(prefix: str) -> list[Atspi.Accessible]:
        return [
            node
            for node in descendants(app)
            if is_showing(node)
            and node.get_role_name() == "button"
            and thread_identity(node_name(node)).startswith(prefix)
        ]

    def selected_bulk_rows(prefix: str) -> list[Atspi.Accessible]:
        return [
            node
            for node in matching_bulk_rows(prefix)
            if node.get_state_set().contains(Atspi.StateType.SELECTED)
        ]

    def recorded_mutations() -> list[str]:
        try:
            with open(log_path, encoding="utf-8") as handle:
                return [
                    record["method"]
                    for line in handle
                    if (record := json.loads(line)).get("method")
                    in {"thread/archive", "thread/delete"}
                ]
        except FileNotFoundError:
            return []

    press_named(app, "Chat")
    wait_until(
        lambda: len(matching_bulk_rows("Bulk archive fixture ")) == 2,
        timeout=20.0,
    )
    archive_before = recorded_mutations().count("thread/archive")
    print("RUN bulk tasks: select and archive two fixtures", flush=True)
    press_named(app, "Select tasks")
    wait_until(lambda: find_named(app, "0 selected", role="label"))
    for expected, title in enumerate(
        ("Bulk archive fixture one", "Bulk archive fixture two"), start=1
    ):
        press(
            wait_until(
                lambda title=title: find_matching(
                    app,
                    lambda node: node.get_role_name() == "button"
                    and thread_identity(node_name(node)).startswith(title),
                )
            )
        )
        wait_until(
            lambda expected=expected: len(
                selected_bulk_rows("Bulk archive fixture ")
            )
            == expected
        )
    wait_until(lambda: len(selected_bulk_rows("Bulk archive fixture ")) == 2)
    wait_until(lambda: find_named(app, "2 selected", role="label"))
    press_named(app, "Archive selected tasks")
    wait_until(
        lambda: recorded_mutations().count("thread/archive") == archive_before + 2,
        timeout=20.0,
    )
    wait_until(
        lambda: not matching_bulk_rows("Bulk archive fixture "), timeout=20.0
    )

    wait_until(
        lambda: len(matching_bulk_rows("Bulk delete fixture ")) == 2,
        timeout=20.0,
    )
    delete_before = recorded_mutations().count("thread/delete")
    print("RUN bulk tasks: select and delete two fixtures", flush=True)
    press_named(app, "Select tasks")
    wait_until(lambda: find_named(app, "0 selected", role="label"))
    for expected, title in enumerate(
        ("Bulk delete fixture one", "Bulk delete fixture two"), start=1
    ):
        press(
            wait_until(
                lambda title=title: find_matching(
                    app,
                    lambda node: node.get_role_name() == "button"
                    and thread_identity(node_name(node)).startswith(title),
                )
            )
        )
        wait_until(
            lambda expected=expected: len(
                selected_bulk_rows("Bulk delete fixture ")
            )
            == expected
        )
    wait_until(lambda: len(selected_bulk_rows("Bulk delete fixture ")) == 2)
    press_named(app, "Delete selected tasks")
    wait_until(lambda: find_named(app, "Delete 2 selected tasks?", role="label"))
    press_named(app, "Delete tasks")
    wait_until(
        lambda: recorded_mutations().count("thread/delete") == delete_before + 2,
        timeout=20.0,
    )
    wait_until(
        lambda: not matching_bulk_rows("Bulk delete fixture "), timeout=20.0
    )
    assert_responsive(app)
    print("PASS bulk task actions: select two, archive, confirm delete")


def smoke_rich_transcript(app: Atspi.Accessible) -> None:
    press_named(app, "Chat")
    task_search = None
    fixture_row = find_matching(
        app,
        lambda node: node.get_role_name() == "button"
        and normalized_thread_name(node_name(node)).startswith(
            "Codex Native rich transcript smoke"
        ),
        showing=False,
    )
    if fixture_row is not None:
        if not is_showing(fixture_row):
            task_search = find_matching(
                app,
                lambda node: node.get_role_name() == "entry"
                and node.get_state_set().contains(Atspi.StateType.EDITABLE),
            )
            if task_search is None:
                raise RuntimeError("task search is unavailable for spinner verification")
            set_text(task_search, "Codex Native rich transcript smoke")
            fixture_row = wait_until(
                lambda: find_matching(
                    app,
                    lambda node: node.get_role_name() == "button"
                    and normalized_thread_name(node_name(node)).startswith(
                        "Codex Native rich transcript smoke"
                    ),
                ),
                timeout=5.0,
            )
        press(fixture_row)
        step_label = wait_until(
            lambda: find_named(app, "Step 2 / 3", role="label"), timeout=5.0
        )
        step_description = step_label.get_description() or ""
        for expected in (
            "Completed: Inspect protocol",
            "Current: Render native controls",
            "Pending: Verify interactions",
        ):
            if expected not in step_description:
                raise RuntimeError(
                    f"step progress tooltip is missing plan detail: {expected}"
                )
        print("PASS hoverable step plan tooltip")
        if find_named(app, "2 files changed", role="label") is None:
            raise RuntimeError("live progress pill is missing its file counter")
        activity_frames = {"◴", "◷", "◶", "◵"}
        spinner = find_matching(
            fixture_row,
            lambda node: node.get_role_name() == "label"
            and node_name(node) in activity_frames,
        )
        if spinner is None:
            raise RuntimeError("running task is missing its animated spinner")
        first_frame = node_name(spinner)
        time.sleep(0.35)
        if node_name(spinner) == first_frame:
            raise RuntimeError("running task activity indicator did not animate")
        if find_named(fixture_row, "1", role="label") is None:
            raise RuntimeError("task row is missing its spawned-agent count")
        row_nodes = list(descendants(fixture_row))
        spinner_index = row_nodes.index(spinner)
        title_index = next(
            index
            for index, node in enumerate(row_nodes)
            if node.get_role_name() == "label"
            and node_name(node).startswith("Codex Native rich transcript smoke")
        )
        agent_index = next(
            index
            for index, node in enumerate(row_nodes)
            if node.get_role_name() == "label" and node_name(node) == "1"
        )
        if spinner_index <= max(title_index, agent_index):
            raise RuntimeError("running spinner is not the trailing task-row indicator")
        print("PASS trailing running-task spinner")
        if task_search is not None:
            set_text(task_search, "")

    def image_activity() -> Atspi.Accessible | None:
        return find_matching(
            app,
            lambda node: node.get_role_name() == "button"
            and any(
                phrase in node_name(node)
                for phrase in ("Viewed an image", "Attached an image", "Generated an image")
            )
            or (
                node.get_role_name() == "button"
                and " images" in node_name(node)
                and any(
                    node_name(node).startswith(verb)
                    for verb in ("Viewed ", "Attached ", "Generated ")
                )
            ),
        )

    activity = image_activity()
    if activity is None:
        task_rows = [
            node
            for node in descendants(app)
            if is_showing(node)
            and node.get_role_name() == "button"
            and " · " in node_name(node)
        ][:16]
        for task_row in task_rows:
            press(task_row)
            try:
                activity = wait_until(image_activity, timeout=1.5)
                break
            except TimeoutError:
                continue
    if activity is None:
        raise RuntimeError("no clickable image activity found in the loaded task history")

    thumbnail = find_matching(
        app,
        lambda node: node.get_role_name() == "button"
        and node_name(node).startswith("Open image 1:"),
    )
    if thumbnail is None:
        raise RuntimeError("image activity is missing its clickable thumbnail preview")
    file_action = find_named(app, "Cargo.toml", role="button")
    if file_action is None or not is_sensitive(file_action):
        raise RuntimeError("file-change activity is missing its clickable editor action")
    print("PASS clickable image thumbnail and file action")

    activity_name = node_name(activity)
    press(activity)
    viewer = wait_until(
        lambda: find_matching(
            app,
            lambda node: node.get_role_name() in {"frame", "window", "dialog"}
            and node_name(node) == "Image viewer",
        ),
        timeout=10.0,
    )
    if find_named(viewer, "Previous image", role="button") is None:
        raise RuntimeError("image viewer is missing previous-image navigation")
    if find_named(viewer, "Next image", role="button") is None:
        raise RuntimeError("image viewer is missing next-image navigation")
    close = find_named(viewer, "Close", role="button")
    if close is None:
        raise RuntimeError("image viewer is missing its close action")
    press(close)
    wait_until(lambda: None if is_showing(viewer) else True, timeout=5.0)
    assert_responsive(app)
    print(f"PASS clickable image viewer: {activity_name}")


def smoke_existing_task_send(app: Atspi.Accessible) -> None:
    log_path = os.environ.get("CODEX_NATIVE_FAKE_LOG")
    if not log_path:
        raise RuntimeError("CODEX_NATIVE_FAKE_LOG is required for the send smoke")

    def recorded_methods() -> list[str]:
        try:
            with open(log_path, encoding="utf-8") as handle:
                return [json.loads(line)["method"] for line in handle if line.strip()]
        except FileNotFoundError:
            return []

    def recorded_requests() -> list[dict[str, object]]:
        try:
            with open(log_path, encoding="utf-8") as handle:
                return [json.loads(line) for line in handle if line.strip()]
        except FileNotFoundError:
            return []

    def text_node(fragment: str) -> Atspi.Accessible | None:
        return find_matching(app, lambda node: fragment in accessible_text(node))

    press_named(app, "Chat")
    row = wait_until(
        lambda: find_matching(
            app,
            lambda node: node.get_role_name() == "button"
            and normalized_thread_name(node_name(node)).startswith(
                "Existing task send fixture"
            ),
        ),
        timeout=10.0,
    )
    press(row)
    wait_until(lambda: recorded_methods().count("thread/resume") >= 1)
    stable_text = "Existing task send fixture loaded from the local smoke fixture."
    stable = wait_until(lambda: text_node(stable_text), timeout=10.0)
    stable_identity = (stable.get_accessible_id(), stable.get_id())
    baseline = len(recorded_methods())
    resumes_before_send = recorded_methods().count("thread/resume")

    composer = wait_until(lambda: find_named(app, "Task message"), timeout=5.0)
    if not os.environ.get("CODEX_NATIVE_SMOKE_CLIPBOARD_IMAGE"):
        install_clipboard_image_fixture()
    press_sensitive_named(app, "Paste image or files from clipboard")
    wait_until(
        lambda: find_matching(
            app,
            lambda node: node.get_role_name() == "label"
            and "clipboard-image-" in node_name(node),
        ),
        timeout=10.0,
    )
    set_text(composer, "Native resume smoke")
    # AT-SPI's global synthetic keyboard event is blocked by KDE Wayland.
    # The Rust unit suite covers Return/Shift+Return routing; activate the same
    # send path through its accessible button for this end-to-end protocol test.
    press_sensitive_named(app, "Send")

    def sent_without_redundant_resume() -> bool:
        methods = recorded_methods()[baseline:]
        try:
            turn_index = methods.index("turn/start")
        except ValueError:
            return False
        return (
            resumes_before_send >= 1
            and "thread/resume" not in methods[:turn_index]
        )

    wait_until(sent_without_redundant_resume, timeout=10.0)
    request = wait_until(
        lambda: next(
            (
                request
                for request in recorded_requests()[baseline:]
                if request.get("method") == "turn/start"
            ),
            None,
        ),
        timeout=10.0,
    )
    params = request.get("params") or {}
    inputs = params.get("input") or []
    if not any(
        part.get("type") == "localImage"
        and str(part.get("path", "")).endswith(".png")
        for part in inputs
    ):
        raise RuntimeError("pasted clipboard image was not sent as localImage input")
    lean_context = (params.get("additionalContext") or {}).get(
        "codex-native.lean-context"
    )
    if not isinstance(lean_context, dict) or "Codex remains sole writer" not in str(
        lean_context.get("value", "")
    ):
        raise RuntimeError("turn/start is missing guarded Lean Context guidance")
    if not (params.get("responsesapiClientMetadata") or {}).get("lean_context"):
        raise RuntimeError("turn/start is missing Lean Context checkpoint metadata")
    wait_until(lambda: text_node("Native resume smoke"), timeout=10.0)
    wait_until(lambda: text_node("Message accepted after thread/resume."), timeout=10.0)
    savings_receipt = wait_until(
        lambda: find_matching(
            app,
            lambda node: node.get_role_name() == "label"
            and node_name(node).startswith("Potential tokens saved: ~"),
        ),
        timeout=10.0,
    )
    receipt_description = savings_receipt.get_description() or ""
    if "never added to the prompt" not in receipt_description:
        raise RuntimeError("token-savings receipt does not disclose its zero-token UI boundary")
    if any(
        "tokenSavings" in json.dumps(request)
        or "Potential tokens saved" in json.dumps(request)
        for request in recorded_requests()[baseline:]
    ):
        raise RuntimeError("local token-savings receipt leaked into app-server requests")
    stable_after = wait_until(lambda: text_node(stable_text), timeout=10.0)
    if stable_identity != (stable_after.get_accessible_id(), stable_after.get_id()):
        raise RuntimeError("an unchanged transcript row was destroyed during streaming")
    automatic_compact_requests = [
        request
        for request in recorded_requests()[baseline:]
        if request.get("method") == "thread/compact/start"
    ]
    if automatic_compact_requests:
        raise RuntimeError("Lean Context duplicated Codex automatic compaction")
    if "thread/fork" in recorded_methods()[baseline:]:
        raise RuntimeError("Lean Context forked the task instead of compacting the same thread")
    wait_until(
        lambda: find_named(
            app,
            "Current context 30,400 / 100,000 tokens (30%) · Thread total 4,970,297 tokens",
            role="label",
        ),
        timeout=10.0,
    )

    press_named(app, "Context")
    wait_until(lambda: find_named(app, "Lean Context Engine", role="label"))
    associated_control(app, "Checkpoint mode", "combo box")
    associated_control(app, "Checkpoint at context usage (%)", "spin button")
    associated_control(app, "Use OpenCode for large-file analysis", "switch")
    wait_until(
        lambda: find_matching(
            app,
            lambda node: node.get_role_name() == "label"
            and node_name(node).startswith("Latest checkpoint ")
            and "1 canonical compactions" in node_name(node),
        ),
        timeout=15.0,
    )
    compact_count = recorded_methods().count("thread/compact/start")
    press_sensitive_named(app, "Checkpoint & compact now")
    wait_until(
        lambda: recorded_methods().count("thread/compact/start") > compact_count,
        timeout=15.0,
    )
    wait_until(
        lambda: (button := find_named(app, "Checkpoint & compact now", role="button"))
        is not None
        and is_sensitive(button),
        timeout=15.0,
    )
    assert_responsive(app)
    print(
        "PASS composer + Lean Context: image send, stable streaming row, guarded metadata, zero-token local savings receipt, checkpoint-only automation, and canonical automatic/manual compaction"
    )


def smoke_stale_steer_recovery(app: Atspi.Accessible) -> None:
    log_path = os.environ.get("CODEX_NATIVE_FAKE_LOG")
    if not log_path:
        raise RuntimeError("CODEX_NATIVE_FAKE_LOG is required for stale-steer smoke")

    def requests() -> list[dict[str, object]]:
        try:
            with open(log_path, encoding="utf-8") as handle:
                return [json.loads(line) for line in handle if line.strip()]
        except FileNotFoundError:
            return []

    press_named(app, "Chat")
    row = wait_until(
        lambda: find_matching(
            app,
            lambda node: node.get_role_name() == "button"
            and normalized_thread_name(node_name(node)).startswith(
                "Stale steer recovery fixture"
            ),
        ),
        timeout=10.0,
    )
    press(row)
    wait_until(
        lambda: any(
            request.get("method") == "thread/resume"
            and (request.get("params") or {}).get("threadId")
            == "stale-steer-recovery-fixture"
            for request in requests()
        ),
        timeout=10.0,
    )
    baseline = len(requests())
    composer = wait_until(lambda: find_named(app, "Task message"), timeout=5.0)
    set_text(composer, "Recover stale steer smoke")
    press_sensitive_named(app, "Send")

    def recovered() -> bool:
        sent = requests()[baseline:]
        methods = [request.get("method") for request in sent]
        if "turn/steer" not in methods or "turn/start" not in methods:
            return False
        return methods.index("turn/steer") < methods.index("turn/start")

    wait_until(recovered, timeout=10.0)
    wait_until(
        lambda: find_matching(
            app, lambda node: "Recover stale steer smoke" in accessible_text(node)
        ),
        timeout=10.0,
    )
    if find_matching(app, lambda node: "no active turn to steer" in accessible_text(node)):
        raise RuntimeError("stale steer error leaked into the UI after recovery")
    assert_responsive(app)
    print("PASS stale steer: failed steer automatically retried as a fresh turn")


def smoke_task_switching(app: Atspi.Accessible) -> None:
    log_path = os.environ.get("CODEX_NATIVE_FAKE_LOG")
    if not log_path:
        raise RuntimeError("CODEX_NATIVE_FAKE_LOG is required for task switching")

    def recorded_requests() -> list[dict[str, object]]:
        try:
            with open(log_path, encoding="utf-8") as handle:
                return [json.loads(line) for line in handle if line.strip()]
        except FileNotFoundError:
            return []

    def resume_count(thread_id: str) -> int:
        return sum(
            request.get("method") == "thread/resume"
            and (request.get("params") or {}).get("threadId") == thread_id
            for request in recorded_requests()
        )

    def unsubscribe_count(thread_id: str) -> int:
        return sum(
            request.get("method") == "thread/unsubscribe"
            and (request.get("params") or {}).get("threadId") == thread_id
            for request in recorded_requests()
        )

    def text_node(fragment: str) -> Atspi.Accessible | None:
        return find_matching(app, lambda node: fragment in accessible_text(node))

    def thread_row(prefix: str) -> Atspi.Accessible | None:
        return find_matching(
            app,
            lambda node: node.get_role_name() == "button"
            and normalized_thread_name(node_name(node)).startswith(prefix),
        )

    def wait_task_controls(expected: tuple[str, ...]) -> None:
        wait_until(
            lambda: all(find_named(app, name, role="combo box") for name in expected),
            timeout=10.0,
        )

    def press_thread(prefix: str) -> None:
        def activate() -> bool:
            row = thread_row(prefix)
            if row is None:
                return False
            press(row)
            return True

        wait_until(activate, timeout=10.0)

    press_named(app, "Chat")
    wait_until(lambda: thread_row("Existing task send fixture"), timeout=10.0)
    wait_until(lambda: thread_row("Second existing task fixture"), timeout=10.0)
    baseline = len(recorded_requests())
    initial_a = resume_count("existing-task-send-fixture")
    initial_b = resume_count("second-existing-task-fixture")
    initial_a_unsubscribes = unsubscribe_count("existing-task-send-fixture")

    press_thread("Existing task send fixture")
    wait_task_controls(
        (
            "Model: GPT Smoke",
            "Reasoning effort: Max",
            "Response speed: Standard",
            "Sandbox: Workspace write",
            "Approval policy: On request",
        )
    )
    wait_until(
        lambda: text_node(
            "Existing task send fixture loaded from the local smoke fixture."
        ),
        timeout=10.0,
    )
    first_a = resume_count("existing-task-send-fixture")
    if first_a not in {initial_a, initial_a + 1} or first_a == 0:
        raise RuntimeError("first task did not establish exactly one live resume")
    press_thread("Second existing task fixture")
    wait_task_controls(
        (
            "Model: GPT Smoke Alt",
            "Reasoning effort: Light",
            "Response speed: Fast",
            "Sandbox: Read only",
            "Approval policy: Never ask",
        )
    )
    wait_until(
        lambda: text_node(
            "Second existing task fixture loaded from the local smoke fixture."
        ),
        timeout=10.0,
    )
    first_b = resume_count("second-existing-task-fixture")
    if first_b not in {initial_b, initial_b + 1} or first_b == 0:
        raise RuntimeError("second task did not establish exactly one live resume")
    if not any(
        request.get("method") == "thread/turns/list"
        and (request.get("params") or {}).get("threadId")
        == "second-existing-task-fixture"
        and (request.get("params") or {}).get("itemsView") == "full"
        for request in recorded_requests()[baseline:]
    ):
        raise RuntimeError(
            "resume initialTurnsPage did not auto-hydrate recent full activity"
        )
    wait_until(
        lambda: unsubscribe_count("existing-task-send-fixture")
        == initial_a_unsubscribes + 1,
        timeout=10.0,
    )
    press_thread("Existing task send fixture")
    wait_task_controls(
        (
            "Model: GPT Smoke",
            "Reasoning effort: Max",
            "Response speed: Standard",
            "Sandbox: Workspace write",
            "Approval policy: On request",
        )
    )
    wait_until(
        lambda: resume_count("existing-task-send-fixture") == first_a + 1,
        timeout=10.0,
    )
    wait_until(
        lambda: text_node(
            "Existing task send fixture loaded from the local smoke fixture."
        ),
        timeout=10.0,
    )
    second_a = resume_count("existing-task-send-fixture")

    requests = recorded_requests()[baseline:]
    if any(request.get("method") == "thread/read" for request in requests):
        raise RuntimeError("task switching used stale thread/read instead of resume")
    if any(request.get("method") == "thread/settings/update" for request in requests):
        raise RuntimeError("hydrating task controls wrote settings back to the server")

    press_thread("Existing task send fixture")
    time.sleep(0.3)
    if resume_count("existing-task-send-fixture") != second_a:
        raise RuntimeError("reselecting the open task redundantly resumed it")
    settings_baseline = len(recorded_requests())
    select_combo_option(app, "Response speed: Standard", "Fast")
    wait_until(
        lambda: find_named(app, "Response speed: Fast", role="combo box"),
        timeout=10.0,
    )
    wait_until(
        lambda: any(
            request.get("method") == "thread/settings/update"
            and (request.get("params") or {}).get("threadId")
            == "existing-task-send-fixture"
            and (request.get("params") or {}).get("serviceTier") == "priority"
            for request in recorded_requests()[settings_baseline:]
        ),
        timeout=10.0,
    )
    select_combo_option(app, "Response speed: Fast", "Standard")
    wait_until(
        lambda: find_named(app, "Response speed: Standard", role="combo box"),
        timeout=10.0,
    )
    wait_until(
        lambda: any(
            request.get("method") == "thread/settings/update"
            and (request.get("params") or {}).get("threadId")
            == "existing-task-send-fixture"
            and (request.get("params") or {}).get("serviceTier") is None
            for request in recorded_requests()[settings_baseline:]
        ),
        timeout=10.0,
    )
    time.sleep(0.3)
    if find_named(app, "Response speed: default", role="combo box") is not None:
        raise RuntimeError("Standard speed snapped back to the raw server 'default' tier")
    if any(
        request.get("method") == "thread/resume"
        and (request.get("params") or {}).get("threadId")
        == "codex-native-smoke-task"
        for request in requests
    ):
        raise RuntimeError("local-only smoke task was incorrectly sent to app-server")
    assert_responsive(app)
    print(
        "PASS task switching: server A -> B -> A with one resume per live subscription and task-scoped controls"
    )
    print("PASS response speed: Fast -> Standard stays Standard after server normalization")


def smoke_history_recovery(app: Atspi.Accessible) -> None:
    codex_home = os.environ.get("CODEX_HOME")
    if not codex_home or os.environ.get("CODEX_NATIVE_FAKE_HISTORY_RECOVERY") != "1":
        raise RuntimeError(
            "CODEX_HOME and CODEX_NATIVE_FAKE_HISTORY_RECOVERY=1 are required"
        )

    def text_node(fragment: str) -> Atspi.Accessible | None:
        return find_matching(app, lambda node: fragment in accessible_text(node))

    def thread_row(prefix: str) -> Atspi.Accessible | None:
        return find_matching(
            app,
            lambda node: node.get_role_name() == "button"
            and normalized_thread_name(node_name(node)).startswith(prefix),
        )

    press_named(app, "Chat")
    recovery = wait_until(
        lambda: thread_row("Stale iOS history recovery fixture"), timeout=10.0
    )
    press(recovery)
    wait_until(
        lambda: text_node("Canonical iOS prompt missing from the stale projection."),
        timeout=15.0,
    )
    wait_until(
        lambda: text_node("Canonical iOS answer restored from the rollout."),
        timeout=15.0,
    )
    history_database = Path(codex_home) / "thread_history_1.sqlite"
    wait_until(
        lambda: sqlite3.connect(history_database)
        .execute(
            "SELECT next_rollout_ordinal FROM thread_history_projection_state "
            "WHERE thread_id = ?",
            ("019f9f86-8517-7bd3-ae7a-43146df7e5f7",),
        )
        .fetchone()
        == (5,),
        timeout=15.0,
    )
    with sqlite3.connect(history_database) as database:
        projected_types = {
            row[0]
            for row in database.execute(
                "SELECT item_type FROM thread_items WHERE thread_id = ?",
                ("019f9f86-8517-7bd3-ae7a-43146df7e5f7",),
            )
        }
    if projected_types != {"userMessage", "agentMessage"}:
        raise RuntimeError(
            f"shared projection repair stored unexpected items: {projected_types}"
        )

    rollout = next(
        Path(codex_home).glob(
            "sessions/**/rollout-*-019f9f86-8517-7bd3-ae7a-43146df7e5f7.jsonl"
        ),
        None,
    )
    if rollout is None:
        raise RuntimeError("history recovery rollout was not created")
    turn_id = "019f9f86-bbbb-7493-898e-0b93180d2a18"
    records = [
        {
            "ordinal": 5,
            "type": "event_msg",
            "payload": {
                "type": "task_started",
                "turn_id": turn_id,
                "started_at": 30,
            },
        },
        {
            "ordinal": 6,
            "type": "event_msg",
            "payload": {
                "type": "item_completed",
                "thread_id": "019f9f86-8517-7bd3-ae7a-43146df7e5f7",
                "turn_id": turn_id,
                "item": {
                    "type": "UserMessage",
                    "id": "history-recovery-user-2",
                    "content": [
                        {
                            "type": "text",
                            "text": "New iOS turn appeared without a projection update.",
                        }
                    ],
                },
            },
        },
        {
            "ordinal": 7,
            "type": "event_msg",
            "payload": {
                "type": "item_completed",
                "thread_id": "019f9f86-8517-7bd3-ae7a-43146df7e5f7",
                "turn_id": turn_id,
                "item": {
                    "type": "AgentMessage",
                    "id": "history-recovery-agent-2",
                    "content": [
                        {
                            "type": "Text",
                            "text": "Native picked up the live canonical transcript change.",
                        }
                    ],
                    "phase": "final_answer",
                },
            },
        },
        {
            "ordinal": 8,
            "type": "event_msg",
            "payload": {
                "type": "task_complete",
                "turn_id": turn_id,
                "started_at": 30,
                "completed_at": 40,
            },
        },
    ]
    with rollout.open("a", encoding="utf-8") as handle:
        for record in records:
            handle.write(json.dumps(record, separators=(",", ":")) + "\n")
        handle.flush()
        os.fsync(handle.fileno())

    wait_until(
        lambda: text_node("New iOS turn appeared without a projection update."),
        timeout=15.0,
    )
    wait_until(
        lambda: text_node("Native picked up the live canonical transcript change."),
        timeout=15.0,
    )

    other = wait_until(
        lambda: thread_row("Second existing task fixture"), timeout=10.0
    )
    press(other)
    wait_until(
        lambda: text_node(
            "Second existing task fixture loaded from the local smoke fixture."
        ),
        timeout=10.0,
    )
    recovery = wait_until(
        lambda: thread_row("Stale iOS history recovery fixture"), timeout=10.0
    )
    press(recovery)
    wait_until(
        lambda: text_node("Canonical iOS answer restored from the rollout."),
        timeout=15.0,
    )
    wait_until(
        lambda: text_node("Native picked up the live canonical transcript change."),
        timeout=15.0,
    )
    assert_responsive(app)
    print(
        "PASS iOS history recovery: stale projection filled from canonical rollout, live append appeared, and both survived A -> B -> A switching"
    )


def smoke_task_routing(app: Atspi.Accessible) -> None:
    """Exercise explicit model selection and backend-specific effort menus."""
    log_path = os.environ.get("CODEX_NATIVE_FAKE_LOG")
    if not log_path:
        raise RuntimeError("CODEX_NATIVE_FAKE_LOG is required for routing smoke")

    def recorded_requests() -> list[dict[str, object]]:
        try:
            with open(log_path, encoding="utf-8") as handle:
                return [json.loads(line) for line in handle if line.strip()]
        except FileNotFoundError:
            return []

    def thread_row(prefix: str) -> Atspi.Accessible | None:
        return find_matching(
            app,
            lambda node: node.get_role_name() == "button"
            and normalized_thread_name(node_name(node)).startswith(prefix),
        )

    def press_thread(prefix: str) -> None:
        node = wait_until(lambda: thread_row(prefix), timeout=10.0)
        press(node)

    press_named(app, "Chat")
    press_thread("Existing task send fixture")
    baseline = len(recorded_requests())
    if find_named(app, "Auto") is not None or find_named(app, "Manual") is not None:
        raise RuntimeError("retired Auto/Manual routing controls are still visible")

    model = wait_until(
        lambda: find_matching(
            app,
            lambda node: node.get_role_name() == "combo box"
            and node_name(node).startswith("Model:"),
        )
    )
    select_combo_option(app, node_name(model), "OpenCode")
    wait_until(
        lambda: find_named(app, "Model: OpenCode (Qwen Local)", role="combo box")
    )
    wait_until(
        lambda: find_named(app, "Reasoning effort: Non-thinking", role="combo box")
    )
    select_combo_option(app, "Reasoning effort: Non-thinking", "High")
    wait_until(lambda: find_named(app, "Reasoning effort: High", role="combo box"))
    select_combo_option(app, "Model: OpenCode (Qwen Local)", "Gemini")
    wait_until(lambda: find_named(app, "Model: Gemini", role="combo box"))
    select_combo_option(app, "Reasoning effort: High", "Minimal")
    wait_until(lambda: find_named(app, "Reasoning effort: Minimal", role="combo box"))

    updates = [
        request
        for request in recorded_requests()[baseline:]
        if request.get("method") == "thread/settings/update"
    ]
    if not updates:
        raise RuntimeError("delegate model selection did not update task settings")
    latest = updates[-1].get("params") or {}
    if latest.get("model") != "gpt-5.6-terra" or latest.get("effort") != "low":
        raise RuntimeError(f"Gemini host mapping is incorrect: {latest!r}")

    direct_baseline = len(recorded_requests())
    composer = wait_until(lambda: find_named(app, "Task message"), timeout=5.0)
    set_text(composer, "Answer through Gemini with zero Codex allowance.")
    press_sensitive_named(app, "Send")
    wait_until(
        lambda: find_matching(
            app,
            lambda node: "quota-independent Buddy fixture" in accessible_text(node),
        ),
        timeout=15.0,
    )
    wait_until(
        lambda: find_named(
            app,
            "Potential GPT tokens saved: ~150 · Buddy ~150 · local estimate",
            role="label",
        ),
        timeout=10.0,
    )
    direct_requests = recorded_requests()[direct_baseline:]
    if any(
        request.get("method") in {"turn/start", "turn/steer", "thread/inject_items"}
        for request in direct_requests
    ):
        raise RuntimeError("selected Gemini still depended on the Codex turn transport")

    select_combo_option(app, "Model: Gemini", "OpenRouter Free")
    wait_until(lambda: find_named(app, "Model: OpenRouter Free", role="combo box"))
    wait_until(lambda: find_named(app, "Reasoning effort: Minimal", role="combo box"))
    openrouter_baseline = len(recorded_requests())
    set_text(composer, "Answer through OpenRouter Free with zero Codex allowance.")
    press_sensitive_named(app, "Send")
    wait_until(
        lambda: find_matching(
            app,
            lambda node: node_name(node).startswith(
                "OpenRouter Free: quota-independent Buddy fixture"
            ),
        ),
        timeout=15.0,
    )
    openrouter_requests = recorded_requests()[openrouter_baseline:]
    if any(
        request.get("method") in {"turn/start", "turn/steer", "thread/inject_items"}
        for request in openrouter_requests
    ):
        raise RuntimeError("selected OpenRouter still depended on the Codex turn transport")

    select_combo_option(app, "Model: OpenRouter Free", "Mistral AI")
    wait_until(lambda: find_named(app, "Model: Mistral AI", role="combo box"))
    wait_until(
        lambda: find_named(app, "Reasoning effort: High", role="combo box")
    )
    select_combo_option(app, "Reasoning effort: High", "Non-thinking")
    wait_until(
        lambda: find_named(app, "Reasoning effort: Non-thinking", role="combo box")
    )
    select_combo_option(app, "Reasoning effort: Non-thinking", "High")
    wait_until(lambda: find_named(app, "Reasoning effort: High", role="combo box"))
    mistral_baseline = len(recorded_requests())
    set_text(composer, "Answer through Mistral AI with zero Codex allowance.")
    press_sensitive_named(app, "Send")
    wait_until(
        lambda: find_matching(
            app,
            lambda node: node_name(node).startswith(
                "Mistral AI: quota-independent Buddy fixture"
            ),
        ),
        timeout=15.0,
    )
    mistral_requests = recorded_requests()[mistral_baseline:]
    if any(
        request.get("method") in {"turn/start", "turn/steer", "thread/inject_items"}
        for request in mistral_requests
    ):
        raise RuntimeError("selected Mistral still depended on the Codex turn transport")

    select_combo_option(app, "Model: Mistral AI", "OpenCode")
    wait_until(
        lambda: find_named(app, "Model: OpenCode (Qwen Local)", role="combo box")
    )
    qwen_effort = wait_until(
        lambda: find_matching(
            app,
            lambda node: node.get_role_name() == "combo box"
            and node_name(node).startswith("Reasoning effort:"),
        )
    )
    select_combo_option(app, node_name(qwen_effort), "High")
    wait_until(
        lambda: find_named(app, "Reasoning effort: High", role="combo box")
    )
    qwen_baseline = len(recorded_requests())
    set_text(composer, "Answer through Qwen with zero Codex allowance.")
    press_sensitive_named(app, "Send")
    wait_until(
        lambda: find_matching(
            app,
            lambda node: node_name(node).startswith(
                "Qwen: quota-independent Buddy fixture"
            ),
        ),
        timeout=15.0,
    )
    wait_until(lambda: find_named(app, "Qwen verbose thinking and activity"), timeout=10.0)
    wait_until(
        lambda: find_matching(
            app,
            lambda node: "Read completed — tools/fake_qwen_buddy_code.py L1-L20 of 44"
            in accessible_text(node),
        ),
        timeout=10.0,
    )
    wait_until(
        lambda: find_named(
            app,
            "Qwen context 150 / 65,536 tokens (0%) · 42.5 tok/s · Qwen task total 150 tokens · All Buddy models 600 tokens",
            role="label",
        ),
        timeout=10.0,
    )
    qwen_requests = recorded_requests()[qwen_baseline:]
    if any(
        request.get("method") in {"turn/start", "turn/steer", "thread/inject_items"}
        for request in qwen_requests
    ):
        raise RuntimeError("selected Qwen still depended on the Codex turn transport")
    assert_responsive(app)
    print(
        "PASS model routing: Qwen/Gemini/OpenRouter/Mistral efforts are backend-specific and quota-independent; Qwen shows durable activity plus current/cumulative context usage"
    )


def smoke_qwen_max_savings(app: Atspi.Accessible) -> None:
    """Require a Qwen Sol subagent and count only its hidden numeric evidence."""
    log_path = os.environ.get("CODEX_NATIVE_FAKE_LOG")
    if not log_path:
        raise RuntimeError("CODEX_NATIVE_FAKE_LOG is required for Qwen savings smoke")
    if not os.environ.get("CODEX_NATIVE_FAKE_QWEN_READY"):
        raise RuntimeError("CODEX_NATIVE_FAKE_QWEN_READY is required for Qwen savings smoke")

    def recorded_requests() -> list[dict[str, object]]:
        try:
            with open(log_path, encoding="utf-8") as handle:
                return [json.loads(line) for line in handle if line.strip()]
        except FileNotFoundError:
            return []

    def checked_button(name: str) -> Atspi.Accessible | None:
        return find_matching(
            app,
            lambda node: node_name(node) == name
            and has_activation_action(node)
            and is_checked(node),
        )

    press_named(app, "Chat")
    row = wait_until(
        lambda: find_matching(
            app,
            lambda node: node.get_role_name() == "button"
            and normalized_thread_name(node_name(node)).startswith(
                "Existing task send fixture"
            ),
        ),
        timeout=10.0,
    )
    press(row)
    wait_until(lambda: find_named(app, "Task message"), timeout=10.0)
    if checked_button("Manual") is None:
        press_sensitive_named(app, "Manual")
        wait_until(lambda: checked_button("Manual"), timeout=10.0)
    press_sensitive_named(app, "Auto")
    wait_until(lambda: checked_button("Auto"), timeout=10.0)

    baseline = len(recorded_requests())
    prompt = (
        "Investigate why this task switching bug occurs in the app and propose "
        "one bounded code fix."
    )
    composer = wait_until(lambda: find_named(app, "Task message"), timeout=5.0)
    set_text(composer, prompt)
    press_sensitive_named(app, "Send")

    request = wait_until(
        lambda: next(
            (
                request
                for request in recorded_requests()[baseline:]
                if request.get("method") == "turn/start"
                and (request.get("params") or {}).get("threadId")
                == "existing-task-send-fixture"
            ),
            None,
        ),
        timeout=10.0,
    )
    params = request.get("params") or {}
    qwen_context = (params.get("additionalContext") or {}).get(
        "codex-native.qwen-buddy-routing"
    )
    qwen_value = qwen_context.get("value", "") if isinstance(qwen_context, dict) else ""
    if "local_qwen_agent" not in qwen_value or "delegation is required" not in qwen_value:
        raise RuntimeError("Qwen Assist did not require its bounded Qwen Sol subagent")
    metadata = params.get("responsesapiClientMetadata") or {}
    if metadata.get("qwen_buddy_routing") != "Qwen Sol subagent":
        raise RuntimeError("turn metadata did not identify the selected Qwen Sol subagent")

    receipt = wait_until(
        lambda: find_matching(
            app,
            lambda node: node.get_role_name() == "label"
            and "Qwen ~700" in node_name(node),
        ),
        timeout=10.0,
    )
    description = receipt.get_description() or ""
    for fragment in (
        "Qwen evidence: 1 successful use",
        "3,900 measured local tokens",
        "unobservable counterfactual reasoning and input savings are excluded",
        "never added to the prompt",
    ):
        if fragment not in description:
            raise RuntimeError(f"Qwen savings receipt is missing: {fragment}")
    if any(
        "potentialCodexTokensSavedApprox" in json.dumps(request)
        or "Potential tokens saved" in json.dumps(request)
        for request in recorded_requests()[baseline:]
    ):
        raise RuntimeError("hidden Qwen savings evidence leaked into an app-server request")

    press_sensitive_named(app, "Manual")
    wait_until(lambda: checked_button("Manual"), timeout=10.0)
    assert_responsive(app)
    print(
        "PASS Qwen Assist: preserved Codex settings, required a bounded Qwen Sol subagent, counted ~700 conservative tokens, and restored Manual"
    )


def smoke_new_task_auto_routing(app: Atspi.Accessible) -> None:
    """Create a task and prove Qwen Assist preserves its Codex settings."""
    log_path = os.environ.get("CODEX_NATIVE_FAKE_LOG")
    if not log_path:
        raise RuntimeError("CODEX_NATIVE_FAKE_LOG is required for new-task routing smoke")

    def recorded_requests() -> list[dict[str, object]]:
        try:
            with open(log_path, encoding="utf-8") as handle:
                return [json.loads(line) for line in handle if line.strip()]
        except FileNotFoundError:
            return []

    def checked_button(name: str) -> Atspi.Accessible | None:
        return find_matching(
            app,
            lambda node: node_name(node) == name
            and has_activation_action(node)
            and is_checked(node),
        )

    def request_for(
        requests: list[dict[str, object]], method: str
    ) -> dict[str, object] | None:
        return next(
            (request for request in requests if request.get("method") == method),
            None,
        )

    press_named(app, "Chat")
    press_sensitive_named(app, "New task")
    wait_until(lambda: checked_button("Auto"), timeout=10.0)
    wait_until(
        lambda: find_matching(
            app,
            lambda node: node.get_role_name() == "label"
            and node_name(node).startswith("Qwen Assist"),
        ),
        timeout=10.0,
    )

    baseline = len(recorded_requests())
    composer = wait_until(lambda: find_named(app, "Task message"), timeout=5.0)
    set_text(composer, "Summarize this local routing fixture.")
    press_sensitive_named(app, "Send")

    created = wait_until(
        lambda: request_for(recorded_requests()[baseline:], "thread/start"),
        timeout=10.0,
    )
    created_params = created.get("params") or {}
    selected_model = created_params.get("model")
    selected_effort = (created_params.get("config") or {}).get(
        "model_reasoning_effort"
    )
    selected_speed = created_params.get("serviceTier")
    if not selected_model:
        raise RuntimeError("fresh Qwen Assist task did not preserve a selected Codex model")
    if not selected_effort:
        raise RuntimeError(
            "fresh Qwen Assist task did not preserve a selected reasoning effort"
        )

    def assisted_turn() -> dict[str, object] | None:
        requests = recorded_requests()[baseline:]
        turns = [
            request
            for request in requests
            if request.get("method") == "turn/start"
            and str((request.get("params") or {}).get("threadId", "")).startswith(
                "auto-routing-new-task-"
            )
        ]
        return turns[-1] if turns else None

    turn_request = wait_until(assisted_turn, timeout=10.0)
    turn_params = turn_request.get("params") or {}
    if turn_params.get("model") != selected_model:
        raise RuntimeError(
            "Qwen Assist replaced the selected Codex model: "
            f"thread/start={selected_model!r}, turn/start={turn_params.get('model')!r}"
        )
    if turn_params.get("effort") != selected_effort:
        raise RuntimeError(
            "Qwen Assist replaced the selected reasoning effort: "
            f"thread/start={selected_effort!r}, turn/start={turn_params.get('effort')!r}"
        )
    if turn_params.get("serviceTier") != selected_speed:
        raise RuntimeError(
            "Qwen Assist replaced the selected response speed: "
            f"thread/start={selected_speed!r}, turn/start={turn_params.get('serviceTier')!r}"
        )
    if any(
        request.get("method") == "thread/settings/update"
        and str((request.get("params") or {}).get("threadId", "")).startswith(
            "auto-routing-new-task-"
        )
        for request in recorded_requests()[baseline:]
    ):
        raise RuntimeError("Qwen Assist rewrote the new task's shared Codex settings")

    wait_until(
        lambda: find_matching(
            app,
            lambda node: node.get_role_name() == "button"
            and normalized_thread_name(node_name(node)).startswith(
                "Auto routing new task 1"
            ),
        ),
        timeout=10.0,
    )
    wait_until(lambda: checked_button("Auto"), timeout=10.0)
    assert_responsive(app)
    print(
        "PASS fresh task Qwen Assist: created task and first turn preserved the selected Codex model, effort, and speed without a settings rewrite"
    )


def chatgpt_is_unloaded(app: Atspi.Accessible) -> bool:
    return bool(
        find_matching(
            app,
            lambda node: node.get_role_name() == "label"
            and (
                node_name(node) == "ChatGPT is unloaded."
                or node_name(node).startswith("ChatGPT unloaded;")
            ),
        )
    )


def chatgpt_is_loaded(app: Atspi.Accessible) -> bool:
    return bool(
        find_matching(
            app,
            lambda node: node.get_role_name() == "label"
            and node_name(node)
            in {
                "ChatGPT ready",
                "ChatGPT sign-in required",
                "ChatGPT is loaded. Sign in once here to use Pro chat and Voice.",
                "ChatGPT is running in-app. Select Pro in ChatGPT’s model picker or use its Voice button.",
            },
        )
    )


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--dump", action="store_true", help="print the accessible tree")
    parser.add_argument(
        "--all-nodes", action="store_true", help="include hidden nodes in --dump"
    )
    parser.add_argument(
        "--navigation", action="store_true", help="activate every workspace page"
    )
    parser.add_argument(
        "--memoria-jobs",
        action="store_true",
        help="verify the live Memoria work count in the top header",
    )
    parser.add_argument(
        "--reset-dialog", action="store_true", help="open and cancel reset confirmation"
    )
    parser.add_argument(
        "--settings", action="store_true", help="smoke reversible settings and save"
    )
    parser.add_argument(
        "--pin-menu", action="store_true", help="toggle and restore a task pin by right-click"
    )
    parser.add_argument(
        "--goals", action="store_true", help="create, stop, resume, and clear a test goal"
    )
    parser.add_argument(
        "--chatgpt", action="store_true", help="load and fully unload in-app ChatGPT"
    )
    parser.add_argument(
        "--active-task", action="store_true", help="verify the open task is highlighted"
    )
    parser.add_argument(
        "--bulk-tasks",
        action="store_true",
        help="select multiple fake tasks, archive them, and confirm bulk deletion",
    )
    parser.add_argument(
        "--rich-transcript",
        action="store_true",
        help="open a transcript image activity in the native viewer",
    )
    parser.add_argument(
        "--existing-task-send",
        action="store_true",
        help="send through the deterministic resumed-thread app-server fixture",
    )
    parser.add_argument(
        "--task-switching",
        action="store_true",
        help="switch between two deterministic app-server tasks and back",
    )
    parser.add_argument(
        "--history-recovery",
        action="store_true",
        help="recover stale iOS chat from a canonical rollout and survive task switching",
    )
    parser.add_argument(
        "--task-routing",
        action="store_true",
        help="toggle task-local Qwen Assist/Manual without rewriting Codex settings",
    )
    parser.add_argument(
        "--new-task-routing",
        action="store_true",
        help="create a fake-server task and verify Qwen Assist preserves its Codex settings",
    )
    parser.add_argument(
        "--qwen-savings",
        action="store_true",
        help="require a fake Qwen Sol subagent and verify zero-inference savings accounting",
    )
    parser.add_argument(
        "--stale-steer-recovery",
        action="store_true",
        help="retry a stale turn/steer failure through turn/start",
    )
    parser.add_argument(
        "--safe-actions",
        action="store_true",
        help="exercise reversible actions without inference or external mutation",
    )
    parser.add_argument(
        "--stop-turn",
        action="store_true",
        help="interrupt the active local fixture and verify the Stop action",
    )
    args = parser.parse_args()

    try:
        app = wait_until(find_app)
        if args.dump:
            dump_tree(app, showing_only=not args.all_nodes)
        if args.stop_turn:
            smoke_stop_turn(app)
        if args.navigation:
            smoke_navigation(app)
        if args.memoria_jobs:
            smoke_memoria_jobs(app)
        if args.reset_dialog:
            smoke_reset_dialog(app)
        if args.settings:
            smoke_settings(app)
        if args.pin_menu:
            smoke_task_pin_context_menu(app)
        if args.goals:
            smoke_goal_lifecycle(app)
        if args.chatgpt:
            smoke_chatgpt(app)
        if args.active_task:
            smoke_active_task_highlight(app)
        if args.bulk_tasks:
            smoke_bulk_task_actions(app)
        if args.rich_transcript:
            smoke_rich_transcript(app)
        if args.existing_task_send:
            smoke_existing_task_send(app)
        if args.task_switching:
            smoke_task_switching(app)
        if args.history_recovery:
            smoke_history_recovery(app)
        if args.task_routing:
            smoke_task_routing(app)
        if args.new_task_routing:
            smoke_new_task_auto_routing(app)
        if args.qwen_savings:
            smoke_qwen_max_savings(app)
        if args.stale_steer_recovery:
            smoke_stale_steer_recovery(app)
        if args.safe_actions:
            smoke_safe_actions(app)
        if not any(
            [
                args.dump,
                args.navigation,
                args.memoria_jobs,
                args.reset_dialog,
                args.settings,
                args.pin_menu,
                args.goals,
                args.chatgpt,
                args.active_task,
                args.bulk_tasks,
                args.rich_transcript,
                args.existing_task_send,
                args.task_switching,
                args.history_recovery,
                args.task_routing,
                args.new_task_routing,
                args.qwen_savings,
                args.stale_steer_recovery,
                args.safe_actions,
                args.stop_turn,
            ]
        ):
            parser.error("choose at least one smoke action")
    except Exception as error:  # noqa: BLE001 - release smoke should report all failures
        print(f"FAIL: {error}", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
