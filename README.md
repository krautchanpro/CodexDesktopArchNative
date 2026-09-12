# Codex Native

Codex Native is a native GTK4 desktop client for the Codex CLI on Arch Linux.
It provides a fast, Linux-first workspace for coding tasks without requiring
the official Codex Desktop app.

> **Status:** community project for Arch Linux. You need a ChatGPT account with
> Codex access and the Codex CLI installed separately.

## What it does

- Native GTK4/libadwaita interface with ChatGPT and Arch Linux branding.
- Create, resume, search, organize, and review coding tasks.
- Choose a model, reasoning level, speed, sandbox, and approval policy for
  each task.
- View streaming answers, plans, tool activity, file changes, diffs, images,
  and attachments.
- Keep two ChatGPT accounts separate while showing a local task timeline.
- Optional Remote Control integration for continuing a task from a paired
  device.
- Native terminal, project list, plugins, skills, diagnostics, and settings.
- Per-task **Subagents: On/Off** control. When it is off, Codex Native does
  not route that task through a subagent provider.

## What it does not need

Codex Native does **not** depend on, update from, launch, or require the
official Codex Desktop app. You can uninstall the desktop app and continue to
use Codex Native as long as the Codex CLI is available.

## Requirements

- Arch Linux or an Arch-based distribution
- Rust 1.92 or newer
- GTK 4.18+, libadwaita, GtkSourceView 5, VTE 4, and WebKitGTK 6
- Node.js and npm
- The [Codex CLI](https://github.com/openai/codex)
- A ChatGPT account with Codex access

## Install

Install dependencies and the Codex CLI:

```sh
sudo pacman -S --needed base-devel rust gtk4 libadwaita gtksourceview5 vte4 webkitgtk-6.0 sqlite nodejs npm
npm install --global @openai/codex
codex --version
```

Build and install Codex Native:

```sh
git clone https://github.com/krautchanpro/CodexDesktopArchNative.git codex-native
cd codex-native
make
sudo make install
```

Launch **Codex Native** from the desktop menu or run:

```sh
codex-native
```

If the CLI is not on `PATH`, set `CODEX_CLI_PATH` to its full path before
launching, or select it in Codex Native settings.

## First use

1. Open Codex Native.
2. Sign in with ChatGPT when prompted. Sign-in happens in your normal browser;
   Codex Native does not store your password.
3. Add a project folder, then start a task.
4. Use the bottom-bar **Subagents** button to choose whether that task may use
   subagents.

Remote Control is optional. Enable it from the app only if you want to access
the same task from a paired device.

## Development

```sh
make check    # formatting and clippy
make test     # Rust and remote-host tests
make          # release build
```

The Arch package recipe is in [`packaging/PKGBUILD`](packaging/PKGBUILD).

## License

[MIT](LICENSE)

Codex and ChatGPT are trademarks of OpenAI. Codex Native is an independent
community project and is not an official OpenAI desktop application.
