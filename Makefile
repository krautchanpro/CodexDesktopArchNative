PREFIX ?= /usr
DESTDIR ?=
CARGO ?= cargo

.PHONY: all check test install uninstall clean

all:
	$(CARGO) build --release --locked

check:
	$(CARGO) fmt --all -- --check
	$(CARGO) clippy --all-targets --all-features -- -D warnings

test:
	$(CARGO) test --all-targets --all-features --locked
	python3 -m unittest tools/test_remote_host.py

install: all
	install -Dm755 target/release/codex-native "$(DESTDIR)$(PREFIX)/bin/codex-native"
	install -Dm755 target/release/codex-native-macro "$(DESTDIR)$(PREFIX)/lib/codex-native/codex-native-macro"
	install -Dm755 tools/codex-native-launch "$(DESTDIR)$(PREFIX)/bin/codex-native-launch"
	install -Dm644 data/io.codexnative.Arch.desktop "$(DESTDIR)$(PREFIX)/share/applications/io.codexnative.Arch.desktop"
	install -Dm644 data/io.codexnative.Arch.metainfo.xml "$(DESTDIR)$(PREFIX)/share/metainfo/io.codexnative.Arch.metainfo.xml"
	install -Dm644 data/io.codexnative.Arch.svg "$(DESTDIR)$(PREFIX)/share/icons/hicolor/scalable/apps/io.codexnative.Arch.svg"
	install -Dm644 data/icons/chatgpt-symbol.ico "$(DESTDIR)$(PREFIX)/share/icons/hicolor/48x48/apps/chatgpt-symbol.ico"
	install -Dm644 data/icons/arch-linux-symbol.svg "$(DESTDIR)$(PREFIX)/share/icons/hicolor/scalable/apps/arch-linux-symbol.svg"
	install -Dm644 data/codex-native-remote.service "$(DESTDIR)$(PREFIX)/lib/systemd/user/codex-native-remote.service"
	install -Dm644 data/codex-native-remote@.service "$(DESTDIR)$(PREFIX)/lib/systemd/user/codex-native-remote@.service"
	install -Dm755 tools/codex-native-remote-host "$(DESTDIR)$(PREFIX)/lib/codex-native/codex-native-remote-host"
	install -Dm755 tools/codex-native-remote-watchdog "$(DESTDIR)$(PREFIX)/lib/codex-native/codex-native-remote-watchdog"
	install -Dm644 data/codex-native-remote-watchdog.service "$(DESTDIR)$(PREFIX)/lib/systemd/user/codex-native-remote-watchdog.service"
	install -Dm644 data/codex-native-remote-watchdog.timer "$(DESTDIR)$(PREFIX)/lib/systemd/user/codex-native-remote-watchdog.timer"
	install -Dm644 data/codex-native-app.service "$(DESTDIR)$(PREFIX)/lib/systemd/user/codex-native-app.service"
	install -Dm644 LICENSE "$(DESTDIR)$(PREFIX)/share/licenses/codex-native/LICENSE"

uninstall:
	rm -f "$(DESTDIR)$(PREFIX)/bin/codex-native"
	rm -f "$(DESTDIR)$(PREFIX)/lib/codex-native/codex-native-macro"
	rm -f "$(DESTDIR)$(PREFIX)/bin/codex-native-launch"
	rm -f "$(DESTDIR)$(PREFIX)/share/applications/io.codexnative.Arch.desktop"
	rm -f "$(DESTDIR)$(PREFIX)/share/metainfo/io.codexnative.Arch.metainfo.xml"
	rm -f "$(DESTDIR)$(PREFIX)/share/icons/hicolor/scalable/apps/io.codexnative.Arch.svg"
	rm -f "$(DESTDIR)$(PREFIX)/share/icons/hicolor/48x48/apps/chatgpt-symbol.ico"
	rm -f "$(DESTDIR)$(PREFIX)/share/icons/hicolor/scalable/apps/arch-linux-symbol.svg"
	rm -f "$(DESTDIR)$(PREFIX)/lib/systemd/user/codex-native-remote.service"
	rm -f "$(DESTDIR)$(PREFIX)/lib/systemd/user/codex-native-remote@.service"
	rm -f "$(DESTDIR)$(PREFIX)/lib/systemd/user/codex-native-app.service"
	rm -f "$(DESTDIR)$(PREFIX)/lib/codex-native/codex-native-remote-host"
	rm -f "$(DESTDIR)$(PREFIX)/lib/codex-native/codex-native-remote-watchdog"
	rm -f "$(DESTDIR)$(PREFIX)/lib/systemd/user/codex-native-remote-watchdog.service"
	rm -f "$(DESTDIR)$(PREFIX)/lib/systemd/user/codex-native-remote-watchdog.timer"
	rm -f "$(DESTDIR)$(PREFIX)/lib/codex-native/codex-routed"
	rm -f "$(DESTDIR)$(PREFIX)/lib/codex-native/codex-routed.provenance.json"
	rm -f "$(DESTDIR)$(PREFIX)/lib/codex-native/router/codex-native-router-update"
	rm -f "$(DESTDIR)$(PREFIX)/lib/codex-native/router/build-routed-codex"
	rm -f "$(DESTDIR)$(PREFIX)/lib/codex-native/router/remote-auto-router.patch"
	rm -f "$(DESTDIR)$(PREFIX)/lib/systemd/user/codex-native-router-update.service"
	rm -f "$(DESTDIR)$(PREFIX)/lib/systemd/user/codex-native-router-update.timer"
	rm -f "$(DESTDIR)$(PREFIX)/lib/systemd/user/codex-native-router-update.path"
	rm -f "$(DESTDIR)$(PREFIX)/share/licenses/codex-native/LICENSE"
	rm -f "$(DESTDIR)$(PREFIX)/share/licenses/codex-native/CODEX-APACHE-2.0"
	rm -f "$(DESTDIR)$(PREFIX)/share/licenses/codex-native/CODEX-NOTICE"

clean:
	$(CARGO) clean
