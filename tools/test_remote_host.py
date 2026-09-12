import os
import json
from pathlib import Path
import subprocess
import tempfile
import unittest


SCRIPT = Path(__file__).with_name("codex-native-remote-host")


class RemoteHostSelectionTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        root = Path(self.temp.name)
        self.managed = root / "managed-codex"
        self.routed = root / "routed-codex"
        self.pid_file = root / "app-server.pid"
        self.write_binary(self.managed, "codex-cli 0.144.6")
        self.write_binary(self.routed, "codex-cli 0.144.6")

    def tearDown(self):
        self.temp.cleanup()

    @staticmethod
    def write_binary(path: Path, version: str):
        path.write_text(f"#!/bin/sh\necho '{version}'\n", encoding="utf-8")
        path.chmod(0o755)

    def environment(self):
        env = os.environ.copy()
        env.update(
            {
                "CODEX_NATIVE_MANAGED_CODEX": str(self.managed),
                "CODEX_NATIVE_ROUTED_CODEX": str(self.routed),
                "CODEX_NATIVE_DAEMON_PID_FILE": str(self.pid_file),
            }
        )
        return env

    def select(self):
        result = subprocess.run(
            ["python3", str(SCRIPT), "select"],
            check=True,
            capture_output=True,
            text=True,
            env=self.environment(),
        )
        return json.loads(result.stdout)

    def test_select_always_uses_stock_managed_host(self):
        selected = self.select()
        self.assertEqual(selected["binary"], str(self.managed))
        self.assertFalse(selected["routed"])
        self.assertIn("package-managed codex-cli 0.144.6", selected["reason"])

    def test_legacy_routed_override_is_ignored(self):
        self.write_binary(self.routed, "codex-cli 99.0.0")
        selected = self.select()
        self.assertEqual(selected["binary"], str(self.managed))
        self.assertFalse(selected["routed"])

    def test_missing_managed_host_fails_cleanly(self):
        self.managed.unlink()
        environment = self.environment()
        result = subprocess.run(
            ["python3", str(SCRIPT), "select"],
            check=False,
            capture_output=True,
            text=True,
            env=environment,
        )
        self.assertEqual(result.returncode, 1)
        self.assertIn("system package manager", result.stderr)

    def test_foreground_host_publishes_and_cleans_managed_pid_metadata(self):
        process = subprocess.Popen(
            ["python3", str(SCRIPT), "run"],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
            env=self.environment(),
        )
        process.communicate(timeout=10)
        self.assertEqual(process.returncode, 0)
        metadata = json.loads(self.pid_file.read_text(encoding="utf-8"))
        self.assertEqual(metadata["pid"], process.pid)
        self.assertTrue(metadata["processStartTime"])
        self.assertEqual(self.pid_file.stat().st_mode & 0o777, 0o600)

        subprocess.run(
            ["python3", str(SCRIPT), "cleanup", str(process.pid + 1)],
            check=True,
            env=self.environment(),
        )
        self.assertTrue(self.pid_file.exists())
        subprocess.run(
            ["python3", str(SCRIPT), "cleanup", str(process.pid)],
            check=True,
            env=self.environment(),
        )
        self.assertFalse(self.pid_file.exists())

    def test_stock_host_receives_only_remote_control_arguments(self):
        arguments = Path(self.temp.name) / "arguments.json"
        self.managed.write_text(
            "#!/bin/sh\n"
            "if [ \"$1\" = \"--version\" ]; then\n"
            "  echo 'codex-cli 0.144.6'\n"
            "else\n"
            f"  printf '%s\\n' \"$@\" | python3 -c 'import json,sys; json.dump(sys.stdin.read().splitlines(), open(\"{arguments}\", \"w\"))'\n"
            "fi\n",
            encoding="utf-8",
        )
        self.managed.chmod(0o755)

        subprocess.run(
            ["python3", str(SCRIPT), "run"],
            check=True,
            capture_output=True,
            text=True,
            env=self.environment(),
        )

        self.assertEqual(
            json.loads(arguments.read_text(encoding="utf-8")),
            [
                "app-server",
                "--remote-control",
                "--listen",
                "unix://",
            ],
        )

    def test_secondary_profile_uses_an_isolated_codex_home(self):
        codex_home = Path(self.temp.name) / "codex-home.txt"
        self.managed.write_text(
            "#!/bin/sh\n"
            "if [ \"$1\" = \"--version\" ]; then\n"
            "  echo 'codex-cli 0.144.6'\n"
            "else\n"
            f"  printf '%s' \"$CODEX_HOME\" > '{codex_home}'\n"
            "fi\n",
            encoding="utf-8",
        )
        self.managed.chmod(0o755)
        environment = self.environment()
        data_home = Path(self.temp.name) / "data"
        environment["XDG_DATA_HOME"] = str(data_home)
        profile = "account-0123456789abcdef0123456789abcdef"

        subprocess.run(
            ["python3", str(SCRIPT), "run", "--profile", profile],
            check=True,
            capture_output=True,
            text=True,
            env=environment,
        )

        expected = data_home / "codex-native" / "accounts" / profile / "codex"
        self.assertEqual(codex_home.read_text(encoding="utf-8"), str(expected))
        self.assertEqual(expected.stat().st_mode & 0o777, 0o700)

    def test_cleanup_without_a_pid_removes_stopped_service_metadata(self):
        self.pid_file.write_text(
            json.dumps({"pid": 1234, "processStartTime": "now"}),
            encoding="utf-8",
        )
        subprocess.run(
            ["python3", str(SCRIPT), "cleanup"],
            check=True,
            env=self.environment(),
        )
        self.assertFalse(self.pid_file.exists())


if __name__ == "__main__":
    unittest.main()
