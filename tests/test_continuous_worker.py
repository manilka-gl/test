from __future__ import annotations

import importlib.util
import json
import os
import shutil
import signal
import subprocess
import sys
import tempfile
import threading
import time
import unittest
import zipfile
from pathlib import Path
from unittest import mock

REPOSITORY = Path(__file__).resolve().parents[1]
WORKER_SCRIPT = REPOSITORY / "scripts" / "continuous_worker.py"
SPEC = importlib.util.spec_from_file_location("continuous_worker", WORKER_SCRIPT)
if SPEC is None or SPEC.loader is None:
    raise RuntimeError(f"Could not import {WORKER_SCRIPT}")
worker = importlib.util.module_from_spec(SPEC)
sys.modules[SPEC.name] = worker
SPEC.loader.exec_module(worker)

REPOSITORY_NAME = "example/project"
BRANCH = "main"


def run(
    args: list[str],
    *,
    cwd: Path,
    check: bool = True,
    env: dict[str, str] | None = None,
    timeout: float = 90,
) -> subprocess.CompletedProcess[str]:
    result = subprocess.run(
        args,
        cwd=cwd,
        env=env,
        check=False,
        text=True,
        encoding="utf-8",
        errors="replace",
        capture_output=True,
        timeout=timeout,
    )
    if check and result.returncode:
        raise AssertionError(
            f"{args!r} failed with {result.returncode}\n"
            f"stdout:\n{result.stdout}\nstderr:\n{result.stderr}"
        )
    return result


def git(cwd: Path, *args: str, check: bool = True) -> subprocess.CompletedProcess[str]:
    return run(["git", *args], cwd=cwd, check=check)


def configure_repository(repository: Path) -> None:
    git(repository, "config", "user.name", "Worker Tests")
    git(repository, "config", "user.email", "worker-tests@example.invalid")


def initialize_remote(root: Path) -> tuple[Path, Path, str]:
    remote = root / "remote.git"
    seed = root / "seed"
    remote.mkdir()
    seed.mkdir()
    git(remote, "init", "--bare")
    git(seed, "init", "-b", BRANCH)
    configure_repository(seed)
    (seed / "existing.txt").write_text("before\n", encoding="utf-8")
    (seed / "delete.txt").write_text("delete\n", encoding="utf-8")
    git(seed, "add", ".")
    git(seed, "commit", "-m", "initial source")
    commit = git(seed, "rev-parse", "HEAD").stdout.strip()
    git(seed, "remote", "add", "origin", str(remote))
    git(seed, "push", "-u", "origin", BRANCH)
    git(remote, "symbolic-ref", "HEAD", f"refs/heads/{BRANCH}")
    return remote, seed, commit


def workspace_drive(root: Path) -> worker.LocalDirectoryDrive:
    return worker.LocalDirectoryDrive(
        root / "Github" / "example" / "project" / worker.sanitize_branch_folder(BRANCH)
    )


def command_value(
    command_id: str,
    base_commit: str,
    *,
    payloads: list[dict[str, object]],
    steps: list[dict[str, object]],
    artifacts: list[dict[str, object]] | None = None,
    timeout_seconds: float | None = None,
    retry_of: str | None = None,
) -> dict[str, object]:
    return {
        "schema_version": 1,
        "command_id": command_id,
        "repository": REPOSITORY_NAME,
        "branch": BRANCH,
        "created_utc": "2026-07-30T12:00:00Z",
        "base_commit": base_commit,
        "payloads": payloads,
        "steps": steps,
        "commit_message": f"Apply {command_id}",
        "timeout_seconds": timeout_seconds,
        "artifacts": artifacts or [],
        "retry_of": retry_of,
    }


def upload_script_command(
    drive: worker.LocalDirectoryDrive,
    source_root: Path,
    *,
    command_id: str,
    base_commit: str,
    script: str,
    interpreter: str = "python",
    artifacts: list[dict[str, object]] | None = None,
) -> None:
    suffix = {"python": ".py", "pwsh": ".ps1"}[interpreter]
    script_path = source_root / f"{command_id}{suffix}"
    script_path.write_text(script, encoding="utf-8")
    payload_path = f"Input/files/{command_id}/command{suffix}"
    drive.upload(script_path, payload_path)
    value = command_value(
        command_id,
        base_commit,
        payloads=[
            {
                "name": "command-script",
                "path": payload_path,
                "size_bytes": script_path.stat().st_size,
                "sha256": worker.sha256_file(script_path),
            }
        ],
        steps=[
            {
                "type": "script",
                "payload": "command-script",
                "interpreter": interpreter,
            }
        ],
        artifacts=artifacts,
    )
    envelope = source_root / f"{command_id}.json"
    worker.atomic_write_json(envelope, value)
    drive.upload(envelope, f"Input/commands/{command_id}.json")


def upload_patch_command(
    drive: worker.LocalDirectoryDrive,
    source_root: Path,
    *,
    command_id: str,
    base_commit: str,
    patch: Path,
) -> None:
    payload_path = f"Input/files/{command_id}/changes.patch"
    drive.upload(patch, payload_path)
    value = command_value(
        command_id,
        base_commit,
        payloads=[
            {
                "name": "binary-patch",
                "path": payload_path,
                "size_bytes": patch.stat().st_size,
                "sha256": worker.sha256_file(patch),
            }
        ],
        steps=[{"type": "apply_patch", "payload": "binary-patch"}],
    )
    envelope = source_root / f"{command_id}.json"
    worker.atomic_write_json(envelope, value)
    drive.upload(envelope, f"Input/commands/{command_id}.json")


def process_exists(pid: int) -> bool:
    if os.name == "nt":
        import ctypes
        from ctypes import wintypes

        process_query_limited_information = 0x1000
        kernel32 = ctypes.WinDLL("kernel32", use_last_error=True)
        kernel32.OpenProcess.restype = wintypes.HANDLE
        handle = kernel32.OpenProcess(process_query_limited_information, False, pid)
        if not handle:
            return False
        kernel32.CloseHandle(handle)
        return True
    try:
        os.kill(pid, 0)
        return True
    except ProcessLookupError:
        return False


class CommandEnvelopeTests(unittest.TestCase):
    def test_valid_envelope_and_default_no_timeout(self) -> None:
        command_id = "20260730T120000Z-0001"
        value = command_value(
            command_id,
            "a" * 40,
            payloads=[],
            steps=[{"type": "exec", "argv": ["cargo", "test"]}],
        )
        parsed = worker.CommandEnvelope.parse(
            value,
            filename=f"{command_id}.json",
            repository=REPOSITORY_NAME,
            branch=BRANCH,
        )
        self.assertIsNone(parsed.timeout_seconds)
        self.assertEqual(parsed.steps[0].argv, ("cargo", "test"))

    def test_rejects_unsafe_payload_and_wrong_branch(self) -> None:
        command_id = "20260730T120000Z-0002"
        value = command_value(
            command_id,
            "a" * 40,
            payloads=[
                {
                    "name": "payload-file",
                    "path": "../secret",
                    "size_bytes": 1,
                    "sha256": "0" * 64,
                }
            ],
            steps=[{"type": "script", "payload": "payload-file"}],
        )
        with self.assertRaises(worker.WorkerError):
            worker.CommandEnvelope.parse(
                value,
                filename=f"{command_id}.json",
                repository=REPOSITORY_NAME,
                branch=BRANCH,
            )
        value["branch"] = "wrong"
        with self.assertRaises(worker.WorkerError):
            worker.CommandEnvelope.parse(
                value,
                filename=f"{command_id}.json",
                repository=REPOSITORY_NAME,
                branch=BRANCH,
            )

    def test_duplicate_drive_names_fail_closed(self) -> None:
        entries = [
            worker.RemoteEntry("a", "same.json", 1, "1"),
            worker.RemoteEntry("b", "same.json", 1, "2"),
        ]
        with self.assertRaisesRegex(worker.WorkerError, "Duplicate"):
            worker.unique_entries(entries)

    def test_powershell_script_requires_a_ps1_drive_path(self) -> None:
        command_id = "20260730T120000Z-pwshsuffix"
        payload = {
            "name": "package-script",
            "path": f"Input/files/{command_id}/package-script",
            "size_bytes": 1,
            "sha256": "0" * 64,
        }
        value = command_value(
            command_id,
            "a" * 40,
            payloads=[payload],
            steps=[
                {
                    "type": "script",
                    "payload": "package-script",
                    "interpreter": "pwsh",
                }
            ],
        )
        with self.assertRaisesRegex(worker.WorkerError, r"must end in \.ps1"):
            worker.CommandEnvelope.parse(
                value,
                filename=f"{command_id}.json",
                repository=REPOSITORY_NAME,
                branch=BRANCH,
            )

    def test_branch_folder_is_readable_deterministic_and_collision_resistant(
        self,
    ) -> None:
        first = worker.sanitize_branch_folder("feature/a")
        second = worker.sanitize_branch_folder("feature_a")
        self.assertEqual(first, worker.sanitize_branch_folder("feature/a"))
        self.assertNotEqual(first, second)
        self.assertNotIn("/", first)


class SecretAndEnvironmentTests(unittest.TestCase):
    @unittest.skipUnless(os.name == "nt", "the hosted worker downloads Windows rclone")
    def test_rclone_bootstrap_verifies_the_pinned_archive(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            fixture = root / "fixture.zip"
            with zipfile.ZipFile(fixture, "w") as bundle:
                bundle.writestr("rclone-v-test-windows-amd64/rclone.exe", b"fixture")
            expected = worker.sha256_file(fixture)

            def copy_fixture(_url: str, destination: Path) -> None:
                shutil.copy2(fixture, destination)

            with (
                mock.patch.object(
                    worker.urllib.request,
                    "urlretrieve",
                    side_effect=copy_fixture,
                ),
                mock.patch.object(worker, "RCLONE_WINDOWS_AMD64_SHA256", expected),
            ):
                executable = worker.prepare_rclone(root / "runtime")
            self.assertEqual(executable.read_bytes(), b"fixture")

            corrupted_root = root / "corrupted"
            with (
                mock.patch.object(
                    worker.urllib.request,
                    "urlretrieve",
                    side_effect=copy_fixture,
                ),
                mock.patch.object(worker, "RCLONE_WINDOWS_AMD64_SHA256", "0" * 64),
                self.assertRaisesRegex(worker.WorkerError, "checksum mismatch"),
            ):
                worker.prepare_rclone(corrupted_root)

    def test_config_is_bounded_and_removed_from_environment(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            previous = os.environ.get("RCLONE_CONFIG")
            os.environ["RCLONE_CONFIG"] = "[gdrive]\ntype = drive\n"
            try:
                path = worker.write_rclone_config(root, os.environ["RCLONE_CONFIG"])
                self.assertTrue(path.is_file())
                self.assertNotIn("RCLONE_CONFIG", os.environ)
            finally:
                if previous is not None:
                    os.environ["RCLONE_CONFIG"] = previous
                else:
                    os.environ.pop("RCLONE_CONFIG", None)
            with self.assertRaises(worker.WorkerError):
                worker.write_rclone_config(root, "x" * (worker.MAX_SECRET_BYTES + 1))

    def test_command_environment_removes_worker_credentials(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            protected = {
                "GH_TOKEN": "secret",
                "GITHUB_TOKEN": "secret",
                "RCLONE_CONFIG_OTHER_TOKEN": "secret",
                "GIT_CONFIG_VALUE_0": "secret",
            }
            old = {key: os.environ.get(key) for key in protected}
            os.environ.update(protected)
            try:
                environment = worker.command_environment(
                    root / "artifacts", BRANCH, "worker"
                )
            finally:
                for key, value in old.items():
                    if value is None:
                        os.environ.pop(key, None)
                    else:
                        os.environ[key] = value
            for key in protected:
                self.assertNotIn(key, environment)


class FileLockTests(unittest.TestCase):
    def test_second_worker_cannot_lock_the_same_checkout(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            lock_path = Path(temporary) / "worker.lock"
            with (
                worker.FileLock(lock_path),
                self.assertRaises(worker.WorkerError),
                worker.FileLock(lock_path),
            ):
                self.fail("second lock unexpectedly succeeded")
            with worker.FileLock(lock_path):
                pass


class PayloadMaterializationTests(unittest.TestCase):
    def test_download_preserves_python_and_powershell_suffixes(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            repository = root / "repository"
            repository.mkdir()
            drive = worker.LocalDirectoryDrive(root / "drive")
            command_id = "20260730T120000Z-payloadsuffix"
            payload_values = []
            steps = []
            expected: dict[str, bytes] = {}
            for logical_name, filename, interpreter, content in (
                ("python-script", "command.py", "python", b"print('ok')\n"),
                ("package-script", "package.ps1", "pwsh", b"Write-Host 'ok'\n"),
            ):
                source = root / filename
                source.write_bytes(content)
                remote_path = f"Input/files/{command_id}/{filename}"
                drive.upload(source, remote_path)
                payload_values.append(
                    {
                        "name": logical_name,
                        "path": remote_path,
                        "size_bytes": len(content),
                        "sha256": worker.sha256_file(source),
                    }
                )
                steps.append(
                    {
                        "type": "script",
                        "payload": logical_name,
                        "interpreter": interpreter,
                    }
                )
                expected[logical_name] = content
            envelope_value = command_value(
                command_id,
                "a" * 40,
                payloads=payload_values,
                steps=steps,
            )
            envelope_source = root / f"{command_id}.json"
            worker.atomic_write_json(envelope_source, envelope_value)
            drive.upload(
                envelope_source,
                f"Input/commands/{command_id}.json",
            )
            instance = worker.ContinuousWorker(
                repository=repository,
                repository_full_name=REPOSITORY_NAME,
                branch=BRANCH,
                drive=drive,
                token="",
                poll_seconds=0.1,
                max_runtime_minutes=2,
                once=True,
                shutdown_reserve_seconds=1,
            )
            entry = drive.list_files("Input/commands")[0]
            with tempfile.TemporaryDirectory() as download_directory:
                _, payload_paths = instance.download_envelope(
                    entry,
                    Path(download_directory),
                )
                self.assertEqual(payload_paths["python-script"].suffix, ".py")
                self.assertEqual(payload_paths["package-script"].suffix, ".ps1")
                for name, content in expected.items():
                    self.assertEqual(payload_paths[name].read_bytes(), content)


class ProcessRunnerTests(unittest.TestCase):
    def test_no_default_timeout_and_repeated_heartbeats(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            state = worker.StateStore(root / "state.json", "worker", BRANCH)
            runner = worker.ProcessRunner(threading.Event(), state)
            heartbeats: list[dict[str, object]] = []
            result = runner.run_argv(
                [sys.executable, "-c", "import time; time.sleep(0.65)"],
                root,
                os.environ.copy(),
                None,
                {"command_id": "heartbeat"},
                deadline_monotonic=time.monotonic() + 5,
                heartbeat=heartbeats.append,
                heartbeat_seconds=0.05,
            )
            try:
                self.assertEqual(result.status, "success")
                self.assertFalse(result.timed_out)
                self.assertFalse(result.interrupted)
                self.assertGreaterEqual(len(heartbeats), 3)
                self.assertTrue(all("command_pid" in item for item in heartbeats))
            finally:
                shutil.rmtree(result.capture_directory, ignore_errors=True)

    def test_runner_deadline_is_interrupted_not_timed_out(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            state = worker.StateStore(root / "state.json", "worker", BRANCH)
            runner = worker.ProcessRunner(threading.Event(), state)
            result = runner.run_argv(
                [sys.executable, "-c", "import time; time.sleep(60)"],
                root,
                os.environ.copy(),
                None,
                {"command_id": "deadline"},
                deadline_monotonic=time.monotonic() + 0.3,
            )
            try:
                self.assertEqual(result.status, "interrupted_by_runner_deadline")
                self.assertTrue(result.interrupted)
                self.assertTrue(result.deadline_interrupted)
                self.assertFalse(result.timed_out)
            finally:
                shutil.rmtree(result.capture_directory, ignore_errors=True)

    def test_timeout_records_pid_and_terminates_descendant_process(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            child_pid_path = root / "child.pid"
            command = root / "timeout.py"
            command.write_text(
                "\n".join(  # noqa: FLY002 - generated test command
                    [
                        "import os",
                        "import subprocess",
                        "import sys",
                        "import time",
                        "from pathlib import Path",
                        "child = subprocess.Popen([sys.executable, '-c', 'import time; time.sleep(60)'])",
                        "Path(os.environ['CHILD_PID_PATH']).write_text(str(child.pid))",
                        "time.sleep(60)",
                        "",
                    ]
                ),
                encoding="utf-8",
            )
            state = worker.StateStore(root / "state.json", "worker", BRANCH)
            runner = worker.ProcessRunner(threading.Event(), state)
            environment = os.environ.copy()
            environment["CHILD_PID_PATH"] = str(child_pid_path)
            result = runner.run(
                command,
                root,
                environment,
                timeout_seconds=0.6,
                state_fields={"command_path": "timeout.py"},
            )
            try:
                self.assertEqual(result.status, "timed_out")
                self.assertEqual(result.exit_code, worker.EXIT_TIMEOUT)
                self.assertTrue(result.timed_out)
                child_pid = int(child_pid_path.read_text(encoding="utf-8"))
                deadline = time.monotonic() + 5
                while process_exists(child_pid) and time.monotonic() < deadline:
                    time.sleep(0.05)
                self.assertFalse(process_exists(child_pid))
            finally:
                shutil.rmtree(result.capture_directory, ignore_errors=True)


class SnapshotTests(unittest.TestCase):
    def test_initial_snapshot_supports_an_empty_git_tree(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            repository = root / "repository"
            repository.mkdir()
            git(repository, "init", "-b", BRANCH)
            configure_repository(repository)
            git(repository, "commit", "--allow-empty", "-m", "bootstrap")
            commit = git(repository, "rev-parse", "HEAD").stdout.strip()
            drive = worker.LocalDirectoryDrive(root / "drive")
            instance = worker.ContinuousWorker(
                repository=repository,
                repository_full_name=REPOSITORY_NAME,
                branch=BRANCH,
                drive=drive,
                token="",
                poll_seconds=0.1,
                max_runtime_minutes=2,
                once=True,
                shutdown_reserve_seconds=1,
            )
            snapshot = instance.ensure_codebase_snapshot(commit)
            self.assertEqual(snapshot["sequence"], 1)
            with zipfile.ZipFile(drive.root / snapshot["drive_path"]) as bundle:
                self.assertIn("_bridge/manifest.json", bundle.namelist())

    def test_versioned_git_archives_and_latest_pointer(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            repository = root / "repository"
            repository.mkdir()
            git(repository, "init", "-b", BRANCH)
            configure_repository(repository)
            (repository / "source.txt").write_text("one\n", encoding="utf-8")
            git(repository, "add", ".")
            git(repository, "commit", "-m", "one")
            first_commit = git(repository, "rev-parse", "HEAD").stdout.strip()
            drive = worker.LocalDirectoryDrive(root / "drive")
            instance = worker.ContinuousWorker(
                repository=repository,
                repository_full_name=REPOSITORY_NAME,
                branch=BRANCH,
                drive=drive,
                token="",
                poll_seconds=0.1,
                max_runtime_minutes=2,
                once=True,
                shutdown_reserve_seconds=1,
            )
            first = instance.ensure_codebase_snapshot(first_commit)
            repeated = instance.ensure_codebase_snapshot(first_commit)
            self.assertEqual(first["sequence"], 1)
            self.assertEqual(first, repeated)

            (repository / "source.txt").write_text("two\n", encoding="utf-8")
            git(repository, "add", ".")
            git(repository, "commit", "-m", "two")
            second_commit = git(repository, "rev-parse", "HEAD").stdout.strip()
            second = instance.ensure_codebase_snapshot(second_commit)
            self.assertEqual(second["sequence"], 2)
            self.assertNotEqual(first["sha256"], second["sha256"])

            archive = drive.root / second["drive_path"]
            with zipfile.ZipFile(archive) as bundle:
                self.assertIn("repository/source.txt", bundle.namelist())
                self.assertNotIn("repository/.git/", bundle.namelist())
                manifest = json.loads(
                    bundle.read("_bridge/manifest.json").decode("utf-8")
                )
            self.assertEqual(manifest["commit"], second_commit)
            self.assertEqual(
                drive.read_json("Output/files/codebase/latest.json")["sequence"],
                2,
            )

            archive.unlink()
            repaired = instance.ensure_codebase_snapshot(second_commit)
            self.assertEqual(repaired["sequence"], 3)
            self.assertTrue((drive.root / repaired["drive_path"]).is_file())


class PatchIntegrationTests(unittest.TestCase):
    def test_conflicting_patch_is_rejected_without_changing_the_branch(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            remote, seed, base_commit = initialize_remote(root)
            checkout = root / "worker"
            editor = root / "editor"
            git(root, "clone", str(remote), str(checkout))
            git(root, "clone", str(remote), str(editor))
            configure_repository(checkout)
            configure_repository(editor)

            (editor / "existing.txt").write_text("patch version\n", encoding="utf-8")
            git(editor, "add", "existing.txt")
            patch = root / "conflict.patch"
            patch.write_bytes(
                subprocess.run(
                    [
                        "git",
                        "diff",
                        "--binary",
                        "--full-index",
                        "--no-renames",
                        "--cached",
                        "HEAD",
                        "--",
                    ],
                    cwd=editor,
                    check=True,
                    stdout=subprocess.PIPE,
                ).stdout
            )

            (seed / "existing.txt").write_text("remote version\n", encoding="utf-8")
            git(seed, "add", "existing.txt")
            git(seed, "commit", "-m", "conflicting remote edit")
            git(seed, "push", "origin", BRANCH)
            remote_commit = git(seed, "rev-parse", "HEAD").stdout.strip()

            drive = workspace_drive(root / "drive")
            command_id = "20260730T120000Z-conflict"
            upload_patch_command(
                drive,
                root,
                command_id=command_id,
                base_commit=base_commit,
                patch=patch,
            )
            instance = worker.ContinuousWorker(
                repository=checkout,
                repository_full_name=REPOSITORY_NAME,
                branch=BRANCH,
                drive=drive,
                token="",
                poll_seconds=0.05,
                max_runtime_minutes=2,
                once=True,
                shutdown_reserve_seconds=1,
                heartbeat_seconds=0.05,
            )
            self.assertEqual(instance.run(), 0)

            result = drive.read_json(f"Output/command_output/{command_id}.result.json")
            self.assertEqual(result["status"], "rejected")
            self.assertTrue(result["conflict"])
            self.assertTrue(result["retry_required"])
            self.assertFalse(result["commit_created"])
            self.assertFalse(result["pushed"])
            self.assertFalse(result["codebase_changed"])
            self.assertEqual(result["latest_codebase"]["commit"], remote_commit)
            self.assertEqual(
                git(seed, "rev-parse", "origin/main").stdout.strip(),
                remote_commit,
            )

    def test_binary_capable_patch_commits_pushes_and_creates_snapshot(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            remote, seed, base_commit = initialize_remote(root)
            checkout = root / "worker"
            editor = root / "editor"
            git(root, "clone", str(remote), str(checkout))
            git(root, "clone", str(remote), str(editor))
            configure_repository(checkout)
            configure_repository(editor)
            (editor / "existing.txt").write_text("after\n", encoding="utf-8")
            (editor / "delete.txt").unlink()
            (editor / "new.bin").write_bytes(b"\x00\xff\x10binary")
            git(editor, "add", "-A")
            patch = root / "changes.patch"
            patch.write_bytes(
                subprocess.run(
                    [
                        "git",
                        "diff",
                        "--binary",
                        "--full-index",
                        "--no-renames",
                        "--cached",
                        "HEAD",
                        "--",
                    ],
                    cwd=editor,
                    check=True,
                    stdout=subprocess.PIPE,
                ).stdout
            )
            # The patch is now stale but non-conflicting. Three-way application
            # must retain the new remote file while applying the older patch.
            (seed / "unrelated.txt").write_text("remote advance\n", encoding="utf-8")
            git(seed, "add", "unrelated.txt")
            git(seed, "commit", "-m", "advance branch independently")
            git(seed, "push", "origin", BRANCH)
            drive = workspace_drive(root / "drive")
            upload_patch_command(
                drive,
                root,
                command_id="20260730T120000Z-patch1",
                base_commit=base_commit,
                patch=patch,
            )
            environment = os.environ.copy()
            environment["RUNNER_TEMP"] = str(root / "runner-temp")
            previous = os.environ.get("RUNNER_TEMP")
            os.environ["RUNNER_TEMP"] = environment["RUNNER_TEMP"]
            try:
                instance = worker.ContinuousWorker(
                    repository=checkout,
                    repository_full_name=REPOSITORY_NAME,
                    branch=BRANCH,
                    drive=drive,
                    token="",
                    poll_seconds=0.05,
                    max_runtime_minutes=2,
                    once=True,
                    shutdown_reserve_seconds=1,
                    heartbeat_seconds=0.05,
                )
                self.assertEqual(instance.run(), 0)
            finally:
                if previous is None:
                    os.environ.pop("RUNNER_TEMP", None)
                else:
                    os.environ["RUNNER_TEMP"] = previous
            git(seed, "fetch", "origin", BRANCH)
            git(seed, "reset", "--hard", f"origin/{BRANCH}")
            self.assertEqual(
                (seed / "existing.txt").read_text(encoding="utf-8"), "after\n"
            )
            self.assertFalse((seed / "delete.txt").exists())
            self.assertEqual((seed / "new.bin").read_bytes(), b"\x00\xff\x10binary")
            self.assertEqual(
                (seed / "unrelated.txt").read_text(encoding="utf-8"),
                "remote advance\n",
            )
            result = drive.read_json(
                "Output/command_output/20260730T120000Z-patch1.result.json"
            )
            self.assertEqual(result["status"], "success")
            self.assertTrue(result["commit_created"])
            self.assertTrue(result["pushed"])
            self.assertTrue(result["codebase_changed"])
            self.assertEqual(result["codebase"]["sequence"], 2)


class QueueIntegrationTests(unittest.TestCase):
    @unittest.skipUnless(shutil.which("pwsh"), "pwsh is required")
    def test_powershell_script_payload_executes_with_preserved_extension(
        self,
    ) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            remote, seed, base_commit = initialize_remote(root)
            checkout = root / "worker"
            git(root, "clone", str(remote), str(checkout))
            configure_repository(checkout)
            drive = workspace_drive(root / "drive")
            command_id = "20260730T120000Z-pwshrun"
            upload_script_command(
                drive,
                root,
                command_id=command_id,
                base_commit=base_commit,
                interpreter="pwsh",
                script=(
                    "Set-Content -LiteralPath 'powershell-script.txt' "
                    "-Value 'powershell-ok' -NoNewline\n"
                ),
            )
            previous = os.environ.get("RUNNER_TEMP")
            os.environ["RUNNER_TEMP"] = str(root / "runner-temp")
            try:
                instance = worker.ContinuousWorker(
                    repository=checkout,
                    repository_full_name=REPOSITORY_NAME,
                    branch=BRANCH,
                    drive=drive,
                    token="",
                    poll_seconds=0.05,
                    max_runtime_minutes=2,
                    once=True,
                    shutdown_reserve_seconds=1,
                )
                self.assertEqual(instance.run(), 0)
            finally:
                if previous is None:
                    os.environ.pop("RUNNER_TEMP", None)
                else:
                    os.environ["RUNNER_TEMP"] = previous
            git(seed, "fetch", "origin", BRANCH)
            git(seed, "reset", "--hard", f"origin/{BRANCH}")
            self.assertEqual(
                (seed / "powershell-script.txt").read_text(encoding="utf-8"),
                "powershell-ok",
            )
            result = drive.read_json(f"Output/command_output/{command_id}.result.json")
            self.assertEqual(result["status"], "success")
            self.assertTrue(result["commit_created"])
            self.assertTrue(result["pushed"])

    def test_failed_command_discards_source_changes(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            remote, seed, base_commit = initialize_remote(root)
            checkout = root / "worker"
            git(root, "clone", str(remote), str(checkout))
            configure_repository(checkout)
            drive = workspace_drive(root / "drive")
            command_id = "20260730T120000Z-failed1"
            upload_script_command(
                drive,
                root,
                command_id=command_id,
                base_commit=base_commit,
                script="\n".join(  # noqa: FLY002 - generated test command
                    [
                        "from pathlib import Path",
                        "Path('must-not-commit.txt').write_text('partial')",
                        "raise SystemExit(3)",
                        "",
                    ]
                ),
            )
            previous = os.environ.get("RUNNER_TEMP")
            os.environ["RUNNER_TEMP"] = str(root / "runner-temp")
            try:
                instance = worker.ContinuousWorker(
                    repository=checkout,
                    repository_full_name=REPOSITORY_NAME,
                    branch=BRANCH,
                    drive=drive,
                    token="",
                    poll_seconds=0.05,
                    max_runtime_minutes=2,
                    once=True,
                    shutdown_reserve_seconds=1,
                )
                self.assertEqual(instance.run(), 0)
            finally:
                if previous is None:
                    os.environ.pop("RUNNER_TEMP", None)
                else:
                    os.environ["RUNNER_TEMP"] = previous
            git(seed, "fetch", "origin", BRANCH)
            git(seed, "reset", "--hard", f"origin/{BRANCH}")
            self.assertFalse((seed / "must-not-commit.txt").exists())
            result = drive.read_json(f"Output/command_output/{command_id}.result.json")
            self.assertEqual(result["status"], "command_failed")
            self.assertEqual(result["exit_code"], 3)
            self.assertTrue(result["workspace_changed"])
            self.assertFalse(result["commit_created"])
            self.assertFalse(result["codebase_changed"])

    def test_input_arriving_during_long_command_is_queued_once(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            remote, _, base_commit = initialize_remote(root)
            checkout = root / "worker"
            git(root, "clone", str(remote), str(checkout))
            configure_repository(checkout)
            drive_root = root / "drive"
            drive = workspace_drive(drive_root)
            marker = root / "first-running.marker"
            upload_script_command(
                drive,
                root,
                command_id="20260730T120000Z-queue1",
                base_commit=base_commit,
                script="\n".join(  # noqa: FLY002 - generated test command
                    [
                        "import os",
                        "import time",
                        "from pathlib import Path",
                        "Path(os.environ['QUEUE_TEST_MARKER']).write_text('running')",
                        "time.sleep(1.0)",
                        "Path('order.txt').write_text('first\\n')",
                        "artifact = Path(os.environ['WORKER_ARTIFACT_DIRECTORY']) / 'first.bin'",
                        "artifact.write_bytes(b'first-artifact')",
                        "",
                    ]
                ),
                artifacts=[
                    {
                        "source": "artifact_directory",
                        "path": "first.bin",
                        "required": True,
                    }
                ],
            )
            environment = os.environ.copy()
            environment["QUEUE_TEST_MARKER"] = str(marker)
            environment["RUNNER_TEMP"] = str(root / "runner-temp")
            process = subprocess.Popen(
                [
                    sys.executable,
                    str(WORKER_SCRIPT),
                    "run-drive",
                    "--repository",
                    str(checkout),
                    "--repository-full-name",
                    REPOSITORY_NAME,
                    "--branch",
                    BRANCH,
                    "--local-drive-root",
                    str(drive_root),
                    "--poll-seconds",
                    "0.05",
                    "--heartbeat-seconds",
                    "0.05",
                    "--max-runtime-minutes",
                    "2",
                    "--shutdown-reserve-minutes",
                    "0.02",
                    "--once",
                ],
                cwd=REPOSITORY,
                env=environment,
                text=True,
                encoding="utf-8",
                errors="replace",
                stdout=subprocess.PIPE,
                stderr=subprocess.STDOUT,
            )
            try:
                deadline = time.monotonic() + 30
                while not marker.exists() and time.monotonic() < deadline:
                    if process.poll() is not None:
                        break
                    time.sleep(0.05)
                self.assertTrue(marker.exists(), "first command did not start")
                upload_script_command(
                    drive,
                    root,
                    command_id="20260730T120001Z-queue2",
                    base_commit=base_commit,
                    script="\n".join(  # noqa: FLY002 - generated test command
                        [
                            "from pathlib import Path",
                            "with Path('order.txt').open('a', encoding='utf-8') as stream:",
                            "    stream.write('second\\n')",
                            "",
                        ]
                    ),
                )
                output, _ = process.communicate(timeout=75)
                self.assertEqual(process.returncode, 0, output)
            finally:
                if process.poll() is None:
                    if os.name == "nt":
                        process.terminate()
                    else:
                        process.send_signal(signal.SIGTERM)
                    process.communicate(timeout=10)

            git(checkout, "fetch", "origin", BRANCH)
            git(checkout, "reset", "--hard", f"origin/{BRANCH}")
            self.assertEqual(
                (checkout / "order.txt").read_text(encoding="utf-8"),
                "first\nsecond\n",
            )
            for command_id in (
                "20260730T120000Z-queue1",
                "20260730T120001Z-queue2",
            ):
                result = drive.read_json(
                    f"Output/command_output/{command_id}.result.json"
                )
                self.assertEqual(result["status"], "success")
                self.assertIsInstance(result["command_pid"], int)
            artifact = (
                drive.root / "Output/files/artifacts/20260730T120000Z-queue1/first.bin"
            )
            self.assertEqual(artifact.read_bytes(), b"first-artifact")

            count_before = int(
                git(checkout, "rev-list", "--count", f"origin/{BRANCH}").stdout
            )
            restarted = run(
                [
                    sys.executable,
                    str(WORKER_SCRIPT),
                    "run-drive",
                    "--repository",
                    str(checkout),
                    "--repository-full-name",
                    REPOSITORY_NAME,
                    "--branch",
                    BRANCH,
                    "--local-drive-root",
                    str(drive_root),
                    "--max-runtime-minutes",
                    "2",
                    "--shutdown-reserve-minutes",
                    "0.02",
                    "--once",
                ],
                cwd=REPOSITORY,
                env=environment,
            )
            self.assertIn("One-shot Drive queue drained", restarted.stdout)
            count_after = int(
                git(checkout, "rev-list", "--count", f"origin/{BRANCH}").stdout
            )
            self.assertEqual(count_before, count_after)

    def test_existing_claim_becomes_interrupted_without_reexecution(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            repository = root / "repository"
            repository.mkdir()
            git(repository, "init", "-b", BRANCH)
            configure_repository(repository)
            (repository / "source.txt").write_text("source", encoding="utf-8")
            git(repository, "add", ".")
            git(repository, "commit", "-m", "source")
            drive = worker.LocalDirectoryDrive(root / "drive")
            command_id = "20260730T120000Z-claimed"
            envelope = root / f"{command_id}.json"
            envelope.write_text("{}", encoding="utf-8")
            drive.upload(envelope, f"Input/commands/{command_id}.json")
            drive.write_json(
                f"Output/command_output/{command_id}.claim.json",
                {"state": "claimed"},
            )
            instance = worker.ContinuousWorker(
                repository=repository,
                repository_full_name=REPOSITORY_NAME,
                branch=BRANCH,
                drive=drive,
                token="",
                poll_seconds=0.1,
                max_runtime_minutes=2,
                once=True,
                shutdown_reserve_seconds=1,
            )
            entry = drive.list_files("Input/commands")[0]
            self.assertTrue(instance.process_one(entry))
            result = drive.read_json(f"Output/command_output/{command_id}.result.json")
            self.assertEqual(result["status"], "interrupted_unknown")
            self.assertTrue(result["retry_required"])


if __name__ == "__main__":
    unittest.main()
