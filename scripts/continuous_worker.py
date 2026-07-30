#!/usr/bin/env python3
"""Google Drive backed continuous command worker for GitHub-hosted Windows.

GitHub is the source of truth for code. Google Drive is an immutable command
queue and result/artifact transport. Commands execute sequentially in isolated
Git worktrees and successful source changes are committed and pushed
immediately. The worker itself uses only the Python standard library and Git;
it downloads a pinned rclone binary when the workflow does not provide one.
"""

from __future__ import annotations

import argparse
import base64
import contextlib
import ctypes
import dataclasses
import datetime as dt
import hashlib
import json
import os
import re
import shutil
import signal
import subprocess
import sys
import tempfile
import threading
import time
import urllib.error
import urllib.parse
import urllib.request
import uuid
import zipfile
from collections.abc import Iterable, Sequence
from pathlib import Path, PurePosixPath
from typing import Any, BinaryIO, Protocol, Self

EXIT_TIMEOUT = 124
EXIT_LAUNCHER_ERROR = 125
EXIT_CANCELLED = 130
EXIT_REJECTED = 126
SHUTDOWN_RESERVE_SECONDS = 600.0
HEARTBEAT_SECONDS = 60.0
UTF8 = "utf-8"
RCLONE_VERSION = "1.74.4"
RCLONE_WINDOWS_AMD64_SHA256 = (
    "ef097ef9de37a57feb7d9f9c7afb34148ad3c65be8025f1d8f7f521554a701ea"
)
RCLONE_WINDOWS_AMD64_URL = (
    f"https://downloads.rclone.org/v{RCLONE_VERSION}/"
    f"rclone-v{RCLONE_VERSION}-windows-amd64.zip"
)
COMMAND_ID_PATTERN = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._-]{7,127}$")
SHA256_PATTERN = re.compile(r"^[0-9a-f]{64}$")
COMMIT_PATTERN = re.compile(r"^[0-9a-fA-F]{40,64}$")
MAX_SECRET_BYTES = 48 * 1024
MAX_JSON_BYTES = 2 * 1024 * 1024
TAIL_BYTES = 32 * 1024
PROTECTED_ENVIRONMENT_KEYS = {
    "ACTIONS_ID_TOKEN_REQUEST_TOKEN",
    "ACTIONS_ID_TOKEN_REQUEST_URL",
    "ACTIONS_RUNTIME_TOKEN",
    "GH_TOKEN",
    "GITHUB_TOKEN",
    "RCLONE_CONFIG",
    "RCLONE_CONFIG_PASS",
}


class WorkerError(RuntimeError):
    """An infrastructure error that should fail the worker."""


class GitError(WorkerError):
    """A failed Git operation."""

    def __init__(self, args: Sequence[str], result: subprocess.CompletedProcess[str]):
        command = "git " + " ".join(args)
        details = (result.stderr or result.stdout or "").strip()
        suffix = f": {details}" if details else ""
        super().__init__(f"{command} exited with {result.returncode}{suffix}")
        self.args_run = tuple(args)
        self.returncode = result.returncode
        self.stdout = result.stdout
        self.stderr = result.stderr


def utc_now() -> dt.datetime:
    return dt.datetime.now(dt.timezone.utc)


def iso_utc(value: dt.datetime | None = None) -> str:
    return (
        (value or utc_now()).isoformat(timespec="milliseconds").replace("+00:00", "Z")
    )


def log(message: str, *, level: str = "INFO") -> None:
    print(f"{iso_utc()} [{level}] {message}", flush=True)


def sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as source:
        for chunk in iter(lambda: source.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def atomic_write_json(path: Path, value: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(f".{path.name}.{uuid.uuid4().hex}.tmp")
    try:
        with temporary.open("w", encoding=UTF8, newline="\n") as output:
            json.dump(value, output, ensure_ascii=False, indent=2)
            output.write("\n")
            output.flush()
            os.fsync(output.fileno())
        os.replace(temporary, path)
    finally:
        temporary.unlink(missing_ok=True)


def is_relative_to(path: Path, parent: Path) -> bool:
    try:
        path.relative_to(parent)
        return True
    except ValueError:
        return False


def resolve_inside(root: Path, relative: str | Path) -> Path:
    root = root.resolve()
    candidate = (root / relative).resolve(strict=False)
    if candidate == root or not is_relative_to(candidate, root):
        raise WorkerError(f"Path escapes {root}: {relative}")
    return candidate


def validate_directory_pair(
    repository: Path, input_dir: Path, output_dir: Path
) -> None:
    repository = repository.resolve()
    input_dir = input_dir.resolve()
    output_dir = output_dir.resolve()
    for name, path in (("input", input_dir), ("output", output_dir)):
        if path == repository or not is_relative_to(path, repository):
            raise WorkerError(f"{name} directory must be inside the repository: {path}")
    if (
        input_dir == output_dir
        or is_relative_to(input_dir, output_dir)
        or is_relative_to(output_dir, input_dir)
    ):
        raise WorkerError("Input and output directories must not overlap")


class Git:
    def __init__(self, repository: Path, token: str = ""):
        self.repository = repository.resolve()
        self.token = token

    def run(
        self,
        *args: str,
        check: bool = True,
        capture: bool = True,
        timeout: float | None = 120,
        input_bytes: bytes | None = None,
    ) -> subprocess.CompletedProcess[str]:
        environment = os.environ.copy()
        if self.token and args and args[0] in {"fetch", "ls-remote", "push"}:
            credential = base64.b64encode(
                f"x-access-token:{self.token}".encode(UTF8)
            ).decode("ascii")
            environment.update(
                {
                    "GIT_CONFIG_COUNT": "1",
                    "GIT_CONFIG_KEY_0": "http.https://github.com/.extraheader",
                    "GIT_CONFIG_VALUE_0": f"AUTHORIZATION: basic {credential}",
                }
            )
        result = subprocess.run(
            ["git", *args],
            cwd=self.repository,
            env=environment,
            check=False,
            text=True,
            encoding=UTF8,
            errors="replace",
            input=None if input_bytes is None else input_bytes.decode(UTF8, "replace"),
            stdout=subprocess.PIPE if capture else None,
            stderr=subprocess.PIPE if capture else None,
            timeout=timeout,
        )
        if check and result.returncode != 0:
            raise GitError(args, result)
        return result

    def text(self, *args: str) -> str:
        return self.run(*args).stdout.strip()

    def exists_at_head(self, git_path: str) -> bool:
        return (
            self.run("cat-file", "-e", f"HEAD:{git_path}", check=False).returncode == 0
        )

    def abort_operations(self) -> None:
        for operation in (
            ("rebase", "--abort"),
            ("merge", "--abort"),
            ("cherry-pick", "--abort"),
            ("revert", "--abort"),
            ("am", "--abort"),
        ):
            self.run(*operation, check=False)


class FileLock:
    """One worker per checkout, even if workflow concurrency is misconfigured."""

    def __init__(self, path: Path):
        self.path = path
        self.handle: BinaryIO | None = None

    def __enter__(self) -> Self:
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self.handle = self.path.open("a+b")
        try:
            self.handle.seek(0)
            if self.handle.read(1) == b"":
                self.handle.write(b"\0")
                self.handle.flush()
            self.handle.seek(0)
            if os.name == "nt":
                import msvcrt

                msvcrt.locking(self.handle.fileno(), msvcrt.LK_NBLCK, 1)
            else:
                import fcntl

                fcntl.flock(self.handle.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
        except OSError as error:
            self.handle.close()
            self.handle = None
            raise WorkerError(f"Another worker holds {self.path}") from error
        return self

    def __exit__(self, *_: object) -> None:
        if self.handle is None:
            return
        self.handle.seek(0)
        with contextlib.suppress(OSError):
            if os.name == "nt":
                import msvcrt

                msvcrt.locking(self.handle.fileno(), msvcrt.LK_UNLCK, 1)
            else:
                import fcntl

                fcntl.flock(self.handle.fileno(), fcntl.LOCK_UN)
        self.handle.close()
        self.handle = None


class StateStore:
    def __init__(self, path: Path, worker_id: str, branch: str):
        self.path = path
        self.worker_id = worker_id
        self.branch = branch

    def read(self) -> dict[str, Any] | None:
        try:
            value = json.loads(self.path.read_text(encoding=UTF8))
            return value if isinstance(value, dict) else None
        except (FileNotFoundError, json.JSONDecodeError, OSError):
            return None

    def write(self, status: str, **fields: Any) -> None:
        value = {
            "schema_version": 1,
            "worker_id": self.worker_id,
            "worker_pid": os.getpid(),
            "branch": self.branch,
            "status": status,
            "updated_utc": iso_utc(),
            **fields,
        }
        atomic_write_json(self.path, value)


class WindowsJob:
    """Kill-on-close Windows Job Object for the full child process tree."""

    JOB_OBJECT_LIMIT_KILL_ON_JOB_CLOSE = 0x00002000
    JOB_OBJECT_EXTENDED_LIMIT_INFORMATION = 9

    def __init__(self) -> None:
        self.handle: int | None = None
        self.assigned = False
        if os.name != "nt":
            return

        from ctypes import wintypes

        class IoCounters(ctypes.Structure):
            _fields_ = [
                ("ReadOperationCount", ctypes.c_ulonglong),
                ("WriteOperationCount", ctypes.c_ulonglong),
                ("OtherOperationCount", ctypes.c_ulonglong),
                ("ReadTransferCount", ctypes.c_ulonglong),
                ("WriteTransferCount", ctypes.c_ulonglong),
                ("OtherTransferCount", ctypes.c_ulonglong),
            ]

        class BasicLimitInformation(ctypes.Structure):
            _fields_ = [
                ("PerProcessUserTimeLimit", ctypes.c_longlong),
                ("PerJobUserTimeLimit", ctypes.c_longlong),
                ("LimitFlags", wintypes.DWORD),
                ("MinimumWorkingSetSize", ctypes.c_size_t),
                ("MaximumWorkingSetSize", ctypes.c_size_t),
                ("ActiveProcessLimit", wintypes.DWORD),
                ("Affinity", ctypes.c_size_t),
                ("PriorityClass", wintypes.DWORD),
                ("SchedulingClass", wintypes.DWORD),
            ]

        class ExtendedLimitInformation(ctypes.Structure):
            _fields_ = [
                ("BasicLimitInformation", BasicLimitInformation),
                ("IoInfo", IoCounters),
                ("ProcessMemoryLimit", ctypes.c_size_t),
                ("JobMemoryLimit", ctypes.c_size_t),
                ("PeakProcessMemoryUsed", ctypes.c_size_t),
                ("PeakJobMemoryUsed", ctypes.c_size_t),
            ]

        kernel32 = ctypes.WinDLL("kernel32", use_last_error=True)
        kernel32.CreateJobObjectW.restype = wintypes.HANDLE
        kernel32.CreateJobObjectW.argtypes = (ctypes.c_void_p, wintypes.LPCWSTR)
        kernel32.SetInformationJobObject.restype = wintypes.BOOL
        kernel32.SetInformationJobObject.argtypes = (
            wintypes.HANDLE,
            ctypes.c_int,
            ctypes.c_void_p,
            wintypes.DWORD,
        )
        self._kernel32 = kernel32

        handle = kernel32.CreateJobObjectW(None, None)
        if not handle:
            return
        information = ExtendedLimitInformation()
        information.BasicLimitInformation.LimitFlags = (
            self.JOB_OBJECT_LIMIT_KILL_ON_JOB_CLOSE
        )
        configured = kernel32.SetInformationJobObject(
            handle,
            self.JOB_OBJECT_EXTENDED_LIMIT_INFORMATION,
            ctypes.byref(information),
            ctypes.sizeof(information),
        )
        if not configured:
            kernel32.CloseHandle(handle)
            return
        self.handle = int(handle)

    def assign(self, process: subprocess.Popen[bytes]) -> bool:
        if os.name != "nt" or self.handle is None:
            return False
        from ctypes import wintypes

        self._kernel32.AssignProcessToJobObject.restype = wintypes.BOOL
        self._kernel32.AssignProcessToJobObject.argtypes = (
            wintypes.HANDLE,
            wintypes.HANDLE,
        )
        process_handle = wintypes.HANDLE(int(process._handle))  # type: ignore[attr-defined]
        self.assigned = bool(
            self._kernel32.AssignProcessToJobObject(
                wintypes.HANDLE(self.handle), process_handle
            )
        )
        return self.assigned

    def terminate(self, exit_code: int) -> bool:
        if os.name != "nt" or self.handle is None or not self.assigned:
            return False
        from ctypes import wintypes

        self._kernel32.TerminateJobObject.restype = wintypes.BOOL
        self._kernel32.TerminateJobObject.argtypes = (wintypes.HANDLE, wintypes.UINT)
        return bool(
            self._kernel32.TerminateJobObject(wintypes.HANDLE(self.handle), exit_code)
        )

    def close(self) -> None:
        if os.name == "nt" and self.handle is not None:
            from ctypes import wintypes

            self._kernel32.CloseHandle(wintypes.HANDLE(self.handle))
            self.handle = None


@dataclasses.dataclass(frozen=True)
class CommandResult:
    status: str
    exit_code: int
    timed_out: bool
    interrupted: bool
    deadline_interrupted: bool
    started_utc: str
    finished_utc: str
    duration_seconds: float
    pid: int | None
    process_tree_strategy: str
    stdout_path: Path
    stderr_path: Path
    capture_directory: Path


class ProcessRunner:
    def __init__(self, stop_event: threading.Event, state: StateStore):
        self.stop_event = stop_event
        self.state = state
        self.current: subprocess.Popen[bytes] | None = None
        self.current_job: WindowsJob | None = None
        self._guard = threading.Lock()

    @staticmethod
    def command_for(path: Path) -> list[str]:
        suffix = path.suffix.casefold()
        if suffix == ".py":
            return [sys.executable, str(path)]
        if suffix == ".ps1":
            executable = shutil.which("pwsh")
            if executable is None:
                raise WorkerError("pwsh was not found for a .ps1 input command")
            return [
                executable,
                "-NoLogo",
                "-NoProfile",
                "-NonInteractive",
                "-ExecutionPolicy",
                "Bypass",
                "-File",
                str(path),
            ]
        raise WorkerError(f"Unsupported input extension: {path.suffix}")

    def terminate_current(self, exit_code: int = EXIT_CANCELLED) -> None:
        with self._guard:
            process = self.current
            job = self.current_job
        if process is None or process.poll() is not None:
            return
        if job is not None and job.terminate(exit_code):
            strategy = "windows_job"
        elif os.name == "nt":
            subprocess.run(
                ["taskkill", "/PID", str(process.pid), "/T", "/F"],
                check=False,
                stdout=subprocess.DEVNULL,
                stderr=subprocess.DEVNULL,
            )
            strategy = "taskkill"
        else:
            with contextlib.suppress(ProcessLookupError):
                os.killpg(process.pid, signal.SIGTERM)
            strategy = "process_group"
        log(f"Terminated command PID {process.pid} using {strategy}", level="WARNING")
        try:
            process.wait(timeout=15)
        except subprocess.TimeoutExpired:
            if os.name != "nt":
                with contextlib.suppress(ProcessLookupError):
                    os.killpg(process.pid, signal.SIGKILL)
            else:
                process.kill()
            with contextlib.suppress(subprocess.TimeoutExpired):
                process.wait(timeout=10)

    def run_argv(
        self,
        command: Sequence[str],
        working_directory: Path,
        environment: dict[str, str],
        timeout_seconds: float | None,
        state_fields: dict[str, Any],
        *,
        deadline_monotonic: float | None = None,
        heartbeat: Any | None = None,
        heartbeat_seconds: float = HEARTBEAT_SECONDS,
    ) -> CommandResult:
        capture_directory = Path(tempfile.mkdtemp(prefix="continuous-worker-capture-"))
        stdout_path = capture_directory / "stdout.bin"
        stderr_path = capture_directory / "stderr.bin"
        started = utc_now()
        started_clock = time.monotonic()
        process: subprocess.Popen[bytes] | None = None
        job = WindowsJob()
        status = "launcher_error"
        exit_code = EXIT_LAUNCHER_ERROR
        timed_out = False
        interrupted = False
        deadline_interrupted = False
        strategy = "none"

        with stdout_path.open("wb") as stdout, stderr_path.open("wb") as stderr:
            try:
                creationflags = (
                    subprocess.CREATE_NEW_PROCESS_GROUP if os.name == "nt" else 0
                )
                process = subprocess.Popen(
                    command,
                    cwd=working_directory,
                    env=environment,
                    stdin=subprocess.DEVNULL,
                    stdout=stdout,
                    stderr=stderr,
                    creationflags=creationflags,
                    start_new_session=os.name != "nt",
                )
                if os.name == "nt":
                    strategy = "windows_job" if job.assign(process) else "taskkill"
                else:
                    strategy = "process_group"
                with self._guard:
                    self.current = process
                    self.current_job = job
                self.state.write(
                    "running_command",
                    command_pid=process.pid,
                    process_tree_strategy=strategy,
                    started_utc=iso_utc(started),
                    **state_fields,
                )

                explicit_deadline = (
                    None if timeout_seconds is None else started_clock + timeout_seconds
                )
                next_heartbeat = started_clock
                while process.poll() is None:
                    now = time.monotonic()
                    if heartbeat is not None and now >= next_heartbeat:
                        heartbeat(
                            {
                                "command_pid": process.pid,
                                "elapsed_seconds": round(now - started_clock, 3),
                                "stdout_bytes": stdout_path.stat().st_size,
                                "stderr_bytes": stderr_path.stat().st_size,
                                "process_tree_strategy": strategy,
                            }
                        )
                        next_heartbeat = now + heartbeat_seconds
                    if self.stop_event.wait(0.2):
                        status = "cancelled"
                        exit_code = EXIT_CANCELLED
                        interrupted = True
                        self.terminate_current(EXIT_CANCELLED)
                        break
                    if (
                        deadline_monotonic is not None
                        and time.monotonic() >= deadline_monotonic
                    ):
                        status = "interrupted_by_runner_deadline"
                        exit_code = EXIT_CANCELLED
                        interrupted = True
                        deadline_interrupted = True
                        self.terminate_current(EXIT_CANCELLED)
                        break
                    if (
                        explicit_deadline is not None
                        and time.monotonic() >= explicit_deadline
                    ):
                        status = "timed_out"
                        exit_code = EXIT_TIMEOUT
                        timed_out = True
                        self.terminate_current(EXIT_TIMEOUT)
                        break
                else:
                    exit_code = int(process.returncode)
                    status = "success" if exit_code == 0 else "command_failed"

                if process.poll() is None:
                    with contextlib.suppress(subprocess.TimeoutExpired):
                        process.wait(timeout=10)
                if status not in {
                    "timed_out",
                    "cancelled",
                    "interrupted_by_runner_deadline",
                }:
                    exit_code = int(process.returncode)
                    status = "success" if exit_code == 0 else "command_failed"
            except Exception as error:  # noqa: BLE001 - launcher boundary
                stderr.write(
                    f"\nWorker launcher error: {type(error).__name__}: {error}\n".encode(
                        UTF8, errors="replace"
                    )
                )
                status = "launcher_error"
                exit_code = EXIT_LAUNCHER_ERROR
                if process is not None and process.poll() is None:
                    self.terminate_current(EXIT_LAUNCHER_ERROR)
            finally:
                with self._guard:
                    self.current = None
                    self.current_job = None
                job.close()

        finished = utc_now()
        return CommandResult(
            status=status,
            exit_code=exit_code,
            timed_out=timed_out,
            interrupted=interrupted,
            deadline_interrupted=deadline_interrupted,
            started_utc=iso_utc(started),
            finished_utc=iso_utc(finished),
            duration_seconds=round(time.monotonic() - started_clock, 3),
            pid=process.pid if process is not None else None,
            process_tree_strategy=strategy,
            stdout_path=stdout_path,
            stderr_path=stderr_path,
            capture_directory=capture_directory,
        )

    def run(
        self,
        command_file: Path,
        working_directory: Path,
        environment: dict[str, str],
        timeout_seconds: float | None,
        state_fields: dict[str, Any],
        *,
        deadline_monotonic: float | None = None,
        heartbeat: Any | None = None,
        heartbeat_seconds: float = HEARTBEAT_SECONDS,
    ) -> CommandResult:
        return self.run_argv(
            self.command_for(command_file),
            working_directory,
            environment,
            timeout_seconds,
            state_fields,
            deadline_monotonic=deadline_monotonic,
            heartbeat=heartbeat,
            heartbeat_seconds=heartbeat_seconds,
        )


def directory_state(directory: Path) -> dict[str, str]:
    state: dict[str, str] = {}
    if not directory.exists():
        return state
    for path in sorted(directory.rglob("*")):
        if path.is_symlink():
            raise WorkerError(
                f"Symbolic links are not allowed in worker output: {path}"
            )
        if path.is_file():
            relative = path.relative_to(directory).as_posix()
            state[relative] = sha256_file(path)
    return state


@dataclasses.dataclass
class OutputDelta:
    stage_directory: Path
    changed: tuple[str, ...]
    deleted: tuple[str, ...]

    @classmethod
    def create(
        cls,
        before: dict[str, str],
        after: dict[str, str],
        output_directory: Path,
    ) -> OutputDelta:
        stage = Path(tempfile.mkdtemp(prefix="continuous-worker-output-"))
        changed = tuple(
            sorted(path for path, digest in after.items() if before.get(path) != digest)
        )
        deleted = tuple(sorted(path for path in before if path not in after))
        for relative in changed:
            source = resolve_inside(output_directory, relative)
            destination = resolve_inside(stage, relative)
            destination.parent.mkdir(parents=True, exist_ok=True)
            shutil.copy2(source, destination)
        return cls(stage, changed, deleted)

    def apply(self, output_directory: Path) -> None:
        output_directory.mkdir(parents=True, exist_ok=True)
        for relative in self.deleted:
            destination = resolve_inside(output_directory, relative)
            if destination.is_dir() and not destination.is_symlink():
                shutil.rmtree(destination)
            else:
                destination.unlink(missing_ok=True)
        for relative in self.changed:
            source = resolve_inside(self.stage_directory, relative)
            destination = resolve_inside(output_directory, relative)
            if destination.is_dir() and not destination.is_symlink():
                shutil.rmtree(destination)
            destination.parent.mkdir(parents=True, exist_ok=True)
            shutil.copy2(source, destination)

    def cleanup(self) -> None:
        shutil.rmtree(self.stage_directory, ignore_errors=True)


def write_combined_log(destination: Path, result: CommandResult) -> None:
    destination.parent.mkdir(parents=True, exist_ok=True)
    with destination.open("wb") as output:
        output.write(b"=== STANDARD OUTPUT ===\n")
        with result.stdout_path.open("rb") as source:
            shutil.copyfileobj(source, output)
        output.write(b"\n\n=== STANDARD ERROR ===\n")
        with result.stderr_path.open("rb") as source:
            shutil.copyfileobj(source, output)


class TemporaryWorktree:
    def __init__(self, git: Git, commit: str):
        self.git = git
        self.commit = commit
        self.path = Path(tempfile.mkdtemp(prefix="continuous-worker-worktree-"))
        self.added = False

    def __enter__(self) -> Path:
        shutil.rmtree(self.path)
        self.git.run(
            "worktree", "add", "--detach", "--force", str(self.path), self.commit
        )
        self.added = True
        return self.path

    def __exit__(self, *_: object) -> None:
        if self.added:
            self.git.run("worktree", "remove", "--force", str(self.path), check=False)
        shutil.rmtree(self.path, ignore_errors=True)
        self.git.run("worktree", "prune", check=False)


def validated_relative_path(value: Any, field: str) -> str:
    if not isinstance(value, str) or not value or "\\" in value:
        raise WorkerError(f"{field} must be a non-empty forward-slash path")
    path = PurePosixPath(value)
    if path.is_absolute() or any(part in {"", ".", ".."} for part in path.parts):
        raise WorkerError(f"{field} is not a safe relative path: {value!r}")
    if ":" in path.parts[0]:
        raise WorkerError(f"{field} must not contain a drive or URI prefix")
    return path.as_posix()


def read_json_file(
    path: Path, *, maximum_bytes: int = MAX_JSON_BYTES
) -> dict[str, Any]:
    if path.stat().st_size > maximum_bytes:
        raise WorkerError(f"JSON file exceeds {maximum_bytes} bytes: {path}")
    try:
        value = json.loads(path.read_text(encoding=UTF8))
    except (OSError, UnicodeDecodeError, json.JSONDecodeError) as error:
        raise WorkerError(f"Invalid JSON file {path}: {error}") from error
    if not isinstance(value, dict):
        raise WorkerError(f"Expected a JSON object in {path}")
    return value


def read_text_tail(path: Path, maximum_bytes: int = TAIL_BYTES) -> str:
    try:
        with path.open("rb") as source:
            source.seek(0, os.SEEK_END)
            size = source.tell()
            source.seek(max(0, size - maximum_bytes))
            return source.read().decode(UTF8, errors="replace")
    except OSError:
        return ""


def command_environment(
    artifact_directory: Path, branch: str, worker_id: str
) -> dict[str, str]:
    environment: dict[str, str] = {}
    for key, value in os.environ.items():
        upper = key.upper()
        if upper in PROTECTED_ENVIRONMENT_KEYS:
            continue
        if upper.startswith("RCLONE_CONFIG_"):
            continue
        if upper.startswith("GIT_CONFIG_"):
            continue
        environment[key] = value
    environment.update(
        {
            "WORKER_BRANCH": branch,
            "WORKER_ID": worker_id,
            "WORKER_ARTIFACT_DIRECTORY": str(artifact_directory),
            "WORKER_OUTPUT_DIRECTORY": str(artifact_directory),
        }
    )
    return environment


def sanitize_branch_folder(branch: str) -> str:
    readable = re.sub(r"[^A-Za-z0-9._-]+", "_", branch).strip("._-")
    readable = readable[:72] or "branch"
    digest = hashlib.sha256(branch.encode(UTF8)).hexdigest()[:12]
    return f"{readable}--{digest}"


@dataclasses.dataclass(frozen=True)
class RemoteEntry:
    path: str
    name: str
    size: int
    file_id: str
    hashes: dict[str, str] = dataclasses.field(default_factory=dict)


class DriveTransport(Protocol):
    def ensure_directory(self, relative: str) -> None: ...

    def list_files(self, relative: str) -> list[RemoteEntry]: ...

    def exists(self, relative: str) -> bool: ...

    def download(self, relative: str, destination: Path) -> RemoteEntry: ...

    def upload(
        self, source: Path, relative: str, *, overwrite: bool = True
    ) -> RemoteEntry: ...

    def read_json(self, relative: str) -> dict[str, Any] | None: ...

    def write_json(
        self, relative: str, value: dict[str, Any], *, overwrite: bool = True
    ) -> RemoteEntry: ...


class LocalDirectoryDrive:
    """Filesystem implementation used for deterministic integration tests."""

    def __init__(self, root: Path):
        self.root = root.resolve()
        self.root.mkdir(parents=True, exist_ok=True)

    def _path(self, relative: str) -> Path:
        return resolve_inside(
            self.root, validated_relative_path(relative, "Drive path")
        )

    @staticmethod
    def _entry(path: Path, root: Path) -> RemoteEntry:
        relative = path.relative_to(root).as_posix()
        return RemoteEntry(
            path=relative,
            name=path.name,
            size=path.stat().st_size,
            file_id=hashlib.sha256(relative.encode(UTF8)).hexdigest()[:24],
            hashes={"SHA-256": sha256_file(path)},
        )

    def ensure_directory(self, relative: str) -> None:
        self._path(relative).mkdir(parents=True, exist_ok=True)

    def list_files(self, relative: str) -> list[RemoteEntry]:
        directory = self._path(relative)
        if not directory.exists():
            return []
        if not directory.is_dir():
            raise WorkerError(f"Drive path is not a directory: {relative}")
        return [
            self._entry(path, self.root)
            for path in sorted(directory.iterdir(), key=lambda item: item.name)
            if path.is_file() and not path.is_symlink()
        ]

    def exists(self, relative: str) -> bool:
        return self._path(relative).is_file()

    def download(self, relative: str, destination: Path) -> RemoteEntry:
        source = self._path(relative)
        if not source.is_file() or source.is_symlink():
            raise WorkerError(f"Drive file does not exist: {relative}")
        destination.parent.mkdir(parents=True, exist_ok=True)
        shutil.copy2(source, destination)
        return self._entry(source, self.root)

    def upload(
        self, source: Path, relative: str, *, overwrite: bool = True
    ) -> RemoteEntry:
        if not source.is_file() or source.is_symlink():
            raise WorkerError(f"Upload source is not a regular file: {source}")
        destination = self._path(relative)
        if destination.exists() and not overwrite:
            raise WorkerError(f"Drive file already exists: {relative}")
        destination.parent.mkdir(parents=True, exist_ok=True)
        temporary = destination.with_name(f".{destination.name}.{uuid.uuid4().hex}.tmp")
        try:
            shutil.copy2(source, temporary)
            os.replace(temporary, destination)
        finally:
            temporary.unlink(missing_ok=True)
        return self._entry(destination, self.root)

    def read_json(self, relative: str) -> dict[str, Any] | None:
        path = self._path(relative)
        return None if not path.is_file() else read_json_file(path)

    def write_json(
        self, relative: str, value: dict[str, Any], *, overwrite: bool = True
    ) -> RemoteEntry:
        with tempfile.TemporaryDirectory(prefix="continuous-worker-json-") as temporary:
            local = Path(temporary) / "value.json"
            atomic_write_json(local, value)
            return self.upload(local, relative, overwrite=overwrite)


class RcloneDrive:
    def __init__(
        self, executable: Path, config_file: Path, remote: str, root: str
    ) -> None:
        if not remote or ":" in remote:
            raise WorkerError("RCLONE_REMOTE must be a configured remote name")
        self.executable = executable.resolve()
        self.config_file = config_file.resolve()
        self.remote = remote
        self.root = validated_relative_path(root, "Drive workspace root")

    def _remote_path(self, relative: str) -> str:
        relative = validated_relative_path(relative, "Drive path")
        return f"{self.remote}:{self.root}/{relative}"

    def _run(
        self, *args: str, check: bool = True, timeout: float | None = 900
    ) -> subprocess.CompletedProcess[str]:
        environment = os.environ.copy()
        environment.pop("RCLONE_CONFIG", None)
        result = subprocess.run(
            [str(self.executable), "--config", str(self.config_file), *args],
            check=False,
            text=True,
            encoding=UTF8,
            errors="replace",
            capture_output=True,
            env=environment,
            timeout=timeout,
        )
        if check and result.returncode:
            details = (result.stderr or result.stdout).strip()
            raise WorkerError(
                f"rclone {' '.join(args[:2])} exited with {result.returncode}: "
                f"{details[:2000]}"
            )
        return result

    def ensure_directory(self, relative: str) -> None:
        self._run("mkdir", self._remote_path(relative))

    def list_files(self, relative: str) -> list[RemoteEntry]:
        result = self._run(
            "lsjson",
            self._remote_path(relative),
            "--files-only",
            "--hash",
            "--no-modtime",
        )
        try:
            values = json.loads(result.stdout)
        except json.JSONDecodeError as error:
            raise WorkerError(f"rclone returned invalid JSON: {error}") from error
        if not isinstance(values, list):
            raise WorkerError("rclone lsjson did not return a list")
        entries: list[RemoteEntry] = []
        for value in values:
            if not isinstance(value, dict):
                raise WorkerError("rclone lsjson returned a non-object entry")
            name = str(value.get("Name") or "")
            path = str(value.get("Path") or name)
            entries.append(
                RemoteEntry(
                    path=path,
                    name=name,
                    size=int(value.get("Size") or 0),
                    file_id=str(value.get("ID") or value.get("OrigID") or ""),
                    hashes={
                        str(key): str(item)
                        for key, item in (value.get("Hashes") or {}).items()
                    },
                )
            )
        return entries

    def _stat(self, relative: str) -> RemoteEntry:
        result = self._run(
            "lsjson",
            self._remote_path(relative),
            "--stat",
            "--hash",
            "--no-modtime",
        )
        try:
            value = json.loads(result.stdout)
        except json.JSONDecodeError as error:
            raise WorkerError(f"rclone stat returned invalid JSON: {error}") from error
        if not isinstance(value, dict):
            raise WorkerError(f"Drive file is missing after transfer: {relative}")
        return RemoteEntry(
            path=relative,
            name=PurePosixPath(relative).name,
            size=int(value.get("Size") or 0),
            file_id=str(value.get("ID") or value.get("OrigID") or ""),
            hashes={
                str(key): str(item) for key, item in (value.get("Hashes") or {}).items()
            },
        )

    def exists(self, relative: str) -> bool:
        return (
            self._run(
                "lsjson",
                self._remote_path(relative),
                "--stat",
                "--no-modtime",
                check=False,
            ).returncode
            == 0
        )

    def download(self, relative: str, destination: Path) -> RemoteEntry:
        destination.parent.mkdir(parents=True, exist_ok=True)
        self._run(
            "copyto",
            self._remote_path(relative),
            str(destination),
            "--retries",
            "6",
            "--low-level-retries",
            "10",
        )
        return self._stat(relative)

    def upload(
        self, source: Path, relative: str, *, overwrite: bool = True
    ) -> RemoteEntry:
        if not source.is_file() or source.is_symlink():
            raise WorkerError(f"Upload source is not a regular file: {source}")
        if not overwrite and self.exists(relative):
            raise WorkerError(f"Drive file already exists: {relative}")
        suffix = f".upload-{uuid.uuid4().hex}.tmp"
        temporary_relative = f"{relative}{suffix}"
        self._run(
            "copyto",
            str(source),
            self._remote_path(temporary_relative),
            "--retries",
            "6",
            "--low-level-retries",
            "10",
        )
        try:
            self._run(
                "moveto",
                self._remote_path(temporary_relative),
                self._remote_path(relative),
                "--retries",
                "6",
                "--low-level-retries",
                "10",
            )
        except Exception:
            self._run("deletefile", self._remote_path(temporary_relative), check=False)
            raise
        entry = self._stat(relative)
        if entry.size != source.stat().st_size:
            raise WorkerError(
                f"Drive size mismatch for {relative}: "
                f"{entry.size} != {source.stat().st_size}"
            )
        return entry

    def read_json(self, relative: str) -> dict[str, Any] | None:
        if not self.exists(relative):
            return None
        with tempfile.TemporaryDirectory(prefix="continuous-worker-download-") as temp:
            local = Path(temp) / "value.json"
            self.download(relative, local)
            return read_json_file(local)

    def write_json(
        self, relative: str, value: dict[str, Any], *, overwrite: bool = True
    ) -> RemoteEntry:
        with tempfile.TemporaryDirectory(prefix="continuous-worker-json-") as temp:
            local = Path(temp) / "value.json"
            atomic_write_json(local, value)
            return self.upload(local, relative, overwrite=overwrite)


def prepare_rclone(runtime_root: Path, explicit: str | None = None) -> Path:
    if explicit:
        executable = Path(explicit).resolve()
        if not executable.is_file():
            raise WorkerError(f"rclone executable does not exist: {executable}")
        return executable
    if os.name != "nt":
        discovered = shutil.which("rclone")
        if discovered:
            return Path(discovered).resolve()
        raise WorkerError("rclone was not found; pass --rclone-executable")
    installation = runtime_root / f"rclone-v{RCLONE_VERSION}"
    executable = installation / "rclone.exe"
    if executable.is_file():
        return executable
    installation.mkdir(parents=True, exist_ok=True)
    archive = installation / "rclone.zip"
    log(f"Downloading pinned rclone v{RCLONE_VERSION}")
    urllib.request.urlretrieve(RCLONE_WINDOWS_AMD64_URL, archive)
    actual = sha256_file(archive)
    if actual != RCLONE_WINDOWS_AMD64_SHA256:
        archive.unlink(missing_ok=True)
        raise WorkerError(
            f"rclone archive checksum mismatch: {actual} "
            f"!= {RCLONE_WINDOWS_AMD64_SHA256}"
        )
    with zipfile.ZipFile(archive) as bundle:
        candidates = [
            name
            for name in bundle.namelist()
            if name.casefold().endswith("/rclone.exe")
        ]
        if len(candidates) != 1:
            raise WorkerError("Pinned rclone archive has an unexpected layout")
        with bundle.open(candidates[0]) as source, executable.open("wb") as output:
            shutil.copyfileobj(source, output)
    archive.unlink(missing_ok=True)
    return executable


def write_rclone_config(runtime_root: Path, value: str) -> Path:
    encoded = value.encode(UTF8)
    if not encoded:
        raise WorkerError("RCLONE_CONFIG secret is empty")
    if len(encoded) > MAX_SECRET_BYTES:
        raise WorkerError(
            f"RCLONE_CONFIG exceeds the supported {MAX_SECRET_BYTES}-byte limit"
        )
    path = runtime_root / "continuous-worker-rclone.conf"
    path.write_bytes(encoded)
    with contextlib.suppress(OSError):
        path.chmod(0o600)
    os.environ.pop("RCLONE_CONFIG", None)
    return path


@dataclasses.dataclass(frozen=True)
class PayloadReference:
    name: str
    path: str
    size_bytes: int
    sha256: str


def materialized_payload_filename(payload: PayloadReference) -> str:
    """Keep the declared file suffix without duplicating it in the logical name."""
    suffix = PurePosixPath(payload.path).suffix
    if suffix and not payload.name.casefold().endswith(suffix.casefold()):
        return f"{payload.name}{suffix}"
    return payload.name


@dataclasses.dataclass(frozen=True)
class StepSpecification:
    kind: str
    payload: str | None = None
    argv: tuple[str, ...] = ()
    interpreter: str | None = None
    args: tuple[str, ...] = ()
    cwd: str = "."


@dataclasses.dataclass(frozen=True)
class ArtifactSpecification:
    path: str
    source: str
    required: bool
    when: str
    name: str | None


@dataclasses.dataclass(frozen=True)
class CommandEnvelope:
    command_id: str
    repository: str
    branch: str
    created_utc: str
    base_commit: str | None
    payloads: dict[str, PayloadReference]
    steps: tuple[StepSpecification, ...]
    commit_message: str
    timeout_seconds: float | None
    artifacts: tuple[ArtifactSpecification, ...]
    retry_of: str | None

    @classmethod
    def parse(
        cls,
        value: dict[str, Any],
        *,
        filename: str,
        repository: str,
        branch: str,
    ) -> CommandEnvelope:
        if value.get("schema_version") != 1:
            raise WorkerError("Command schema_version must be 1")
        command_id = value.get("command_id")
        if not isinstance(command_id, str) or not COMMAND_ID_PATTERN.fullmatch(
            command_id
        ):
            raise WorkerError("command_id has an invalid format")
        if filename != f"{command_id}.json":
            raise WorkerError("Command filename must match command_id")
        if value.get("repository") != repository or value.get("branch") != branch:
            raise WorkerError("Command repository or branch does not match this worker")
        created_utc = value.get("created_utc")
        if not isinstance(created_utc, str):
            raise WorkerError("created_utc must be an ISO-8601 string")
        try:
            dt.datetime.fromisoformat(created_utc.replace("Z", "+00:00"))
        except ValueError as error:
            raise WorkerError("created_utc is not valid ISO-8601") from error

        base_commit = value.get("base_commit")
        if base_commit is not None and (
            not isinstance(base_commit, str)
            or not COMMIT_PATTERN.fullmatch(base_commit)
        ):
            raise WorkerError("base_commit must be a full hexadecimal commit ID")

        raw_payloads = value.get("payloads", [])
        if not isinstance(raw_payloads, list):
            raise WorkerError("payloads must be an array")
        payloads: dict[str, PayloadReference] = {}
        for index, raw in enumerate(raw_payloads):
            if not isinstance(raw, dict):
                raise WorkerError(f"payloads[{index}] must be an object")
            name = raw.get("name")
            if not isinstance(name, str) or not COMMAND_ID_PATTERN.fullmatch(name):
                raise WorkerError(f"payloads[{index}].name is invalid")
            if name in payloads:
                raise WorkerError(f"Duplicate payload name: {name}")
            path = validated_relative_path(raw.get("path"), f"payloads[{index}].path")
            prefix = f"Input/files/{command_id}/"
            if not path.startswith(prefix):
                raise WorkerError(f"Payload path must be under {prefix}")
            size = raw.get("size_bytes")
            digest = raw.get("sha256")
            if not isinstance(size, int) or size < 0:
                raise WorkerError(f"payloads[{index}].size_bytes is invalid")
            if not isinstance(digest, str) or not SHA256_PATTERN.fullmatch(digest):
                raise WorkerError(f"payloads[{index}].sha256 is invalid")
            payloads[name] = PayloadReference(name, path, size, digest)

        raw_steps = value.get("steps")
        if not isinstance(raw_steps, list) or not raw_steps:
            raise WorkerError("steps must be a non-empty array")
        steps: list[StepSpecification] = []
        has_patch = False
        for index, raw in enumerate(raw_steps):
            if not isinstance(raw, dict):
                raise WorkerError(f"steps[{index}] must be an object")
            kind = raw.get("type")
            if kind not in {"apply_patch", "exec", "script", "collect_artifacts"}:
                raise WorkerError(f"steps[{index}].type is unsupported")
            cwd = raw.get("cwd", ".")
            if cwd != ".":
                cwd = validated_relative_path(cwd, f"steps[{index}].cwd")
            payload = raw.get("payload")
            argv: tuple[str, ...] = ()
            args: tuple[str, ...] = ()
            interpreter = raw.get("interpreter")
            if kind == "apply_patch":
                has_patch = True
                if not isinstance(payload, str) or payload not in payloads:
                    raise WorkerError(f"steps[{index}] references a missing payload")
            elif kind == "exec":
                raw_argv = raw.get("argv")
                if (
                    not isinstance(raw_argv, list)
                    or not raw_argv
                    or not all(isinstance(item, str) and item for item in raw_argv)
                ):
                    raise WorkerError(f"steps[{index}].argv is invalid")
                argv = tuple(raw_argv)
            elif kind == "script":
                if not isinstance(payload, str) or payload not in payloads:
                    raise WorkerError(f"steps[{index}] references a missing payload")
                if interpreter not in {"python", "pwsh"}:
                    raise WorkerError(
                        f"steps[{index}].interpreter must be python or pwsh"
                    )
                if (
                    interpreter == "pwsh"
                    and PurePosixPath(payloads[payload].path).suffix.casefold()
                    != ".ps1"
                ):
                    raise WorkerError(
                        f"steps[{index}] PowerShell payload path must end in .ps1"
                    )
                raw_args = raw.get("args", [])
                if not isinstance(raw_args, list) or not all(
                    isinstance(item, str) for item in raw_args
                ):
                    raise WorkerError(f"steps[{index}].args is invalid")
                args = tuple(raw_args)
            steps.append(StepSpecification(kind, payload, argv, interpreter, args, cwd))
        if has_patch and base_commit is None:
            raise WorkerError("base_commit is required for apply_patch")

        commit_message = value.get("commit_message", "Apply automated changes")
        if (
            not isinstance(commit_message, str)
            or not commit_message.strip()
            or len(commit_message) > 200
        ):
            raise WorkerError("commit_message must contain 1-200 characters")
        raw_timeout = value.get("timeout_seconds")
        timeout_seconds: float | None
        if raw_timeout is None:
            timeout_seconds = None
        elif isinstance(raw_timeout, (int, float)) and raw_timeout > 0:
            timeout_seconds = float(raw_timeout)
        else:
            raise WorkerError("timeout_seconds must be null or a positive number")

        raw_artifacts = value.get("artifacts", [])
        if not isinstance(raw_artifacts, list):
            raise WorkerError("artifacts must be an array")
        artifacts: list[ArtifactSpecification] = []
        for index, raw in enumerate(raw_artifacts):
            if not isinstance(raw, dict):
                raise WorkerError(f"artifacts[{index}] must be an object")
            path = validated_relative_path(raw.get("path"), f"artifacts[{index}].path")
            source = raw.get("source", "workspace")
            when = raw.get("when", "success")
            required = raw.get("required", True)
            name = raw.get("name")
            if source not in {"workspace", "artifact_directory"}:
                raise WorkerError(f"artifacts[{index}].source is invalid")
            if when not in {"success", "always"}:
                raise WorkerError(f"artifacts[{index}].when is invalid")
            if not isinstance(required, bool):
                raise WorkerError(f"artifacts[{index}].required must be boolean")
            if name is not None:
                name = validated_relative_path(name, f"artifacts[{index}].name")
            artifacts.append(ArtifactSpecification(path, source, required, when, name))

        retry_of = value.get("retry_of")
        if retry_of is not None and (
            not isinstance(retry_of, str) or not COMMAND_ID_PATTERN.fullmatch(retry_of)
        ):
            raise WorkerError("retry_of has an invalid format")
        return cls(
            command_id=command_id,
            repository=repository,
            branch=branch,
            created_utc=created_utc,
            base_commit=base_commit,
            payloads=payloads,
            steps=tuple(steps),
            commit_message=commit_message.strip(),
            timeout_seconds=timeout_seconds,
            artifacts=tuple(artifacts),
            retry_of=retry_of,
        )


def unique_entries(entries: Iterable[RemoteEntry]) -> dict[str, RemoteEntry]:
    indexed: dict[str, RemoteEntry] = {}
    duplicates: set[str] = set()
    for entry in entries:
        if entry.name in indexed:
            duplicates.add(entry.name)
        indexed[entry.name] = entry
    if duplicates:
        raise WorkerError(
            "Duplicate Drive filenames are ambiguous: " + ", ".join(sorted(duplicates))
        )
    return indexed


@dataclasses.dataclass(frozen=True)
class StagedArtifact:
    local_path: Path
    relative_name: str
    source_path: str
    required: bool


class CommandConflict(WorkerError):
    """A safe Git or patch conflict that should become a command result."""


class ContinuousWorker:
    def __init__(
        self,
        *,
        repository: Path,
        repository_full_name: str,
        branch: str,
        drive: DriveTransport,
        token: str,
        poll_seconds: float,
        max_runtime_minutes: float,
        once: bool = False,
        shutdown_reserve_seconds: float = SHUTDOWN_RESERVE_SECONDS,
        heartbeat_seconds: float = HEARTBEAT_SECONDS,
    ) -> None:
        self.repository = repository.resolve()
        self.repository_full_name = repository_full_name
        self.branch = branch
        self.drive = drive
        self.git = Git(self.repository, token)
        self.poll_seconds = poll_seconds
        self.max_runtime_seconds = max_runtime_minutes * 60
        self.shutdown_reserve_seconds = shutdown_reserve_seconds
        self.heartbeat_seconds = heartbeat_seconds
        self.once = once
        self.worker_id = uuid.uuid4().hex
        runtime_root = Path(
            os.environ.get("RUNNER_TEMP") or tempfile.gettempdir()
        ).resolve()
        checkout_key = hashlib.sha256(
            f"{self.repository_full_name}\0{self.branch}".encode(UTF8)
        ).hexdigest()[:20]
        self.lock_path = runtime_root / f"drive-worker-{checkout_key}.lock"
        self.state = StateStore(
            runtime_root / f"drive-worker-{checkout_key}.json",
            self.worker_id,
            self.branch,
        )
        self.stop_event = threading.Event()
        self.process_runner = ProcessRunner(self.stop_event, self.state)
        self.origin_url = ""
        self.deadline = 0.0
        self.processed_count = 0

    @property
    def remote_ref(self) -> str:
        return f"refs/remotes/origin/{self.branch}"

    @property
    def remote_head(self) -> str:
        return f"refs/heads/{self.branch}"

    @property
    def fetch_refspec(self) -> str:
        return f"+{self.remote_head}:{self.remote_ref}"

    def request_stop(self, signum: int, _frame: object) -> None:
        log(f"Received signal {signum}; requesting shutdown", level="WARNING")
        self.stop_event.set()

    def validate(self) -> None:
        if self.repository_full_name.count("/") != 1:
            raise WorkerError(f"Invalid repository name: {self.repository_full_name}")
        if self.poll_seconds <= 0 or self.heartbeat_seconds <= 0:
            raise WorkerError("Poll and heartbeat intervals must be positive")
        if self.max_runtime_seconds <= self.shutdown_reserve_seconds:
            raise WorkerError("Runtime must exceed its shutdown reserve")
        if self.git.run(
            "check-ref-format", "--branch", self.branch, check=False
        ).returncode:
            raise WorkerError(f"Invalid branch name: {self.branch}")
        top_level = Path(self.git.text("rev-parse", "--show-toplevel")).resolve()
        if top_level != self.repository:
            raise WorkerError(
                f"Repository root mismatch: expected {self.repository}, found {top_level}"
            )

    def ensure_origin(self) -> None:
        current = self.git.run("remote", "get-url", "origin", check=False)
        if current.returncode != 0:
            self.git.run("remote", "add", "origin", self.origin_url)
        elif current.stdout.strip() != self.origin_url:
            self.git.run("remote", "set-url", "origin", self.origin_url)

    def fetch_branch(self, restore_commit: str | None = None) -> None:
        self.ensure_origin()
        fetched = self.git.run(
            "fetch", "--no-tags", "origin", self.fetch_refspec, check=False
        )
        if fetched.returncode == 0:
            return
        lookup = self.git.run(
            "ls-remote",
            "--exit-code",
            "--heads",
            "origin",
            self.remote_head,
            check=False,
        )
        if lookup.returncode != 2:
            raise GitError(
                ("fetch", "--no-tags", "origin", self.fetch_refspec), fetched
            )
        source = restore_commit or self.git.text("rev-parse", "--verify", "HEAD")
        log(
            f"Remote branch {self.branch} disappeared; restoring it from {source}",
            level="WARNING",
        )
        pushed = self.git.run(
            "push", "origin", f"{source}:{self.remote_head}", check=False
        )
        if pushed.returncode != 0:
            raced = self.git.run(
                "ls-remote",
                "--exit-code",
                "--heads",
                "origin",
                self.remote_head,
                check=False,
            )
            if raced.returncode != 0:
                raise GitError(
                    ("push", "origin", f"{source}:{self.remote_head}"), pushed
                )
        self.git.run("fetch", "--no-tags", "origin", self.fetch_refspec)

    def set_checkout(self, commit: str) -> None:
        self.git.abort_operations()
        self.git.run("checkout", "--force", "-B", self.branch, commit)
        self.git.run("reset", "--hard", commit)
        current = self.git.text("branch", "--show-current")
        if current != self.branch:
            raise WorkerError(f"Expected branch {self.branch}, found {current}")

    def sync(self) -> None:
        current = self.git.text("branch", "--show-current")
        if current != self.branch:
            self.set_checkout("HEAD")
        local_commit = self.git.text("rev-parse", "HEAD")
        self.fetch_branch(local_commit)
        remote_commit = self.git.text("rev-parse", self.remote_ref)
        if local_commit != remote_commit:
            log(f"Branch changed: {local_commit} -> {remote_commit}")
            self.set_checkout(remote_commit)

    def committed_stop(self) -> bool:
        return self.git.exists_at_head(".continuous-worker.stop")

    @staticmethod
    def _command_paths(command_id: str) -> dict[str, str]:
        base = f"Output/command_output/{command_id}"
        return {
            "claim": f"{base}.claim.json",
            "status": f"{base}.status.json",
            "result": f"{base}.result.json",
            "log": f"Output/files/logs/{command_id}.log",
            "artifact_root": f"Output/files/artifacts/{command_id}",
        }

    def _base_metadata(self, command_id: str) -> dict[str, Any]:
        return {
            "schema_version": 1,
            "command_id": command_id,
            "repository": self.repository_full_name,
            "branch": self.branch,
            "worker_id": self.worker_id,
            "worker_pid": os.getpid(),
            "workflow_run_id": os.environ.get("GITHUB_RUN_ID", ""),
            "workflow_run_attempt": os.environ.get("GITHUB_RUN_ATTEMPT", ""),
            "runner_name": os.environ.get("RUNNER_NAME", ""),
        }

    def write_status(
        self, command_id: str, state: str, **fields: Any
    ) -> RemoteEntry | None:
        value = {
            **self._base_metadata(command_id),
            "state": state,
            "updated_utc": iso_utc(),
            **fields,
        }
        try:
            return self.drive.write_json(
                self._command_paths(command_id)["status"], value
            )
        except Exception as error:  # noqa: BLE001 - heartbeat must not kill command
            log(
                f"Could not publish status for {command_id}: {error}",
                level="WARNING",
            )
            return None

    def initialize_drive(self) -> None:
        for directory in (
            "Input/commands",
            "Input/files",
            "Output/command_output",
            "Output/files/logs",
            "Output/files/artifacts",
            "Output/files/codebase",
        ):
            self.drive.ensure_directory(directory)
        owner, repository = self.repository_full_name.split("/", 1)
        self.drive.write_json(
            "Output/branch.json",
            {
                "schema_version": 1,
                "repository": self.repository_full_name,
                "owner": owner,
                "name": repository,
                "branch": self.branch,
                "branch_folder": sanitize_branch_folder(self.branch),
                "worker_protocol": "google-drive-continuous-worker-v1",
                "updated_utc": iso_utc(),
            },
        )

    def initialize(self) -> None:
        self.origin_url = self.git.text("remote", "get-url", "origin")
        self.git.run("config", "user.name", "github-actions[bot]")
        self.git.run(
            "config",
            "user.email",
            "41898282+github-actions[bot]@users.noreply.github.com",
        )
        previous = self.state.read()
        if previous and previous.get("status") == "running_command":
            log(
                "Recovered local stale command state "
                f"(PID {previous.get('command_pid')}); Drive claim recovery "
                "will prevent automatic re-execution",
                level="WARNING",
            )
        self.fetch_branch()
        self.set_checkout(self.remote_ref)
        self.initialize_drive()
        self.ensure_codebase_snapshot(self.git.text("rev-parse", "HEAD"))

    def discover_commands(self) -> list[RemoteEntry]:
        indexed = unique_entries(self.drive.list_files("Input/commands"))
        commands = []
        for name, entry in indexed.items():
            if not name.casefold().endswith(".json"):
                continue
            command_id = name[:-5]
            if not COMMAND_ID_PATTERN.fullmatch(command_id):
                log(f"Ignoring invalid command filename: {name}", level="WARNING")
                continue
            commands.append(entry)
        return sorted(commands, key=lambda entry: entry.name)

    def publish_interrupted_claim(self, command_id: str) -> None:
        paths = self._command_paths(command_id)
        if self.drive.exists(paths["result"]):
            return
        value = {
            **self._base_metadata(command_id),
            "status": "interrupted_unknown",
            "exit_code": EXIT_LAUNCHER_ERROR,
            "timed_out": False,
            "interrupted": True,
            "deadline_interrupted": False,
            "started_utc": None,
            "finished_utc": iso_utc(),
            "duration_seconds": None,
            "current_step": None,
            "source_commit": None,
            "resulting_commit": None,
            "workspace_changed": None,
            "changed_files": [],
            "commit_created": False,
            "pushed": False,
            "codebase_changed": False,
            "codebase": None,
            "artifacts": [],
            "log": None,
            "stdout_tail": "",
            "stderr_tail": "",
            "retry_required": True,
            "conflict": False,
            "latest_codebase": None,
            "errors": [
                (
                    "A previous worker claimed this command but did not publish "
                    "a result. Submit a new command_id with retry_of to run it "
                    "again."
                )
            ],
        }
        self.drive.write_json(paths["result"], value, overwrite=False)
        self.write_status(command_id, "interrupted_unknown", retry_required=True)

    def publish_rejected(
        self, command_id: str, error: Exception, *, claimed: bool = False
    ) -> None:
        paths = self._command_paths(command_id)
        if self.drive.exists(paths["result"]):
            return
        conflict = isinstance(error, CommandConflict)
        latest_codebase: dict[str, Any] | None = None
        if conflict:
            try:
                latest_codebase = self.drive.read_json(
                    "Output/files/codebase/latest.json"
                )
            except Exception as latest_error:  # noqa: BLE001 - rejection is final
                log(
                    f"Could not read latest codebase after conflict: {latest_error}",
                    level="WARNING",
                )
        value = {
            **self._base_metadata(command_id),
            "status": "rejected",
            "exit_code": EXIT_REJECTED,
            "timed_out": False,
            "interrupted": False,
            "deadline_interrupted": False,
            "started_utc": None,
            "finished_utc": iso_utc(),
            "duration_seconds": 0.0,
            "current_step": None,
            "source_commit": None,
            "resulting_commit": None,
            "workspace_changed": False,
            "changed_files": [],
            "commit_created": False,
            "pushed": False,
            "codebase_changed": False,
            "codebase": None,
            "artifacts": [],
            "log": None,
            "stdout_tail": "",
            "stderr_tail": "",
            "retry_required": conflict,
            "conflict": conflict,
            "latest_codebase": latest_codebase,
            "claim_created": claimed,
            "errors": [f"{type(error).__name__}: {error}"],
        }
        self.drive.write_json(paths["result"], value, overwrite=False)
        self.write_status(command_id, "rejected", error=str(error))

    def download_envelope(
        self, entry: RemoteEntry, directory: Path
    ) -> tuple[CommandEnvelope, dict[str, Path]]:
        if entry.size > MAX_JSON_BYTES:
            raise WorkerError(
                f"Command envelope exceeds {MAX_JSON_BYTES} bytes: {entry.name}"
            )
        local_envelope = directory / entry.name
        self.drive.download(f"Input/commands/{entry.name}", local_envelope)
        envelope = CommandEnvelope.parse(
            read_json_file(local_envelope),
            filename=entry.name,
            repository=self.repository_full_name,
            branch=self.branch,
        )
        payload_paths: dict[str, Path] = {}
        payload_root = directory / "payloads"
        payload_directories: dict[str, dict[str, RemoteEntry]] = {}
        materialized_names: set[str] = set()
        for payload in envelope.payloads.values():
            remote_path = PurePosixPath(payload.path)
            remote_parent = remote_path.parent.as_posix()
            if remote_parent not in payload_directories:
                payload_directories[remote_parent] = unique_entries(
                    self.drive.list_files(remote_parent)
                )
            remote_entry = payload_directories[remote_parent].get(remote_path.name)
            if remote_entry is None:
                raise WorkerError(f"Payload is missing from Drive: {payload.path}")
            if remote_entry.size != payload.size_bytes:
                raise WorkerError(
                    f"Payload Drive size mismatch for {payload.name}: "
                    f"{remote_entry.size} != {payload.size_bytes}"
                )
            destination_name = materialized_payload_filename(payload)
            collision_key = destination_name.casefold()
            if collision_key in materialized_names:
                raise WorkerError(
                    f"Payloads resolve to the same local filename: {destination_name}"
                )
            materialized_names.add(collision_key)
            destination = resolve_inside(payload_root, destination_name)
            downloaded_entry = self.drive.download(payload.path, destination)
            if (
                remote_entry.file_id
                and downloaded_entry.file_id
                and remote_entry.file_id != downloaded_entry.file_id
            ):
                raise WorkerError(f"Payload changed during download: {payload.name}")
            if destination.stat().st_size != payload.size_bytes:
                raise WorkerError(
                    f"Payload size mismatch for {payload.name}: "
                    f"{destination.stat().st_size} != {payload.size_bytes}"
                )
            digest = sha256_file(destination)
            if digest != payload.sha256:
                raise WorkerError(
                    f"Payload SHA-256 mismatch for {payload.name}: "
                    f"{digest} != {payload.sha256}"
                )
            payload_paths[payload.name] = destination
        return envelope, payload_paths

    @staticmethod
    def append_step_log(
        destination: Path,
        index: int,
        step: StepSpecification,
        result: CommandResult,
    ) -> None:
        with destination.open("ab") as output:
            output.write(
                (
                    f"\n=== STEP {index + 1}: {step.kind} ===\n"
                    f"status={result.status} exit_code={result.exit_code} "
                    f"duration_seconds={result.duration_seconds}\n"
                    "=== STANDARD OUTPUT ===\n"
                ).encode(UTF8)
            )
            with result.stdout_path.open("rb") as source:
                shutil.copyfileobj(source, output)
            output.write(b"\n=== STANDARD ERROR ===\n")
            with result.stderr_path.open("rb") as source:
                shutil.copyfileobj(source, output)
            output.write(b"\n")

    @staticmethod
    def changed_files(git: Git) -> list[dict[str, str]]:
        raw = git.run("status", "--porcelain=v1", "-z", "--untracked-files=all").stdout
        values = raw.split("\0")
        changed: list[dict[str, str]] = []
        index = 0
        while index < len(values):
            value = values[index]
            index += 1
            if not value:
                continue
            status = value[:2]
            path = value[3:] if len(value) > 3 else ""
            record = {"status": status, "path": path}
            if (
                (status[0] in {"R", "C"} or status[1] in {"R", "C"})
                and index < len(values)
                and values[index]
            ):
                record["source_path"] = values[index]
                index += 1
            changed.append(record)
        return changed

    def push_commit(self, work_git: Git, source_commit: str) -> tuple[str, bool]:
        base = source_commit
        for attempt in range(1, 11):
            self.fetch_branch(base)
            remote_commit = self.git.text("rev-parse", self.remote_ref)
            if remote_commit != base:
                rebased = work_git.run(
                    "rebase",
                    "--onto",
                    remote_commit,
                    base,
                    "HEAD",
                    check=False,
                    timeout=600,
                )
                if rebased.returncode:
                    work_git.run("rebase", "--abort", check=False)
                    raise CommandConflict(
                        "The branch advanced and the successful command changes "
                        "conflict with the new branch head"
                    )
                base = remote_commit
            pushed = work_git.run(
                "push", "origin", f"HEAD:{self.remote_head}", check=False, timeout=600
            )
            if pushed.returncode == 0:
                return work_git.text("rev-parse", "HEAD"), True
            if attempt == 10:
                raise GitError(("push", "origin", f"HEAD:{self.remote_head}"), pushed)
            log(
                f"Push attempt {attempt}/10 raced; retrying from latest branch",
                level="WARNING",
            )
            time.sleep(min(attempt * 2, 15))
        raise WorkerError("Unreachable push retry state")

    @staticmethod
    def stage_artifacts(
        envelope: CommandEnvelope,
        *,
        worktree: Path,
        artifact_directory: Path,
        successful: bool,
        stage: Path,
    ) -> tuple[list[StagedArtifact], list[str]]:
        staged: list[StagedArtifact] = []
        errors: list[str] = []
        for specification in envelope.artifacts:
            if specification.when == "success" and not successful:
                continue
            root = (
                worktree if specification.source == "workspace" else artifact_directory
            )
            matches = [
                path
                for path in sorted(root.glob(specification.path))
                if path.is_file() and not path.is_symlink()
            ]
            if not matches:
                message = (
                    f"Artifact pattern matched no regular files: "
                    f"{specification.source}:{specification.path}"
                )
                if specification.required:
                    errors.append(message)
                continue
            for matched in matches:
                resolved = matched.resolve()
                if not is_relative_to(resolved, root.resolve()):
                    errors.append(f"Artifact escapes its allowed root: {matched}")
                    continue
                relative = matched.relative_to(root).as_posix()
                if specification.name:
                    if len(matches) == 1:
                        target_name = specification.name
                    else:
                        target_name = f"{specification.name.rstrip('/')}/{relative}"
                else:
                    target_name = relative
                target_name = validated_relative_path(target_name, "artifact name")
                destination = resolve_inside(stage, target_name)
                destination.parent.mkdir(parents=True, exist_ok=True)
                shutil.copy2(matched, destination)
                staged.append(
                    StagedArtifact(
                        destination,
                        target_name,
                        f"{specification.source}:{relative}",
                        specification.required,
                    )
                )
        return staged, errors

    def upload_artifacts(
        self, command_id: str, staged: Iterable[StagedArtifact]
    ) -> list[dict[str, Any]]:
        uploaded: list[dict[str, Any]] = []
        root = self._command_paths(command_id)["artifact_root"]
        for artifact in staged:
            remote_path = f"{root}/{artifact.relative_name}"
            entry = self.drive.upload(artifact.local_path, remote_path)
            uploaded.append(
                {
                    "source_path": artifact.source_path,
                    "drive_path": remote_path,
                    "drive_file_id": entry.file_id,
                    "name": artifact.relative_name,
                    "size_bytes": artifact.local_path.stat().st_size,
                    "sha256": sha256_file(artifact.local_path),
                    "required": artifact.required,
                }
            )
        return uploaded

    def ensure_codebase_snapshot(self, commit: str) -> dict[str, Any]:
        commit = self.git.text("rev-parse", f"{commit}^{{commit}}")
        tree = self.git.text("rev-parse", f"{commit}^{{tree}}")
        latest_path = "Output/files/codebase/latest.json"
        latest = self.drive.read_json(latest_path)
        archive_entries = unique_entries(self.drive.list_files("Output/files/codebase"))
        if latest and latest.get("commit") == commit:
            archive_path = latest.get("drive_path")
            archive_name = latest.get("file_name")
            expected_size = latest.get("size_bytes")
            expected_digest = latest.get("sha256")
            if (
                isinstance(archive_path, str)
                and isinstance(archive_name, str)
                and archive_path == f"Output/files/codebase/{archive_name}"
                and isinstance(expected_size, int)
                and expected_size >= 0
                and isinstance(expected_digest, str)
            ):
                entry = archive_entries.get(archive_name)
                if entry is not None and entry.size == expected_size:
                    normalized_hashes = {
                        key.casefold().replace("-", ""): value.casefold()
                        for key, value in entry.hashes.items()
                    }
                    remote_digest = normalized_hashes.get("sha256")
                    if remote_digest is None or remote_digest == expected_digest:
                        return latest
        sequence = 1
        if latest and isinstance(latest.get("sequence"), int):
            sequence = int(latest["sequence"]) + 1
        else:
            for entry in archive_entries.values():
                match = re.match(r"^(\d{6})_[0-9a-f]{7,64}\.zip$", entry.name)
                if match:
                    sequence = max(sequence, int(match.group(1)) + 1)
        filename = f"{sequence:06d}_{commit[:12]}.zip"
        remote_path = f"Output/files/codebase/{filename}"
        with tempfile.TemporaryDirectory(prefix="continuous-worker-archive-") as temp:
            archive = Path(temp) / filename
            self.git.run(
                "archive",
                "--format=zip",
                "--prefix=repository/",
                f"--output={archive}",
                commit,
                timeout=600,
            )
            manifest = {
                "schema_version": 1,
                "repository": self.repository_full_name,
                "branch": self.branch,
                "commit": commit,
                "tree": tree,
                "sequence": sequence,
                "generated_utc": iso_utc(),
            }
            with zipfile.ZipFile(
                archive, "a", compression=zipfile.ZIP_DEFLATED, compresslevel=6
            ) as bundle:
                bundle.writestr(
                    "_bridge/manifest.json",
                    json.dumps(manifest, ensure_ascii=False, indent=2) + "\n",
                )
            digest = sha256_file(archive)
            entry = self.drive.upload(archive, remote_path, overwrite=False)
            metadata = {
                **manifest,
                "drive_path": remote_path,
                "drive_file_id": entry.file_id,
                "file_name": filename,
                "size_bytes": archive.stat().st_size,
                "sha256": digest,
                "remote_hashes": entry.hashes,
            }
            self.drive.write_json(latest_path, metadata)
            return metadata

    def execute_command(
        self,
        envelope: CommandEnvelope,
        payload_paths: dict[str, Path],
        working: Path,
    ) -> dict[str, Any]:
        source_commit = self.git.text("rev-parse", "HEAD")
        started = utc_now()
        started_clock = time.monotonic()
        command_deadline = (
            None
            if envelope.timeout_seconds is None
            else started_clock + envelope.timeout_seconds
        )
        log_path = working / f"{envelope.command_id}.log"
        log_path.write_bytes(b"")
        artifact_directory = working / "artifact-directory"
        artifact_directory.mkdir()
        staged_directory = working / "staged-artifacts"
        staged_directory.mkdir()
        step_records: list[dict[str, Any]] = []
        final_result: CommandResult | None = None
        changed: list[dict[str, str]] = []
        resulting_commit: str | None = source_commit
        commit_created = False
        pushed = False
        conflict_error: str | None = None
        staged_artifacts: list[StagedArtifact] = []
        artifact_errors: list[str] = []

        with TemporaryWorktree(self.git, source_commit) as worktree:
            work_git = Git(worktree, self.git.token)
            environment = command_environment(
                artifact_directory, self.branch, self.worker_id
            )
            if envelope.base_commit:
                available = work_git.run(
                    "cat-file",
                    "-e",
                    f"{envelope.base_commit}^{{commit}}",
                    check=False,
                )
                ancestor = work_git.run(
                    "merge-base",
                    "--is-ancestor",
                    envelope.base_commit,
                    source_commit,
                    check=False,
                )
                if available.returncode or ancestor.returncode:
                    raise CommandConflict(
                        "base_commit is unavailable or is not an ancestor of "
                        "the current branch; download the latest codebase ZIP"
                    )

            for index, step in enumerate(envelope.steps):
                if step.kind == "collect_artifacts":
                    step_records.append(
                        {
                            "index": index,
                            "type": step.kind,
                            "status": "success",
                            "exit_code": 0,
                            "duration_seconds": 0.0,
                        }
                    )
                    continue
                cwd = (
                    worktree if step.cwd == "." else resolve_inside(worktree, step.cwd)
                )
                if not cwd.is_dir():
                    raise WorkerError(
                        f"Step working directory does not exist: {step.cwd}"
                    )
                if step.kind == "apply_patch":
                    command = [
                        "git",
                        "apply",
                        "--3way",
                        "--index",
                        "--whitespace=nowarn",
                        str(payload_paths[str(step.payload)]),
                    ]
                elif step.kind == "exec":
                    command = list(step.argv)
                elif step.interpreter == "python":
                    script_path = payload_paths[str(step.payload)]
                    command = [
                        sys.executable,
                        str(script_path),
                        *step.args,
                    ]
                else:
                    pwsh = shutil.which("pwsh")
                    if not pwsh:
                        raise WorkerError("pwsh was not found for a script step")
                    script_path = payload_paths[str(step.payload)]
                    if script_path.suffix.casefold() != ".ps1":
                        raise WorkerError(
                            "Materialized PowerShell script must end in .ps1"
                        )
                    command = [
                        pwsh,
                        "-NoLogo",
                        "-NoProfile",
                        "-NonInteractive",
                        "-ExecutionPolicy",
                        "Bypass",
                        "-File",
                        str(script_path),
                        *step.args,
                    ]
                remaining_timeout = (
                    None
                    if command_deadline is None
                    else max(0.001, command_deadline - time.monotonic())
                )

                def heartbeat(
                    fields: dict[str, Any], *, step_index: int = index
                ) -> None:
                    self.write_status(
                        envelope.command_id,
                        "running",
                        current_step=step_index,
                        total_steps=len(envelope.steps),
                        started_utc=iso_utc(started),
                        **fields,
                    )

                result = self.process_runner.run_argv(
                    command,
                    cwd,
                    environment,
                    remaining_timeout,
                    {
                        "command_id": envelope.command_id,
                        "current_step": index,
                        "source_commit": source_commit,
                    },
                    deadline_monotonic=self.deadline,
                    heartbeat=heartbeat,
                    heartbeat_seconds=self.heartbeat_seconds,
                )
                self.append_step_log(log_path, index, step, result)
                step_records.append(
                    {
                        "index": index,
                        "type": step.kind,
                        "status": result.status,
                        "exit_code": result.exit_code,
                        "timed_out": result.timed_out,
                        "interrupted": result.interrupted,
                        "started_utc": result.started_utc,
                        "finished_utc": result.finished_utc,
                        "duration_seconds": result.duration_seconds,
                        "command_pid": result.pid,
                    }
                )
                if final_result is not None:
                    shutil.rmtree(final_result.capture_directory, ignore_errors=True)
                final_result = result
                if (
                    step.kind == "apply_patch"
                    and result.exit_code != 0
                    and not result.timed_out
                    and not result.interrupted
                ):
                    conflict_error = (
                        "The patch could not be applied cleanly to the current "
                        "branch. Download the latest codebase ZIP and create a "
                        "new patch with a new command ID."
                    )
                if result.status != "success":
                    break

            successful = final_result is None or final_result.status == "success"
            changed = self.changed_files(work_git)
            staged_artifacts, artifact_errors = self.stage_artifacts(
                envelope,
                worktree=worktree,
                artifact_directory=artifact_directory,
                successful=successful,
                stage=staged_directory,
            )
            if successful and changed:
                work_git.run("add", "-A")
                changed_index = work_git.run(
                    "diff", "--cached", "--quiet", "--exit-code", check=False
                )
                if changed_index.returncode == 1:
                    message = (
                        f"{envelope.commit_message}\n\n"
                        f"Continuous-Worker-Command-ID: {envelope.command_id}\n"
                        f"Continuous-Worker-Base-Commit: {source_commit}"
                    )
                    work_git.run("commit", "-m", message)
                    commit_created = True
                    resulting_commit, pushed = self.push_commit(work_git, source_commit)
                elif changed_index.returncode != 0:
                    raise GitError(
                        ("diff", "--cached", "--quiet", "--exit-code"),
                        changed_index,
                    )

        if final_result is None:
            now = iso_utc()
            empty = Path(tempfile.mkdtemp(prefix="continuous-worker-empty-result-"))
            stdout_path = empty / "stdout.bin"
            stderr_path = empty / "stderr.bin"
            stdout_path.write_bytes(b"")
            stderr_path.write_bytes(b"")
            final_result = CommandResult(
                "success",
                0,
                False,
                False,
                False,
                now,
                now,
                0.0,
                None,
                "none",
                stdout_path,
                stderr_path,
                empty,
            )
        return {
            "source_commit": source_commit,
            "resulting_commit": resulting_commit,
            "result": final_result,
            "step_records": step_records,
            "changed_files": changed,
            "commit_created": commit_created,
            "pushed": pushed,
            "log_path": log_path,
            "staged_artifacts": staged_artifacts,
            "artifact_errors": artifact_errors,
            "conflict_error": conflict_error,
            "started_utc": iso_utc(started),
            "duration_seconds": round(time.monotonic() - started_clock, 3),
        }

    def process_one(self, entry: RemoteEntry) -> bool:
        command_id = entry.name[:-5]
        paths = self._command_paths(command_id)
        if self.drive.exists(paths["result"]):
            return False
        if self.drive.exists(paths["claim"]):
            self.publish_interrupted_claim(command_id)
            return True
        with tempfile.TemporaryDirectory(
            prefix=f"continuous-worker-{command_id[:24]}-"
        ) as temporary:
            working = Path(temporary)
            claimed = False
            envelope: CommandEnvelope | None = None
            execution: dict[str, Any] | None = None
            try:
                envelope, payload_paths = self.download_envelope(entry, working)
                claim = {
                    **self._base_metadata(command_id),
                    "state": "claimed",
                    "claimed_utc": iso_utc(),
                    "envelope_sha256": sha256_file(working / entry.name),
                    "retry_of": envelope.retry_of,
                }
                self.drive.write_json(paths["claim"], claim, overwrite=False)
                claimed = True
                self.write_status(
                    command_id,
                    "claimed",
                    claimed_utc=claim["claimed_utc"],
                    total_steps=len(envelope.steps),
                )
                execution = self.execute_command(envelope, payload_paths, working)
            except CommandConflict as error:
                self.publish_rejected(command_id, error, claimed=claimed)
                return True
            except Exception as error:
                if not claimed:
                    self.publish_rejected(command_id, error, claimed=False)
                    return True
                log(
                    f"Claimed command {command_id} failed in worker infrastructure: "
                    f"{error}",
                    level="ERROR",
                )
                raise

            result: CommandResult = execution["result"]
            errors = list(execution["artifact_errors"])
            if execution["conflict_error"]:
                errors.insert(0, execution["conflict_error"])
            log_entry: RemoteEntry | None = None
            artifacts: list[dict[str, Any]] = []
            codebase: dict[str, Any] | None = None
            try:
                log_entry = self.drive.upload(execution["log_path"], paths["log"])
                artifacts = self.upload_artifacts(
                    command_id, execution["staged_artifacts"]
                )
                if execution["commit_created"] and execution["pushed"]:
                    codebase = self.ensure_codebase_snapshot(
                        execution["resulting_commit"]
                    )
            except Exception as error:  # noqa: BLE001 - publication boundary
                errors.append(f"Publication error: {type(error).__name__}: {error}")

            status = result.status
            if execution["conflict_error"]:
                status = "rejected"
            if status == "success" and errors:
                status = "partial_failure"
            latest_codebase = (
                self.drive.read_json("Output/files/codebase/latest.json")
                if execution["conflict_error"]
                else None
            )
            metadata = {
                **self._base_metadata(command_id),
                "status": status,
                "exit_code": result.exit_code,
                "timed_out": result.timed_out,
                "interrupted": result.interrupted,
                "deadline_interrupted": result.deadline_interrupted,
                "started_utc": execution["started_utc"],
                "finished_utc": iso_utc(),
                "duration_seconds": execution["duration_seconds"],
                "current_step": (
                    execution["step_records"][-1]["index"]
                    if execution["step_records"]
                    else None
                ),
                "command_pid": result.pid,
                "process_tree_strategy": result.process_tree_strategy,
                "source_commit": execution["source_commit"],
                "resulting_commit": execution["resulting_commit"],
                "workspace_changed": bool(execution["changed_files"]),
                "changed_files": execution["changed_files"],
                "commit_created": execution["commit_created"],
                "pushed": execution["pushed"],
                "codebase_changed": bool(
                    execution["commit_created"] and execution["pushed"]
                ),
                "codebase": codebase,
                "artifacts": artifacts,
                "required_artifacts_uploaded": not execution["artifact_errors"],
                "steps": execution["step_records"],
                "log": (
                    None
                    if log_entry is None
                    else {
                        "drive_path": paths["log"],
                        "drive_file_id": log_entry.file_id,
                        "size_bytes": execution["log_path"].stat().st_size,
                        "sha256": sha256_file(execution["log_path"]),
                    }
                ),
                "stdout_tail": read_text_tail(result.stdout_path),
                "stderr_tail": read_text_tail(result.stderr_path),
                "retry_required": status
                in {
                    "interrupted_by_runner_deadline",
                    "cancelled",
                    "partial_failure",
                }
                or bool(execution["conflict_error"]),
                "conflict": bool(execution["conflict_error"]),
                "latest_codebase": latest_codebase,
                "errors": errors,
            }
            self.drive.write_json(paths["result"], metadata, overwrite=False)
            self.write_status(
                command_id,
                "completed",
                status=status,
                exit_code=result.exit_code,
                finished_utc=metadata["finished_utc"],
                resulting_commit=execution["resulting_commit"],
                codebase_changed=metadata["codebase_changed"],
            )
            shutil.rmtree(result.capture_directory, ignore_errors=True)
            self.processed_count += 1
            return True

    def run(self) -> int:
        self.validate()
        with FileLock(self.lock_path):
            old_handlers: dict[int, Any] = {}
            for signum in (signal.SIGINT, signal.SIGTERM):
                old_handlers[signum] = signal.getsignal(signum)
                signal.signal(signum, self.request_stop)
            try:
                self.deadline = (
                    time.monotonic()
                    + self.max_runtime_seconds
                    - self.shutdown_reserve_seconds
                )
                self.initialize()
                self.state.write(
                    "idle",
                    command_deadline_utc=iso_utc(
                        utc_now()
                        + dt.timedelta(
                            seconds=self.max_runtime_seconds
                            - self.shutdown_reserve_seconds
                        )
                    ),
                )
                log(
                    f"Monitoring Drive commands for "
                    f"{self.repository_full_name}@{self.branch}"
                )
                consecutive_failures = 0
                while not self.stop_event.is_set():
                    if time.monotonic() >= self.deadline:
                        log("Hosted-runner rollover reserve reached")
                        break
                    try:
                        self.sync()
                        if self.committed_stop():
                            log("Committed stop file detected")
                            break
                        self.ensure_codebase_snapshot(
                            self.git.text("rev-parse", "HEAD")
                        )
                        processed = False
                        for entry in self.discover_commands():
                            if self.drive.exists(
                                self._command_paths(entry.name[:-5])["result"]
                            ):
                                continue
                            processed = self.process_one(entry)
                            break
                        consecutive_failures = 0
                    except Exception as error:  # noqa: BLE001 - service retry boundary
                        consecutive_failures += 1
                        delay = min(
                            self.poll_seconds * (2 ** min(consecutive_failures - 1, 5)),
                            60,
                        )
                        log(
                            f"Worker cycle failed ({consecutive_failures}): "
                            f"{type(error).__name__}: {error}; retrying in {delay:.1f}s",
                            level="WARNING",
                        )
                        self.state.write(
                            "retrying",
                            consecutive_failures=consecutive_failures,
                            error=f"{type(error).__name__}: {error}",
                        )
                        self.stop_event.wait(delay)
                        continue
                    if processed:
                        continue
                    if self.once:
                        log("One-shot Drive queue drained")
                        break
                    self.state.write("idle")
                    self.stop_event.wait(self.poll_seconds)
                self.state.write(
                    "stopped",
                    processed_count=self.processed_count,
                    stop_requested=self.stop_event.is_set(),
                )
                return 0
            except Exception as error:
                self.state.write(
                    "failed",
                    processed_count=self.processed_count,
                    error=f"{type(error).__name__}: {error}",
                )
                raise
            finally:
                self.process_runner.terminate_current(EXIT_CANCELLED)
                for signum, handler in old_handlers.items():
                    signal.signal(signum, handler)


class LegacyGitQueueWorker:
    """Deprecated Git-file queue retained only for migration reference."""

    def __init__(
        self,
        *,
        repository: Path,
        branch: str,
        input_directory: str,
        output_directory: str,
        poll_seconds: float,
        max_runtime_minutes: float,
        maximum_command_minutes: float,
        extensions: Iterable[str],
        once: bool = False,
        shutdown_reserve_seconds: float = SHUTDOWN_RESERVE_SECONDS,
    ):
        self.repository = repository.resolve()
        self.git = Git(self.repository)
        self.branch = branch
        self.input_directory_name = input_directory.replace("\\", "/").strip("/")
        self.output_directory_name = output_directory.replace("\\", "/").strip("/")
        self.input_directory = resolve_inside(
            self.repository, self.input_directory_name
        )
        self.output_directory = resolve_inside(
            self.repository, self.output_directory_name
        )
        validate_directory_pair(
            self.repository, self.input_directory, self.output_directory
        )
        self.poll_seconds = poll_seconds
        self.max_runtime_seconds = max_runtime_minutes * 60
        self.maximum_command_seconds = maximum_command_minutes * 60
        self.extensions = tuple(
            sorted(
                {
                    extension.casefold()
                    if extension.startswith(".")
                    else f".{extension.casefold()}"
                    for extension in extensions
                }
            )
        )
        self.once = once
        self.shutdown_reserve_seconds = min(
            shutdown_reserve_seconds, max(0.0, self.max_runtime_seconds / 4)
        )
        self.worker_id = uuid.uuid4().hex
        runtime_root = Path(
            os.environ.get("RUNNER_TEMP") or tempfile.gettempdir()
        ).resolve()
        checkout_key = hashlib.sha256(
            f"{self.repository}|{self.branch}".encode(UTF8)
        ).hexdigest()[:20]
        self.lock_path = runtime_root / f"continuous-worker-{checkout_key}.lock"
        self.state = StateStore(
            runtime_root / f"continuous-worker-{checkout_key}.json",
            self.worker_id,
            self.branch,
        )
        self.stop_event = threading.Event()
        self.process_runner = ProcessRunner(self.stop_event, self.state)
        self.origin_url = ""
        self.deadline = 0.0
        self.processed_count = 0

    def request_stop(self, signum: int, _frame: object) -> None:
        log(f"Received signal {signum}; requesting graceful shutdown", level="WARNING")
        # Keep the signal handler lock-free. The process wait loop observes this
        # event within 200 ms and performs process-tree termination safely.
        self.stop_event.set()

    def validate(self) -> None:
        if not self.extensions:
            raise WorkerError("At least one input extension is required")
        if self.poll_seconds <= 0:
            raise WorkerError("Poll seconds must be positive")
        if self.max_runtime_seconds <= 0 or self.maximum_command_seconds <= 0:
            raise WorkerError("Runtime and command timeouts must be positive")
        if self.git.run(
            "check-ref-format", "--branch", self.branch, check=False
        ).returncode:
            raise WorkerError(f"Invalid branch name: {self.branch}")
        top_level = Path(self.git.text("rev-parse", "--show-toplevel")).resolve()
        if top_level != self.repository:
            raise WorkerError(
                f"Repository root mismatch: expected {self.repository}, found {top_level}"
            )

    def ensure_origin(self) -> None:
        current = self.git.run("remote", "get-url", "origin", check=False)
        if current.returncode != 0:
            self.git.run("remote", "add", "origin", self.origin_url)
        elif current.stdout.strip() != self.origin_url:
            self.git.run("remote", "set-url", "origin", self.origin_url)

    @property
    def remote_ref(self) -> str:
        return f"refs/remotes/origin/{self.branch}"

    @property
    def remote_head(self) -> str:
        return f"refs/heads/{self.branch}"

    @property
    def fetch_refspec(self) -> str:
        return f"+{self.remote_head}:{self.remote_ref}"

    def fetch_branch(self, restore_commit: str | None = None) -> None:
        self.ensure_origin()
        fetched = self.git.run(
            "fetch", "--no-tags", "origin", self.fetch_refspec, check=False
        )
        if fetched.returncode == 0:
            return
        lookup = self.git.run(
            "ls-remote",
            "--exit-code",
            "--heads",
            "origin",
            self.remote_head,
            check=False,
        )
        if lookup.returncode != 2:
            raise GitError(
                ("fetch", "--no-tags", "origin", self.fetch_refspec), fetched
            )
        source = restore_commit or self.git.text("rev-parse", "--verify", "HEAD")
        log(
            f"Remote branch {self.branch} disappeared; restoring it from {source}",
            level="WARNING",
        )
        pushed = self.git.run(
            "push", "origin", f"{source}:{self.remote_head}", check=False
        )
        if pushed.returncode != 0:
            raced = self.git.run(
                "ls-remote",
                "--exit-code",
                "--heads",
                "origin",
                self.remote_head,
                check=False,
            )
            if raced.returncode != 0:
                raise GitError(
                    ("push", "origin", f"{source}:{self.remote_head}"), pushed
                )
        self.git.run("fetch", "--no-tags", "origin", self.fetch_refspec)

    def set_checkout(self, commit: str) -> None:
        self.git.abort_operations()
        self.git.run("checkout", "--force", "-B", self.branch, commit)
        self.git.run("reset", "--hard", commit)
        current = self.git.text("branch", "--show-current")
        if current != self.branch:
            raise WorkerError(f"Expected branch {self.branch}, found {current}")

    def initialize(self) -> None:
        self.origin_url = self.git.text("remote", "get-url", "origin")
        self.git.run("config", "user.name", "github-actions[bot]")
        self.git.run(
            "config",
            "user.email",
            "41898282+github-actions[bot]@users.noreply.github.com",
        )
        previous = self.state.read()
        if previous and previous.get("status") == "running_command":
            log(
                "Recovered stale command state "
                f"(worker PID {previous.get('worker_pid')}, "
                f"command PID {previous.get('command_pid')}); the content hash "
                "will be retried unless a committed result exists",
                level="WARNING",
            )
        self.fetch_branch()
        self.set_checkout(self.remote_ref)
        self.input_directory.mkdir(parents=True, exist_ok=True)
        self.output_directory.mkdir(parents=True, exist_ok=True)

    def sync(self) -> None:
        current = self.git.text("branch", "--show-current")
        if current != self.branch:
            self.set_checkout("HEAD")
        local_commit = self.git.text("rev-parse", "HEAD")
        self.fetch_branch(local_commit)
        remote_commit = self.git.text("rev-parse", self.remote_ref)
        if local_commit != remote_commit:
            log(f"Branch changed: {local_commit} -> {remote_commit}")
            self.set_checkout(remote_commit)

    def committed_stop(self) -> bool:
        return self.git.exists_at_head(".continuous-worker.stop")

    def discover_commands(self) -> list[Path]:
        self.input_directory.mkdir(parents=True, exist_ok=True)
        commands = [
            path
            for path in self.input_directory.rglob("*")
            if path.is_file()
            and not path.is_symlink()
            and path.suffix.casefold() in self.extensions
        ]
        return sorted(
            commands, key=lambda path: path.relative_to(self.repository).as_posix()
        )

    def result_committed(self, command_hash: str) -> bool:
        return self.git.exists_at_head(
            f"{self.output_directory_name}/{command_hash}.json"
        )

    def execute_isolated(
        self,
        command_path: str,
        source_commit: str,
        command_hash: str,
        timeout_seconds: float,
    ) -> tuple[CommandResult, OutputDelta]:
        with TemporaryWorktree(self.git, source_commit) as worktree:
            command_file = resolve_inside(worktree, command_path)
            command_output = resolve_inside(worktree, self.output_directory_name)
            command_output.mkdir(parents=True, exist_ok=True)
            before = directory_state(command_output)
            environment = os.environ.copy()
            environment.update(
                {
                    "WORKER_BRANCH": self.branch,
                    "WORKER_OUTPUT_DIRECTORY": str(command_output),
                    "WORKER_ID": self.worker_id,
                    "CARGO_TARGET_DIR": str(self.repository / "target"),
                }
            )
            result = self.process_runner.run(
                command_file,
                worktree,
                environment,
                timeout_seconds,
                {
                    "command_path": command_path,
                    "command_sha256": command_hash,
                    "source_commit": source_commit,
                },
            )
            after = directory_state(command_output)
            delta = OutputDelta.create(before, after, command_output)
            return result, delta

    def metadata(
        self,
        *,
        command_path: str,
        command_hash: str,
        source_commit: str,
        result: CommandResult,
        log_file: str,
    ) -> dict[str, Any]:
        return {
            "schema_version": 2,
            "command_file": Path(command_path).name,
            "command_path": command_path,
            "command_sha256": command_hash,
            "source_commit": source_commit,
            "status": result.status,
            "exit_code": result.exit_code,
            "timed_out": result.timed_out,
            "started_utc": result.started_utc,
            "finished_utc": result.finished_utc,
            "duration_seconds": result.duration_seconds,
            "log_file": log_file,
            "branch": self.branch,
            "repository": os.environ.get("GITHUB_REPOSITORY", ""),
            "workflow_run_id": os.environ.get("GITHUB_RUN_ID", ""),
            "workflow_run_attempt": os.environ.get("GITHUB_RUN_ATTEMPT", ""),
            "runner_name": os.environ.get("RUNNER_NAME", ""),
            "worker_id": self.worker_id,
            "worker_pid": os.getpid(),
            "command_pid": result.pid,
            "process_tree_strategy": result.process_tree_strategy,
        }

    def publish(
        self,
        *,
        command_path: str,
        command_hash: str,
        source_commit: str,
        result: CommandResult,
        delta: OutputDelta,
    ) -> None:
        result_name = f"{command_hash}.json"
        log_name = f"{command_hash}.log"
        metadata = self.metadata(
            command_path=command_path,
            command_hash=command_hash,
            source_commit=source_commit,
            result=result,
            log_file=log_name,
        )
        staged_log = Path(tempfile.mkdtemp(prefix="continuous-worker-log-")) / log_name
        write_combined_log(staged_log, result)
        try:
            for attempt in range(1, 11):
                self.fetch_branch(source_commit)
                remote_commit = self.git.text("rev-parse", self.remote_ref)
                self.set_checkout(remote_commit)
                if self.result_committed(command_hash):
                    log(
                        f"Result already committed for {command_path}; not publishing twice"
                    )
                    return

                delta.apply(self.output_directory)
                shutil.copy2(staged_log, self.output_directory / log_name)
                atomic_write_json(self.output_directory / result_name, metadata)
                self.git.run(
                    "add",
                    "-A",
                    "-f",
                    "--",
                    self.output_directory_name,
                )
                changed = self.git.run(
                    "diff", "--cached", "--quiet", "--exit-code", check=False
                )
                if changed.returncode == 0:
                    raise WorkerError(
                        f"No output changes to publish for {command_path}"
                    )
                if changed.returncode != 1:
                    raise GitError(
                        ("diff", "--cached", "--quiet", "--exit-code"), changed
                    )
                self.git.run(
                    "commit",
                    "-m",
                    f"automation: save result for {command_path} [{command_hash[:16]}]",
                )
                pushed = self.git.run(
                    "push", "origin", f"HEAD:{self.remote_head}", check=False
                )
                if pushed.returncode == 0:
                    log(f"Output pushed for {command_path}")
                    return
                log(
                    f"Push attempt {attempt}/10 failed for {command_path}; "
                    "rebasing the staged result on the latest branch",
                    level="WARNING",
                )
                if attempt == 10:
                    raise GitError(
                        ("push", "origin", f"HEAD:{self.remote_head}"), pushed
                    )
                time.sleep(min(2 * attempt, 15))
        finally:
            shutil.rmtree(staged_log.parent, ignore_errors=True)

    def process_one(self, command_file: Path, remaining_seconds: float) -> bool:
        command_path = command_file.relative_to(self.repository).as_posix()
        command_hash = sha256_file(command_file)
        if self.result_committed(command_hash):
            return False
        source_commit = self.git.text("rev-parse", "HEAD")
        timeout_seconds = min(self.maximum_command_seconds, remaining_seconds)
        log(
            f"Executing {command_path} from {source_commit}; "
            f"SHA-256 {command_hash}; timeout {timeout_seconds:.1f}s"
        )
        result: CommandResult | None = None
        delta: OutputDelta | None = None
        try:
            result, delta = self.execute_isolated(
                command_path, source_commit, command_hash, timeout_seconds
            )
            self.state.write(
                "publishing",
                command_path=command_path,
                command_sha256=command_hash,
                command_pid=result.pid,
                command_status=result.status,
            )
            self.publish(
                command_path=command_path,
                command_hash=command_hash,
                source_commit=source_commit,
                result=result,
                delta=delta,
            )
            self.processed_count += 1
            return True
        finally:
            if delta is not None:
                delta.cleanup()
            if result is not None:
                shutil.rmtree(result.capture_directory, ignore_errors=True)

    def run(self) -> int:
        self.validate()
        with FileLock(self.lock_path):
            old_handlers: dict[int, Any] = {}
            for signum in (signal.SIGINT, signal.SIGTERM):
                old_handlers[signum] = signal.getsignal(signum)
                signal.signal(signum, self.request_stop)
            try:
                self.initialize()
                self.deadline = time.monotonic() + self.max_runtime_seconds
                self.state.write(
                    "idle",
                    deadline_utc=iso_utc(
                        utc_now() + dt.timedelta(seconds=self.max_runtime_seconds)
                    ),
                )
                log(f"Monitoring branch {self.branch}")
                log(f"Input extensions: {', '.join(self.extensions)}")
                consecutive_sync_failures = 0

                while not self.stop_event.is_set():
                    remaining = (
                        self.deadline - time.monotonic() - self.shutdown_reserve_seconds
                    )
                    if remaining < 1:
                        log("Rollover deadline reached")
                        break
                    try:
                        self.sync()
                        consecutive_sync_failures = 0
                    except Exception as error:  # noqa: BLE001 - retry infrastructure
                        consecutive_sync_failures += 1
                        delay = min(
                            self.poll_seconds
                            * (2 ** min(consecutive_sync_failures - 1, 5)),
                            60,
                        )
                        log(
                            f"Repository synchronization failed "
                            f"({consecutive_sync_failures}): {error}; retrying in {delay:.1f}s",
                            level="WARNING",
                        )
                        self.stop_event.wait(delay)
                        continue

                    if self.committed_stop():
                        log("Committed stop file detected")
                        break

                    processed = False
                    for command_file in self.discover_commands():
                        if self.result_committed(sha256_file(command_file)):
                            continue
                        processed = self.process_one(command_file, remaining)
                        break

                    if processed:
                        continue
                    if self.once:
                        log("One-shot queue drained")
                        break
                    self.state.write("idle")
                    self.stop_event.wait(self.poll_seconds)

                self.state.write(
                    "stopped",
                    processed_count=self.processed_count,
                    stop_requested=self.stop_event.is_set(),
                )
                return 0
            except Exception as error:
                self.state.write(
                    "failed",
                    processed_count=self.processed_count,
                    error=f"{type(error).__name__}: {error}",
                )
                raise
            finally:
                self.process_runner.terminate_current(EXIT_CANCELLED)
                for signum, handler in old_handlers.items():
                    signal.signal(signum, handler)


class GitHubClient:
    def __init__(self, repository: str, token: str, api_url: str | None = None):
        if repository.count("/") != 1:
            raise WorkerError(f"Invalid repository name: {repository}")
        if not token:
            raise WorkerError("GH_TOKEN or GITHUB_TOKEN is required")
        self.repository = repository
        self.token = token
        self.api_url = (api_url or "https://api.github.com").rstrip("/")

    def request(
        self,
        method: str,
        path: str,
        *,
        body: dict[str, Any] | None = None,
        expected: tuple[int, ...] = (200,),
        attempts: int = 6,
    ) -> tuple[int, Any]:
        encoded = None if body is None else json.dumps(body).encode(UTF8)
        url = f"{self.api_url}/{path.lstrip('/')}"
        last_error: Exception | None = None
        for attempt in range(1, attempts + 1):
            request = urllib.request.Request(
                url,
                data=encoded,
                method=method,
                headers={
                    "Accept": "application/vnd.github+json",
                    "Authorization": f"Bearer {self.token}",
                    "X-GitHub-Api-Version": "2022-11-28",
                    "User-Agent": "github-drive-continuous-worker",
                    "Content-Type": "application/json",
                },
            )
            try:
                with urllib.request.urlopen(request, timeout=30) as response:
                    raw = response.read()
                    value = json.loads(raw) if raw else None
                    if response.status not in expected:
                        raise WorkerError(
                            f"GitHub API {method} {path} returned {response.status}"
                        )
                    return response.status, value
            except urllib.error.HTTPError as error:
                raw = error.read().decode(UTF8, errors="replace")
                if error.code in expected:
                    value = json.loads(raw) if raw else None
                    return error.code, value
                last_error = WorkerError(
                    f"GitHub API {method} {path} returned {error.code}: {raw[:1000]}"
                )
                if error.code not in {408, 429, 500, 502, 503, 504}:
                    break
            except (urllib.error.URLError, TimeoutError, OSError) as error:
                last_error = error
            if attempt < attempts:
                delay = min(2 ** (attempt - 1), 30)
                log(
                    f"GitHub API attempt {attempt}/{attempts} failed: "
                    f"{last_error}; retrying in {delay}s",
                    level="WARNING",
                )
                time.sleep(delay)
        raise WorkerError(f"GitHub API request failed: {last_error}")

    def ref_sha(self, branch: str) -> str | None:
        encoded_branch = urllib.parse.quote(branch, safe="")
        status, value = self.request(
            "GET",
            f"repos/{self.repository}/git/ref/heads/{encoded_branch}",
            expected=(200, 404),
        )
        if status == 404:
            return None
        return str(value["object"]["sha"])

    def ensure_branch(self, branch: str, source_sha: str) -> str:
        existing = self.ref_sha(branch)
        if existing:
            return existing
        log(f"Creating missing branch {branch} from {source_sha}", level="WARNING")
        status, _ = self.request(
            "POST",
            f"repos/{self.repository}/git/refs",
            body={"ref": f"refs/heads/{branch}", "sha": source_sha},
            expected=(201, 422),
        )
        if status == 422:
            existing = self.ref_sha(branch)
            if existing:
                return existing
            raise WorkerError(f"Could not create branch {branch}")
        return source_sha

    def stop_file_exists(self, branch: str) -> bool:
        encoded_branch = urllib.parse.quote(branch, safe="")
        status, _ = self.request(
            "GET",
            f"repos/{self.repository}/contents/.continuous-worker.stop"
            f"?ref={encoded_branch}",
            expected=(200, 404),
        )
        return status == 200

    def dispatch(self, workflow_file: str, control_branch: str, branch: str) -> None:
        encoded_workflow = urllib.parse.quote(workflow_file, safe="")
        self.request(
            "POST",
            f"repos/{self.repository}/actions/workflows/{encoded_workflow}/dispatches",
            body={"ref": control_branch, "inputs": {"branch": branch}},
            expected=(204,),
        )


def token_from_environment() -> str:
    return os.environ.get("GH_TOKEN") or os.environ.get("GITHUB_TOKEN") or ""


def ensure_branch_command(args: argparse.Namespace) -> int:
    git = Git(Path(args.checkout))
    if git.run("check-ref-format", "--branch", args.branch, check=False).returncode:
        raise WorkerError(f"Invalid branch name: {args.branch}")
    client = GitHubClient(
        args.repository_full_name,
        token_from_environment(),
        os.environ.get("GITHUB_API_URL"),
    )
    source_sha = client.ref_sha(args.control_branch)
    if not source_sha:
        raise WorkerError(f"Control branch does not exist: {args.control_branch}")
    resolved = client.ensure_branch(args.branch, source_sha)
    log(f"Monitored branch {args.branch} is available at {resolved}")
    return 0


def dispatch_replacement_command(args: argparse.Namespace) -> int:
    checkout = Path(args.checkout)
    validation_root = checkout if checkout.exists() else Path.cwd()
    validation_git = Git(validation_root)
    if validation_git.run(
        "check-ref-format", "--branch", args.branch, check=False
    ).returncode:
        raise WorkerError(f"Invalid branch name: {args.branch}")
    client = GitHubClient(
        args.repository_full_name,
        token_from_environment(),
        os.environ.get("GITHUB_API_URL"),
    )
    source_sha: str | None = None
    if (checkout / ".git").exists():
        git = Git(checkout)
        result = git.run("rev-parse", "--verify", "HEAD", check=False)
        if result.returncode == 0:
            source_sha = result.stdout.strip()
    source_sha = source_sha or client.ref_sha(args.control_branch)
    if not source_sha:
        raise WorkerError("Could not resolve a source commit for branch recovery")
    client.ensure_branch(args.branch, source_sha)
    if client.stop_file_exists(args.branch):
        log(f"Stop file found on {args.branch}; no replacement will be dispatched")
        return 0
    client.dispatch(args.workflow_file, args.control_branch, args.branch)
    log(f"Replacement dispatched for {args.branch}")
    return 0


def create_drive_transport(
    args: argparse.Namespace,
) -> tuple[DriveTransport, Path | None]:
    owner, repository = args.repository_full_name.split("/", 1)
    workspace_root = f"{owner}/{repository}/{sanitize_branch_folder(args.branch)}"
    if args.local_drive_root:
        root = Path(args.local_drive_root).resolve() / "Github" / workspace_root
        return LocalDirectoryDrive(root), None
    runtime_root = Path(
        os.environ.get("RUNNER_TEMP") or tempfile.gettempdir()
    ).resolve()
    runtime_root.mkdir(parents=True, exist_ok=True)
    executable = prepare_rclone(runtime_root, args.rclone_executable)
    if args.rclone_config_file:
        secret = Path(args.rclone_config_file).read_text(encoding=UTF8)
    else:
        secret = os.environ.get("RCLONE_CONFIG", "")
    config_path = write_rclone_config(runtime_root, secret)
    remote = args.rclone_remote or os.environ.get("RCLONE_REMOTE") or "gdrive"
    return RcloneDrive(executable, config_path, remote, workspace_root), config_path


def drive_run_command(args: argparse.Namespace) -> int:
    drive, config_path = create_drive_transport(args)
    try:
        worker = ContinuousWorker(
            repository=Path(args.repository),
            repository_full_name=args.repository_full_name,
            branch=args.branch,
            drive=drive,
            token=token_from_environment(),
            poll_seconds=args.poll_seconds,
            max_runtime_minutes=args.max_runtime_minutes,
            once=args.once,
            shutdown_reserve_seconds=args.shutdown_reserve_minutes * 60,
            heartbeat_seconds=args.heartbeat_seconds,
        )
        return worker.run()
    finally:
        if config_path is not None:
            config_path.unlink(missing_ok=True)


def service_command(args: argparse.Namespace) -> int:
    return_code = 1
    worker_error: BaseException | None = None
    try:
        return_code = drive_run_command(args)
    except BaseException as error:  # noqa: BLE001 - replacement must still dispatch
        worker_error = error
    try:
        dispatch_replacement_command(args)
    except Exception:
        if worker_error is None:
            raise
        log("Replacement dispatch also failed", level="ERROR")
    if worker_error is not None:
        raise worker_error
    return return_code


def add_control_arguments(parser: argparse.ArgumentParser) -> None:
    parser.add_argument("--branch", required=True)
    parser.add_argument("--control-branch", required=True)
    parser.add_argument("--repository-full-name", required=True)
    parser.add_argument("--checkout", default=".")


def add_drive_arguments(parser: argparse.ArgumentParser) -> None:
    parser.add_argument("--repository", default=".")
    parser.add_argument("--branch", required=True)
    parser.add_argument("--repository-full-name", required=True)
    parser.add_argument("--poll-seconds", type=float, default=10)
    parser.add_argument("--heartbeat-seconds", type=float, default=60)
    parser.add_argument("--max-runtime-minutes", type=float, default=360)
    parser.add_argument("--shutdown-reserve-minutes", type=float, default=10)
    parser.add_argument("--rclone-remote")
    parser.add_argument("--rclone-executable")
    parser.add_argument("--rclone-config-file")
    parser.add_argument("--local-drive-root", help=argparse.SUPPRESS)
    parser.add_argument(
        "--once",
        action="store_true",
        help="drain the currently visible queue and exit",
    )


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    subparsers = parser.add_subparsers(dest="command", required=True)

    run_parser = subparsers.add_parser(
        "run-drive", help="run the Google Drive command worker"
    )
    add_drive_arguments(run_parser)

    service_parser = subparsers.add_parser(
        "service",
        help="run the Drive worker and dispatch its replacement before exit",
    )
    add_drive_arguments(service_parser)
    service_parser.add_argument("--control-branch", required=True)
    service_parser.add_argument("--workflow-file", default="continuous-worker.yml")
    service_parser.add_argument("--checkout", default=".")

    ensure_parser = subparsers.add_parser(
        "ensure-branch", help="create a missing monitored branch"
    )
    add_control_arguments(ensure_parser)

    dispatch_parser = subparsers.add_parser(
        "dispatch-replacement", help="restore and dispatch the next worker run"
    )
    add_control_arguments(dispatch_parser)
    dispatch_parser.add_argument("--workflow-file", default="continuous-worker.yml")
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    try:
        if args.command == "run-drive":
            return drive_run_command(args)
        if args.command == "service":
            return service_command(args)
        if args.command == "ensure-branch":
            return ensure_branch_command(args)
        if args.command == "dispatch-replacement":
            return dispatch_replacement_command(args)
        raise WorkerError(f"Unknown command: {args.command}")
    except KeyboardInterrupt:
        return EXIT_CANCELLED
    except Exception as error:  # noqa: BLE001 - CLI error boundary
        log(f"{type(error).__name__}: {error}", level="ERROR")
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
