"""HTTP gateway and scheduler for persistent Steam statistics workers.

The public API accepts target SteamID64 values only. Worker credentials stay in
the local configuration consumed by stats.js or stats.exe and are never sent by
the client.
"""

from __future__ import annotations

import argparse
import calendar
import collections
import concurrent.futures
import copy
import hmac
import json
import logging
import os
import queue
import signal
import subprocess
import sys
import threading
import time
import uuid
from dataclasses import dataclass, field
from http import HTTPStatus
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import Any
from urllib.parse import urlparse


LOGGER = logging.getLogger("steam_stats_server")
MAX_STEAM_ID_LENGTH = 17
MIN_STEAM_ID_LENGTH = 17
MAX_REMOTE_WORKER_ID_LENGTH = 80
MAX_REMOTE_ACCOUNT_NAME_LENGTH = 128


DEFAULT_CONFIG: dict[str, Any] = {
    "workerMode": "local",
    "server": {
        "host": "0.0.0.0",
        "port": 19222,
        "apiKeyEnv": "SWI_STATS_API_KEY",
        "requireApiKey": True,
        "maxRequestBodyBytes": 1_048_576,
        "maxBatchSize": 500,
        "maxSynchronousWaitSeconds": 25,
    },
    "statsWorker": {
        "command": [
            "node",
            "stats.js",
            "--stdio",
            "--config",
            "stats-worker-config.json",
            "--account",
            "{account}",
        ],
        "accounts": [],
        "requestTimeoutSeconds": 40,
        "restartDelaySeconds": 3,
    },
    "remoteWorkers": {
        "workerApiKeyEnv": "SWI_STATS_WORKER_API_KEY",
        "maxWorkers": 32,
        "jobLeaseSeconds": 75,
    },
    "provisioning": {
        "enabled": False,
        "provisionKeyEnv": "SWI_STATS_PROVISION_KEY",
        "tokenCacheFile": "./private/data/client-refresh-tokens.json",
    },
    "pool": {
        "maxQueueSize": 10_000,
        "maxAttemptsPerJob": 3,
        "retryDelaySeconds": 1,
        "resultCacheTtlSeconds": 300,
        "maxCacheEntries": 20_000,
        "jobTtlSeconds": 600,
    },
}


class ConfigurationError(ValueError):
    """Raised when deployment configuration is incomplete or invalid."""


class QueueFullError(RuntimeError):
    """Raised when the public request queue reaches its configured limit."""


class WorkerProtocolError(RuntimeError):
    """Raised when a stats worker emits invalid JSON Lines protocol data."""


class RemoteWorkerLimitError(RuntimeError):
    """Raised when a remote worker registration exceeds the configured limit."""


class RemoteWorkerLeaseError(RuntimeError):
    """Raised when a remote worker tries to change another worker's job."""


def utc_now() -> str:
    return time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())


def merge_config(base: dict[str, Any], override: dict[str, Any]) -> dict[str, Any]:
    result = copy.deepcopy(base)

    for key, value in override.items():
        existing = result.get(key)
        if isinstance(existing, dict) and isinstance(value, dict):
            result[key] = merge_config(existing, value)
        else:
            result[key] = value

    return result


def as_positive_integer(value: Any, field_name: str, *, allow_zero: bool = False) -> int:
    try:
        number = int(value)
    except (TypeError, ValueError) as error:
        raise ConfigurationError(f"{field_name} must be an integer.") from error

    if number < 0 or (number == 0 and not allow_zero):
        operator = "non-negative" if allow_zero else "positive"
        raise ConfigurationError(f"{field_name} must be a {operator} integer.")

    return number


def as_positive_number(value: Any, field_name: str, *, allow_zero: bool = False) -> float:
    try:
        number = float(value)
    except (TypeError, ValueError) as error:
        raise ConfigurationError(f"{field_name} must be a number.") from error

    if number < 0 or (number == 0 and not allow_zero):
        operator = "non-negative" if allow_zero else "positive"
        raise ConfigurationError(f"{field_name} must be a {operator} number.")

    return number


def normalize_steam_id(value: Any) -> str:
    steam_id = str(value or "").strip()
    if (
        len(steam_id) != MAX_STEAM_ID_LENGTH
        or len(steam_id) < MIN_STEAM_ID_LENGTH
        or not steam_id.isdecimal()
        or not steam_id.startswith("7656")
    ):
        raise ValueError("steamId must be a 17-digit SteamID64.")
    return steam_id


def normalize_remote_worker_id(value: Any) -> str:
    worker_id = str(value or "").strip()
    if (
        not worker_id
        or len(worker_id) > MAX_REMOTE_WORKER_ID_LENGTH
        or any(character not in "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789-_." for character in worker_id)
    ):
        raise ValueError("agentId must contain only letters, numbers, '-', '_' or '.'.")
    return worker_id


def normalize_remote_account_name(value: Any) -> str:
    account_name = str(value or "").strip()
    if not account_name or len(account_name) > MAX_REMOTE_ACCOUNT_NAME_LENGTH:
        raise ValueError("accountName must be between 1 and 128 characters.")
    return account_name


def load_config(config_path: Path) -> dict[str, Any]:
    if not config_path.is_file():
        raise ConfigurationError(
            f"Configuration file not found: {config_path}. "
            "Copy server-config.example.json to server-config.json first."
        )

    try:
        raw_config = json.loads(config_path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as error:
        raise ConfigurationError(f"Could not read {config_path}: {error}") from error

    if not isinstance(raw_config, dict):
        raise ConfigurationError("Configuration root must be a JSON object.")

    config = merge_config(DEFAULT_CONFIG, raw_config)
    config["runtimeDirectory"] = str(config_path.parent.resolve())

    server_config = config["server"]
    worker_config = config["statsWorker"]
    remote_worker_config = config["remoteWorkers"]
    provisioning_config = config["provisioning"]
    pool_config = config["pool"]

    worker_mode = str(config.get("workerMode", "local")).strip().lower()
    if worker_mode not in {"local", "remote"}:
        raise ConfigurationError("workerMode must be either 'local' or 'remote'.")
    config["workerMode"] = worker_mode

    server_config["port"] = as_positive_integer(server_config["port"], "server.port")
    server_config["maxRequestBodyBytes"] = as_positive_integer(
        server_config["maxRequestBodyBytes"], "server.maxRequestBodyBytes"
    )
    server_config["maxBatchSize"] = as_positive_integer(
        server_config["maxBatchSize"], "server.maxBatchSize"
    )
    server_config["maxSynchronousWaitSeconds"] = as_positive_number(
        server_config["maxSynchronousWaitSeconds"], "server.maxSynchronousWaitSeconds", allow_zero=True
    )

    if worker_mode == "local":
        command = worker_config.get("command")
        if not isinstance(command, list) or not command or not all(isinstance(item, str) and item for item in command):
            raise ConfigurationError("statsWorker.command must be a non-empty JSON string array.")
        worker_config["command"] = command
        worker_config["requestTimeoutSeconds"] = as_positive_number(
            worker_config["requestTimeoutSeconds"], "statsWorker.requestTimeoutSeconds"
        )
        worker_config["restartDelaySeconds"] = as_positive_number(
            worker_config["restartDelaySeconds"], "statsWorker.restartDelaySeconds", allow_zero=True
        )

        account_names = worker_config.get("accounts")
        if not isinstance(account_names, list):
            raise ConfigurationError("statsWorker.accounts must be a JSON array.")
        worker_config["accounts"] = list(
            dict.fromkeys(str(account_name).strip() for account_name in account_names if str(account_name).strip())
        )
        if not worker_config["accounts"]:
            raise ConfigurationError("statsWorker.accounts must contain at least one local worker account name.")

        provisioning_config["enabled"] = bool(provisioning_config.get("enabled", False))
        if provisioning_config["enabled"]:
            provision_key_env = str(provisioning_config.get("provisionKeyEnv", "")).strip()
            token_cache_file = str(provisioning_config.get("tokenCacheFile", "")).strip()
            if not provision_key_env:
                raise ConfigurationError("provisioning.provisionKeyEnv is required when provisioning is enabled.")
            if not os.environ.get(provision_key_env):
                raise ConfigurationError(
                    f"Environment variable {provision_key_env} is required when provisioning is enabled."
                )
            if not token_cache_file:
                raise ConfigurationError("provisioning.tokenCacheFile is required when provisioning is enabled.")
            provisioning_config["provisionKeyEnv"] = provision_key_env
            provisioning_config["tokenCacheFile"] = token_cache_file
    else:
        worker_api_key_env = str(remote_worker_config.get("workerApiKeyEnv", "")).strip()
        if not worker_api_key_env:
            raise ConfigurationError("remoteWorkers.workerApiKeyEnv is required for remote workers.")
        if not os.environ.get(worker_api_key_env):
            raise ConfigurationError(
                f"Environment variable {worker_api_key_env} is required for remote workers."
            )
        remote_worker_config["workerApiKeyEnv"] = worker_api_key_env
        remote_worker_config["maxWorkers"] = as_positive_integer(
            remote_worker_config["maxWorkers"], "remoteWorkers.maxWorkers"
        )
        remote_worker_config["jobLeaseSeconds"] = as_positive_number(
            remote_worker_config["jobLeaseSeconds"], "remoteWorkers.jobLeaseSeconds"
        )

    for setting_name in ("maxQueueSize", "maxAttemptsPerJob", "maxCacheEntries", "jobTtlSeconds"):
        pool_config[setting_name] = as_positive_integer(pool_config[setting_name], f"pool.{setting_name}")
    for setting_name in ("retryDelaySeconds", "resultCacheTtlSeconds"):
        pool_config[setting_name] = as_positive_number(pool_config[setting_name], f"pool.{setting_name}", allow_zero=True)

    if bool(server_config.get("requireApiKey", True)):
        api_key_env = str(server_config.get("apiKeyEnv", "")).strip()
        if not api_key_env:
            raise ConfigurationError("server.apiKeyEnv is required when server.requireApiKey is true.")
        if not os.environ.get(api_key_env):
            raise ConfigurationError(f"Environment variable {api_key_env} is required for this public server.")

    return config


@dataclass
class XpJob:
    steam_id: str
    cached: bool = False
    id: str = field(default_factory=lambda: str(uuid.uuid4()))
    state: str = "queued"
    created_at: str = field(default_factory=utc_now)
    started_at: str | None = None
    finished_at: str | None = None
    attempts: int = 0
    result: dict[str, Any] | None = None
    error: str | None = None
    completed: threading.Event = field(default_factory=threading.Event, repr=False)

    def to_public(self, queue_position: int | None = None) -> dict[str, Any]:
        return {
            "id": self.id,
            "steamId": self.steam_id,
            "status": self.state,
            "cached": self.cached,
            "attempts": self.attempts,
            "queuePosition": queue_position,
            "createdAt": self.created_at,
            "startedAt": self.started_at,
            "finishedAt": self.finished_at,
            "result": self.result,
            "error": self.error,
        }


@dataclass
class CachedResult:
    result: dict[str, Any]
    expires_at: float


@dataclass
class RemoteWorker:
    worker_id: str
    account_name: str
    state: str = "starting"
    last_error: str | None = None
    last_seen_at: str = field(default_factory=utc_now)
    last_seen_monotonic: float = field(default_factory=time.monotonic, repr=False)
    active_job_id: str | None = None

    def status(self) -> dict[str, Any]:
        return {
            "agentId": self.worker_id,
            "accountName": self.account_name,
            "state": self.state,
            "lastError": self.last_error,
            "lastSeenAt": self.last_seen_at,
            "activeJobId": self.active_job_id,
        }


@dataclass
class RemoteJobLease:
    worker_id: str
    token: str
    expires_at: float


class StatsWorkerProcess:
    """A serial JSON Lines client for one persistent stats.js/stats.exe process."""

    def __init__(
        self,
        account_name: str,
        command_template: list[str],
        working_directory: Path,
        request_timeout_seconds: float,
        restart_delay_seconds: float,
    ) -> None:
        self.account_name = account_name
        self.command_template = command_template
        self.working_directory = working_directory
        self.request_timeout_seconds = request_timeout_seconds
        self.restart_delay_seconds = restart_delay_seconds
        self._process: subprocess.Popen[str] | None = None
        self._state_lock = threading.RLock()
        self._request_lock = threading.Lock()
        self._pending: dict[str, queue.Queue[dict[str, Any]]] = {}
        self._last_start_at = 0.0
        self._state = "stopped"
        self._last_error: str | None = None

    @property
    def state(self) -> str:
        with self._state_lock:
            return self._state

    @property
    def last_error(self) -> str | None:
        with self._state_lock:
            return self._last_error

    def status(self) -> dict[str, Any]:
        return {
            "accountName": self.account_name,
            "state": self.state,
            "lastError": self.last_error,
        }

    def start(self) -> None:
        with self._state_lock:
            if self._process is not None and self._process.poll() is None:
                return

            if self._process is not None:
                self._fail_pending("Stats worker exited before sending a response.")

            now = time.monotonic()
            remaining_delay = self.restart_delay_seconds - (now - self._last_start_at)
            if remaining_delay > 0:
                time.sleep(remaining_delay)

            command = [part.replace("{account}", self.account_name) for part in self.command_template]
            try:
                self._process = subprocess.Popen(
                    command,
                    cwd=str(self.working_directory),
                    stdin=subprocess.PIPE,
                    stdout=subprocess.PIPE,
                    stderr=subprocess.PIPE,
                    text=True,
                    encoding="utf-8",
                    errors="replace",
                    bufsize=1,
                    shell=False,
                )
            except OSError as error:
                self._process = None
                self._state = "failed"
                self._last_error = f"Could not start stats worker: {error}"
                raise RuntimeError(self._last_error) from error

            self._last_start_at = time.monotonic()
            self._state = "starting"
            self._last_error = None
            process = self._process
            threading.Thread(target=self._read_stdout, args=(process,), daemon=True).start()
            threading.Thread(target=self._read_stderr, args=(process,), daemon=True).start()
            LOGGER.info("Started stats worker for %s (pid %s).", self.account_name, process.pid)

    def request(self, steam_id: str) -> dict[str, Any]:
        with self._request_lock:
            self.start()

            with self._state_lock:
                process = self._process
                if process is None or process.stdin is None:
                    raise RuntimeError(f"Stats worker {self.account_name} is not available.")

                request_id = str(uuid.uuid4())
                response_queue: queue.Queue[dict[str, Any]] = queue.Queue(maxsize=1)
                self._pending[request_id] = response_queue

                payload = {"id": request_id, "action": "getXp", "steamId": steam_id}
                try:
                    process.stdin.write(json.dumps(payload, separators=(",", ":")) + "\n")
                    process.stdin.flush()
                except (BrokenPipeError, OSError) as error:
                    self._pending.pop(request_id, None)
                    self._mark_failed(f"Worker stdin is unavailable: {error}")
                    raise RuntimeError(self._last_error) from error

            try:
                response = response_queue.get(timeout=self.request_timeout_seconds)
            except queue.Empty as error:
                with self._state_lock:
                    self._pending.pop(request_id, None)
                self._mark_failed("Stats worker timed out while waiting for Steam.")
                raise TimeoutError(self._last_error) from error

            response_type = str(response.get("type", "")).strip()
            if response_type == "result" and isinstance(response.get("result"), dict):
                return response["result"]

            detail = str(response.get("error") or "Stats worker returned an invalid response.")
            raise RuntimeError(detail)

    def stop(self) -> None:
        with self._state_lock:
            process = self._process
            self._process = None
            self._state = "stopped"
            self._fail_pending("Stats worker stopped.")

        if process is None:
            return

        try:
            if process.stdin is not None:
                process.stdin.close()
        except OSError:
            pass

        try:
            process.terminate()
            process.wait(timeout=5)
        except (OSError, subprocess.TimeoutExpired):
            try:
                process.kill()
            except OSError:
                pass

    def _read_stdout(self, process: subprocess.Popen[str]) -> None:
        if process.stdout is None:
            return

        try:
            for raw_line in process.stdout:
                line = raw_line.strip()
                if not line:
                    continue

                try:
                    message = json.loads(line)
                except json.JSONDecodeError:
                    self._mark_failed("Stats worker emitted non-JSON data to stdout.")
                    LOGGER.error("Worker %s emitted non-JSON stdout.", self.account_name)
                    continue

                if not isinstance(message, dict):
                    continue

                message_type = str(message.get("type", "")).strip()
                if message_type == "status":
                    with self._state_lock:
                        previous_state = self._state
                        self._state = str(message.get("state") or "unknown")
                        detail = message.get("error")
                        self._last_error = str(detail) if detail else None
                    if previous_state != self._state or detail:
                        LOGGER.info(
                            "Stats worker state: account=%s state=%s error=%s",
                            self.account_name,
                            self._state,
                            self._last_error,
                        )
                    continue

                request_id = str(message.get("id") or "").strip()
                if not request_id:
                    continue

                with self._state_lock:
                    response_queue = self._pending.pop(request_id, None)
                if response_queue is not None:
                    response_queue.put(message)
        finally:
            with self._state_lock:
                is_current_process = self._process is process
            if is_current_process:
                exit_code = process.poll()
                self._mark_failed(f"Stats worker exited with code {exit_code}.")

    def _read_stderr(self, process: subprocess.Popen[str]) -> None:
        if process.stderr is None:
            return

        for raw_line in process.stderr:
            line = raw_line.strip()
            if line:
                LOGGER.warning("stats[%s]: %s", self.account_name, line)

    def _mark_failed(self, detail: str) -> None:
        with self._state_lock:
            self._state = "failed"
            self._last_error = detail
            self._fail_pending(detail)

    def _fail_pending(self, detail: str) -> None:
        pending_items = list(self._pending.items())
        self._pending.clear()
        for request_id, response_queue in pending_items:
            response_queue.put({"type": "error", "id": request_id, "error": detail})


class XpWorkerPool:
    """FIFO queue that distributes SteamID requests across serial worker processes."""

    def __init__(self, workers: list[StatsWorkerProcess], config: dict[str, Any]) -> None:
        self._workers = workers
        self._config = config
        self._queue: collections.deque[XpJob] = collections.deque()
        self._jobs: dict[str, XpJob] = {}
        self._pending_by_steam_id: dict[str, XpJob] = {}
        self._cache: collections.OrderedDict[str, CachedResult] = collections.OrderedDict()
        self._busy_workers: set[int] = set()
        self._round_robin_index = 0
        self._dispatch_retry_timer: threading.Timer | None = None
        self._lock = threading.RLock()
        self._executor = concurrent.futures.ThreadPoolExecutor(
            max_workers=len(workers), thread_name_prefix="xp-worker"
        )
        self._stopped = False

    def start(self) -> None:
        for worker in self._workers:
            try:
                worker.start()
            except RuntimeError as error:
                LOGGER.warning("Worker %s did not start yet: %s", worker.account_name, error)

    def stop(self) -> None:
        retry_timer = None
        with self._lock:
            self._stopped = True
            retry_timer = self._dispatch_retry_timer
            self._dispatch_retry_timer = None
        if retry_timer is not None:
            retry_timer.cancel()
        for worker in self._workers:
            worker.stop()
        self._executor.shutdown(wait=False, cancel_futures=True)

    def submit(self, steam_id: str) -> XpJob:
        return self.submit_many([steam_id])[0]

    def submit_many(self, steam_ids: list[str]) -> list[XpJob]:
        normalized_steam_ids = list(dict.fromkeys(normalize_steam_id(steam_id) for steam_id in steam_ids))
        if not normalized_steam_ids:
            return []

        with self._lock:
            self._prune_locked()
            new_steam_ids = [
                steam_id
                for steam_id in normalized_steam_ids
                if steam_id not in self._cache and steam_id not in self._pending_by_steam_id
            ]
            if len(self._queue) + len(new_steam_ids) > self._config["maxQueueSize"]:
                raise QueueFullError(f"The request queue is full ({self._config['maxQueueSize']}).")

            jobs = []
            for steam_id in normalized_steam_ids:
                cached = self._cache.get(steam_id)
                if cached is not None:
                    job = XpJob(steam_id=steam_id, cached=True, state="completed")
                    job.result = {**cached.result, "cached": True}
                    job.finished_at = utc_now()
                    job.completed.set()
                    self._jobs[job.id] = job
                    jobs.append(job)
                    continue

                pending = self._pending_by_steam_id.get(steam_id)
                if pending is not None:
                    jobs.append(pending)
                    continue

                job = XpJob(steam_id=steam_id)
                self._queue.append(job)
                self._jobs[job.id] = job
                self._pending_by_steam_id[steam_id] = job
                jobs.append(job)

            self._dispatch_locked()
            return jobs

    def get_job(self, job_id: str) -> XpJob | None:
        with self._lock:
            self._prune_locked()
            return self._jobs.get(job_id)

    def get_job_payload(self, job: XpJob) -> dict[str, Any]:
        with self._lock:
            queue_position = None
            if job.state == "queued":
                try:
                    queue_position = list(self._queue).index(job) + 1
                except ValueError:
                    queue_position = None
            return job.to_public(queue_position)

    def get_status(self) -> dict[str, Any]:
        with self._lock:
            self._prune_locked()
            return {
                "queuedJobs": len(self._queue),
                "pendingJobs": len(self._pending_by_steam_id),
                "cachedResults": len(self._cache),
                "workers": [worker.status() for worker in self._workers],
            }

    def wait_for_job(self, job: XpJob, timeout_seconds: float) -> None:
        job.completed.wait(timeout=max(0.0, timeout_seconds))

    def _dispatch_locked(self) -> None:
        if self._stopped:
            return

        while self._queue:
            worker_index = self._next_available_worker_index_locked()
            if worker_index is None:
                # Do not hand jobs to workers that are still connecting/cooling down.
                # Keep the jobs queued and retry shortly; as soon as any worker becomes
                # READY, that worker will start draining the queue.
                self._schedule_dispatch_retry_locked()
                return

            job = self._queue.popleft()
            if job.state != "queued":
                continue

            self._busy_workers.add(worker_index)
            job.state = "running"
            job.started_at = utc_now()
            job.attempts += 1
            worker = self._workers[worker_index]
            LOGGER.info(
                "XP job assigned: job=%s steamId=%s worker=%s attempt=%s",
                job.id,
                job.steam_id,
                worker.account_name,
                job.attempts,
            )
            self._executor.submit(self._run_job, worker_index, job)

    def _next_available_worker_index_locked(self) -> int | None:
        worker_count = len(self._workers)
        if worker_count == 0:
            return None

        for offset in range(worker_count):
            worker_index = (self._round_robin_index + offset) % worker_count
            if worker_index in self._busy_workers:
                continue

            worker = self._workers[worker_index]
            # Test/dummy workers may not expose a state property; treat them as ready.
            if str(getattr(worker, "state", "ready")).strip().lower() != "ready":
                continue

            self._round_robin_index = (worker_index + 1) % worker_count
            return worker_index

        return None

    def _schedule_dispatch_retry_locked(self) -> None:
        if self._stopped or not self._queue:
            return

        current_timer = self._dispatch_retry_timer
        if current_timer is not None and current_timer.is_alive():
            return

        retry_timer = threading.Timer(0.5, self._retry_dispatch)
        retry_timer.daemon = True
        self._dispatch_retry_timer = retry_timer
        retry_timer.start()

    def _retry_dispatch(self) -> None:
        with self._lock:
            self._dispatch_retry_timer = None
            if self._stopped:
                return
            self._dispatch_locked()

    def _run_job(self, worker_index: int, job: XpJob) -> None:
        worker = self._workers[worker_index]
        try:
            result = worker.request(job.steam_id)
            if not isinstance(result, dict):
                raise WorkerProtocolError("Stats worker result must be a JSON object.")
        except Exception as error:
            self._finish_error(worker_index, job, error)
            return

        with self._lock:
            self._busy_workers.discard(worker_index)
            job.state = "completed"
            job.result = result
            job.finished_at = utc_now()
            job.error = None
            job.completed.set()
            LOGGER.info(
                "XP job completed: job=%s steamId=%s worker=%s attempt=%s",
                job.id,
                job.steam_id,
                worker.account_name,
                job.attempts,
            )
            self._pending_by_steam_id.pop(job.steam_id, None)
            self._cache[job.steam_id] = CachedResult(
                result=result,
                expires_at=time.monotonic() + self._config["resultCacheTtlSeconds"],
            )
            self._cache.move_to_end(job.steam_id)
            while len(self._cache) > self._config["maxCacheEntries"]:
                self._cache.popitem(last=False)
            self._dispatch_locked()

    def _finish_error(self, worker_index: int, job: XpJob, error: Exception) -> None:
        detail = str(error) or error.__class__.__name__
        worker = self._workers[worker_index]
        with self._lock:
            self._busy_workers.discard(worker_index)
            job.error = detail
            should_retry = not self._stopped and job.attempts < self._config["maxAttemptsPerJob"]
            LOGGER.warning(
                "XP job failed: job=%s steamId=%s worker=%s attempt=%s retry=%s error=%s",
                job.id,
                job.steam_id,
                worker.account_name,
                job.attempts,
                should_retry,
                detail,
            )
            if should_retry:
                job.state = "queued"
                threading.Timer(self._config["retryDelaySeconds"], self._requeue_job, args=(job,)).start()
            else:
                job.state = "failed"
                job.finished_at = utc_now()
                job.completed.set()
                self._pending_by_steam_id.pop(job.steam_id, None)
            self._dispatch_locked()

    def _requeue_job(self, job: XpJob) -> None:
        with self._lock:
            if self._stopped or job.state != "queued":
                return
            self._queue.append(job)
            self._dispatch_locked()

    def _prune_locked(self) -> None:
        now = time.monotonic()
        expired_steam_ids = [
            steam_id for steam_id, entry in self._cache.items() if entry.expires_at <= now
        ]
        for steam_id in expired_steam_ids:
            self._cache.pop(steam_id, None)

        job_ttl = self._config["jobTtlSeconds"]
        if job_ttl <= 0:
            return

        cutoff = time.time() - job_ttl
        stale_job_ids = []
        for job_id, job in self._jobs.items():
            if not job.finished_at:
                continue
            try:
                finished_timestamp = calendar.timegm(time.strptime(job.finished_at, "%Y-%m-%dT%H:%M:%SZ"))
            except ValueError:
                continue
            if finished_timestamp <= cutoff:
                stale_job_ids.append(job_id)
        for job_id in stale_job_ids:
            self._jobs.pop(job_id, None)


class RemoteXpWorkerPool:
    """FIFO queue served by authenticated agents that run outside the HTTP server."""

    def __init__(self, pool_config: dict[str, Any], remote_worker_config: dict[str, Any]) -> None:
        self._config = pool_config
        self._remote_worker_config = remote_worker_config
        self._queue: collections.deque[XpJob] = collections.deque()
        self._jobs: dict[str, XpJob] = {}
        self._pending_by_steam_id: dict[str, XpJob] = {}
        self._cache: collections.OrderedDict[str, CachedResult] = collections.OrderedDict()
        self._workers: dict[str, RemoteWorker] = {}
        self._leases: dict[str, RemoteJobLease] = {}
        self._lock = threading.RLock()
        self._stopped = False

    def start(self) -> None:
        return None

    def stop(self) -> None:
        with self._lock:
            self._stopped = True
            self._queue.clear()
            self._leases.clear()
            for worker in self._workers.values():
                worker.active_job_id = None
            for job in self._pending_by_steam_id.values():
                if job.state not in {"completed", "failed"}:
                    job.state = "failed"
                    job.error = "Stats server stopped."
                    job.finished_at = utc_now()
                    job.completed.set()
            self._pending_by_steam_id.clear()

    def submit(self, steam_id: str) -> XpJob:
        return self.submit_many([steam_id])[0]

    def submit_many(self, steam_ids: list[str]) -> list[XpJob]:
        normalized_steam_ids = list(dict.fromkeys(normalize_steam_id(steam_id) for steam_id in steam_ids))
        if not normalized_steam_ids:
            return []

        with self._lock:
            self._prune_locked()
            new_steam_ids = [
                steam_id
                for steam_id in normalized_steam_ids
                if steam_id not in self._cache and steam_id not in self._pending_by_steam_id
            ]
            if len(self._queue) + len(new_steam_ids) > self._config["maxQueueSize"]:
                raise QueueFullError(f"The request queue is full ({self._config['maxQueueSize']}).")

            jobs = []
            for steam_id in normalized_steam_ids:
                cached = self._cache.get(steam_id)
                if cached is not None:
                    job = XpJob(steam_id=steam_id, cached=True, state="completed")
                    job.result = {**cached.result, "cached": True}
                    job.finished_at = utc_now()
                    job.completed.set()
                    self._jobs[job.id] = job
                    jobs.append(job)
                    continue

                pending = self._pending_by_steam_id.get(steam_id)
                if pending is not None:
                    jobs.append(pending)
                    continue

                job = XpJob(steam_id=steam_id)
                self._queue.append(job)
                self._jobs[job.id] = job
                self._pending_by_steam_id[steam_id] = job
                jobs.append(job)

            return jobs

    def get_job(self, job_id: str) -> XpJob | None:
        with self._lock:
            self._prune_locked()
            return self._jobs.get(job_id)

    def get_job_payload(self, job: XpJob) -> dict[str, Any]:
        with self._lock:
            queue_position = None
            if job.state == "queued":
                try:
                    queue_position = list(self._queue).index(job) + 1
                except ValueError:
                    queue_position = None
            return job.to_public(queue_position)

    def get_status(self) -> dict[str, Any]:
        with self._lock:
            self._prune_locked()
            return {
                "queuedJobs": len(self._queue),
                "pendingJobs": len(self._pending_by_steam_id),
                "cachedResults": len(self._cache),
                "workers": [worker.status() for worker in sorted(self._workers.values(), key=lambda item: item.worker_id)],
                "activeLeases": len(self._leases),
            }

    def wait_for_job(self, job: XpJob, timeout_seconds: float) -> None:
        job.completed.wait(timeout=max(0.0, timeout_seconds))

    def register_worker(
        self,
        worker_id: Any,
        account_name: Any,
        state: Any = "starting",
        error: Any = None,
    ) -> RemoteWorker:
        normalized_worker_id = normalize_remote_worker_id(worker_id)
        normalized_account_name = normalize_remote_account_name(account_name)
        normalized_state = str(state or "unknown").strip().lower() or "unknown"
        normalized_error = str(error).strip() if error is not None else ""

        with self._lock:
            self._prune_locked()
            return self._register_worker_locked(
                normalized_worker_id,
                normalized_account_name,
                normalized_state,
                normalized_error or None,
            )

    def claim_next_job(
        self,
        worker_id: Any,
        account_name: Any,
        state: Any = "ready",
        error: Any = None,
    ) -> dict[str, Any] | None:
        normalized_worker_id = normalize_remote_worker_id(worker_id)
        normalized_account_name = normalize_remote_account_name(account_name)
        normalized_state = str(state or "unknown").strip().lower() or "unknown"
        normalized_error = str(error).strip() if error is not None else ""

        with self._lock:
            self._prune_locked()
            worker = self._register_worker_locked(
                normalized_worker_id,
                normalized_account_name,
                normalized_state,
                normalized_error or None,
            )
            if self._stopped:
                return None

            if worker.active_job_id:
                active_job = self._jobs.get(worker.active_job_id)
                active_lease = self._leases.get(worker.active_job_id)
                if active_job is not None and active_job.state == "running" and active_lease is not None:
                    return self._to_assignment_payload(active_job, active_lease)
                worker.active_job_id = None

            if worker.state != "ready":
                return None

            while self._queue:
                job = self._queue.popleft()
                if job.state != "queued":
                    continue

                job.state = "running"
                job.started_at = utc_now()
                job.attempts += 1
                lease = RemoteJobLease(
                    worker_id=worker.worker_id,
                    token=str(uuid.uuid4()),
                    expires_at=time.monotonic() + self._remote_worker_config["jobLeaseSeconds"],
                )
                self._leases[job.id] = lease
                worker.active_job_id = job.id
                worker.state = "busy"
                worker.last_error = None
                return self._to_assignment_payload(job, lease)

            return None

    def complete_job(self, worker_id: Any, job_id: Any, lease_token: Any, result: Any) -> XpJob:
        normalized_worker_id = normalize_remote_worker_id(worker_id)
        normalized_job_id = str(job_id or "").strip()
        normalized_lease_token = str(lease_token or "").strip()

        with self._lock:
            self._prune_locked()
            job, worker = self._validate_lease_locked(
                normalized_worker_id,
                normalized_job_id,
                normalized_lease_token,
            )
            normalized_result = self._normalize_result(job, result)
            self._leases.pop(job.id, None)
            worker.active_job_id = None
            worker.state = "ready"
            worker.last_error = None
            job.state = "completed"
            job.result = normalized_result
            job.error = None
            job.finished_at = utc_now()
            job.completed.set()
            self._pending_by_steam_id.pop(job.steam_id, None)
            self._cache[job.steam_id] = CachedResult(
                result=normalized_result,
                expires_at=time.monotonic() + self._config["resultCacheTtlSeconds"],
            )
            self._cache.move_to_end(job.steam_id)
            while len(self._cache) > self._config["maxCacheEntries"]:
                self._cache.popitem(last=False)
            return job

    def fail_job(
        self,
        worker_id: Any,
        job_id: Any,
        lease_token: Any,
        error: Any,
        state: Any = "cooldown",
    ) -> XpJob:
        normalized_worker_id = normalize_remote_worker_id(worker_id)
        normalized_job_id = str(job_id or "").strip()
        normalized_lease_token = str(lease_token or "").strip()
        detail = str(error or "Remote worker could not complete the job.").strip()
        normalized_state = str(state or "cooldown").strip().lower() or "cooldown"

        with self._lock:
            self._prune_locked()
            job, worker = self._validate_lease_locked(
                normalized_worker_id,
                normalized_job_id,
                normalized_lease_token,
            )
            self._leases.pop(job.id, None)
            worker.active_job_id = None
            worker.state = normalized_state
            worker.last_error = detail
            self._finish_error_locked(job, detail)
            return job

    def _register_worker_locked(
        self,
        worker_id: str,
        account_name: str,
        state: str,
        error: str | None,
    ) -> RemoteWorker:
        worker = self._workers.get(worker_id)
        if worker is None:
            if len(self._workers) >= self._remote_worker_config["maxWorkers"]:
                raise RemoteWorkerLimitError(
                    f"The remote worker limit is {self._remote_worker_config['maxWorkers']}."
                )
            worker = RemoteWorker(worker_id=worker_id, account_name=account_name)
            self._workers[worker_id] = worker
        elif worker.account_name != account_name:
            raise RemoteWorkerLeaseError("agentId is already registered for another accountName.")

        worker.state = state
        worker.last_error = error
        worker.last_seen_at = utc_now()
        worker.last_seen_monotonic = time.monotonic()
        return worker

    def _to_assignment_payload(self, job: XpJob, lease: RemoteJobLease) -> dict[str, Any]:
        return {
            "id": job.id,
            "steamId": job.steam_id,
            "leaseToken": lease.token,
            "leaseSeconds": self._remote_worker_config["jobLeaseSeconds"],
        }

    def _validate_lease_locked(
        self,
        worker_id: str,
        job_id: str,
        lease_token: str,
    ) -> tuple[XpJob, RemoteWorker]:
        job = self._jobs.get(job_id)
        lease = self._leases.get(job_id)
        worker = self._workers.get(worker_id)
        if job is None or lease is None or worker is None:
            raise RemoteWorkerLeaseError("The job lease is missing or has expired.")
        if lease.worker_id != worker_id or not hmac.compare_digest(lease.token, lease_token):
            raise RemoteWorkerLeaseError("The job lease does not belong to this agent.")
        if job.state != "running" or worker.active_job_id != job.id:
            raise RemoteWorkerLeaseError("The job is not active for this agent.")
        return job, worker

    def _normalize_result(self, job: XpJob, result: Any) -> dict[str, Any]:
        if not isinstance(result, dict):
            raise ValueError("Worker result must be a JSON object.")
        result_steam_id = normalize_steam_id(result.get("steamId"))
        if result_steam_id != job.steam_id:
            raise ValueError("Worker result steamId does not match the assigned job.")

        try:
            current_xp = int(result.get("currentXp"))
            xp_per_level = int(result.get("xpPerLevel", 5000))
        except (TypeError, ValueError) as error:
            raise ValueError("Worker result contains invalid XP values.") from error
        if current_xp < 0 or xp_per_level <= 0 or current_xp > xp_per_level:
            raise ValueError("Worker result XP values are out of range.")

        profile_level = result.get("profileLevel")
        try:
            profile_level = int(profile_level) if profile_level is not None else None
        except (TypeError, ValueError):
            profile_level = None

        fetched_at = str(result.get("fetchedAt") or utc_now()).strip() or utc_now()
        return {
            "steamId": job.steam_id,
            "currentXp": current_xp,
            "xpPerLevel": xp_per_level,
            "progressPercent": round((current_xp / xp_per_level) * 100, 2),
            "xpRemaining": xp_per_level - current_xp,
            "profileLevel": profile_level,
            "fetchedAt": fetched_at,
        }

    def _finish_error_locked(self, job: XpJob, detail: str) -> None:
        job.error = detail
        should_retry = not self._stopped and job.attempts < self._config["maxAttemptsPerJob"]
        if should_retry:
            job.state = "queued"
            threading.Timer(self._config["retryDelaySeconds"], self._requeue_job, args=(job,)).start()
            return

        job.state = "failed"
        job.finished_at = utc_now()
        job.completed.set()
        self._pending_by_steam_id.pop(job.steam_id, None)

    def _requeue_job(self, job: XpJob) -> None:
        with self._lock:
            if self._stopped or job.state != "queued":
                return
            self._queue.append(job)

    def _prune_locked(self) -> None:
        now = time.monotonic()
        expired_steam_ids = [
            steam_id for steam_id, entry in self._cache.items() if entry.expires_at <= now
        ]
        for steam_id in expired_steam_ids:
            self._cache.pop(steam_id, None)

        expired_job_ids = [
            job_id for job_id, lease in self._leases.items() if lease.expires_at <= now
        ]
        for job_id in expired_job_ids:
            lease = self._leases.pop(job_id, None)
            job = self._jobs.get(job_id)
            if lease is None or job is None or job.state != "running":
                continue
            worker = self._workers.get(lease.worker_id)
            if worker is not None:
                worker.active_job_id = None
                worker.state = "offline"
                worker.last_error = "Job lease expired."
            self._finish_error_locked(job, "Remote worker lease expired.")

        stale_after_seconds = max(60.0, self._remote_worker_config["jobLeaseSeconds"] * 2)
        stale_worker_ids = [
            worker_id
            for worker_id, worker in self._workers.items()
            if worker.active_job_id is None and now - worker.last_seen_monotonic > stale_after_seconds
        ]
        for worker_id in stale_worker_ids:
            self._workers.pop(worker_id, None)

        job_ttl = self._config["jobTtlSeconds"]
        if job_ttl <= 0:
            return

        cutoff = time.time() - job_ttl
        stale_job_ids = []
        for job_id, job in self._jobs.items():
            if not job.finished_at:
                continue
            try:
                finished_timestamp = calendar.timegm(time.strptime(job.finished_at, "%Y-%m-%dT%H:%M:%SZ"))
            except ValueError:
                continue
            if finished_timestamp <= cutoff:
                stale_job_ids.append(job_id)
        for job_id in stale_job_ids:
            self._jobs.pop(job_id, None)


class ServerRuntime:
    def __init__(self, config: dict[str, Any]) -> None:
        self.config = config
        self.worker_mode = str(config.get("workerMode", "local"))
        if self.worker_mode == "local":
            worker_config = config["statsWorker"]
            runtime_directory = Path(config["runtimeDirectory"])
            workers = [
                StatsWorkerProcess(
                    account_name=account_name,
                    command_template=worker_config["command"],
                    working_directory=runtime_directory,
                    request_timeout_seconds=worker_config["requestTimeoutSeconds"],
                    restart_delay_seconds=worker_config["restartDelaySeconds"],
                )
                for account_name in worker_config["accounts"]
            ]
            self.pool: XpWorkerPool | RemoteXpWorkerPool = XpWorkerPool(workers, config["pool"])
        else:
            self.pool = RemoteXpWorkerPool(config["pool"], config["remoteWorkers"])
        api_key_env = str(config["server"].get("apiKeyEnv", ""))
        self.api_key = os.environ.get(api_key_env, "")
        worker_api_key_env = str(config.get("remoteWorkers", {}).get("workerApiKeyEnv", ""))
        self.worker_api_key = os.environ.get(worker_api_key_env, "") if self.worker_mode == "remote" else ""

    def is_authorized(self, supplied_key: str | None) -> bool:
        if not bool(self.config["server"].get("requireApiKey", True)):
            return True
        if not supplied_key or not self.api_key:
            return False
        return hmac.compare_digest(supplied_key.encode("utf-8"), self.api_key.encode("utf-8"))

    def is_remote_worker_mode(self) -> bool:
        return self.worker_mode == "remote" and isinstance(self.pool, RemoteXpWorkerPool)

    def is_worker_authorized(self, supplied_key: str | None) -> bool:
        if not self.is_remote_worker_mode() or not supplied_key or not self.worker_api_key:
            return False
        return hmac.compare_digest(supplied_key.encode("utf-8"), self.worker_api_key.encode("utf-8"))


def create_request_handler(runtime: ServerRuntime) -> type[BaseHTTPRequestHandler]:
    class StatsRequestHandler(BaseHTTPRequestHandler):
        server_version = "SteamStatsPython/1.0"
        protocol_version = "HTTP/1.1"

        def log_message(self, format_string: str, *args: Any) -> None:
            LOGGER.info("%s - %s", self.client_address[0], format_string % args)

        def do_GET(self) -> None:
            if not self._require_authorization():
                return

            path = urlparse(self.path).path
            if path == "/health":
                self._send_json(HTTPStatus.OK, {"ok": True, **runtime.pool.get_status()})
                return

            if path.startswith("/v1/jobs/"):
                job_id = path.removeprefix("/v1/jobs/").strip()
                job = runtime.pool.get_job(job_id)
                if job is None:
                    self._send_error_json(HTTPStatus.NOT_FOUND, "job_not_found", "Job was not found or has expired.")
                    return
                self._send_json(HTTPStatus.OK, runtime.pool.get_job_payload(job))
                return

            self._send_error_json(HTTPStatus.NOT_FOUND, "not_found", "Endpoint was not found.")

        def do_POST(self) -> None:
            path = urlparse(self.path).path
            if path.startswith("/v1/worker/"):
                if not self._require_worker_authorization():
                    return
                try:
                    payload = self._read_json_body()
                except ValueError as error:
                    self._send_error_json(HTTPStatus.BAD_REQUEST, "invalid_json", str(error))
                    return
                self._handle_worker_request(path, payload)
                return

            if not self._require_authorization():
                return

            try:
                payload = self._read_json_body()
            except ValueError as error:
                self._send_error_json(HTTPStatus.BAD_REQUEST, "invalid_json", str(error))
                return

            if path == "/v1/xp":
                self._submit_single(payload)
                return
            if path == "/v1/xp/batch":
                self._submit_batch(payload)
                return
            if path == "/v1/jobs/batch":
                self._get_batch_jobs(payload)
                return

            self._send_error_json(HTTPStatus.NOT_FOUND, "not_found", "Endpoint was not found.")

        def _submit_single(self, payload: dict[str, Any]) -> None:
            try:
                steam_id = normalize_steam_id(payload.get("steamId"))
                job = runtime.pool.submit(steam_id)
            except ValueError as error:
                self._send_error_json(HTTPStatus.BAD_REQUEST, "invalid_steam_id", str(error))
                return
            except QueueFullError as error:
                self._send_error_json(HTTPStatus.TOO_MANY_REQUESTS, "queue_full", str(error))
                return

            self._wait_if_requested(job, payload)
            self._send_job(job)

        def _submit_batch(self, payload: dict[str, Any]) -> None:
            raw_steam_ids = payload.get("steamIds")
            if not isinstance(raw_steam_ids, list) or not raw_steam_ids:
                self._send_error_json(HTTPStatus.BAD_REQUEST, "invalid_steam_ids", "steamIds must be a non-empty JSON array.")
                return
            if len(raw_steam_ids) > runtime.config["server"]["maxBatchSize"]:
                self._send_error_json(
                    HTTPStatus.BAD_REQUEST,
                    "batch_too_large",
                    f"steamIds may contain at most {runtime.config['server']['maxBatchSize']} values.",
                )
                return

            try:
                steam_ids = list(dict.fromkeys(normalize_steam_id(steam_id) for steam_id in raw_steam_ids))
                jobs = runtime.pool.submit_many(steam_ids)
            except ValueError as error:
                self._send_error_json(HTTPStatus.BAD_REQUEST, "invalid_steam_id", str(error))
                return
            except QueueFullError as error:
                self._send_error_json(HTTPStatus.TOO_MANY_REQUESTS, "queue_full", str(error))
                return

            wait_seconds = self._get_wait_seconds(payload)
            if wait_seconds > 0:
                deadline = time.monotonic() + wait_seconds
                for job in jobs:
                    remaining = deadline - time.monotonic()
                    if remaining <= 0:
                        break
                    runtime.pool.wait_for_job(job, remaining)

            job_payloads = [runtime.pool.get_job_payload(job) for job in jobs]
            all_done = all(job["status"] in {"completed", "failed"} for job in job_payloads)
            status = HTTPStatus.OK if all_done else HTTPStatus.ACCEPTED
            self._send_json(status, {"jobs": job_payloads})

        def _get_batch_jobs(self, payload: dict[str, Any]) -> None:
            raw_job_ids = payload.get("jobIds")
            if not isinstance(raw_job_ids, list) or not raw_job_ids:
                self._send_error_json(HTTPStatus.BAD_REQUEST, "invalid_job_ids", "jobIds must be a non-empty JSON array.")
                return
            if len(raw_job_ids) > runtime.config["server"]["maxBatchSize"]:
                self._send_error_json(
                    HTTPStatus.BAD_REQUEST,
                    "batch_too_large",
                    f"jobIds may contain at most {runtime.config['server']['maxBatchSize']} values.",
                )
                return

            jobs = []
            for raw_job_id in dict.fromkeys(str(job_id).strip() for job_id in raw_job_ids if str(job_id).strip()):
                job = runtime.pool.get_job(raw_job_id)
                if job is not None:
                    jobs.append(runtime.pool.get_job_payload(job))

            self._send_json(HTTPStatus.OK, {"jobs": jobs})

        def _handle_worker_request(self, path: str, payload: dict[str, Any]) -> None:
            if not runtime.is_remote_worker_mode():
                self._send_error_json(HTTPStatus.NOT_FOUND, "not_found", "Remote workers are disabled.")
                return

            remote_pool = runtime.pool
            if not isinstance(remote_pool, RemoteXpWorkerPool):
                self._send_error_json(HTTPStatus.NOT_FOUND, "not_found", "Remote workers are disabled.")
                return

            try:
                if path == "/v1/worker/register":
                    worker = remote_pool.register_worker(
                        payload.get("agentId"),
                        payload.get("accountName"),
                        payload.get("state", "starting"),
                        payload.get("error"),
                    )
                    self._send_json(HTTPStatus.OK, {"ok": True, "worker": worker.status()})
                    return

                if path == "/v1/worker/next":
                    assignment = remote_pool.claim_next_job(
                        payload.get("agentId"),
                        payload.get("accountName"),
                        payload.get("state", "ready"),
                        payload.get("error"),
                    )
                    self._send_json(HTTPStatus.OK, {"job": assignment})
                    return

                job_prefix = "/v1/worker/jobs/"
                if path.startswith(job_prefix) and path.endswith("/complete"):
                    job_id = path.removeprefix(job_prefix).removesuffix("/complete").strip("/")
                    job = remote_pool.complete_job(
                        payload.get("agentId"),
                        job_id,
                        payload.get("leaseToken"),
                        payload.get("result"),
                    )
                    self._send_json(HTTPStatus.OK, {"job": remote_pool.get_job_payload(job)})
                    return

                if path.startswith(job_prefix) and path.endswith("/failed"):
                    job_id = path.removeprefix(job_prefix).removesuffix("/failed").strip("/")
                    job = remote_pool.fail_job(
                        payload.get("agentId"),
                        job_id,
                        payload.get("leaseToken"),
                        payload.get("error"),
                        payload.get("state", "cooldown"),
                    )
                    self._send_json(HTTPStatus.OK, {"job": remote_pool.get_job_payload(job)})
                    return
            except RemoteWorkerLimitError as error:
                self._send_error_json(HTTPStatus.TOO_MANY_REQUESTS, "worker_limit", str(error))
                return
            except RemoteWorkerLeaseError as error:
                self._send_error_json(HTTPStatus.CONFLICT, "invalid_lease", str(error))
                return
            except ValueError as error:
                self._send_error_json(HTTPStatus.BAD_REQUEST, "invalid_worker_request", str(error))
                return

            self._send_error_json(HTTPStatus.NOT_FOUND, "not_found", "Endpoint was not found.")

        def _wait_if_requested(self, job: XpJob, payload: dict[str, Any]) -> None:
            wait_seconds = self._get_wait_seconds(payload)
            if wait_seconds > 0 and job.state not in {"completed", "failed"}:
                runtime.pool.wait_for_job(job, wait_seconds)

        def _get_wait_seconds(self, payload: dict[str, Any]) -> float:
            requested = payload.get("waitSeconds", 0)
            try:
                seconds = float(requested)
            except (TypeError, ValueError):
                return 0.0
            return max(0.0, min(seconds, runtime.config["server"]["maxSynchronousWaitSeconds"]))

        def _send_job(self, job: XpJob) -> None:
            payload = runtime.pool.get_job_payload(job)
            if payload["status"] == "completed":
                self._send_json(HTTPStatus.OK, payload)
            elif payload["status"] == "failed":
                self._send_json(HTTPStatus.BAD_GATEWAY, payload)
            else:
                self._send_json(HTTPStatus.ACCEPTED, payload)

        def _require_authorization(self) -> bool:
            supplied_key = self.headers.get("X-Api-Key")
            if runtime.is_authorized(supplied_key):
                return True
            self._send_error_json(HTTPStatus.UNAUTHORIZED, "unauthorized", "A valid X-Api-Key header is required.")
            return False

        def _require_worker_authorization(self) -> bool:
            supplied_key = self.headers.get("X-Worker-Key")
            if runtime.is_worker_authorized(supplied_key):
                return True
            self._send_error_json(
                HTTPStatus.UNAUTHORIZED,
                "unauthorized_worker",
                "A valid X-Worker-Key header is required.",
            )
            return False

        def _read_json_body(self) -> dict[str, Any]:
            length_header = self.headers.get("Content-Length")
            if not length_header:
                raise ValueError("Content-Length header is required.")
            try:
                content_length = int(length_header)
            except ValueError as error:
                raise ValueError("Content-Length must be an integer.") from error
            if content_length < 0 or content_length > runtime.config["server"]["maxRequestBodyBytes"]:
                raise ValueError("Request body is too large.")

            raw_body = self.rfile.read(content_length)
            try:
                payload = json.loads(raw_body.decode("utf-8"))
            except (UnicodeDecodeError, json.JSONDecodeError) as error:
                raise ValueError("Request body must be valid UTF-8 JSON.") from error
            if not isinstance(payload, dict):
                raise ValueError("Request body must be a JSON object.")
            return payload

        def _send_error_json(self, status: HTTPStatus, code: str, message: str) -> None:
            self._send_json(status, {"error": {"code": code, "message": message}})

        def _send_json(self, status: HTTPStatus, payload: dict[str, Any]) -> None:
            raw_payload = json.dumps(payload, ensure_ascii=False, separators=(",", ":")).encode("utf-8")
            self.send_response(status.value)
            self.send_header("Content-Type", "application/json; charset=utf-8")
            self.send_header("Content-Length", str(len(raw_payload)))
            self.send_header("Cache-Control", "no-store")
            self.send_header("Connection", "close")
            self.end_headers()
            self.wfile.write(raw_payload)

    return StatsRequestHandler


def build_argument_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="HTTP queue server for Steam XP statistics workers.")
    parser.add_argument(
        "--config",
        type=Path,
        default=Path(__file__).with_name("server-config.json"),
        help="Path to server-config.json.",
    )
    parser.add_argument(
        "--check-config",
        action="store_true",
        help="Validate configuration and exit without starting worker processes.",
    )
    return parser


def configure_logging() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
        stream=sys.stdout,
    )


def main(argv: list[str] | None = None) -> int:
    configure_logging()
    args = build_argument_parser().parse_args(argv)

    try:
        config = load_config(args.config.resolve())
    except ConfigurationError as error:
        LOGGER.error("Configuration error: %s", error)
        return 2

    if args.check_config:
        LOGGER.info("Configuration is valid: %s", args.config.resolve())
        return 0

    runtime = ServerRuntime(config)
    handler = create_request_handler(runtime)
    server_config = config["server"]
    http_server = ThreadingHTTPServer((server_config["host"], server_config["port"]), handler)
    http_server.daemon_threads = True

    shutdown_started = threading.Event()

    def request_shutdown(*_: Any) -> None:
        if shutdown_started.is_set():
            return
        shutdown_started.set()
        LOGGER.info("Shutdown requested.")
        threading.Thread(target=http_server.shutdown, daemon=True).start()

    for signal_name in ("SIGINT", "SIGTERM"):
        signal_value = getattr(signal, signal_name, None)
        if signal_value is not None:
            signal.signal(signal_value, request_shutdown)

    try:
        runtime.pool.start()
        LOGGER.info("Steam stats server listening on %s:%s.", server_config["host"], server_config["port"])
        http_server.serve_forever(poll_interval=0.5)
    except OSError as error:
        LOGGER.error("Server error: %s", error)
        return 1
    finally:
        http_server.server_close()
        runtime.pool.stop()

    return 0


if __name__ == "__main__":
    raise SystemExit(main())