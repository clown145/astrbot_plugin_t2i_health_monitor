"""Persistent health statistics for the t2i monitor."""

from __future__ import annotations

import asyncio
import json
import logging
import os
import tempfile
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Literal

from .monitor import ProbeResult


logger = logging.getLogger("astrbot")
STATE_VERSION = 1


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


@dataclass
class HealthStats:
    """Counters for either the current report period or all recorded probes."""

    cycles: int = 0
    successful_cycles: int = 0
    attempts: int = 0
    total_latency_ms: int = 0

    def record(self, result: ProbeResult) -> None:
        self.cycles += 1
        self.attempts += max(1, result.attempts)
        self.total_latency_ms += max(0, result.latency_ms)
        if result.success:
            self.successful_cycles += 1

    @property
    def failed_cycles(self) -> int:
        return max(0, self.cycles - self.successful_cycles)

    @property
    def health_percent(self) -> float | None:
        if self.cycles == 0:
            return None
        return self.successful_cycles * 100 / self.cycles

    @property
    def average_latency_ms(self) -> int | None:
        if self.cycles == 0:
            return None
        return round(self.total_latency_ms / self.cycles)

    def to_dict(self) -> dict[str, int]:
        return {
            "cycles": self.cycles,
            "successful_cycles": self.successful_cycles,
            "attempts": self.attempts,
            "total_latency_ms": self.total_latency_ms,
        }

    @classmethod
    def from_dict(cls, raw: object) -> "HealthStats":
        if not isinstance(raw, dict):
            return cls()
        cycles = _non_negative_int(raw.get("cycles", 0))
        successful_cycles = min(
            cycles,
            _non_negative_int(raw.get("successful_cycles", 0)),
        )
        return cls(
            cycles=cycles,
            successful_cycles=successful_cycles,
            attempts=_non_negative_int(raw.get("attempts", 0)),
            total_latency_ms=_non_negative_int(raw.get("total_latency_ms", 0)),
        )


@dataclass
class TargetHealth:
    """Persistent status and counters for one stable target id."""

    name: str
    cumulative: HealthStats = field(default_factory=HealthStats)
    period: HealthStats = field(default_factory=HealthStats)
    status: Literal["unknown", "healthy", "failed"] = "unknown"
    last_probe_at: str | None = None
    last_success_at: str | None = None
    last_failure_at: str | None = None
    last_latency_ms: int | None = None
    last_error: str | None = None
    last_attempts: int = 0
    consecutive_failures: int = 0
    outage_started_at: str | None = None
    outage_notified: bool = False
    last_failure_notification_at: str | None = None

    def to_dict(self) -> dict[str, Any]:
        return {
            "name": self.name,
            "cumulative": self.cumulative.to_dict(),
            "period": self.period.to_dict(),
            "status": self.status,
            "last_probe_at": self.last_probe_at,
            "last_success_at": self.last_success_at,
            "last_failure_at": self.last_failure_at,
            "last_latency_ms": self.last_latency_ms,
            "last_error": self.last_error,
            "last_attempts": self.last_attempts,
            "consecutive_failures": self.consecutive_failures,
            "outage_started_at": self.outage_started_at,
            "outage_notified": self.outage_notified,
            "last_failure_notification_at": self.last_failure_notification_at,
        }

    @classmethod
    def from_dict(cls, raw: object) -> "TargetHealth":
        if not isinstance(raw, dict):
            return cls(name="unknown")
        status = raw.get("status", "unknown")
        if status not in {"unknown", "healthy", "failed"}:
            status = "unknown"
        return cls(
            name=_safe_text(raw.get("name"), "unknown"),
            cumulative=HealthStats.from_dict(raw.get("cumulative")),
            period=HealthStats.from_dict(raw.get("period")),
            status=status,
            last_probe_at=_optional_text(raw.get("last_probe_at")),
            last_success_at=_optional_text(raw.get("last_success_at")),
            last_failure_at=_optional_text(raw.get("last_failure_at")),
            last_latency_ms=_optional_non_negative_int(raw.get("last_latency_ms")),
            last_error=_optional_text(raw.get("last_error")),
            last_attempts=_non_negative_int(raw.get("last_attempts", 0)),
            consecutive_failures=_non_negative_int(raw.get("consecutive_failures", 0)),
            outage_started_at=_optional_text(raw.get("outage_started_at")),
            outage_notified=bool(raw.get("outage_notified", False)),
            last_failure_notification_at=_optional_text(
                raw.get("last_failure_notification_at"),
            ),
        )


@dataclass
class MonitorState:
    """The complete JSON-persisted state."""

    version: int = STATE_VERSION
    period_started_at: str = field(default_factory=utc_now_iso)
    last_daily_report_at: str | None = None
    targets: dict[str, TargetHealth] = field(default_factory=dict)

    def target(self, target_id: str, name: str) -> TargetHealth:
        target = self.targets.get(target_id)
        if target is None:
            target = TargetHealth(name=name)
            self.targets[target_id] = target
        elif name:
            target.name = name
        return target

    def reset_period(self, started_at: str) -> None:
        self.period_started_at = started_at
        self.last_daily_report_at = started_at
        for target in self.targets.values():
            target.period = HealthStats()

    def to_dict(self) -> dict[str, Any]:
        return {
            "version": self.version,
            "period_started_at": self.period_started_at,
            "last_daily_report_at": self.last_daily_report_at,
            "targets": {
                target_id: health.to_dict()
                for target_id, health in self.targets.items()
            },
        }

    @classmethod
    def from_dict(cls, raw: object) -> "MonitorState":
        if not isinstance(raw, dict):
            return cls()
        target_data = raw.get("targets", {})
        targets: dict[str, TargetHealth] = {}
        if isinstance(target_data, dict):
            for target_id, target in target_data.items():
                if isinstance(target_id, str) and target_id:
                    targets[target_id] = TargetHealth.from_dict(target)
        return cls(
            version=STATE_VERSION,
            period_started_at=_safe_text(raw.get("period_started_at"), utc_now_iso()),
            last_daily_report_at=_optional_text(raw.get("last_daily_report_at")),
            targets=targets,
        )


@dataclass(frozen=True)
class NotificationEvent:
    """An outage or recovery notification to deliver after state is persisted."""

    kind: Literal["outage", "recovery"]
    result: ProbeResult
    consecutive_failures: int
    outage_started_at: str | None


def record_probe_result(
    state: MonitorState,
    result: ProbeResult,
    *,
    notification_enabled: bool,
    send_recovery_notice: bool,
    cooldown_seconds: int,
) -> NotificationEvent | None:
    """Record a completed cycle and decide whether it crosses a notification boundary."""

    health = state.target(result.target_id, result.target_name)
    health.cumulative.record(result)
    health.period.record(result)
    health.last_probe_at = result.finished_at
    health.last_latency_ms = result.latency_ms
    health.last_attempts = result.attempts

    if result.success:
        was_outage = health.outage_notified
        outage_started_at = health.outage_started_at
        health.status = "healthy"
        health.last_success_at = result.finished_at
        health.last_error = None
        health.consecutive_failures = 0
        health.outage_started_at = None
        health.outage_notified = False
        health.last_failure_notification_at = None
        if was_outage and notification_enabled and send_recovery_notice:
            return NotificationEvent(
                kind="recovery",
                result=result,
                consecutive_failures=0,
                outage_started_at=outage_started_at,
            )
        return None

    health.status = "failed"
    health.last_failure_at = result.finished_at
    health.last_error = result.error
    health.consecutive_failures += 1
    if health.outage_started_at is None:
        health.outage_started_at = result.finished_at

    if not notification_enabled:
        return None
    if health.outage_notified and not _cooldown_elapsed(
        health.last_failure_notification_at,
        result.finished_at,
        cooldown_seconds,
    ):
        return None

    health.outage_notified = True
    health.last_failure_notification_at = result.finished_at
    return NotificationEvent(
        kind="outage",
        result=result,
        consecutive_failures=health.consecutive_failures,
        outage_started_at=health.outage_started_at,
    )


class JsonStateStore:
    """Atomic async JSON persistence backed by AstrBot's plugin data directory."""

    def __init__(self, path: Path) -> None:
        self.path = path

    async def load(self) -> MonitorState:
        return await asyncio.to_thread(self._load_sync)

    async def save(self, state: MonitorState) -> None:
        payload = state.to_dict()
        await asyncio.to_thread(self._save_sync, payload)

    def _load_sync(self) -> MonitorState:
        if not self.path.exists():
            return MonitorState()
        try:
            with self.path.open("r", encoding="utf-8") as file:
                return MonitorState.from_dict(json.load(file))
        except (OSError, json.JSONDecodeError) as exc:
            backup = self.path.with_name(
                f"{self.path.stem}.corrupt-{int(datetime.now().timestamp())}{self.path.suffix}",
            )
            try:
                self.path.replace(backup)
                logger.warning(
                    "t2i health state was invalid and moved to %s: %s",
                    backup,
                    exc,
                )
            except OSError:
                logger.warning("t2i health state was invalid: %s", exc)
            return MonitorState()

    def _save_sync(self, payload: dict[str, Any]) -> None:
        self.path.parent.mkdir(parents=True, exist_ok=True)
        fd, temporary_name = tempfile.mkstemp(
            prefix=f".{self.path.name}.",
            suffix=".tmp",
            dir=self.path.parent,
        )
        try:
            with os.fdopen(fd, "w", encoding="utf-8") as file:
                json.dump(payload, file, ensure_ascii=False, indent=2, sort_keys=True)
                file.write("\n")
                file.flush()
                os.fsync(file.fileno())
            os.replace(temporary_name, self.path)
        except BaseException:
            try:
                os.unlink(temporary_name)
            except OSError:
                pass
            raise


def _cooldown_elapsed(previous: str | None, current: str, cooldown_seconds: int) -> bool:
    if cooldown_seconds <= 0 or previous is None:
        return True
    try:
        elapsed = datetime.fromisoformat(current) - datetime.fromisoformat(previous)
    except ValueError:
        return True
    return elapsed.total_seconds() >= cooldown_seconds


def _non_negative_int(value: object) -> int:
    try:
        return max(0, int(value))
    except (TypeError, ValueError):
        return 0


def _optional_non_negative_int(value: object) -> int | None:
    if value is None:
        return None
    return _non_negative_int(value)


def _optional_text(value: object) -> str | None:
    if not isinstance(value, str) or not value.strip():
        return None
    return value.strip()


def _safe_text(value: object, default: str) -> str:
    return _optional_text(value) or default
