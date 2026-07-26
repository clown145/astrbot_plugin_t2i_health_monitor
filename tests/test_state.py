from __future__ import annotations

import sys
import tempfile
import unittest
from pathlib import Path


PROJECTS_DIR = Path(__file__).resolve().parents[2]
if str(PROJECTS_DIR) not in sys.path:
    sys.path.insert(0, str(PROJECTS_DIR))

from astrbot_plugin_t2i_health_monitor.monitor import ProbeResult  # noqa: E402
from astrbot_plugin_t2i_health_monitor.state import (  # noqa: E402
    JsonStateStore,
    MonitorState,
    record_probe_result,
)


def result(*, success: bool, finished_at: str, attempts: int = 1) -> ProbeResult:
    return ProbeResult(
        target_id="primary",
        target_name="Primary",
        success=success,
        attempts=attempts,
        latency_ms=42,
        finished_at=finished_at,
        error=None if success else "HTTP 503",
    )


class StateTests(unittest.TestCase):
    def test_outage_is_notified_once_then_recovery_is_notified(self) -> None:
        state = MonitorState()
        record_probe_result(
            state,
            result(success=True, finished_at="2026-01-01T00:00:00+00:00"),
            notification_enabled=True,
            send_recovery_notice=True,
            cooldown_seconds=1800,
        )
        outage = record_probe_result(
            state,
            result(success=False, finished_at="2026-01-01T00:01:00+00:00", attempts=3),
            notification_enabled=True,
            send_recovery_notice=True,
            cooldown_seconds=1800,
        )
        duplicate = record_probe_result(
            state,
            result(success=False, finished_at="2026-01-01T00:02:00+00:00", attempts=3),
            notification_enabled=True,
            send_recovery_notice=True,
            cooldown_seconds=1800,
        )
        recovery = record_probe_result(
            state,
            result(success=True, finished_at="2026-01-01T00:03:00+00:00"),
            notification_enabled=True,
            send_recovery_notice=True,
            cooldown_seconds=1800,
        )

        self.assertEqual(outage.kind if outage else None, "outage")
        self.assertIsNone(duplicate)
        self.assertEqual(recovery.kind if recovery else None, "recovery")
        health = state.targets["primary"]
        self.assertEqual(health.cumulative.cycles, 4)
        self.assertEqual(health.cumulative.successful_cycles, 2)
        self.assertEqual(health.period.health_percent, 50.0)
        self.assertEqual(health.status, "healthy")


class JsonStateStoreTests(unittest.IsolatedAsyncioTestCase):
    async def test_persisted_cumulative_stats_survive_reload(self) -> None:
        with tempfile.TemporaryDirectory() as temporary_directory:
            store = JsonStateStore(Path(temporary_directory) / "health_state.json")
            state = MonitorState()
            record_probe_result(
                state,
                result(success=True, finished_at="2026-01-01T00:00:00+00:00"),
                notification_enabled=False,
                send_recovery_notice=False,
                cooldown_seconds=0,
            )
            await store.save(state)
            loaded = await store.load()

        health = loaded.targets["primary"]
        self.assertEqual(health.cumulative.cycles, 1)
        self.assertEqual(health.cumulative.successful_cycles, 1)
        self.assertEqual(health.last_latency_ms, 42)
