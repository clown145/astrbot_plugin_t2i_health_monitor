from __future__ import annotations

import asyncio
import sys
import unittest
from pathlib import Path
from unittest.mock import AsyncMock
from zoneinfo import ZoneInfo


PROJECTS_DIR = Path(__file__).resolve().parents[2]
if str(PROJECTS_DIR) not in sys.path:
    sys.path.insert(0, str(PROJECTS_DIR))

from astrbot_plugin_t2i_health_monitor.main import T2IHealthMonitor  # noqa: E402
from astrbot_plugin_t2i_health_monitor.monitor import ProbeResult, T2ITarget  # noqa: E402
from astrbot_plugin_t2i_health_monitor.state import MonitorState  # noqa: E402


class FakeReply:
    def __init__(self, text: str) -> None:
        self.text = text
        self.t2i_enabled: bool | None = None

    def use_t2i(self, enabled: bool) -> "FakeReply":
        self.t2i_enabled = enabled
        return self


class FakeEvent:
    def plain_result(self, text: str) -> FakeReply:
        return FakeReply(text)


async def collect(generator):
    return [result async for result in generator]


def probe_result(
    *,
    target_id: str,
    target_name: str,
    success: bool,
    attempts: int = 1,
    error: str | None = None,
) -> ProbeResult:
    return ProbeResult(
        target_id=target_id,
        target_name=target_name,
        success=success,
        attempts=attempts,
        latency_ms=42,
        finished_at="2026-01-01T00:00:00+00:00",
        error=error,
    )


class CommandTests(unittest.IsolatedAsyncioTestCase):
    async def test_probe_without_target_id_runs_all_targets(self) -> None:
        plugin = object.__new__(T2IHealthMonitor)
        plugin._run_probe_cycle = AsyncMock(
            return_value=[
                probe_result(
                    target_id="main",
                    target_name="主服务",
                    success=True,
                ),
                probe_result(
                    target_id="backup",
                    target_name="备用服务",
                    success=True,
                ),
            ],
        )

        replies = await collect(plugin.health_probe(FakeEvent()))

        plugin._run_probe_cycle.assert_awaited_once_with(None)
        self.assertIn("共 2 个目标：2 成功，0 失败。", replies[0].text)
        self.assertIn("[主服务] 成功", replies[0].text)
        self.assertIn("[备用服务] 成功", replies[0].text)

    async def test_probe_with_target_id_only_runs_the_selected_target(self) -> None:
        plugin = object.__new__(T2IHealthMonitor)
        plugin._configured_targets = lambda: [
            T2ITarget("main", "主服务", "https://main.example", True, 5),
            T2ITarget("backup", "备用服务", "https://backup.example", True, 5),
        ]
        plugin._run_probe_cycle = AsyncMock(
            return_value=[
                probe_result(
                    target_id="backup",
                    target_name="备用服务",
                    success=False,
                    attempts=3,
                    error="HTTP 503",
                ),
            ],
        )

        replies = await collect(plugin.health_probe(FakeEvent(), "backup"))

        plugin._run_probe_cycle.assert_awaited_once_with("backup")
        self.assertEqual(len(replies), 1)
        self.assertFalse(replies[0].t2i_enabled)
        self.assertIn("[备用服务] 失败", replies[0].text)
        self.assertIn("尝试: 3 次（含重试）", replies[0].text)
        self.assertIn("耗时: 42 ms", replies[0].text)
        self.assertIn("错误: HTTP 503", replies[0].text)

    async def test_report_command_does_not_reset_the_period(self) -> None:
        plugin = object.__new__(T2IHealthMonitor)
        plugin._send_daily_report = AsyncMock(return_value=True)

        replies = await collect(plugin.health_report(FakeEvent()))

        plugin._send_daily_report.assert_awaited_once_with(reset_period=False)
        self.assertEqual(replies[0].text, "日报已推送，统计周期未重置。")


class DailyReportTests(unittest.IsolatedAsyncioTestCase):
    async def test_manual_daily_report_keeps_existing_period_statistics(self) -> None:
        plugin = object.__new__(T2IHealthMonitor)
        plugin.config = {}
        plugin._reported_missing_daily_umo = False
        plugin._state = MonitorState(period_started_at="2026-01-01T00:00:00+00:00")
        plugin._state_lock = asyncio.Lock()
        plugin._daily_push_umos = lambda: ["aiocqhttp:group:100"]
        plugin._configured_targets = lambda: []
        plugin._configured_timezone = lambda: ZoneInfo("UTC")
        plugin._send_to_umos = AsyncMock(return_value=True)
        plugin._save_state_locked = AsyncMock()

        delivered = await plugin._send_daily_report(reset_period=False)

        self.assertTrue(delivered)
        self.assertEqual(plugin._state.period_started_at, "2026-01-01T00:00:00+00:00")
        plugin._send_to_umos.assert_awaited_once()
        plugin._save_state_locked.assert_not_awaited()
