"""AstrBot plugin entry point for direct t2i health monitoring."""

from __future__ import annotations

import asyncio
from datetime import datetime, timedelta, timezone
from typing import Any
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

import httpx
from astrbot.api import AstrBotConfig, logger
from astrbot.api.event import AstrMessageEvent, MessageChain, filter
from astrbot.api.star import Context, Star, StarTools

from .monitor import ProbeResult, T2IProbeClient, T2ITarget, parse_targets
from .state import (
    JsonStateStore,
    MonitorState,
    NotificationEvent,
    record_probe_result,
    utc_now_iso,
)


PLUGIN_DATA_NAME = "astrbot_plugin_t2i_health_monitor"


class T2IHealthMonitor(Star):
    """Probe configured t2i services and actively publish their health."""

    def __init__(self, context: Context, config: AstrBotConfig) -> None:
        super().__init__(context, config)
        self.config = config
        self._http_client: httpx.AsyncClient | None = None
        self._probe_client: T2IProbeClient | None = None
        self._state_store: JsonStateStore | None = None
        self._state = MonitorState()
        self._state_lock = asyncio.Lock()
        self._cycle_lock = asyncio.Lock()
        self._stopping = asyncio.Event()
        self._tasks: list[asyncio.Task[None]] = []
        self._reported_config_errors: set[str] = set()
        self._reported_missing_targets = False
        self._reported_missing_daily_umo = False

    async def initialize(self) -> None:
        """Load persisted statistics and start the probe and daily-report tasks."""

        data_name = getattr(self, "name", "") or PLUGIN_DATA_NAME
        data_dir = StarTools.get_data_dir(data_name)
        self._state_store = JsonStateStore(data_dir / "health_state.json")
        self._state = await self._state_store.load()
        self._stopping.clear()
        self._http_client = httpx.AsyncClient(
            follow_redirects=True,
            headers={"User-Agent": "AstrBot-t2i-health-monitor/0.1"},
            trust_env=True,
        )
        self._probe_client = T2IProbeClient(self._http_client)
        self._tasks = [
            asyncio.create_task(self._probe_loop(), name="t2i-health-probe"),
            asyncio.create_task(
                self._daily_report_loop(),
                name="t2i-health-daily-report",
            ),
        ]
        logger.info("t2i health monitor initialized")

    async def terminate(self) -> None:
        """Stop background work and close the direct t2i HTTP client."""

        self._stopping.set()
        tasks = list(self._tasks)
        self._tasks.clear()
        for task in tasks:
            task.cancel()
        if tasks:
            await asyncio.gather(*tasks, return_exceptions=True)
        if self._http_client is not None:
            await self._http_client.aclose()
            self._http_client = None
        self._probe_client = None
        logger.info("t2i health monitor terminated")

    async def _probe_loop(self) -> None:
        first_cycle = True
        while not self._stopping.is_set():
            if not first_cycle or bool(self.config.get("probe_on_startup", True)):
                try:
                    await self._run_probe_cycle()
                except asyncio.CancelledError:
                    raise
                except Exception as exc:
                    logger.exception(f"t2i scheduled probe cycle failed: {exc}")
            first_cycle = False
            if await self._wait_for_stop(self._probe_interval_seconds()):
                return

    async def _daily_report_loop(self) -> None:
        while not self._stopping.is_set():
            next_run = self._next_daily_report_time()
            wait_seconds = max(0.0, (next_run - datetime.now(next_run.tzinfo)).total_seconds())
            if await self._wait_for_stop(wait_seconds):
                return
            try:
                await self._send_daily_report()
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                logger.exception(f"t2i daily report failed: {exc}")

    async def _wait_for_stop(self, seconds: float) -> bool:
        try:
            await asyncio.wait_for(self._stopping.wait(), timeout=max(0.1, seconds))
            return True
        except asyncio.TimeoutError:
            return False

    async def _run_probe_cycle(self) -> list[ProbeResult]:
        probe_client = self._probe_client
        if probe_client is None:
            return []

        async with self._cycle_lock:
            targets = self._configured_targets()
            enabled_targets = [target for target in targets if target.enabled]
            if not enabled_targets:
                if not self._reported_missing_targets:
                    logger.warning("t2i health monitor has no enabled valid target")
                    self._reported_missing_targets = True
                return []
            self._reported_missing_targets = False

            semaphore = asyncio.Semaphore(
                min(len(enabled_targets), self._max_parallel_probes()),
            )
            retry_count = self._retry_count()
            retry_delay_seconds = self._retry_delay_seconds()

            async def probe_target(target: T2ITarget) -> ProbeResult:
                async with semaphore:
                    return await probe_client.probe(
                        target,
                        retry_count=retry_count,
                        retry_delay_seconds=retry_delay_seconds,
                    )

            results = list(
                await asyncio.gather(
                    *(probe_target(target) for target in enabled_targets),
                ),
            )
            notifications = await self._record_results(results)

        for notification in notifications:
            await self._send_failure_notification(notification)
        return results

    async def _record_results(
        self,
        results: list[ProbeResult],
    ) -> list[NotificationEvent]:
        if not results:
            return []

        notifications: list[NotificationEvent] = []
        async with self._state_lock:
            notification_enabled = bool(self._failure_push_umo())
            for result in results:
                notification = record_probe_result(
                    self._state,
                    result,
                    notification_enabled=notification_enabled,
                    send_recovery_notice=bool(
                        self.config.get("send_recovery_notice", True),
                    ),
                    cooldown_seconds=self._failure_notification_cooldown_seconds(),
                )
                if notification is not None:
                    notifications.append(notification)
            await self._save_state_locked()
        return notifications

    async def _send_daily_report(self) -> None:
        daily_umo = self._daily_push_umo()
        if not daily_umo:
            if not self._reported_missing_daily_umo:
                logger.warning(
                    "t2i daily report is enabled but daily_push_umo is not configured",
                )
                self._reported_missing_daily_umo = True
            return
        self._reported_missing_daily_umo = False

        targets = self._configured_targets()
        timezone_info = self._configured_timezone()
        # Holding this lock through delivery means that a concurrent probe is
        # placed into the next report period instead of being cleared by reset.
        async with self._state_lock:
            report = self._format_health_report(
                targets,
                timezone_info,
                title="t2i 健康日报",
            )
            delivered = await self._send_text(daily_umo, report)
            if not delivered:
                return
            self._state.reset_period(utc_now_iso())
            await self._save_state_locked()

    async def _send_failure_notification(self, notification: NotificationEvent) -> None:
        umo = self._failure_push_umo()
        if not umo:
            return
        timezone_info = self._configured_timezone()
        result = notification.result
        if notification.kind == "outage":
            message = "\n".join(
                [
                    "t2i 探测失败",
                    f"目标: {result.target_name} ({result.target_id})",
                    f"时间: {_format_timestamp(result.finished_at, timezone_info)}",
                    f"尝试: {result.attempts} 次（含重试）",
                    f"连续失败: {notification.consecutive_failures} 轮",
                    f"耗时: {result.latency_ms} ms",
                    f"错误: {_display_text(result.error or 'unknown error')}",
                ],
            )
        else:
            outage_started = _format_timestamp(
                notification.outage_started_at,
                timezone_info,
            )
            message = "\n".join(
                [
                    "t2i 已恢复",
                    f"目标: {result.target_name} ({result.target_id})",
                    f"恢复时间: {_format_timestamp(result.finished_at, timezone_info)}",
                    f"故障开始: {outage_started}",
                    f"本次耗时: {result.latency_ms} ms",
                ],
            )
        await self._send_text(umo, message)

    async def _send_text(self, umo: str, text: str) -> bool:
        try:
            sent = await self.context.send_message(
                umo,
                MessageChain().message(text).use_t2i(False),
            )
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            logger.warning(f"failed to push t2i health message: {exc}")
            return False
        if not sent:
            logger.warning("failed to push t2i health message: no matching platform for UMO")
        return bool(sent)

    async def _save_state_locked(self) -> None:
        if self._state_store is None:
            return
        try:
            await self._state_store.save(self._state)
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            logger.exception(f"failed to save t2i health state: {exc}")

    def _configured_targets(self) -> list[T2ITarget]:
        targets, errors = parse_targets(self.config.get("targets", []))
        current_errors = set(errors)
        for error in sorted(current_errors - self._reported_config_errors):
            logger.warning(f"invalid t2i health monitor configuration: {error}")
        self._reported_config_errors = current_errors
        return targets

    def _format_health_report(
        self,
        targets: list[T2ITarget],
        timezone_info: ZoneInfo,
        *,
        title: str,
    ) -> str:
        now = datetime.now(timezone_info)
        lines = [
            title,
            (
                "报告周期: "
                f"{_format_timestamp(self._state.period_started_at, timezone_info)}"
                f" 至 {now.strftime('%Y-%m-%d %H:%M:%S %Z')}"
            ),
            f"探测间隔: {self._probe_interval_seconds()} 秒",
        ]
        if not targets:
            lines.append("\n未配置有效的 t2i 目标。")
            return "\n".join(lines)

        for target in targets:
            lines.append("")
            if not target.enabled:
                lines.append(f"[{target.name}] 已禁用")
                continue

            health = self._state.targets.get(target.target_id)
            if health is None:
                lines.extend(
                    [
                        f"[{target.name}] 未探测",
                        "周期健康度: 无数据",
                        "累计健康度: 无数据",
                    ],
                )
                continue

            lines.extend(
                [
                    f"[{target.name}] {_status_label(health.status)}",
                    f"周期健康度: {_format_stats(health.period)}",
                    f"累计健康度: {_format_stats(health.cumulative)}",
                    f"周期平均耗时: {_format_latency(health.period.average_latency_ms)}",
                    f"最后探测: {_format_timestamp(health.last_probe_at, timezone_info)}",
                ],
            )
            if health.status == "failed":
                lines.append(f"最后错误: {_display_text(health.last_error or 'unknown error')}")
                lines.append(f"连续失败: {health.consecutive_failures} 轮")

        if self._reported_config_errors:
            lines.append("")
            lines.append(f"配置错误: {len(self._reported_config_errors)} 项，详见 AstrBot 日志")
        return "\n".join(lines)

    def _probe_interval_seconds(self) -> int:
        return _bounded_int(self.config.get("probe_interval_seconds", 60), 60, 10, 86400)

    def _retry_count(self) -> int:
        return _bounded_int(self.config.get("failure_retry_count", 2), 2, 0, 20)

    def _retry_delay_seconds(self) -> float:
        return _bounded_float(self.config.get("retry_delay_seconds", 5), 5, 0, 3600)

    def _max_parallel_probes(self) -> int:
        return _bounded_int(self.config.get("max_parallel_probes", 3), 3, 1, 20)

    def _failure_notification_cooldown_seconds(self) -> int:
        return _bounded_int(
            self.config.get("failure_notification_cooldown_seconds", 1800),
            1800,
            0,
            86400,
        )

    def _daily_push_umo(self) -> str:
        return str(self.config.get("daily_push_umo", "")).strip()

    def _failure_push_umo(self) -> str:
        return str(self.config.get("failure_push_umo", "")).strip()

    def _configured_timezone(self) -> ZoneInfo:
        raw_timezone = str(self.config.get("timezone", "Asia/Shanghai")).strip()
        try:
            return ZoneInfo(raw_timezone or "Asia/Shanghai")
        except ZoneInfoNotFoundError:
            logger.warning(f"invalid t2i monitor timezone '{raw_timezone}', using Asia/Shanghai")
            return ZoneInfo("Asia/Shanghai")

    def _next_daily_report_time(self) -> datetime:
        timezone_info = self._configured_timezone()
        hour, minute = self._daily_clock()
        now = datetime.now(timezone_info)
        scheduled = now.replace(hour=hour, minute=minute, second=0, microsecond=0)
        if scheduled <= now:
            scheduled += timedelta(days=1)
        return scheduled

    def _daily_clock(self) -> tuple[int, int]:
        raw_time = str(self.config.get("daily_push_time", "09:00")).strip()
        try:
            hour_text, minute_text = raw_time.split(":", maxsplit=1)
            hour, minute = int(hour_text), int(minute_text)
            if 0 <= hour <= 23 and 0 <= minute <= 59:
                return hour, minute
        except (TypeError, ValueError):
            pass
        logger.warning(f"invalid t2i monitor daily_push_time '{raw_time}', using 09:00")
        return 9, 0

    @filter.command_group("t2i-health")
    def t2i_health(self):
        """查看或立即执行 t2i 健康探测。"""

    @filter.permission_type(filter.PermissionType.ADMIN)
    @t2i_health.command("status")
    async def health_status(self, event: AstrMessageEvent):
        """查看所有 t2i 目标的当前健康统计。"""

        targets = self._configured_targets()
        async with self._state_lock:
            report = self._format_health_report(
                targets,
                self._configured_timezone(),
                title="t2i 当前健康状态",
            )
        yield event.plain_result(report).use_t2i(False)

    @filter.permission_type(filter.PermissionType.ADMIN)
    @t2i_health.command("probe")
    async def health_probe(self, event: AstrMessageEvent):
        """立即对所有启用的 t2i 目标执行一次完整探测。"""

        results = await self._run_probe_cycle()
        success_count = sum(1 for result in results if result.success)
        if results:
            summary = f"已完成 {len(results)} 个 t2i 目标探测：{success_count} 成功，{len(results) - success_count} 失败。"
        else:
            summary = "没有可探测的启用 t2i 目标。"
        targets = self._configured_targets()
        async with self._state_lock:
            report = self._format_health_report(
                targets,
                self._configured_timezone(),
                title="t2i 当前健康状态",
            )
        yield event.plain_result(f"{summary}\n\n{report}").use_t2i(False)


def _format_stats(stats: Any) -> str:
    health_percent = stats.health_percent
    if health_percent is None:
        return "无数据"
    return f"{health_percent:.2f}% ({stats.successful_cycles}/{stats.cycles} 成功)"


def _format_latency(value: int | None) -> str:
    return "无数据" if value is None else f"{value} ms"


def _format_timestamp(value: str | None, timezone_info: ZoneInfo) -> str:
    if not value:
        return "无"
    try:
        timestamp = datetime.fromisoformat(value)
        if timestamp.tzinfo is None:
            timestamp = timestamp.replace(tzinfo=timezone.utc)
        return timestamp.astimezone(timezone_info).strftime("%Y-%m-%d %H:%M:%S %Z")
    except ValueError:
        return _display_text(value)


def _status_label(status: str) -> str:
    return {"healthy": "正常", "failed": "失败", "unknown": "未探测"}.get(
        status,
        "未探测",
    )


def _display_text(value: str, limit: int = 180) -> str:
    text = " ".join(value.split())
    return text[:limit] if text else "无"


def _bounded_int(value: object, default: int, minimum: int, maximum: int) -> int:
    try:
        parsed = int(value)
    except (TypeError, ValueError):
        return default
    return max(minimum, min(maximum, parsed))


def _bounded_float(
    value: object,
    default: float,
    minimum: float,
    maximum: float,
) -> float:
    try:
        parsed = float(value)
    except (TypeError, ValueError):
        return default
    return max(minimum, min(maximum, parsed))
