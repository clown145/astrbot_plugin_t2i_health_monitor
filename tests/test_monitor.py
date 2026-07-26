from __future__ import annotations

import json
import sys
import unittest
from pathlib import Path

import httpx


PROJECTS_DIR = Path(__file__).resolve().parents[2]
if str(PROJECTS_DIR) not in sys.path:
    sys.path.insert(0, str(PROJECTS_DIR))

from astrbot_plugin_t2i_health_monitor.monitor import (  # noqa: E402
    T2IProbeClient,
    T2ITarget,
    normalize_umos,
    parse_targets,
)


PNG_BYTES = b"\x89PNG\r\n\x1a\n" + b"test image payload"


class T2IProbeClientTests(unittest.IsolatedAsyncioTestCase):
    async def test_probe_renders_then_downloads_the_image(self) -> None:
        calls: list[tuple[str, str]] = []

        def handler(request: httpx.Request) -> httpx.Response:
            calls.append((request.method, request.url.path))
            if request.method == "POST":
                payload = json.loads(request.content)
                self.assertTrue(payload["json"])
                self.assertEqual(payload["options"]["clip"], {"x": 0, "y": 0, "width": 180, "height": 72})
                return httpx.Response(
                    200,
                    json={"code": 0, "message": "success", "data": {"id": "data/probe.png"}},
                )
            return httpx.Response(
                200,
                headers={"content-type": "image/png"},
                content=PNG_BYTES,
            )

        async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as client:
            result = await T2IProbeClient(client).probe(
                T2ITarget(
                    target_id="primary",
                    name="Primary",
                    base_url="https://t2i.example.test",
                    enabled=True,
                    timeout_seconds=5,
                ),
                retry_count=0,
                retry_delay_seconds=0,
            )

        self.assertTrue(result.success)
        self.assertEqual(result.attempts, 1)
        self.assertEqual(
            calls,
            [
                ("POST", "/text2img/generate"),
                ("GET", "/text2img/data/probe.png"),
            ],
        )

    async def test_probe_retries_a_failed_generation(self) -> None:
        post_calls = 0

        def handler(request: httpx.Request) -> httpx.Response:
            nonlocal post_calls
            if request.method == "POST":
                post_calls += 1
                if post_calls == 1:
                    return httpx.Response(503, text="unavailable")
                return httpx.Response(
                    200,
                    json={"code": 0, "data": {"id": "data/after-retry.png"}},
                )
            return httpx.Response(
                200,
                headers={"content-type": "image/png"},
                content=PNG_BYTES,
            )

        async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as client:
            result = await T2IProbeClient(client).probe(
                T2ITarget(
                    target_id="primary",
                    name="Primary",
                    base_url="https://t2i.example.test",
                    enabled=True,
                    timeout_seconds=5,
                ),
                retry_count=1,
                retry_delay_seconds=0,
            )

        self.assertTrue(result.success)
        self.assertEqual(result.attempts, 2)
        self.assertEqual(post_calls, 2)

    async def test_probe_rejects_an_external_image_url(self) -> None:
        def handler(request: httpx.Request) -> httpx.Response:
            return httpx.Response(
                200,
                json={"code": 0, "data": {"id": "https://untrusted.example/image.png"}},
            )

        async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as client:
            result = await T2IProbeClient(client).probe(
                T2ITarget(
                    target_id="primary",
                    name="Primary",
                    base_url="https://t2i.example.test",
                    enabled=True,
                    timeout_seconds=5,
                ),
                retry_count=0,
                retry_delay_seconds=0,
            )

        self.assertFalse(result.success)
        self.assertIn("relative t2i data path", result.error or "")


class TargetParsingTests(unittest.TestCase):
    def test_multiple_targets_and_invalid_entries_are_isolated(self) -> None:
        targets, errors = parse_targets(
            [
                {
                    "id": "first",
                    "name": "First",
                    "base_url": "https://first.example",
                    "enabled": True,
                    "timeout_seconds": 10,
                },
                {
                    "id": "first",
                    "name": "Duplicate",
                    "base_url": "https://duplicate.example",
                    "enabled": True,
                    "timeout_seconds": 10,
                },
                {
                    "id": "second",
                    "name": "Second",
                    "base_url": "https://second.example/",
                    "enabled": True,
                    "timeout_seconds": 12,
                },
            ],
        )

        self.assertEqual([target.target_id for target in targets], ["first", "second"])
        self.assertEqual(targets[1].base_url, "https://second.example")
        self.assertEqual(len(errors), 1)

    def test_normalize_umos_keeps_order_deduplicates_and_migrates_legacy_value(self) -> None:
        umos = normalize_umos(
            [
                "aiocqhttp:group:100",
                " aiocqhttp:group:200 ",
                "aiocqhttp:group:100",
                123,
                "",
            ],
            "aiocqhttp:group:300",
        )

        self.assertEqual(
            umos,
            [
                "aiocqhttp:group:100",
                "aiocqhttp:group:200",
                "aiocqhttp:group:300",
            ],
        )
