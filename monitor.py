"""Direct t2i HTTP probing primitives.

This module deliberately talks to a configured t2i service over HTTP instead of
using AstrBot's renderer. A successful probe means both the render request and
the returned image download completed successfully.
"""

from __future__ import annotations

import asyncio
import json
import re
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any
from urllib.parse import quote, urlparse

import httpx


MAX_IMAGE_BYTES = 2 * 1024 * 1024
TARGET_ID_PATTERN = re.compile(r"^[A-Za-z0-9_.-]{1,64}$")

# Keep the document deterministic and physically small. The t2i API's clip
# option prevents a default browser viewport from becoming the probe image.
SMALL_PROBE_HTML = """<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8">
  <style>
    * { box-sizing: border-box; }
    html, body { width: 180px; height: 72px; margin: 0; overflow: hidden; }
    body { display: flex; align-items: center; justify-content: center; background: #ffffff; color: #17212b; font: 600 14px/1.2 sans-serif; }
    .probe { display: flex; align-items: center; gap: 8px; }
    .dot { width: 10px; height: 10px; border-radius: 50%; background: #1a9a5a; }
  </style>
</head>
<body><div class="probe"><span class="dot"></span><span>t2i health probe</span></div></body>
</html>"""


class TargetConfigError(ValueError):
    """Raised when a configured t2i target cannot be used safely."""


class T2IProtocolError(RuntimeError):
    """Raised when a t2i endpoint responds outside its documented contract."""


@dataclass(frozen=True, slots=True)
class T2ITarget:
    """A configured t2i service."""

    target_id: str
    name: str
    base_url: str
    enabled: bool
    timeout_seconds: float

    @classmethod
    def from_config(cls, raw: dict[str, Any]) -> "T2ITarget":
        target_id = str(raw.get("id", "")).strip()
        if not TARGET_ID_PATTERN.fullmatch(target_id):
            raise TargetConfigError(
                "target id must contain 1-64 letters, numbers, dots, underscores, or hyphens",
            )

        name = str(raw.get("name", "")).strip() or target_id
        base_url = _normalize_base_url(str(raw.get("base_url", "")).strip())
        timeout_seconds = _positive_float(raw.get("timeout_seconds", 15), "timeout_seconds")
        if timeout_seconds > 120:
            raise TargetConfigError("timeout_seconds must not exceed 120")

        return cls(
            target_id=target_id,
            name=name,
            base_url=base_url,
            enabled=bool(raw.get("enabled", True)),
            timeout_seconds=timeout_seconds,
        )


@dataclass(frozen=True, slots=True)
class ProbeResult:
    """The final result of one scheduled probe cycle for one target."""

    target_id: str
    target_name: str
    success: bool
    attempts: int
    latency_ms: int
    finished_at: str
    error: str | None = None
    image_url: str | None = None


def parse_targets(raw_targets: object) -> tuple[list[T2ITarget], list[str]]:
    """Parse the template-list configuration without letting one bad target stop all probes."""

    if raw_targets is None:
        return [], []
    if not isinstance(raw_targets, list):
        return [], ["targets must be a list"]

    targets: list[T2ITarget] = []
    errors: list[str] = []
    seen_ids: set[str] = set()
    for index, raw_target in enumerate(raw_targets, start=1):
        if not isinstance(raw_target, dict):
            errors.append(f"target #{index} must be an object")
            continue

        # An empty disabled template row is harmless and common while editing in
        # the dashboard, so do not turn it into a recurring warning.
        if not raw_target.get("enabled", True) and not (
            raw_target.get("id") or raw_target.get("base_url")
        ):
            continue

        try:
            target = T2ITarget.from_config(raw_target)
        except TargetConfigError as exc:
            errors.append(f"target #{index}: {exc}")
            continue

        if target.target_id in seen_ids:
            errors.append(f"target #{index}: duplicate target id '{target.target_id}'")
            continue
        seen_ids.add(target.target_id)
        targets.append(target)

    return targets, errors


class T2IProbeClient:
    """Probe a t2i service through its public text2img API."""

    def __init__(self, client: httpx.AsyncClient) -> None:
        self._client = client

    async def probe(
        self,
        target: T2ITarget,
        *,
        retry_count: int,
        retry_delay_seconds: float,
    ) -> ProbeResult:
        """Render and retrieve a small image, retrying only failed full attempts."""

        retry_count = max(0, retry_count)
        started = time.perf_counter()
        errors: list[str] = []

        for attempt in range(1, retry_count + 2):
            try:
                image_url = await self._probe_once(target)
                return ProbeResult(
                    target_id=target.target_id,
                    target_name=target.name,
                    success=True,
                    attempts=attempt,
                    latency_ms=_elapsed_ms(started),
                    finished_at=_utc_now_iso(),
                    image_url=image_url,
                )
            except asyncio.CancelledError:
                raise
            except Exception as exc:  # A failed t2i response is an expected probe outcome.
                errors.append(_describe_error(exc))
                if attempt <= retry_count and retry_delay_seconds > 0:
                    await asyncio.sleep(retry_delay_seconds)

        return ProbeResult(
            target_id=target.target_id,
            target_name=target.name,
            success=False,
            attempts=retry_count + 1,
            latency_ms=_elapsed_ms(started),
            finished_at=_utc_now_iso(),
            error="; ".join(errors[-2:]) or "unknown t2i probe failure",
        )

    async def _probe_once(self, target: T2ITarget) -> str:
        response = await self._client.post(
            f"{target.base_url}/text2img/generate",
            json=_render_request_payload(),
            timeout=target.timeout_seconds,
        )
        response.raise_for_status()

        try:
            payload = response.json()
        except json.JSONDecodeError as exc:
            raise T2IProtocolError("generate response is not valid JSON") from exc

        image_id = _extract_image_id(payload)
        image_url = _build_image_url(target.base_url, image_id)
        await self._verify_image(image_url, target.timeout_seconds)
        return image_url

    async def _verify_image(self, image_url: str, timeout_seconds: float) -> None:
        async with self._client.stream(
            "GET",
            image_url,
            timeout=timeout_seconds,
        ) as response:
            response.raise_for_status()
            content_type = response.headers.get("content-type", "").lower()
            image_bytes = bytearray()
            async for chunk in response.aiter_bytes():
                image_bytes.extend(chunk)
                if len(image_bytes) > MAX_IMAGE_BYTES:
                    raise T2IProtocolError(
                        f"returned image exceeds {MAX_IMAGE_BYTES // 1024} KiB limit",
                    )

        if not image_bytes:
            raise T2IProtocolError("returned image is empty")
        if not _looks_like_image(bytes(image_bytes), content_type):
            raise T2IProtocolError("returned data is not an image")


def _render_request_payload() -> dict[str, Any]:
    """Build the documented `/text2img/generate` request payload."""

    return {
        "html": SMALL_PROBE_HTML,
        "options": {
            "type": "png",
            "full_page": False,
            "clip": {"x": 0, "y": 0, "width": 180, "height": 72},
            "animations": "disabled",
            "caret": "hide",
            "scale": "css",
            "viewport_width": 180,
            "device_scale_factor_level": "normal",
        },
        "json": True,
    }


def _extract_image_id(payload: object) -> str:
    if not isinstance(payload, dict):
        raise T2IProtocolError("generate response must be an object")
    if payload.get("code") != 0:
        message = _one_line(payload.get("message", "unknown error"))
        raise T2IProtocolError(f"generate returned code {payload.get('code')}: {message}")

    data = payload.get("data")
    if not isinstance(data, dict):
        raise T2IProtocolError("generate response has no data object")
    image_id = data.get("id")
    if not isinstance(image_id, str) or not image_id.strip():
        raise T2IProtocolError("generate response has no image id")
    return image_id.strip()


def _build_image_url(base_url: str, image_id: str) -> str:
    parsed = urlparse(image_id)
    if (
        parsed.scheme
        or parsed.netloc
        or parsed.query
        or parsed.fragment
        or image_id.startswith("/")
    ):
        raise T2IProtocolError("image id must be a relative t2i data path")

    parts = image_id.split("/")
    if not parts or any(not part or part in {".", ".."} for part in parts):
        raise T2IProtocolError("image id contains an unsafe path")
    return f"{base_url}/text2img/{quote(image_id, safe='/')}"


def _looks_like_image(data: bytes, content_type: str) -> bool:
    magic = (
        data.startswith(b"\x89PNG\r\n\x1a\n")
        or data.startswith(b"\xff\xd8\xff")
        or data.startswith((b"GIF87a", b"GIF89a"))
        or (data.startswith(b"RIFF") and data[8:12] == b"WEBP")
    )
    return magic or (content_type.startswith("image/") and len(data) >= 16)


def _normalize_base_url(value: str) -> str:
    if not value:
        raise TargetConfigError("base_url is required")
    parsed = urlparse(value)
    if parsed.scheme not in {"http", "https"} or not parsed.netloc:
        raise TargetConfigError("base_url must be an absolute http(s) URL")
    if parsed.username or parsed.password or parsed.query or parsed.fragment:
        raise TargetConfigError("base_url must not contain credentials, query, or fragment")
    return value.rstrip("/")


def _positive_float(value: object, field_name: str) -> float:
    try:
        result = float(value)
    except (TypeError, ValueError) as exc:
        raise TargetConfigError(f"{field_name} must be a positive number") from exc
    if result <= 0:
        raise TargetConfigError(f"{field_name} must be a positive number")
    return result


def _elapsed_ms(started: float) -> int:
    return max(0, round((time.perf_counter() - started) * 1000))


def _utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


def _one_line(value: object, limit: int = 160) -> str:
    text = " ".join(str(value).split())
    return text[:limit] if text else "unknown error"


def _describe_error(exc: Exception) -> str:
    if isinstance(exc, httpx.HTTPStatusError):
        return f"HTTP {exc.response.status_code}"
    if isinstance(exc, httpx.TimeoutException):
        return "request timed out"
    if isinstance(exc, httpx.HTTPError):
        return _one_line(exc)
    return _one_line(exc)
