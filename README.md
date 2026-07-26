# t2i Health Monitor

AstrBot plugin for actively monitoring one or more [t2i services](https://t2i.ciallo.de5.net/docs). It renders a small deterministic image through each configured service, verifies that image can be fetched, tracks health across restarts, and pushes a daily report plus outage notifications to configured UMOs.

## Behavior

The plugin does not call AstrBot's `text_to_image` or `html_render` helpers and does not use a local plugin image path for probing. Each probe calls the configured service directly:

1. `POST {base_url}/text2img/generate` with a small HTML document, `json: true`, and constrained screenshot options.
2. Read `data.id` from the documented successful response.
3. `GET {base_url}/text2img/{data.id}` and verify it is a non-empty image.

A probe round is successful only when both steps complete. One round consists of the initial attempt plus the configured retries. Health is calculated as successful rounds divided by all completed rounds, so a successful retry counts as a successful round while the number of attempted requests remains visible in persisted statistics.

## Installation

Place or clone this directory under AstrBot's `data/plugins/` directory, then reload plugins from AstrBot. AstrBot installs `requirements.txt` automatically when needed.

```bash
git clone https://github.com/clown145/astrbot_plugin_t2i_health_monitor.git data/plugins/astrbot_plugin_t2i_health_monitor
```

The plugin requires AstrBot `>=4.16,<5` and uses `httpx` for direct asynchronous HTTP calls.

## Configuration

Configure the plugin in AstrBot WebUI.

| Field | Purpose |
| --- | --- |
| `targets` | Repeated t2i target entries. Each has a stable `id`, display name, service root `base_url`, enabled state, and request timeout. |
| `probe_interval_seconds` | Delay between completed probe cycles. |
| `daily_push_time` / `timezone` | Daily report schedule, such as `09:00` and `Asia/Shanghai`. |
| `daily_push_umo` | UMO that receives the full daily health report. |
| `failure_retry_count` / `retry_delay_seconds` | Number of retries after a failure and the wait between attempts. |
| `failure_push_umo` | UMO that receives an outage message after all retries fail. |
| `failure_notification_cooldown_seconds` | Minimum spacing for repeated notices during the same outage. Set `0` to notify on every failed round. |
| `send_recovery_notice` | Whether to send a recovery message to `failure_push_umo`. |

Use a t2i service root URL, not a docs or endpoint URL. For example:

```text
base_url = https://t2i.ciallo.de5.net
```

The plugin appends `/text2img/generate` and `/text2img/{data.id}` itself.

An AstrBot UMO has the form `platform_id:message_type:session_id`. For a group, it is commonly similar to `aiocqhttp:group:123456`. Send AstrBot's `/sid` command from the target group to obtain the exact UMO for that platform.

Keep each target `id` stable. Its ID is the key used for accumulated health data; changing it starts a new history for that target.

## Daily Report

Every enabled target appears in the report with:

- Current status and the last probe time.
- Health for the complete period since the previous successfully delivered daily report.
- Cumulative health since the plugin first observed that target, including restarts.
- Period average response time.

The report period is reset only after AstrBot confirms delivery to `daily_push_umo`. If delivery fails, the period remains intact for the next report.

## Commands

Both commands require an AstrBot administrator:

```text
/t2i-health status
/t2i-health probe
```

`status` reads the current statistics. `probe` immediately runs one full probe round for every enabled target and applies the same retry and failure-notification rules as scheduled monitoring.

## Development

Run the focused unit suite after installing the dependency:

```bash
python -m unittest discover -s tests -v
```

The tests use `httpx.MockTransport`; they do not call a real t2i service.
