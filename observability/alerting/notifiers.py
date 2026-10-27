"""notifiers.py — Alert sinks. Webhook payload is Slack/PagerDuty/Alertmanager friendly."""
from __future__ import annotations

import logging

import httpx

from ..config import Settings, get_settings
from ..domain.models import Alert
from ..domain.ports import Notifier

log = logging.getLogger(__name__)


class ConsoleNotifier(Notifier):
    def notify(self, alert: Alert) -> bool:
        log.warning("ALERT [%s] %s — %s (%s)", alert.severity.value, alert.title, alert.message, alert.service)
        return True


class InMemoryNotifier(Notifier):
    def __init__(self):
        self.sent: list[Alert] = []

    def notify(self, alert: Alert) -> bool:
        self.sent.append(alert)
        return True


class WebhookNotifier(Notifier):
    def __init__(self, url: str, timeout: float = 5.0):
        if not url.lower().startswith(("http://", "https://")):
            raise ValueError("webhook url must be http(s)")
        self.url = url
        self.timeout = timeout

    def notify(self, alert: Alert) -> bool:
        payload = {
            "text": f"[{alert.severity.value}] {alert.title}\n{alert.message}",
            "alert": alert.to_dict(),
            "labels": {"service": alert.service, "severity": alert.severity.value, **alert.labels},
        }
        try:
            r = httpx.post(self.url, json=payload, timeout=self.timeout)
            return r.status_code < 300
        except httpx.HTTPError as e:
            log.error("webhook notify failed: %s", e)
            return False


def build_notifiers(settings: Settings | None = None) -> list[Notifier]:
    s = settings or get_settings()
    out: list[Notifier] = [ConsoleNotifier()]
    if s.alert_webhook_url:
        out.append(WebhookNotifier(s.alert_webhook_url))
    return out
