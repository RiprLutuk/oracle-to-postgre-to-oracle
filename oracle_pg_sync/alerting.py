from __future__ import annotations

import json
import logging
import smtplib
from email.message import EmailMessage
from typing import Any
from urllib import request

from oracle_pg_sync.config import AppConfig


def send_alert(
    config: AppConfig,
    *,
    event: str,
    payload: dict[str, Any],
    logger: logging.Logger | None = None,
) -> bool:
    alert = config.job.alert
    logger = logger or logging.getLogger("oracle_pg_sync")
    enabled_events = {str(item).lower() for item in (alert.on or [])}
    if not alert.type or event.lower() not in enabled_events:
        return False
    if alert.type == "webhook":
        return _send_webhook(alert.url, payload, timeout_seconds=alert.timeout_seconds, logger=logger)
    if alert.type == "email":
        return _send_email(config, subject=_subject(event, payload), payload=payload, logger=logger)
    logger.warning("Unsupported alert type: %s", alert.type)
    return False


def _send_webhook(url: str, payload: dict[str, Any], *, timeout_seconds: int, logger: logging.Logger) -> bool:
    if not url:
        logger.warning("Webhook alert skipped: url is empty")
        return False
    body = json.dumps(payload, sort_keys=True).encode("utf-8")
    req = request.Request(
        url,
        data=body,
        headers={"Content-Type": "application/json"},
        method="POST",
    )
    try:
        with request.urlopen(req, timeout=max(1, int(timeout_seconds or 10))):
            return True
    except Exception:
        logger.exception("Webhook alert failed")
        return False


def _send_email(config: AppConfig, *, subject: str, payload: dict[str, Any], logger: logging.Logger) -> bool:
    settings = config.job.alert.email
    if not settings.to or not settings.smtp_host:
        logger.warning("Email alert skipped: smtp_host/to missing")
        return False
    message = EmailMessage()
    message["Subject"] = subject
    message["From"] = settings.from_address or settings.username or "oracle-pg-sync@localhost"
    message["To"] = ", ".join(settings.to)
    message.set_content(json.dumps(payload, indent=2, sort_keys=True))
    try:
        with smtplib.SMTP(settings.smtp_host, settings.smtp_port, timeout=max(1, int(config.job.alert.timeout_seconds or 10))) as smtp:
            if settings.use_tls:
                smtp.starttls()
            if settings.username:
                smtp.login(settings.username, settings.password)
            smtp.send_message(message)
        return True
    except Exception:
        logger.exception("Email alert failed")
        return False


def _subject(event: str, payload: dict[str, Any]) -> str:
    run_id = payload.get("run_id") or "-"
    direction = payload.get("direction") or "-"
    return f"[oracle-pg-sync] {event} run={run_id} direction={direction}"
