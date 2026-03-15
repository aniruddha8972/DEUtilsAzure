"""
de_utils.alerts
---------------
Send pipeline alerts via email, Microsoft Teams webhooks, Slack webhooks,
or PagerDuty when jobs fail, DQ checks fail, or thresholds are breached.

Supports:
    EmailAlerter       — SMTP email (works with SendGrid, SES, Office 365)
    TeamsAlerter       — Microsoft Teams incoming webhook
    SlackAlerter       — Slack incoming webhook
    PagerDutyAlerter   — PagerDuty Events API v2
    AlertRouter        — Fan-out to multiple channels at once
    @alert_on_failure  — Decorator that auto-fires on exception

Usage
-----
>>> from de_utils.alerts import AlertRouter, TeamsAlerter, SlackAlerter, alert_on_failure

>>> router = AlertRouter(job_name="daily_orders_etl")
>>> router.add(TeamsAlerter(webhook_url="https://outlook.office.com/webhook/..."))
>>> router.add(SlackAlerter(webhook_url="https://hooks.slack.com/services/..."))

>>> # Manual alert
>>> router.send_failure("silver.orders", "Row count dropped to 0", details={"table": "silver.orders"})

>>> # Auto-alert on unhandled exception
>>> @alert_on_failure(router)
... def run_pipeline():
...     loader.full_load(raw_df, "orders")

>>> # DQ integration
>>> report = qc.run(df)
>>> if not report.passed:
...     router.send_dq_failure(report)
"""

from __future__ import annotations

import json
import smtplib
import urllib.request
import urllib.error
from dataclasses import dataclass, field
from datetime import datetime
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from functools import wraps
from typing import Any, Callable, Dict, List, Optional

from .utils import get_logger, DataEngineeringError

log = get_logger(__name__)


# ── Base alerter ──────────────────────────────────────────────────────────────

class BaseAlerter:
    """Abstract base — implement `_send(subject, body, severity, details)`."""

    name: str = "base"

    def send(
        self,
        subject:  str,
        body:     str,
        severity: str = "ERROR",
        details:  Optional[Dict] = None,
    ) -> bool:
        """Send an alert. Returns True on success, False on failure (never raises)."""
        try:
            self._send(subject, body, severity, details or {})
            log.info("[%s] Alert sent: %s", self.name, subject)
            return True
        except Exception as exc:
            log.error("[%s] Failed to send alert: %s", self.name, exc)
            return False

    def _send(self, subject: str, body: str, severity: str, details: Dict) -> None:
        raise NotImplementedError


# ── Email ─────────────────────────────────────────────────────────────────────

class EmailAlerter(BaseAlerter):
    """
    Send alerts via SMTP.

    Example
    -------
    >>> alerter = EmailAlerter(
    ...     smtp_host="smtp.office365.com",
    ...     smtp_port=587,
    ...     sender="etl-alerts@example.com",
    ...     recipients=["data-team@example.com"],
    ...     username="etl-alerts@example.com",
    ...     password="...",
    ...     use_tls=True,
    ... )
    """

    name = "email"

    def __init__(
        self,
        smtp_host:   str,
        smtp_port:   int,
        sender:      str,
        recipients:  List[str],
        username:    Optional[str] = None,
        password:    Optional[str] = None,
        use_tls:     bool = True,
    ):
        self.smtp_host  = smtp_host
        self.smtp_port  = smtp_port
        self.sender     = sender
        self.recipients = recipients
        self.username   = username
        self.password   = password
        self.use_tls    = use_tls

    def _send(self, subject: str, body: str, severity: str, details: Dict) -> None:
        msg = MIMEMultipart("alternative")
        msg["Subject"] = f"[{severity}] {subject}"
        msg["From"]    = self.sender
        msg["To"]      = ", ".join(self.recipients)

        html_details = "".join(
            f"<tr><td><b>{k}</b></td><td>{v}</td></tr>"
            for k, v in details.items()
        )
        html_body = f"""
        <html><body>
        <h2 style="color:{'red' if severity=='ERROR' else 'orange'}">
            [{severity}] {subject}
        </h2>
        <p>{body}</p>
        {'<table border="1" cellpadding="4">' + html_details + '</table>' if details else ''}
        <p style="color:grey;font-size:11px">
            Sent by de_utils at {datetime.utcnow().isoformat()} UTC
        </p>
        </body></html>
        """
        msg.attach(MIMEText(body, "plain"))
        msg.attach(MIMEText(html_body, "html"))

        with smtplib.SMTP(self.smtp_host, self.smtp_port) as server:
            if self.use_tls:
                server.starttls()
            if self.username and self.password:
                server.login(self.username, self.password)
            server.sendmail(self.sender, self.recipients, msg.as_string())


# ── Microsoft Teams ───────────────────────────────────────────────────────────

class TeamsAlerter(BaseAlerter):
    """
    Send alerts to a Microsoft Teams channel via an Incoming Webhook.

    Example
    -------
    >>> alerter = TeamsAlerter(
    ...     webhook_url="https://outlook.office.com/webhook/YOUR-WEBHOOK-URL"
    ... )
    """

    name = "teams"

    def __init__(self, webhook_url: str, timeout: int = 10):
        self.webhook_url = webhook_url
        self.timeout     = timeout

    def _send(self, subject: str, body: str, severity: str, details: Dict) -> None:
        color = {"ERROR": "FF0000", "WARNING": "FFA500", "INFO": "0078D7"}.get(severity, "808080")
        facts = [{"name": k, "value": str(v)} for k, v in details.items()]
        payload = {
            "@type":      "MessageCard",
            "@context":   "https://schema.org/extensions",
            "themeColor": color,
            "summary":    subject,
            "sections": [{
                "activityTitle":    f"**[{severity}]** {subject}",
                "activitySubtitle": f"de_utils pipeline alert — {datetime.utcnow().strftime('%Y-%m-%d %H:%M UTC')}",
                "activityText":     body,
                "facts":            facts,
            }],
        }
        self._post(payload)

    def _post(self, payload: Dict) -> None:
        data    = json.dumps(payload).encode("utf-8")
        request = urllib.request.Request(
            self.webhook_url,
            data=data,
            headers={"Content-Type": "application/json"},
            method="POST",
        )
        with urllib.request.urlopen(request, timeout=self.timeout) as resp:
            if resp.status not in (200, 202):
                raise DataEngineeringError(f"Teams webhook returned HTTP {resp.status}")


# ── Slack ─────────────────────────────────────────────────────────────────────

class SlackAlerter(BaseAlerter):
    """
    Send alerts to a Slack channel via an Incoming Webhook.

    Example
    -------
    >>> alerter = SlackAlerter(
    ...     webhook_url="https://hooks.slack.com/services/T.../B.../..."
    ... )
    """

    name = "slack"

    def __init__(self, webhook_url: str, timeout: int = 10, username: str = "de_utils"):
        self.webhook_url = webhook_url
        self.timeout     = timeout
        self.username    = username

    def _send(self, subject: str, body: str, severity: str, details: Dict) -> None:
        emoji = {"ERROR": ":red_circle:", "WARNING": ":warning:", "INFO": ":information_source:"}.get(severity, ":bell:")
        fields = [{"title": k, "value": str(v), "short": True} for k, v in details.items()]
        color  = {"ERROR": "danger", "WARNING": "warning", "INFO": "good"}.get(severity, "#808080")
        payload = {
            "username": self.username,
            "attachments": [{
                "fallback": f"[{severity}] {subject}",
                "color":    color,
                "pretext":  f"{emoji} *[{severity}]* {subject}",
                "text":     body,
                "fields":   fields,
                "footer":   f"de_utils | {datetime.utcnow().strftime('%Y-%m-%d %H:%M UTC')}",
            }],
        }
        data    = json.dumps(payload).encode("utf-8")
        request = urllib.request.Request(
            self.webhook_url,
            data=data,
            headers={"Content-Type": "application/json"},
            method="POST",
        )
        with urllib.request.urlopen(request, timeout=self.timeout) as resp:
            if resp.status not in (200, 202):
                raise DataEngineeringError(f"Slack webhook returned HTTP {resp.status}")


# ── PagerDuty ─────────────────────────────────────────────────────────────────

class PagerDutyAlerter(BaseAlerter):
    """
    Create PagerDuty incidents via the Events API v2.

    Example
    -------
    >>> alerter = PagerDutyAlerter(
    ...     integration_key="YOUR-PAGERDUTY-INTEGRATION-KEY",
    ...     severity="critical",
    ... )
    """

    name = "pagerduty"
    _ENDPOINT = "https://events.pagerduty.com/v2/enqueue"

    def __init__(
        self,
        integration_key: str,
        severity:        str = "critical",
        timeout:         int = 10,
    ):
        self.integration_key = integration_key
        self.default_severity= severity
        self.timeout         = timeout

    def _send(self, subject: str, body: str, severity: str, details: Dict) -> None:
        pd_sev = {"ERROR": "critical", "WARNING": "warning", "INFO": "info"}.get(severity, "error")
        payload = {
            "routing_key":  self.integration_key,
            "event_action": "trigger",
            "payload": {
                "summary":   subject,
                "source":    "de_utils",
                "severity":  pd_sev,
                "custom_details": {"body": body, **details},
                "timestamp": datetime.utcnow().isoformat() + "Z",
            },
        }
        data    = json.dumps(payload).encode("utf-8")
        request = urllib.request.Request(
            self._ENDPOINT, data=data,
            headers={"Content-Type": "application/json"},
            method="POST",
        )
        with urllib.request.urlopen(request, timeout=self.timeout) as resp:
            if resp.status not in (200, 202):
                raise DataEngineeringError(f"PagerDuty API returned HTTP {resp.status}")


# ── AlertRouter ───────────────────────────────────────────────────────────────

class AlertRouter:
    """
    Fan-out alerts to multiple channels simultaneously.
    Never raises — a failed channel is logged and skipped.

    Example
    -------
    >>> router = AlertRouter(job_name="daily_orders")
    >>> router.add(TeamsAlerter(webhook_url="..."))
    >>> router.add(SlackAlerter(webhook_url="..."))
    >>> router.add(EmailAlerter(smtp_host="smtp.office365.com", ...))
    """

    def __init__(self, job_name: str = "de_utils_job"):
        self.job_name = job_name
        self._alerters: List[BaseAlerter] = []

    def add(self, alerter: BaseAlerter) -> "AlertRouter":
        self._alerters.append(alerter)
        return self

    def send(
        self,
        subject:  str,
        body:     str,
        severity: str = "ERROR",
        details:  Optional[Dict] = None,
    ) -> Dict[str, bool]:
        """Send to all registered channels. Returns {channel_name: success}."""
        d = {"job": self.job_name, **(details or {})}
        return {a.name: a.send(subject, body, severity, d) for a in self._alerters}

    def send_failure(self, table: str, message: str, error: Optional[Exception] = None, **extra) -> None:
        details = {"table": table, **extra}
        if error:
            details["error"] = str(error)
        self.send(
            subject=f"Pipeline FAILED — {self.job_name} → {table}",
            body=message,
            severity="ERROR",
            details=details,
        )

    def send_warning(self, message: str, **details) -> None:
        self.send(
            subject=f"Pipeline WARNING — {self.job_name}",
            body=message,
            severity="WARNING",
            details=details,
        )

    def send_success(self, table: str, rows_written: int, elapsed: float) -> None:
        self.send(
            subject=f"Pipeline SUCCESS — {self.job_name} → {table}",
            body=f"Loaded {rows_written:,} rows in {elapsed:.1f}s",
            severity="INFO",
            details={"table": table, "rows_written": rows_written, "elapsed_seconds": elapsed},
        )

    def send_dq_failure(self, report: Any) -> None:
        """Fire when a DataQualityChecker report has failures."""
        body = f"{report.error_count} DQ rule(s) failed on '{report.table_name}'"
        details = {
            "table":         report.table_name,
            "total_rows":    report.total_rows,
            "errors":        report.error_count,
            "warnings":      report.warning_count,
            "failed_rules":  ", ".join(
                f"{r.rule_type}({r.column or 'table'})"
                for r in report.results
                if not r.passed
            ),
        }
        self.send(
            subject=f"DQ FAILED — {report.table_name}",
            body=body,
            severity="ERROR",
            details=details,
        )


# ── @alert_on_failure decorator ───────────────────────────────────────────────

def alert_on_failure(router: AlertRouter, table: str = ""):
    """
    Decorator that fires an alert if the wrapped function raises an exception.
    The exception is always re-raised after alerting.

    Example
    -------
    >>> @alert_on_failure(router, table="silver.orders")
    ... def run_silver_load():
    ...     loader.full_load(raw_df, "orders")
    """
    def decorator(fn: Callable) -> Callable:
        @wraps(fn)
        def wrapper(*args, **kwargs):
            try:
                return fn(*args, **kwargs)
            except Exception as exc:
                router.send_failure(
                    table=table or fn.__name__,
                    message=f"Unhandled exception in {fn.__name__}: {exc}",
                    error=exc,
                )
                raise
        return wrapper
    return decorator
