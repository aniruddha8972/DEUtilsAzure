"""tests/test_alerts.py — Tests for alerting system."""
import pytest
from unittest.mock import patch, MagicMock, call
from de_utils.alerts import (
    AlertRouter, TeamsAlerter, SlackAlerter, PagerDutyAlerter,
    EmailAlerter, alert_on_failure, BaseAlerter,
)


class MockAlerter(BaseAlerter):
    """In-memory alerter for testing."""
    name = "mock"
    def __init__(self):
        self.sent = []
    def _send(self, subject, body, severity, details):
        self.sent.append({"subject": subject, "body": body, "severity": severity, "details": details})


class TestBaseAlerter:

    def test_send_returns_true_on_success(self):
        a = MockAlerter()
        assert a.send("test", "body") is True

    def test_send_returns_false_on_exception(self):
        class BrokenAlerter(BaseAlerter):
            name = "broken"
            def _send(self, *a, **k): raise RuntimeError("boom")
        assert BrokenAlerter().send("x", "y") is False

    def test_send_never_raises(self):
        class BrokenAlerter(BaseAlerter):
            name = "broken"
            def _send(self, *a, **k): raise Exception("always fails")
        BrokenAlerter().send("title", "body")  # must not raise


class TestAlertRouter:

    def test_add_returns_self(self):
        router = AlertRouter()
        assert router.add(MockAlerter()) is router

    def test_send_calls_all_alerters(self):
        a1 = MockAlerter()
        a2 = MockAlerter()
        router = AlertRouter()
        router.add(a1).add(a2)
        router.send("title", "body")
        assert len(a1.sent) == 1
        assert len(a2.sent) == 1

    def test_send_failure_includes_table(self):
        a = MockAlerter()
        AlertRouter().add(a).send_failure("silver.orders", "Row count zero")
        assert "silver.orders" in a.sent[0]["subject"]

    def test_send_warning_severity(self):
        a = MockAlerter()
        AlertRouter().add(a).send_warning("Something odd")
        assert a.sent[0]["severity"] == "WARNING"

    def test_send_success_severity(self):
        a = MockAlerter()
        AlertRouter().add(a).send_success("silver.orders", 5000, 12.3)
        assert a.sent[0]["severity"] == "INFO"

    def test_send_dq_failure(self):
        a = MockAlerter()
        router = AlertRouter()
        router.add(a)

        # Mock DQReport
        mock_report = MagicMock()
        mock_report.table_name   = "silver.orders"
        mock_report.total_rows   = 1000
        mock_report.error_count  = 2
        mock_report.warning_count= 0
        result_mock = MagicMock()
        result_mock.passed    = False
        result_mock.rule_type = "not_null"
        result_mock.column    = "order_id"
        mock_report.results = [result_mock]

        router.send_dq_failure(mock_report)
        assert len(a.sent) == 1
        assert "silver.orders" in a.sent[0]["subject"]

    def test_failed_channel_does_not_block_others(self):
        class BrokenAlerter(BaseAlerter):
            name = "broken"
            def _send(self, *a, **k): raise RuntimeError("fail")

        good = MockAlerter()
        router = AlertRouter()
        router.add(BrokenAlerter()).add(good)
        results = router.send("test", "body")
        assert results["mock"] is True
        assert results["broken"] is False
        assert len(good.sent) == 1

    def test_result_dict_keyed_by_channel_name(self):
        a = MockAlerter()
        results = AlertRouter().add(a).send("x", "y")
        assert "mock" in results


class TestAlertOnFailureDecorator:

    def test_fires_on_exception(self):
        a = MockAlerter()
        router = AlertRouter().add(a)

        @alert_on_failure(router, table="test_table")
        def failing_fn():
            raise ValueError("oops")

        with pytest.raises(ValueError):
            failing_fn()

        assert len(a.sent) == 1
        assert "test_table" in a.sent[0]["subject"]

    def test_does_not_fire_on_success(self):
        a = MockAlerter()
        router = AlertRouter().add(a)

        @alert_on_failure(router)
        def ok_fn():
            return "ok"

        ok_fn()
        assert len(a.sent) == 0

    def test_re_raises_exception(self):
        @alert_on_failure(AlertRouter())
        def boom():
            raise RuntimeError("re-raised")
        with pytest.raises(RuntimeError):
            boom()

    def test_preserves_function_name(self):
        @alert_on_failure(AlertRouter())
        def my_pipeline():
            pass
        assert my_pipeline.__name__ == "my_pipeline"


class TestTeamsAlerterPayload:

    def test_builds_correct_payload(self):
        alerter = TeamsAlerter(webhook_url="https://example.com/webhook")
        payloads = []
        with patch.object(alerter, "_post", side_effect=lambda p: payloads.append(p)):
            alerter._send("Test Subject", "Test body", "ERROR", {"key": "val"})
        assert len(payloads) == 1
        p = payloads[0]
        assert p["@type"] == "MessageCard"
        assert "ERROR" in str(p)
        assert "Test Subject" in str(p)


class TestSlackAlerterPayload:

    def test_builds_attachment(self):
        alerter = SlackAlerter(webhook_url="https://hooks.slack.com/test")
        with patch("urllib.request.urlopen") as mock_url:
            mock_resp = MagicMock()
            mock_resp.status = 200
            mock_resp.__enter__ = lambda s: s
            mock_resp.__exit__ = MagicMock(return_value=False)
            mock_url.return_value = mock_resp
            alerter._send("Sub", "Body", "WARNING", {})
        mock_url.assert_called_once()
