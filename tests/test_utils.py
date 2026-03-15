"""
tests/test_utils.py
-------------------
Unit tests for de_utils.utils helpers.
"""

import pytest
from de_utils.utils import (
    get_logger,
    validate_columns,
    quote_identifier,
    build_partition_clause,
    DataEngineeringError,
)


class TestGetLogger:

    def test_returns_logger(self):
        import logging
        log = get_logger("test_logger")
        assert isinstance(log, logging.Logger)

    def test_logger_has_handler(self):
        log = get_logger("test_handler_logger")
        assert len(log.handlers) > 0

    def test_same_name_same_logger(self):
        log1 = get_logger("same")
        log2 = get_logger("same")
        assert log1 is log2


class TestValidateColumns:

    def test_passes_when_all_present(self):
        validate_columns(["a", "b", "c"], ["a", "b"])  # should not raise

    def test_raises_on_missing(self):
        with pytest.raises(DataEngineeringError, match="missing required columns"):
            validate_columns(["a", "b"], ["a", "c"])

    def test_empty_required_always_passes(self):
        validate_columns(["x"], [])  # should not raise

    def test_reports_all_missing_columns(self):
        with pytest.raises(DataEngineeringError) as exc_info:
            validate_columns(["a"], ["b", "c"])
        msg = str(exc_info.value)
        assert "b" in msg
        assert "c" in msg


class TestQuoteIdentifier:

    def test_wraps_in_backticks(self):
        assert quote_identifier("my_column") == "`my_column`"

    def test_handles_spaces(self):
        assert quote_identifier("my column") == "`my column`"

    def test_handles_reserved_words(self):
        assert quote_identifier("select") == "`select`"


class TestBuildPartitionClause:

    def test_single_key_no_alias(self):
        result = build_partition_clause(["year"])
        assert result == "`year` = s.`year`"

    def test_multi_key_no_alias(self):
        result = build_partition_clause(["year", "month"])
        assert "`year` = s.`year`" in result
        assert "`month` = s.`month`" in result
        assert "AND" in result

    def test_with_alias(self):
        result = build_partition_clause(["year"], alias="t")
        assert result == "t.`year` = s.`year`"

    def test_empty_keys_returns_empty_string(self):
        result = build_partition_clause([])
        assert result == ""


class TestDataEngineeringError:

    def test_is_exception(self):
        err = DataEngineeringError("something went wrong")
        assert isinstance(err, Exception)

    def test_message_preserved(self):
        err = DataEngineeringError("bad partition key")
        assert "bad partition key" in str(err)
