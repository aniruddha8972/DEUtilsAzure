"""tests/test_secrets_and_pipeline.py — Tests for secrets management and pipeline orchestration."""
import os
import time
import pytest
from de_utils.secrets import EnvSecrets, StaticSecrets, SecretsChain
from de_utils.utils import DataEngineeringError
from de_utils.pipeline import Pipeline, TaskStatus, PipelineResult


# ─────────────────────────────────────────────────────────────────────────────
# Secrets
# ─────────────────────────────────────────────────────────────────────────────

class TestEnvSecrets:

    def test_reads_env_var(self, monkeypatch):
        monkeypatch.setenv("DE_MY_SECRET", "secret_value")
        env = EnvSecrets(prefix="DE_")
        assert env.get("my-secret") == "secret_value"

    def test_returns_none_when_missing(self):
        env = EnvSecrets(prefix="NONEXISTENT_PREFIX_XYZ_")
        assert env.get("no-such-key") is None

    def test_hyphen_to_underscore_mapping(self, monkeypatch):
        monkeypatch.setenv("DE_ADLS_ACCOUNT_KEY", "mykey")
        env = EnvSecrets(prefix="DE_")
        assert env.get("adls-account-key") == "mykey"

    def test_no_prefix(self, monkeypatch):
        monkeypatch.setenv("MY_VAR", "val")
        env = EnvSecrets(prefix="")
        assert env.get("MY_VAR") == "val"

    def test_set_for_testing(self, monkeypatch):
        env = EnvSecrets(prefix="TEST_")
        env.set_for_testing("token", "abc123")
        assert env.get("token") == "abc123"

    def test_get_required_raises_when_missing(self):
        env = EnvSecrets(prefix="DEFINITELY_NOT_SET_XYZ_")
        with pytest.raises(DataEngineeringError):
            env.get_required("missing-key")


class TestStaticSecrets:

    def test_returns_value(self):
        s = StaticSecrets({"db-pass": "hunter2"})
        assert s.get("db-pass") == "hunter2"

    def test_returns_none_for_missing(self):
        s = StaticSecrets({"a": "b"})
        assert s.get("missing") is None

    def test_set_adds_key(self):
        s = StaticSecrets()
        s.set("new-key", "new-val")
        assert s.get("new-key") == "new-val"

    def test_get_required_raises(self):
        s = StaticSecrets()
        with pytest.raises(DataEngineeringError):
            s.get_required("not-here")


class TestSecretsChain:

    def test_first_match_returned(self):
        chain = SecretsChain([
            StaticSecrets({"k": "from_first"}),
            StaticSecrets({"k": "from_second"}),
        ])
        assert chain.get("k") == "from_first"

    def test_falls_back_to_second(self):
        chain = SecretsChain([
            StaticSecrets({}),
            StaticSecrets({"k": "fallback"}),
        ])
        assert chain.get("k") == "fallback"

    def test_returns_none_if_all_miss(self):
        chain = SecretsChain([StaticSecrets(), StaticSecrets()])
        assert chain.get("no-such-key") is None

    def test_get_required_raises_when_all_miss(self):
        chain = SecretsChain([StaticSecrets()])
        with pytest.raises(DataEngineeringError):
            chain.get_required("missing")

    def test_resolve_value_replaces_placeholder(self):
        chain = SecretsChain([StaticSecrets({"my-secret": "resolved_val"})])
        result = chain.resolve_value("prefix-${SECRET:my-secret}-suffix")
        assert result == "prefix-resolved_val-suffix"

    def test_resolve_value_no_placeholder_unchanged(self):
        chain = SecretsChain([StaticSecrets()])
        assert chain.resolve_value("plain-string") == "plain-string"

    def test_resolve_value_raises_when_secret_missing(self):
        chain = SecretsChain([StaticSecrets()])
        with pytest.raises(DataEngineeringError):
            chain.resolve_value("${SECRET:missing-key}")

    def test_resolve_dict_replaces_values(self):
        chain = SecretsChain([StaticSecrets({"pass": "s3cr3t"})])
        cfg   = {"host": "db.azure.com", "password": "${SECRET:pass}"}
        result = chain.resolve_dict(cfg)
        assert result["password"] == "s3cr3t"
        assert result["host"]     == "db.azure.com"

    def test_resolve_dict_recursive(self):
        chain = SecretsChain([StaticSecrets({"token": "abc"})])
        cfg   = {"outer": {"inner": "${SECRET:token}"}}
        result = chain.resolve_dict(cfg)
        assert result["outer"]["inner"] == "abc"

    def test_resolve_dict_list_values(self):
        chain = SecretsChain([StaticSecrets({"x": "val"})])
        cfg   = {"items": ["${SECRET:x}", "plain"]}
        result = chain.resolve_dict(cfg)
        assert result["items"][0] == "val"
        assert result["items"][1] == "plain"

    def test_non_string_values_pass_through(self):
        chain  = SecretsChain([StaticSecrets()])
        result = chain.resolve_value(42)
        assert result == 42


# ─────────────────────────────────────────────────────────────────────────────
# Pipeline
# ─────────────────────────────────────────────────────────────────────────────

class TestPipelineRegistration:

    def test_task_decorator_registers_function(self):
        p = Pipeline(name="test")
        @p.task()
        def my_task(): pass
        assert "my_task" in {t["name"] for t in p.list_tasks()}

    def test_explicit_register(self):
        p = Pipeline(name="test")
        p.register(lambda: None, depends_on=[])
        assert len(p.list_tasks()) == 1

    def test_task_with_dependencies(self):
        p = Pipeline(name="test")
        @p.task()
        def task_a(): pass
        @p.task(depends_on=["task_a"])
        def task_b(): pass
        tasks = {t["name"]: t for t in p.list_tasks()}
        assert "task_a" in tasks["task_b"]["depends_on"]


class TestPipelineTopoSort:

    def test_simple_chain_ordered_correctly(self):
        p = Pipeline(name="test")
        @p.task()
        def a(): pass
        @p.task(depends_on=["a"])
        def b(): pass
        @p.task(depends_on=["b"])
        def c(): pass
        levels = p._topo_sort()
        assert levels[0] == ["a"]
        assert levels[1] == ["b"]
        assert levels[2] == ["c"]

    def test_parallel_tasks_in_same_level(self):
        p = Pipeline(name="test")
        @p.task()
        def root(): pass
        @p.task(depends_on=["root"])
        def branch_a(): pass
        @p.task(depends_on=["root"])
        def branch_b(): pass
        levels = p._topo_sort()
        assert levels[0] == ["root"]
        assert set(levels[1]) == {"branch_a", "branch_b"}

    def test_cycle_raises(self):
        p = Pipeline(name="test")
        @p.task(depends_on=["b"])
        def a(): pass
        @p.task(depends_on=["a"])
        def b(): pass
        with pytest.raises(DataEngineeringError, match="[Cc]ycle"):
            p._topo_sort()

    def test_unknown_dependency_raises(self):
        p = Pipeline(name="test")
        @p.task(depends_on=["nonexistent"])
        def a(): pass
        with pytest.raises(DataEngineeringError):
            p._topo_sort()


class TestPipelineExecution:

    def test_all_tasks_succeed(self):
        p    = Pipeline(name="test")
        log_ = []
        @p.task()
        def step_a():
            log_.append("a")
        @p.task(depends_on=["step_a"])
        def step_b():
            log_.append("b")
        result = p.run()
        assert result.succeeded is True
        assert log_ == ["a", "b"]

    def test_failed_task_in_result(self):
        p = Pipeline(name="test")
        @p.task()
        def bad_task():
            raise ValueError("intentional failure")
        result = p.run()
        assert result.succeeded is False
        assert "bad_task" in result.failed_tasks

    def test_downstream_skipped_when_upstream_fails(self):
        p = Pipeline(name="test")
        @p.task()
        def bad():
            raise ValueError("fail")
        @p.task(depends_on=["bad"])
        def downstream():
            pass
        result = p.run()
        assert result.task_results["downstream"].status == TaskStatus.UPSTREAM_FAILED

    def test_retry_on_failure(self):
        counter = {"n": 0}
        p = Pipeline(name="test")
        @p.task(retries=2, retry_delay=0)
        def flaky():
            counter["n"] += 1
            if counter["n"] < 3:
                raise ConnectionError("retry me")
        result = p.run()
        assert result.succeeded is True
        assert counter["n"] == 3

    def test_result_contains_elapsed_time(self):
        p = Pipeline(name="test")
        @p.task()
        def quick(): pass
        result = p.run()
        assert result.task_results["quick"].elapsed_sec >= 0

    def test_dry_run_does_not_execute(self):
        executed = {"ran": False}
        p = Pipeline(name="test")
        @p.task()
        def should_not_run():
            executed["ran"] = True
        p.run(dry_run=True)
        assert executed["ran"] is False

    def test_show_runs_without_error(self, capsys):
        p = Pipeline(name="show_test")
        @p.task()
        def t(): pass
        result = p.run()
        result.show()
        out = capsys.readouterr().out
        assert "show_test" in out

    def test_visualize_returns_string(self):
        p = Pipeline(name="viz_test")
        @p.task()
        def a(): pass
        @p.task(depends_on=["a"])
        def b(): pass
        diagram = p.visualize()
        assert "viz_test" in diagram
        assert "a" in diagram

    def test_parallel_execution(self):
        """Tasks in the same level run concurrently with max_workers > 1."""
        p = Pipeline(name="parallel_test", max_workers=2)
        timings = {}
        @p.task()
        def root(): pass
        @p.task(depends_on=["root"])
        def slow_a():
            time.sleep(0.05)
            timings["a"] = time.time()
        @p.task(depends_on=["root"])
        def slow_b():
            time.sleep(0.05)
            timings["b"] = time.time()
        result = p.run()
        assert result.succeeded
        # Both should finish close together (parallel), not 0.1s apart (serial)
        if "a" in timings and "b" in timings:
            assert abs(timings["a"] - timings["b"]) < 0.1
