"""
de_utils.cli
------------
Command-line interface for common de_utils operations.

Commands
--------
  de_utils profile   <table>           — profile a Hive table
  de_utils audit     <table>           — show audit log for a table
  de_utils lineage   <table>           — show upstream/downstream lineage
  de_utils checkpoint <job>            — show checkpoint state for a job
  de_utils dq        <table>           — run stored DQ rules against a table
  de_utils delta     optimize <table>  — run OPTIMIZE on a Delta table
  de_utils delta     vacuum   <table>  — run VACUUM on a Delta table
  de_utils pipeline  run <module.path> — run a Pipeline defined in a Python module
  de_utils version                     — print version info

Usage
-----
    pip install de_utils
    de_utils profile silver.orders
    de_utils audit silver.orders --since 2024-01-01
    de_utils checkpoint daily_orders_etl
    de_utils delta optimize silver.orders --zorder customer_id,order_date
    de_utils pipeline run jobs.daily_orders:pipeline
"""

from __future__ import annotations

import argparse
import importlib
import sys
from typing import Optional

from . import __version__
from .utils import get_logger

log = get_logger("de_utils.cli")


def _get_spark(app_name: str = "de_utils_cli", quiet: bool = True):
    """Bootstrap a minimal local SparkSession for CLI commands."""
    from pyspark.sql import SparkSession
    builder = (
        SparkSession.builder
        .appName(app_name)
        .master("local[*]")
        .config("spark.ui.enabled", "false")
        .config("spark.sql.shuffle.partitions", "8")
        .enableHiveSupport()
    )
    spark = builder.getOrCreate()
    if quiet:
        spark.sparkContext.setLogLevel("ERROR")
    return spark


# ── profile ───────────────────────────────────────────────────────────────────

def cmd_profile(args) -> int:
    from .profiler import DataProfiler
    spark    = _get_spark()
    df       = spark.table(args.table)
    profiler = DataProfiler(spark, top_n=args.top_n)
    profile  = profiler.profile(
        df,
        table_name=args.table,
        sample_fraction=args.sample,
    )
    profile.show()
    if args.save:
        profiler.save(profile, args.save)
        print(f"Profile saved to {args.save}")
    return 0


# ── audit ─────────────────────────────────────────────────────────────────────

def cmd_audit(args) -> int:
    from .audit import AuditLog
    spark = _get_spark()
    audit = AuditLog(spark, audit_table=args.audit_table, job_name="cli")
    df    = audit.get_table_history(args.table)
    if args.since:
        df = df.filter(f"started_at >= '{args.since}'")
    df.orderBy("started_at", ascending=False).show(args.limit, truncate=False)
    return 0


# ── lineage ───────────────────────────────────────────────────────────────────

def cmd_lineage(args) -> int:
    from .lineage import LineageTracker
    spark   = _get_spark()
    tracker = LineageTracker(spark, lineage_table=args.lineage_table, job_name="cli")
    print(f"\n=== Upstream of {args.table} ===")
    tracker.get_upstream(args.table).show(truncate=False)
    print(f"\n=== Downstream of {args.table} ===")
    tracker.get_downstream(args.table).show(truncate=False)
    return 0


# ── checkpoint ────────────────────────────────────────────────────────────────

def cmd_checkpoint(args) -> int:
    from .checkpoint import JobCheckpoint
    spark = _get_spark()
    cp    = JobCheckpoint(spark, state_table=args.state_table, job_name=args.job)
    print(f"\nCheckpoint state for job: {args.job}")
    cp.get_all_state().show(truncate=False)
    if args.clear:
        cp.clear_state()
        print("State cleared.")
    return 0


# ── delta ─────────────────────────────────────────────────────────────────────

def cmd_delta(args) -> int:
    from .delta import DeltaUtils
    spark = _get_spark()
    du    = DeltaUtils(spark)
    if args.delta_cmd == "optimize":
        zorder = args.zorder.split(",") if args.zorder else None
        du.optimize(args.table, zorder_by=zorder)
        print(f"OPTIMIZE completed on {args.table}")
    elif args.delta_cmd == "vacuum":
        du.vacuum(args.table, retention_hours=args.retention)
        print(f"VACUUM completed on {args.table}")
    elif args.delta_cmd == "history":
        du.show_history(args.table).show(truncate=False)
    elif args.delta_cmd == "detail":
        du.detail(args.table).show(vertical=True, truncate=False)
    return 0


# ── pipeline run ──────────────────────────────────────────────────────────────

def cmd_pipeline(args) -> int:
    """
    Run a Pipeline object defined in a Python module.
    Specify as module.path:attribute, e.g. jobs.daily_orders:pipeline
    """
    if ":" not in args.target:
        print(f"ERROR: target must be 'module.path:attribute', got: {args.target}")
        return 1
    module_path, attr = args.target.rsplit(":", 1)
    try:
        module   = importlib.import_module(module_path)
        pipeline = getattr(module, attr)
    except (ImportError, AttributeError) as e:
        print(f"ERROR loading pipeline: {e}")
        return 1

    result = pipeline.run(dry_run=args.dry_run, tags=args.tags or None)
    result.show()
    return 0 if result.succeeded else 1


# ── version ───────────────────────────────────────────────────────────────────

def cmd_version(args) -> int:
    print(f"de_utils v{__version__}")
    return 0


# ── CLI wiring ────────────────────────────────────────────────────────────────

def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="de_utils",
        description="de_utils — Data Engineering Utility CLI",
    )
    sub = parser.add_subparsers(dest="command")

    # profile
    p_profile = sub.add_parser("profile", help="Profile a Hive table")
    p_profile.add_argument("table")
    p_profile.add_argument("--sample",  type=float, default=None, help="Sample fraction (0.0-1.0)")
    p_profile.add_argument("--top-n",   type=int,   default=5,    help="Top N frequent values")
    p_profile.add_argument("--save",    type=str,   default=None, help="Save profile to table")

    # audit
    p_audit = sub.add_parser("audit", help="Query audit log for a table")
    p_audit.add_argument("table")
    p_audit.add_argument("--audit-table", default="gold._audit_log")
    p_audit.add_argument("--since",  type=str, default=None)
    p_audit.add_argument("--limit",  type=int, default=20)

    # lineage
    p_lin = sub.add_parser("lineage", help="Show upstream/downstream lineage")
    p_lin.add_argument("table")
    p_lin.add_argument("--lineage-table", default="gold._lineage")

    # checkpoint
    p_cp = sub.add_parser("checkpoint", help="Inspect job checkpoint state")
    p_cp.add_argument("job")
    p_cp.add_argument("--state-table", default="gold._job_state")
    p_cp.add_argument("--clear", action="store_true", help="Clear all state for this job")

    # delta
    p_delta = sub.add_parser("delta", help="Delta Lake utilities")
    delta_sub = p_delta.add_subparsers(dest="delta_cmd")
    for cmd in ["optimize", "vacuum", "history", "detail"]:
        p = delta_sub.add_parser(cmd)
        p.add_argument("table")
        if cmd == "optimize":
            p.add_argument("--zorder", type=str, default=None, help="Comma-separated columns")
        if cmd == "vacuum":
            p.add_argument("--retention", type=int, default=168, help="Hours (default 168 = 7 days)")

    # pipeline
    p_pipe = sub.add_parser("pipeline", help="Run a pipeline")
    pipe_sub = p_pipe.add_subparsers(dest="pipeline_cmd")
    p_run = pipe_sub.add_parser("run")
    p_run.add_argument("target", help="module.path:pipeline_attribute")
    p_run.add_argument("--dry-run", action="store_true")
    p_run.add_argument("--tags",    nargs="*", help="Only run tasks with these tags")

    # version
    sub.add_parser("version", help="Print version")

    return parser


def main(argv=None) -> int:
    parser  = build_parser()
    args    = parser.parse_args(argv)
    dispatch = {
        "profile":    cmd_profile,
        "audit":      cmd_audit,
        "lineage":    cmd_lineage,
        "checkpoint": cmd_checkpoint,
        "delta":      cmd_delta,
        "pipeline":   cmd_pipeline,
        "version":    cmd_version,
    }
    if args.command not in dispatch:
        parser.print_help()
        return 0
    return dispatch[args.command](args)


if __name__ == "__main__":
    sys.exit(main())
