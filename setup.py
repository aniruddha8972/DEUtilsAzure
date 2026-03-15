from setuptools import setup, find_packages

with open("README.md", encoding="utf-8") as f:
    long_description = f.read()

setup(
    name="de_utils",
    version="2.1.0",
    description=(
        "Data Engineering Utility Library — Azure ADLS, Hive, Spark ETL, "
        "Data Quality, Profiling, Lineage, Delta Lake, Alerts, Secrets, "
        "Retry, Pipeline Orchestration, and Medallion Architecture"
    ),
    long_description=long_description,
    long_description_content_type="text/markdown",
    author="Data Engineering Team",
    author_email="data-engineering@example.com",
    url="https://github.com/your-org/de_utils",
    license="MIT",
    packages=find_packages(exclude=["tests*"]),
    python_requires=">=3.8",

    # ── Core ──────────────────────────────────────────────────────────────────
    install_requires=[
        "pyspark>=3.3.0",
    ],

    # ── Extras ────────────────────────────────────────────────────────────────
    extras_require={
        # Azure ADLS file-system operations
        "azure": [
            "azure-storage-file-datalake>=12.0.0",
            "azure-identity>=1.12.0",
        ],
        # Azure Key Vault secrets
        "keyvault": [
            "azure-keyvault-secrets>=4.7.0",
            "azure-identity>=1.12.0",
        ],
        # YAML config loading
        "yaml": [
            "PyYAML>=6.0",
        ],
        # Delta Lake utilities (optimize, vacuum, time travel)
        "delta": [
            "delta-spark>=2.3.0",
        ],
        # Great Expectations integration
        "gx": [
            "great_expectations>=0.18.0",
        ],
        # Development & testing
        "dev": [
            "pytest>=7.4.0",
            "pytest-mock>=3.11.0",
            "pytest-cov>=4.1.0",
            "black>=23.0.0",
            "flake8>=6.0.0",
            "isort>=5.12.0",
            "mypy>=1.4.0",
            "PyYAML>=6.0",
        ],
        # Full install — everything
        "all": [
            "azure-storage-file-datalake>=12.0.0",
            "azure-keyvault-secrets>=4.7.0",
            "azure-identity>=1.12.0",
            "PyYAML>=6.0",
            "delta-spark>=2.3.0",
            "great_expectations>=0.18.0",
        ],
    },

    # ── CLI entry point ───────────────────────────────────────────────────────
    entry_points={
        "console_scripts": [
            "de_utils=de_utils.cli:main",
        ],
    },

    # ── PyPI metadata ─────────────────────────────────────────────────────────
    classifiers=[
        "Development Status :: 5 - Production/Stable",
        "Intended Audience :: Developers",
        "Intended Audience :: Science/Research",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
        "Programming Language :: Python :: 3",
        "Programming Language :: Python :: 3.8",
        "Programming Language :: Python :: 3.9",
        "Programming Language :: Python :: 3.10",
        "Programming Language :: Python :: 3.11",
        "Topic :: Database",
        "Topic :: Scientific/Engineering :: Information Analysis",
        "Topic :: Software Development :: Libraries :: Python Modules",
    ],
    keywords=[
        "azure", "adls", "hive", "spark", "pyspark", "delta", "etl",
        "data-engineering", "scd", "medallion", "data-quality", "profiling",
        "lineage", "pipeline", "alerting", "secrets", "retry", "circuit-breaker",
    ],
)
