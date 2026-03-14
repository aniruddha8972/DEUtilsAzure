from setuptools import setup, find_packages

setup(
    name="de_utils",
    version="1.0.0",
    description="Data Engineering Utility Library for Azure ADLS / Hive / Spark",
    author="Data Engineering Team",
    packages=find_packages(),
    python_requires=">=3.8",
    install_requires=[
        "pyspark>=3.3.0",
    ],
    extras_require={
        "azure": [
            "azure-storage-file-datalake>=12.0.0",
            "azure-identity>=1.12.0",
        ],
        "dev": [
            "pytest",
            "pytest-mock",
            "black",
            "flake8",
        ],
    },
    classifiers=[
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
        "Topic :: Database",
        "Topic :: Software Development :: Libraries :: Python Modules",
    ],
)
