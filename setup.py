"""Packaging configuration for the medallion data platform."""

from setuptools import find_packages, setup


setup(
    name="medallion-data-platform",
    version="0.1.0",
    description="Configurable PySpark medallion pipeline and Streamlit dashboard",
    package_dir={"": "src"},
    packages=find_packages(where="src"),
    python_requires=">=3.10",
    install_requires=[
        "pyspark>=3.5,<4.0",
        "delta-spark>=3.2,<4.0",
        "streamlit>=1.38,<2.0",
        "pandas>=2.2,<3.0",
        "pyarrow>=16,<20",
        "PyYAML>=6.0,<7.0",
    ],
)
