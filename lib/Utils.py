"""
Utils.py
=========
SparkSession factory. Returns a session configured for the target environment.

LOCAL  — runs on local[2], loads log4j for clean console output.
TEST   — cluster mode without Hive (lightweight, suitable for CI pipelines).
PROD   — cluster mode with Hive metastore enabled for managed table support.
"""

from pyspark.sql import SparkSession
from lib.ConfigReader import get_pyspark_config


def get_spark_session(env: str) -> SparkSession:
    """
    Creates and returns a SparkSession for the given environment.

    Args:
        env: One of LOCAL | TEST | PROD.
             Must match a section in configs/pyspark.conf.

    Returns:
        Configured SparkSession.
    """
    builder = SparkSession.builder.config(conf=get_pyspark_config(env))

    if env == "LOCAL":
        return (
            builder
            .master("local[2]")
            .config(
                "spark.driver.extraJavaOptions",
                "-Dlog4j.configuration=file:log4j.properties"
            )
            .getOrCreate()
        )

    if env == "TEST":
        return builder.getOrCreate()

    # PROD — enable Hive metastore
    return builder.enableHiveSupport().getOrCreate()
