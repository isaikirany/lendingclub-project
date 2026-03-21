"""
ConfigReader.py
================
Reads application and PySpark configs from INI-style .conf files.
Validates that the requested environment section exists before returning,
so misconfiguration fails at startup with a clear message rather than
silently downstream as a KeyError.
"""

import configparser
from pyspark import SparkConf


def get_app_config(env: str) -> dict:
    """
    Reads application config from configs/application.conf for the given environment.

    Args:
        env: Environment name. Must match a section header in application.conf.
             Valid values: LOCAL | TEST | PROD

    Returns:
        Dictionary of config key-value pairs for the given environment.

    Raises:
        ValueError: If the environment section does not exist in the config file.
    """
    config = configparser.ConfigParser()
    config.read("configs/application.conf")

    if env not in config.sections():
        raise ValueError(
            f"[ConfigReader] Environment '{env}' not found in application.conf. "
            f"Available sections: {config.sections()}"
        )

    return dict(config.items(env))


def get_pyspark_config(env: str) -> SparkConf:
    """
    Reads PySpark settings from configs/pyspark.conf and returns a SparkConf object.

    Args:
        env: Environment name. Must match a section header in pyspark.conf.
             Valid values: LOCAL | TEST | PROD

    Returns:
        SparkConf populated with all key-value pairs from the environment section.

    Raises:
        ValueError: If the environment section does not exist in the config file.
    """
    config = configparser.ConfigParser()
    config.read("configs/pyspark.conf")

    if env not in config.sections():
        raise ValueError(
            f"[ConfigReader] Environment '{env}' not found in pyspark.conf. "
            f"Available sections: {config.sections()}"
        )

    spark_conf = SparkConf()
    for key, val in config.items(env):
        spark_conf.set(key, val)

    return spark_conf
