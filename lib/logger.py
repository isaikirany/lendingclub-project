"""
logger.py
==========
Thin wrapper around PySpark's Log4j logger.
Using Log4j (rather than Python's logging module) keeps all log output
routed through Spark's unified logging system, which simplifies log
aggregation in cluster environments (YARN, EMR, Databricks).
"""


class Log4j:

    def __init__(self, spark):
        log4j = spark._jvm.org.apache.log4j
        self.logger = log4j.LogManager.getLogger("lendingclub_pipeline")

    def info(self, message: str) -> None:
        """Log an informational message."""
        self.logger.info(message)

    def warn(self, message: str) -> None:
        """Log a warning."""
        self.logger.warn(message)

    def error(self, message: str) -> None:
        """Log an error."""
        self.logger.error(message)
