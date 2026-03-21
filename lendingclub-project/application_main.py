"""
LendingClub Data Pipeline — Entry Point
========================================
Multi-dataset PySpark cleaning pipeline for LendingClub loan data.
Processes four datasets through isolated transformation chains and writes
clean output to configurable target paths.

Usage:
    spark-submit application_main.py <ENV>

    ENV options: LOCAL | TEST | PROD
"""

import sys
from lib import DataReader, DataManipulation, Utils
from lib.logger import Log4j


def run_pipeline(env: str) -> None:
    # ------------------------------------------------------------------
    # Spark Session
    # ------------------------------------------------------------------
    spark = Utils.get_spark_session(env)
    logger = Log4j(spark)
    logger.info(f"Pipeline started. Environment: {env}")

    # ------------------------------------------------------------------
    # 1. Customers
    # ------------------------------------------------------------------
    logger.info("Processing customers dataset...")

    customers_df = DataReader.read_customers(spark, env)

    customers_clean_df = (
        customers_df
        .transform(DataManipulation.rename_customer_columns)
        .transform(DataManipulation.customers_df_ingested)
        .transform(DataManipulation.customers_df_distinct)
        .transform(lambda df: DataManipulation.customers_income_filtered(df, spark))
        .transform(DataManipulation.customers_emplength_cleaned)
        .transform(lambda df: DataManipulation.avg_emp_length_imputed(df, spark))
        .transform(DataManipulation.customers_state_cleaned)
    )

    customers_clean_df.write \
        .option("header", True) \
        .format("parquet") \
        .mode("overwrite") \
        .save("data/cleaned/customers")

    logger.info(f"Customers written. Count: {customers_clean_df.count()}")

    # ------------------------------------------------------------------
    # 2. Loans
    # ------------------------------------------------------------------
    logger.info("Processing loans dataset...")

    loans_df = DataReader.read_loans(spark, env)

    loans_clean_df = (
        loans_df
        .transform(DataManipulation.loans_ingested)
        .transform(DataManipulation.loans_filtered)
        .transform(DataManipulation.loans_term_modified)
        .transform(DataManipulation.loans_purpose_modified)
    )

    loans_clean_df.write \
        .option("header", True) \
        .format("parquet") \
        .mode("overwrite") \
        .save("data/cleaned/loans")

    logger.info(f"Loans written. Count: {loans_clean_df.count()}")

    # ------------------------------------------------------------------
    # 3. Loan Repayments
    # ------------------------------------------------------------------
    logger.info("Processing loan repayments dataset...")

    repayments_df = DataReader.read_loan_repayments(spark, env)

    repayments_clean_df = (
        repayments_df
        .transform(DataManipulation.repayments_ingested)
        .transform(DataManipulation.repayments_filtered)
        .transform(DataManipulation.repayments_null_amounts_filled)
        .transform(DataManipulation.repayments_total_derived)
        .transform(DataManipulation.repayments_last_date_parsed)
        .transform(DataManipulation.repayments_next_date_parsed)
    )

    repayments_clean_df.write \
        .option("header", True) \
        .format("parquet") \
        .mode("overwrite") \
        .save("data/cleaned/loan_repayments")

    logger.info(f"Repayments written. Count: {repayments_clean_df.count()}")

    # ------------------------------------------------------------------
    # 4. Loan Defaulters
    # ------------------------------------------------------------------
    logger.info("Processing loan defaulters dataset...")

    defaulters_df = DataReader.read_loan_defaulters(spark, env)
    defaulters_base_df = DataManipulation.defaulters_ingested(defaulters_df)

    # Split into two sub-datasets: delinquency records and enquiry records
    defaulters_delinq_df  = DataManipulation.defaulters_delinquency(defaulters_base_df)
    defaulters_enq_df     = DataManipulation.defaulters_public_records(defaulters_base_df)

    defaulters_delinq_df.write \
        .option("header", True) \
        .format("parquet") \
        .mode("overwrite") \
        .save("data/cleaned/loan_defaulters_delinquency")

    defaulters_enq_df.write \
        .option("header", True) \
        .format("parquet") \
        .mode("overwrite") \
        .save("data/cleaned/loan_defaulters_public_records")

    logger.info(
        f"Defaulters written. "
        f"Delinquency: {defaulters_delinq_df.count()} | "
        f"Public records: {defaulters_enq_df.count()}"
    )

    logger.info("Pipeline complete.")


if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage: spark-submit application_main.py <ENV>")
        print("  ENV options: LOCAL | TEST | PROD")
        sys.exit(1)

    run_pipeline(sys.argv[1].upper())
