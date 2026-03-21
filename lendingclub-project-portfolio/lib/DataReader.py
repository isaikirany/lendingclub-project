"""
DataReader.py
==============
Explicit schema definitions and reader functions for all four LendingClub datasets.

Using StructType schemas (not inferSchema=True) because:
  - inferSchema requires a full data scan on every run — slow on large files
  - inferred types on messy CSVs are unreliable (floats inferred as strings, etc.)
  - explicit schemas fail fast with clear errors when source data changes structure
"""

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.types import (
    StructType, StructField,
    StringType, FloatType, IntegerType
)
from lib import ConfigReader


# =============================================================================
# Schemas
# =============================================================================

def get_customers_schema() -> StructType:
    """
    Schema for raw customers CSV.
    Column names match the raw file headers — renaming happens in DataManipulation.
    """
    return StructType([
        StructField("member_id",                 StringType(), True),
        StructField("emp_title",                 StringType(), True),
        StructField("emp_length",                StringType(), True),  # cleaned later: '10+ years' -> 10
        StructField("home_ownership",            StringType(), True),
        StructField("annual_inc",                FloatType(),  True),
        StructField("addr_state",                StringType(), True),
        StructField("zip_code",                  StringType(), True),
        StructField("country",                   StringType(), True),
        StructField("grade",                     StringType(), True),
        StructField("sub_grade",                 StringType(), True),
        StructField("verification_status",       StringType(), True),
        StructField("tot_hi_cred_lim",           FloatType(),  True),
        StructField("application_type",          StringType(), True),
        StructField("annual_inc_joint",          FloatType(),  True),
        StructField("verification_status_joint", StringType(), True),
    ])


def get_loans_schema() -> StructType:
    """
    Schema for raw loans CSV.
    loan_term_months is read as String — it arrives as '36 months' and is
    normalised to Integer in DataManipulation.loans_term_modified.
    """
    return StructType([
        StructField("loan_id",             StringType(), True),
        StructField("member_id",           StringType(), True),
        StructField("loan_amount",         FloatType(),  True),
        StructField("funded_amount",       FloatType(),  True),
        StructField("loan_term_months",    StringType(), True),
        StructField("interest_rate",       FloatType(),  True),
        StructField("monthly_installment", FloatType(),  True),
        StructField("issue_date",          StringType(), True),
        StructField("loan_status",         StringType(), True),
        StructField("loan_purpose",        StringType(), True),
        StructField("loan_title",          StringType(), True),
    ])


def get_loan_repayments_schema() -> StructType:
    """
    Schema for raw loan repayments CSV.
    Date columns are read as String and parsed to DateType in DataManipulation.
    """
    return StructType([
        StructField("loan_id",                   StringType(), True),
        StructField("total_principal_received",  FloatType(),  True),
        StructField("total_interest_received",   FloatType(),  True),
        StructField("total_late_fee_received",   FloatType(),  True),
        StructField("total_payment_received",    FloatType(),  True),
        StructField("last_payment_amount",       FloatType(),  True),
        StructField("last_payment_date",         StringType(), True),  # 'Jan-2020' -> DateType
        StructField("next_payment_date",         StringType(), True),  # 'Feb-2020' -> DateType
    ])


def get_loan_defaulters_schema() -> StructType:
    """Schema for raw loan defaulters CSV."""
    return StructType([
        StructField("member_id",                StringType(), True),
        StructField("delinq_2yrs",              FloatType(),  True),
        StructField("delinq_amnt",              FloatType(),  True),
        StructField("pub_rec",                  FloatType(),  True),
        StructField("pub_rec_bankruptcies",     FloatType(),  True),
        StructField("inq_last_6mths",           FloatType(),  True),
        StructField("total_rec_late_fee",       FloatType(),  True),
        StructField("mths_since_last_delinq",   FloatType(),  True),
        StructField("mths_since_last_record",   FloatType(),  True),
    ])


# =============================================================================
# Readers
# =============================================================================

def _csv_reader(spark: SparkSession, path: str, schema: StructType) -> DataFrame:
    """Shared CSV reader with consistent options applied across all datasets."""
    return (
        spark.read
             .format("csv")
             .option("header", True)
             .option("nullValue", "")
             .option("emptyValue", None)
             .schema(schema)
             .load(path)
    )


def read_customers(spark: SparkSession, env: str) -> DataFrame:
    """Reads raw customer data. Path resolved from application.conf."""
    conf = ConfigReader.get_app_config(env)
    return _csv_reader(spark, conf["raw.customers.file.path"], get_customers_schema())


def read_loans(spark: SparkSession, env: str) -> DataFrame:
    """Reads raw loan data. Path resolved from application.conf."""
    conf = ConfigReader.get_app_config(env)
    return _csv_reader(spark, conf["raw.loans.file.path"], get_loans_schema())


def read_loan_repayments(spark: SparkSession, env: str) -> DataFrame:
    """Reads raw loan repayment data. Path resolved from application.conf."""
    conf = ConfigReader.get_app_config(env)
    return _csv_reader(spark, conf["raw.loan_repayments.file.path"], get_loan_repayments_schema())


def read_loan_defaulters(spark: SparkSession, env: str) -> DataFrame:
    """Reads raw loan defaulter data. Path resolved from application.conf."""
    conf = ConfigReader.get_app_config(env)
    return _csv_reader(spark, conf["raw.loans_defaulters.path"], get_loan_defaulters_schema())
