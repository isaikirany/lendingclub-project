"""
DataManipulation.py
====================
All transformation functions for the LendingClub pipeline.
Each function is pure: takes a DataFrame (and optionally SparkSession),
returns a DataFrame. No side effects. No global state.

Functions are grouped by dataset: Customers, Loans, Repayments, Defaulters.
"""

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window


# =============================================================================
# CUSTOMERS
# =============================================================================

# Valid US state abbreviations used to filter out malformed address_state values
_VALID_US_STATES = {
    "AL", "AK", "AZ", "AR", "CA", "CO", "CT", "DE", "FL", "GA",
    "HI", "ID", "IL", "IN", "IA", "KS", "KY", "LA", "ME", "MD",
    "MA", "MI", "MN", "MS", "MO", "MT", "NE", "NV", "NH", "NJ",
    "NM", "NY", "NC", "ND", "OH", "OK", "OR", "PA", "RI", "SC",
    "SD", "TN", "TX", "UT", "VT", "VA", "WA", "WV", "WI", "WY", "DC"
}


def rename_customer_columns(df: DataFrame) -> DataFrame:
    """
    Renames abbreviated raw column names to readable equivalents.
    Keeps schema self-documenting for downstream consumers.
    """
    return (
        df
        .withColumnRenamed("annual_inc",        "annual_income")
        .withColumnRenamed("addr_state",         "address_state")
        .withColumnRenamed("zip_code",           "address_zipcode")
        .withColumnRenamed("country",            "address_country")
        .withColumnRenamed("tot_hi_cred_lim",    "total_high_credit_limit")
        .withColumnRenamed("annual_inc_joint",   "join_annual_income")
    )


def customers_df_ingested(df: DataFrame) -> DataFrame:
    """Stamps each record with the pipeline load time for lineage tracking."""
    return df.withColumn("ingest_date", F.current_timestamp())


def customers_df_distinct(df: DataFrame) -> DataFrame:
    """
    Removes fully duplicate rows. A row is only a duplicate if every
    field matches — partial duplicates (same member_id, different data) are retained.
    """
    return df.distinct()


def customers_income_filtered(df: DataFrame, spark: SparkSession) -> DataFrame:
    """
    Drops records where annual_income is null.
    Income is required for creditworthiness evaluation — records without it
    cannot be used in downstream loan analysis.
    """
    df.createOrReplaceTempView("customers")
    return spark.sql("SELECT * FROM customers WHERE annual_income IS NOT NULL")


def customers_emplength_cleaned(df: DataFrame) -> DataFrame:
    """
    Normalises the emp_length field:
      '10+ years' -> 10
      '< 1 year'  -> 0
      '5 years'   -> 5
      null        -> null (handled in avg_emp_length_imputed)

    Strips all non-numeric characters, then casts to integer.
    """
    return (
        df
        .withColumn("emp_length", F.regexp_replace(F.col("emp_length"), r"[^0-9]", ""))
        .withColumn("emp_length", F.trim(F.col("emp_length")))
        .withColumn(
            "emp_length",
            F.when(F.col("emp_length") == "", None)
             .otherwise(F.col("emp_length").cast("int"))
        )
    )


def avg_emp_length_imputed(df: DataFrame, spark: SparkSession) -> DataFrame:
    """
    Imputes null emp_length values using the average emp_length within the
    same loan grade group (A-G). Grade is a strong proxy for borrower profile,
    making it a better imputation group than a global mean.

    Records with no grade are imputed with the global mean as fallback.
    """
    df.createOrReplaceTempView("customers_emp")

    avg_by_grade = spark.sql("""
        SELECT grade, AVG(emp_length) AS avg_emp_by_grade
        FROM customers_emp
        WHERE emp_length IS NOT NULL
        GROUP BY grade
    """)
    avg_by_grade.createOrReplaceTempView("avg_emp_by_grade")

    global_avg = spark.sql(
        "SELECT AVG(emp_length) AS global_avg FROM customers_emp WHERE emp_length IS NOT NULL"
    ).collect()[0]["global_avg"]

    return spark.sql(f"""
        SELECT
            c.*,
            CAST(
                CASE
                    WHEN c.emp_length IS NOT NULL THEN c.emp_length
                    WHEN a.avg_emp_by_grade IS NOT NULL THEN ROUND(a.avg_emp_by_grade)
                    ELSE ROUND({global_avg})
                END AS INT
            ) AS emp_length_imputed
        FROM customers_emp c
        LEFT JOIN avg_emp_by_grade a ON c.grade = a.grade
    """).drop("emp_length").withColumnRenamed("emp_length_imputed", "emp_length")


def customers_state_cleaned(df: DataFrame) -> DataFrame:
    """
    Standardises address_state:
      1. Trims whitespace and uppercases
      2. Filters out rows with state codes not in the valid US state list
         (catches nulls, empty strings, and corrupted values)
    """
    valid_states = list(_VALID_US_STATES)

    return (
        df
        .withColumn("address_state", F.upper(F.trim(F.col("address_state"))))
        .filter(F.col("address_state").isin(valid_states))
    )


# =============================================================================
# LOANS
# =============================================================================

# Loan statuses considered valid/active for downstream analysis
_VALID_LOAN_STATUSES = [
    "Current", "Fully Paid", "In Grace Period",
    "Late (16-30 days)", "Late (31-120 days)",
    "Charged Off", "Default"
]


def loans_ingested(df: DataFrame) -> DataFrame:
    """Stamps each loan record with the pipeline load timestamp."""
    return df.withColumn("ingest_date", F.current_timestamp())


def loans_filtered(df: DataFrame) -> DataFrame:
    """
    Removes structurally invalid loan records:
      - Null loan_id or member_id (cannot be joined to other datasets)
      - Null or zero loan_amount (no financial basis for the record)
      - loan_status values outside the known valid set (data corruption)
    """
    return (
        df
        .filter(F.col("loan_id").isNotNull())
        .filter(F.col("member_id").isNotNull())
        .filter(F.col("loan_amount").isNotNull() & (F.col("loan_amount") > 0))
        .filter(F.col("loan_status").isin(_VALID_LOAN_STATUSES))
    )


def loans_term_modified(df: DataFrame) -> DataFrame:
    """
    Normalises loan_term_months from string to integer.
      '36 months' -> 36
      '60 months' -> 60

    Strips all non-numeric characters before casting.
    Records that do not parse to a positive integer are set to null.
    """
    return (
        df
        .withColumn(
            "loan_term_months",
            F.regexp_replace(F.col("loan_term_months"), r"[^0-9]", "").cast("int")
        )
        .withColumn(
            "loan_term_months",
            F.when(F.col("loan_term_months") > 0, F.col("loan_term_months"))
             .otherwise(None)
        )
    )


def loans_purpose_modified(df: DataFrame) -> DataFrame:
    """
    Standardises loan_purpose values:
      - Lowercases and trims
      - Replaces spaces and hyphens with underscores for consistent naming
      - Maps rare/miscellaneous categories to 'other'

    The resulting values are clean categorical labels suitable for
    downstream groupBy aggregations and ML feature encoding.
    """
    rare_purposes = ["educational", "wedding", "renewable_energy", "vacation"]

    return (
        df
        .withColumn(
            "loan_purpose",
            F.lower(F.trim(F.col("loan_purpose")))
        )
        .withColumn(
            "loan_purpose",
            F.regexp_replace(F.col("loan_purpose"), r"[\s\-]+", "_")
        )
        .withColumn(
            "loan_purpose",
            F.when(F.col("loan_purpose").isin(rare_purposes), F.lit("other"))
             .otherwise(F.col("loan_purpose"))
        )
    )


# =============================================================================
# LOAN REPAYMENTS
# =============================================================================

_REPAYMENT_DATE_FORMAT = "MMM-yyyy"   # e.g. "Jan-2020"


def repayments_ingested(df: DataFrame) -> DataFrame:
    """Stamps each repayment record with the pipeline load timestamp."""
    return df.withColumn("ingest_date", F.current_timestamp())


def repayments_filtered(df: DataFrame) -> DataFrame:
    """
    Removes repayment records that cannot be linked or are financially meaningless:
      - Null loan_id (cannot join to loans dataset)
      - total_payment_received is null or <= 0 (no payment activity)
    """
    return (
        df
        .filter(F.col("loan_id").isNotNull())
        .filter(
            F.col("total_payment_received").isNotNull() &
            (F.col("total_payment_received") > 0)
        )
    )


def repayments_null_amounts_filled(df: DataFrame) -> DataFrame:
    """
    Fills null values in individual payment component columns with 0.0.
    These represent missing sub-components (e.g. no late fee was charged),
    not missing records — zero is the correct semantic fill.

    Columns handled:
      total_principal_received, total_interest_received,
      total_late_fee_received, last_payment_amount
    """
    fill_map = {
        "total_principal_received": 0.0,
        "total_interest_received":  0.0,
        "total_late_fee_received":  0.0,
        "last_payment_amount":      0.0,
    }
    return df.fillna(fill_map)


def repayments_total_derived(df: DataFrame) -> DataFrame:
    """
    Derives total_payment_received from its components where the top-level
    total is null but the sub-components are present.

    total_payment_received = principal + interest + late_fee

    If the existing total_payment_received is already populated, it is kept.
    This handles partial data from upstream systems that log components
    before the rolled-up total is available.
    """
    derived = (
        F.col("total_principal_received") +
        F.col("total_interest_received") +
        F.col("total_late_fee_received")
    )
    return df.withColumn(
        "total_payment_received",
        F.when(
            F.col("total_payment_received").isNull(), derived
        ).otherwise(F.col("total_payment_received"))
    )


def repayments_last_date_parsed(df: DataFrame) -> DataFrame:
    """
    Parses last_payment_date from 'MMM-yyyy' string (e.g. 'Jan-2020')
    to a proper DateType column.
    Unparseable values are set to null rather than crashing the pipeline.
    """
    return df.withColumn(
        "last_payment_date",
        F.to_date(F.col("last_payment_date"), _REPAYMENT_DATE_FORMAT)
    )


def repayments_next_date_parsed(df: DataFrame) -> DataFrame:
    """
    Parses next_payment_date from 'MMM-yyyy' string (e.g. 'Feb-2020')
    to a proper DateType column.
    Unparseable values are set to null rather than crashing the pipeline.
    """
    return df.withColumn(
        "next_payment_date",
        F.to_date(F.col("next_payment_date"), _REPAYMENT_DATE_FORMAT)
    )


# =============================================================================
# LOAN DEFAULTERS
# =============================================================================

def defaulters_ingested(df: DataFrame) -> DataFrame:
    """
    Base cleaning pass for defaulter records:
      - Fills null values in all numeric risk columns with 0.0
        (null means no recorded event, not missing data)
      - Stamps each record with the pipeline load timestamp
    """
    fill_map = {
        "delinq_2yrs":            0.0,
        "delinq_amnt":            0.0,
        "pub_rec":                0.0,
        "pub_rec_bankruptcies":   0.0,
        "inq_last_6mths":         0.0,
        "total_rec_late_fee":     0.0,
        "mths_since_last_delinq": 0.0,
        "mths_since_last_record": 0.0,
    }
    return df.fillna(fill_map).withColumn("ingest_date", F.current_timestamp())


def defaulters_delinquency(df: DataFrame) -> DataFrame:
    """
    Extracts members with active delinquency signals:
      - delinq_2yrs > 0    : had at least one delinquency in the past 2 years
      - delinq_amnt > 0    : has an outstanding delinquent amount
      - mths_since_last_delinq > 0 : has a recorded delinquency history

    Uses OR logic — any one signal is sufficient to flag the member.
    Result is used for credit risk scoring and collections prioritisation.
    """
    return df.filter(
        (F.col("delinq_2yrs") > 0) |
        (F.col("delinq_amnt") > 0) |
        (F.col("mths_since_last_delinq") > 0)
    ).withColumn("risk_flag", F.lit("delinquency"))


def defaulters_public_records(df: DataFrame) -> DataFrame:
    """
    Extracts members with public record or enquiry risk signals:
      - pub_rec > 0              : has derogatory public records (e.g. bankruptcies)
      - pub_rec_bankruptcies > 0 : specifically has bankruptcy filings
      - inq_last_6mths > 0      : had credit enquiries in the last 6 months
                                   (indicator of financial stress)

    Result is used for regulatory compliance checks and underwriting review.
    """
    return df.filter(
        (F.col("pub_rec") > 0) |
        (F.col("pub_rec_bankruptcies") > 0) |
        (F.col("inq_last_6mths") > 0)
    ).withColumn("risk_flag", F.lit("public_record"))
