import sys
from lib import DataManipulation, DataReader, Utils, logger
from pyspark.sql.functions import *
from lib.logger import Log4j

if __name__ == '__main__':

    if len(sys.argv) < 2:
        print("Please specify the environment")
        sys.exit(-1)

    job_run_env = sys.argv[1]

    print("Creating Spark Session")

    spark = Utils.get_spark_session(job_run_env)

    logger = Log4j(spark)

    logger.warn("Created Spark Session")

#Customers Data Cleanup
    orders_df = DataReader.read_orders(spark,job_run_env)
    orders_filtered = DataManipulation.filter_closed_orders(orders_df)
    customers_df = DataReader.read_customers(spark,job_run_env)
    rename_customer_columns_df = DataManipulation.rename_customer_columns(customers_df)
    customers_df_ingested_df = DataManipulation.customers_df_ingested(rename_customer_columns_df)
    customers_df_distinct = DataManipulation.customers_df_distinct(customers_df_ingested_df)
    customers_income_filtered_df = DataManipulation.customers_income_filtered(customers_df_distinct,spark)
    customers_emplength_cleaned_df = DataManipulation.customers_emplength_cleaned(customers_income_filtered_df)
    avg_emp_length_df = DataManipulation.avg_emp_length(customers_emplength_cleaned_df,spark)
    customers_state_cleaned_df = DataManipulation.customers_state_cleaned(avg_emp_length_df)
    
    customers_state_cleaned_df.write \
    .option("header", True) \
    .format("csv") \
    .mode("overwrite") \
    .option("path", "data/cleaned/customers_csv") \
    .save()

    loans_df = DataReader.read_loans(spark,job_run_env)
    loans_ingestd_df = DataManipulation.loans_ingestd(loans_df)
    loans_filtered_df = DataManipulation.loans_filtered(loans_ingestd_df)
    loans_term_modified_df = DataManipulation.loans_term_modified(loans_filtered_df)
    loans_purpose_modified_df = DataManipulation.loans_purpose_modified(loans_term_modified_df)

    loans_purpose_modified_df.write \
    .option("header", True) \
    .format("csv") \
    .mode("overwrite") \
    .option("path", "data/cleaned/loans_csv") \
    .save()

    loans_repay_df = DataReader.read_loan_repayments(spark,job_run_env)
    loans_repay_ingested_df = DataManipulation.loans_repay_ingestd(loans_repay_df)
    loans_repay_filtered_df = DataManipulation.loans_repay_filtered(loans_repay_ingested_df)
    loans_payments_fixed_df = DataManipulation.loans_payments_fixed(loans_repay_filtered_df)
    loans_payments_fixed2_df = DataManipulation.loans_payments_fixed2(loans_payments_fixed_df)
    loans_payments_ldate_fixed_df = DataManipulation.loans_payments_ldate_fixed(loans_payments_fixed2_df)
    loans_payments_ndate_fixed_df = DataManipulation.loans_payments_ndate_fixed(loans_payments_ldate_fixed_df)
    #print(aggregated_results.collect())

    loans_payments_ndate_fixed_df.write \
    .option("header", True) \
    .format("csv") \
    .mode("overwrite") \
    .option("path", "data/cleaned/loans_repayments_csv") \
    .save()

    loan_defaulters_df = DataReader.read_loan_defaulters(spark,job_run_env)
    loans_def_processed_df = DataManipulation.loans_def_processed(loan_defaulters_df)
    loans_def_delinq_df = DataManipulation.loans_def_delinq(loans_def_processed_df,spark)
    loans_def_records_enq_df = DataManipulation.loans_def_records_enq(loans_def_processed_df,spark)

    loans_def_delinq_df.write \
    .option("header", True) \
    .format("csv") \
    .mode("overwrite") \
    .option("path", "data/cleaned/customers.csv") \
    .save()

    loans_def_records_enq_df.write \
    .option("header", True) \
    .format("csv") \
    .mode("overwrite") \
    .option("path", "data/cleaned/loans_defaulters_records_enq_csv") \
    .save()

    logger.info("this is the end of main")