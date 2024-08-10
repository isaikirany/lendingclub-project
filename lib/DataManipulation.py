from pyspark.sql.functions import *


def rename_customer_columns(customers_df):
    return customers_df.withColumnRenamed("annual_inc", "annual_income") \
    .withColumnRenamed("addr_state", "address_state") \
    .withColumnRenamed("zip_code", "address_zipcode") \
    .withColumnRenamed("country", "address_country") \
    .withColumnRenamed("tot_hi_credit_lim", "total_high_credit_limit") \
    .withColumnRenamed("annual_inc_joint", "join_annual_income")

def customers_df_ingested(customers_df):
    return customers_df.withColumn("ingest_date",current_timestamp())

def customers_df_distinct(customers_df):
    return customers_df.distinct()

def customers_income_filtered(customers_df,spark):
    customers_df.createOrReplaceTempView("customers")
    return spark.sql("select * from customers where annual_income is not null")  

def customers_emplength_cleaned(customers_income_filtered):
    customers_emplength_cleaned = customers_income_filtered.withColumn("emp_length",regex_replace(col("emp_length"),"(\D)",""))
    return customers_emplength_cleaned.withColumn("emp_length", customers_emplength_cleaned.emp_length.cast('int'))                                

def join_orders_customers(orders_df, customers_df):
    return orders_df.join(customers_df, "customer_id")
def count_orders_state(joined_df):
    return joined_df.groupBy('state').count()
