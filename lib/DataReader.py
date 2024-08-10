from lib import ConfigReader
#defining customers schema
def get_customers_schema():
    schema = 'member_id string, emp_title string, emp_length string, home_ownership string, annual_inc float, \
                                addr_state string, zip_code string, country string, grade string, sub_grade string, verification_status string, tot_hi_cred_lim float,\
                                application_type string, annual_inc_joint float, verification_status_joint string'
    return schema
# creating customers dataframe
def read_customers(spark,env):
    conf = ConfigReader.get_app_config(env)
    customers_file_path = conf["raw.customers.file.path"]
    return spark.read \
    .format("csv") \
    .option("header", "true") \
    .schema(get_customers_schema()) \
    .load(customers_file_path)
#defining loans schema
def get_loans_schema():
    schema = 'loan_id string, member_id string, loan_amount float, funded_amount float, loan_term_months string,\
                             interest_rate float, monthly_installment float, issue_date string, loan_status string, loan_purpose string, loan_title string'
    return schema
#creating loans dataframe
def read_loans(spark,env):
    conf = ConfigReader.get_app_config(env)
    loans_file_path = conf["raw.loans.file.path "]
    return spark.read \
    .format("csv") \
    .option("header", "true") \
    .schema(get_loans_schema()) \
    .load(loans_file_path)

#defining repayments
def get_loan_repayments_schema():
    schema = 'loan_id string, total_principal_received float, total_interest_received float, total_late_fee_received float, total_payment_received float, \
          last_payment_amount float, last_payment_date string, next_payment_date string'
    return schema

#Creating loan_repayments dataframe
def read_loan_repayments(spark,env):
    conf = ConfigReader.get_app_config(env)
    loans_repayments_path = conf["raw.loan_repayments.file.path"]
    return spark.read \
    .format('csv') \
    .option('header',True) \
    .schema(get_loan_repayments_schema()) \
    .load(loans_repayments_path)

#defining loan defaulters
def get_loan_defaulters_schema():
    schema ="member_id string, delinq_2yrs float, delinq_amnt float, pub_rec float, \
    pub_rec_bankruptcies float,inq_last_6mths float, total_rec_late_fee float, mths_since_last_delinq float, mths_since_last_record float"
    return schema

#Creating loan_defaulters dataframe
def read_loan_defaulters(spark,env):
    conf = ConfigReader.get_app_config(env)
    loans_defaulters_path = conf["raw.loans_defaulters.path"]
    return spark.read \
    .format('csv') \
    .option('header',True) \
    .schema(get_loan_defaulters_schema()) \
    .load(loans_defaulters_path)