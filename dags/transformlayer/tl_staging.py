import pandas 
import json
import yaml
import os
from sqlalchemy import create_engine
from datetime import datetime
import numpy

# Load PostgreSQL details from YAML
dag_directory = os.path.dirname(os.path.abspath('dags/etl.py'))
    # Specify the absolute path to the Python file
yaml_file_path = os.path.join(dag_directory, 'resources/postgress.yaml')
rawlayer_josn = os.path.join(dag_directory, 'resources/rawlayer.json')
transform_josn = os.path.join(dag_directory, 'resources/transform_layer.json')
curated_josn = os.path.join(dag_directory, 'resources/curated.json')

    # Run the Python file
with open(yaml_file_path, 'r') as file:
    yaml_data = yaml.load(file, Loader=yaml.FullLoader)
    
host = yaml_data["postgress_details"]["host"]
port = yaml_data["postgress_details"]["port_no"]
username = yaml_data["postgress_details"]["username"]
password = yaml_data["postgress_details"]["password"]
db_name = "poc_bank"

# Define SQLAlchemy connection string
connection_string = f"postgresql://{username}:{password}@{host}:{port}/{db_name}"

# Create SQLAlchemy engine
engine = create_engine(connection_string)

# Load raw layer and transformation layer data from JSON
with open(rawlayer_josn, 'r') as file:
    rawlayer_data = json.load(file)

with open(transform_josn, 'r') as file:
    transformation_data = json.load(file)

with open(curated_josn, 'r') as file:
    curated_data = json.load(file)

# Define DataFrame creation function
def dataframe_creation(tablename):
    # Read data from PostgreSQL into DataFrame
    query = f'SELECT * FROM silver."{tablename}"'
    df = pandas.read_sql_query(query, con=create_engine("postgresql://postgres:postgres_1010@postgress-instance.cbhcjukrp6zc.ap-south-1.rds.amazonaws.com:5432/poc_bank"))
    return df

# Define DataFrame writing function
def writing_dataframe(df, table_name):
    df.to_sql(table_name, con=create_engine("postgresql://postgres:postgres_1010@postgress-instance.cbhcjukrp6zc.ap-south-1.rds.amazonaws.com:5432/poc_bank"), schema='gold', if_exists='replace', index=False)

def calculations(df, column_used, current_month_values, current_year_values, pre_month_values, pre_year_values,total_values):
    
    # Group the DataFrame by 'branch_id', 'fin_month', 'fin_year'
    grouped = df.groupby(['branch_id', 'fin_month', 'fin_year'])
    grouped_2 = df.groupby(["branch_id", "fin_year"])

    # Calculate current_month_deposit
    df[f"{current_month_values}"] = grouped[[f"{column_used}"]].transform('sum')

    # Calculate current_year_deposit
    df[f"{current_year_values}"] = grouped_2[[f"{current_month_values}"]].cumsum()
    

    # Calculate pre_month_deposit
    df[f"{pre_month_values}"] = df[[f"{column_used}"]].shift(1)
    df[f"{pre_month_values}"].fillna(0)

    # Calculate pre_year_deposit
    df[f"{pre_year_values}"] = df.groupby(["branch_id", "fin_year"])[[f"{column_used}"]].transform('sum').shift(12).fillna(0)
    df[f"{total_values}"] = df[f"{current_year_values}"]+df[f"{pre_year_values}"]

    return df

# Process branch employee data
branch_employee = rawlayer_data["tb_branch_employee"]
df = dataframe_creation("tb_branch_employee")
tb_employee = df[['employee_id', 'branch_id', 'name','record_created_date_time']].sort_values(by='employee_id')
tb_branch = df[['branch_id', 'branch_name', 'city', 'region', 'division','record_created_date_time']].drop_duplicates(subset=['branch_id']).sort_values(by='branch_id')



# Process customer account data
customer_account = rawlayer_data["tb_customer_account"]
cus_accdf = dataframe_creation("tb_customer_account")
tb_customer = cus_accdf[['customer_skey', 'customer_id', 'branch_id', 'employee_id', 'age', 'age_level', 'occupation', 'income', 'income_level', 'record_created_date_time']].drop_duplicates(subset=['customer_id']).sort_values(by='customer_id')
tb_account = cus_accdf[['account_skey', 'account_id', 'customer_id', 'account_type', 'balance', 'account_open_date', 'account_end_date', 'status', 'record_created_date_time']].sort_values(by='account_id')

# Process each table from raw layer data
tb_financial_statement = dataframe_creation(transformation_data["tb_branch_financial_statement"])
tb_risk_weighted_asset = dataframe_creation(curated_data["tb_branch_performance"][2])
tb_feedback = dataframe_creation(curated_data["tb_branch_performance"][3])
tb_complaint = dataframe_creation("tb_complaint")
tb_transaction = dataframe_creation("tb_customer_transaction")



df = tb_financial_statement.copy()
df["fin_year"] = df["date"].dt.year
df["fin_month"] = df["date"].dt.month

df = calculations(df,"deposits", "current_month_deposits","current_year_deposits", "pre_month_deposits", "pre_year_deposits","total_deposit")
df = calculations(df,"net_profit", "current_month_profit","current_year_profit", "pre_month_profit", "pre_year_profit","net_profit")
df = calculations(df,"asset", "current_month_asset","current_year_asset","pre_month_asset", "pre_year_asset","total_asset")
df = calculations(df,"interest_income", "current_month_income","current_year_income", "pre_month_income", "pre_year_income","total_income")
df = calculations(df,"loans", "current_month_loan","current_year_loan", "pre_month_loan", "pre_year_loan","total_loan")
df["net_income"] = (df['interest_income'] - df['interest_expense'])
grouped = df.groupby(['branch_id', 'fin_year','fin_month'])

# Calculate net interest margin
df["net_interest_margin"] = ((df['interest_income'] - df['interest_expense']) / (df['asset'] - df['cash_balance'])).round(2)

df["loan_to_deposit_ratio"] = grouped['loans'].transform('sum') / grouped['deposits'].transform('sum')


risk_df = tb_risk_weighted_asset.copy()
risk_df['investment_date'] = pandas.to_datetime(risk_df['investment_date'])
risk_df["fin_year"] = risk_df["investment_date"].dt.year
risk_df["fin_month"] = risk_df["investment_date"].dt.month
grouped = risk_df.groupby(["branch_id","fin_year","fin_month"])
risk_df["risk_weighted_asset"] = (grouped['investment_amount'].transform('sum') * grouped['risk_weight_percent'].transform('sum'))/100

# df["capital_adequate_ratio"] = (grouped['net_profit'].transform('sum') + grouped['cash_balance'].transform('sum'))/grouped['risk_weighted_asset'].transform("sum")


conditions = [
    risk_df['risk_weight_percent'] >= 50,
    risk_df['risk_weight_percent'] >= 30,
]

choices = [
    "High Risk",
    "Medium Risk",
]

default_choice = "Low Risk"
risk_df['risk_classification'] = numpy.select(conditions, choices, default_choice)
fin_df = pandas.merge(df, risk_df, on=["branch_id","fin_year","fin_month"], how='inner')
fin_df = fin_df[["date","cash_balance","interest_expense","liabilites","branch_id","current_month_deposits","pre_month_deposits","current_year_deposits","current_month_profit","pre_month_profit","pre_year_deposits","total_deposit","current_month_asset","pre_month_asset","pre_month_loan","current_month_loan","current_year_profit","pre_month_income","pre_year_profit","net_profit","current_year_asset","pre_year_asset","total_asset","current_year_loan","pre_year_loan","total_loan","current_month_income","current_year_income","pre_year_income","net_income","loan_to_deposit_ratio","net_interest_margin","risk_weighted_asset","fin_month","fin_year","risk_classification"]]
grouped = fin_df.groupby(['branch_id', 'fin_year','fin_month'])
fin_df["capital_adequate_ratio"] = (grouped['net_profit'].transform('sum') + grouped['cash_balance'].transform('sum'))/grouped['risk_weighted_asset'].transform("sum")
merged_df = pandas.merge(tb_account, tb_customer, on='customer_id', how='left')
merged_df = merged_df[["account_skey","account_id","customer_id","branch_id","account_type","balance","account_open_date","account_end_date","status"]]

acc_df = merged_df
acc_df["acc_year"] = acc_df["account_open_date"].dt.year
acc_df["acc_month"] = acc_df["account_open_date"].dt.month
grouped = acc_df.groupby("branch_id")
# Step 3: Calculate current total of accounts for each branch
total_accounts = grouped.size().reset_index(name='total_accounts')
total_accounts['current_total_accounts'] = total_accounts['total_accounts'].cumsum()

# Step 4: Shift current total of accounts to get previous month's accounts
total_accounts['previous_month_accounts'] = total_accounts['current_total_accounts'].shift()

# Fill NaN values in the 'previous_month_accounts' column with 0
total_accounts["previous_month_accounts"] = total_accounts['previous_month_accounts'].fillna(0).astype(int)
acc_df = pandas.merge(acc_df, total_accounts, on="branch_id", how="inner")
fin_df = pandas.merge(fin_df, total_accounts, on="branch_id", how='inner')

tb_branch_comparison = fin_df[["branch_id","fin_year","total_deposit","total_loan","capital_adequate_ratio"]]
tb_branch_comparison = pandas.merge(tb_branch_comparison,tb_branch,on="branch_id",how='left')
tb_branch_comparison = tb_branch_comparison[["branch_id","branch_name","fin_year","total_deposit","total_loan","capital_adequate_ratio"]].drop_duplicates(subset=['branch_id','fin_year'])
# Exclude datetime columns from the summation


# Group by the specified columns and aggregate using sum
# grouped_df = df_numeric.groupby(['branch_id', 'fin_year', 'fin_month'], as_index=False).sum()
tb_dim_date=dataframe_creation("tb_dim_date")
writing_dataframe(tb_dim_date, "vw_dim_date")


writing_dataframe(tb_account, "tb_account")
writing_dataframe(tb_branch, "tb_branch")
writing_dataframe(tb_complaint, curated_data["tb_complaint"])

writing_dataframe(tb_customer, "tb_customer")

writing_dataframe(tb_employee, "tb_employee")

writing_dataframe(tb_feedback, curated_data["tb_feedback"])

writing_dataframe(tb_financial_statement, "vw_branch_financial_statement")

writing_dataframe(tb_risk_weighted_asset, curated_data["tb_risk_weighted_asset"])
writing_dataframe(tb_transaction, "vw_customer_transaction")
# writing_dataframe(fin_df, "tb_branch_performance")
# writing_dataframe(tb_branch_comparison, "tb_branch_comparison")

fin_df.to_csv("output.csv")


