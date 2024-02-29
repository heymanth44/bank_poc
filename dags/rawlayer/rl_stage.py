import pandas 
import json
import yaml
import os
from sqlalchemy import create_engine
from datetime import datetime


# Load PostgreSQL details from YAML
dag_directory = os.path.dirname(os.path.abspath('dags/etl.py'))
    # Specify the absolute path to the Python file
yaml_file_path = os.path.join(dag_directory, 'resources/postgress.yaml')
rawlayer_josn = os.path.join(dag_directory, 'resources/rawlayer.json')
transform_josn = os.path.join(dag_directory, 'resources/transform_layer.json')

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

# Define DataFrame creation function
def dataframe_creation(tablename):
    # Read data from PostgreSQL into DataFrame
    query = f'SELECT * FROM bronze."{tablename}"'
    df = pandas.read_sql_query(query, con=create_engine("postgresql://postgres:postgres_1010@postgress-instance.cbhcjukrp6zc.ap-south-1.rds.amazonaws.com:5432/poc_bank"))
    if tablename != "tb_dim_date":
        df['record_created_date_time'] = datetime.now()
    return df

# Define DataFrame writing function
def writing_dataframe(df, table_name):
    df.to_sql(table_name, con=create_engine("postgresql://postgres:postgres_1010@postgress-instance.cbhcjukrp6zc.ap-south-1.rds.amazonaws.com:5432/poc_bank"), schema='silver', if_exists='replace', index=False)

# Process each table from raw layer data
tb_branch_employee = dataframe_creation(transformation_data["tb_branch_employee"])
tb_branch_financial_statement = dataframe_creation(transformation_data["tb_branch_financial_statement"])
tb_complaint = dataframe_creation(transformation_data["tb_complaint"])
tb_customer_account = dataframe_creation(transformation_data["tb_customer_account"])
tb_customer_transaction = dataframe_creation(transformation_data["tb_customer_transaction"])
tb_feedback = dataframe_creation(transformation_data["tb_feedback"])
tb_risk_weighted_asset = dataframe_creation(transformation_data["tb_risk_weighted_asset"])

# this part is to typecasting tb_risk_weighted_asset table
tb_risk_weighted_asset['investment_date'] = pandas.to_datetime(tb_risk_weighted_asset['investment_date'], format='%d-%m-%y', errors='coerce')
tb_risk_weighted_asset["investment_amount"] = tb_risk_weighted_asset["investment_amount"].astype(float)
tb_risk_weighted_asset["risk_weight_percent"] = tb_risk_weighted_asset["risk_weight_percent"].astype(int)

# this part is to typecasting tb_feedback table
tb_feedback['response_date'] = pandas.to_datetime(tb_feedback['response_date'], format='%d-%m-%y', errors='coerce')
columns_to_convert = ["customer_service","banking_products","transaction_process","digital_banking","atm_services","complaints_and_resolution","financial_advice","security_and_privacy","account_management","fees_and_pricing","nps"]
tb_feedback[columns_to_convert] = tb_feedback[columns_to_convert].astype(int)

# this part is to typecasting tb_complaint table
tb_complaint["complaint_date"] = pandas.to_datetime(tb_complaint["complaint_date"], format='%d-%m-%y', errors='coerce')
tb_complaint["resolution_time"] = tb_complaint["resolution_time"].astype(int)



# this part is to typecasting tb_branch_financial_statement table
tb_branch_financial_statement['date'] = pandas.to_datetime(tb_branch_financial_statement['date'], format='%d-%m-%y', errors='coerce')
columns_to_convert = ["cash_balance","deposits","loans","interest_income","interest_expense","net_profit","asset","liabilites"]
tb_branch_financial_statement[columns_to_convert] = tb_branch_financial_statement[columns_to_convert].astype(float)



# this part is to typecasting tb_customer_acount table
tb_customer_account["account_open_date"] = pandas.to_datetime(tb_customer_account["account_open_date"], format='%d-%m-%y', errors='coerce')
tb_customer_account["account_end_date"] = pandas.to_datetime(tb_customer_account["account_end_date"], format='%d-%m-%y', errors='coerce')
tb_customer_account["balance"] = tb_customer_account["balance"].astype(float)
columns_to_convert = ["age","income"]
tb_customer_account=tb_customer_account.drop(columns=['record_created_datetime','record_modifed_datetime'])
tb_customer_account[columns_to_convert] = tb_customer_account[columns_to_convert].astype(int)

# this part is to typecasting tb_transcation table
tb_customer_transaction['transaction_date'] = pandas.to_datetime(tb_customer_transaction['transaction_date'], format='%d-%m-%y', errors='coerce')
columns_to_convert = ["amount","charges"]
tb_customer_transaction[columns_to_convert] = tb_customer_transaction[columns_to_convert].astype(float)
columns_to_convert = ["age","income"]
tb_customer_transaction[columns_to_convert] = tb_customer_transaction[columns_to_convert].astype(int)

tb_dim_date=dataframe_creation("tb_dim_date")
writing_dataframe(tb_dim_date, "tb_dim_date")




writing_dataframe(tb_branch_employee, transformation_data["tb_branch_employee"])
writing_dataframe(tb_branch_financial_statement, transformation_data["tb_branch_financial_statement"])
writing_dataframe(tb_complaint, transformation_data["tb_complaint"])

writing_dataframe(tb_customer_account, transformation_data["tb_customer_account"])

writing_dataframe(tb_customer_transaction, transformation_data["tb_customer_transaction"])

writing_dataframe(tb_feedback, transformation_data["tb_feedback"])

writing_dataframe(tb_risk_weighted_asset, transformation_data["tb_risk_weighted_asset"])

tb_branch_financial_statement.to_csv("output.csv")