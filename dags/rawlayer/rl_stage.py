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
    df['record_created_date_time'] = datetime.now()
    return df

# Define DataFrame writing function
def writing_dataframe(df, table_name):
    df.to_sql(table_name, con=create_engine("postgresql://postgres:postgres_1010@postgress-instance.cbhcjukrp6zc.ap-south-1.rds.amazonaws.com:5432/poc_bank"), schema='silver', if_exists='replace', index=False)

# Process each table from raw layer data
for table in rawlayer_data["table_name"]:
    df = dataframe_creation(table)
    print(df.info())  # Check DataFrame information
    writing_dataframe(df, table)

# Process branch employee data
branch_employee = rawlayer_data["tb_branch_employee"]
df = dataframe_creation(branch_employee)
employee_df = df[['employee_id', 'branch_id', 'name']].sort_values(by='employee_id')
branch_df = df[['branch_id', 'branch_name', 'city', 'region', 'division']].drop_duplicates(subset=['branch_id']).sort_values(by='branch_id')

branch = transformation_data["tb_branch_employee"][0]
employee = transformation_data["tb_branch_employee"][1]
writing_dataframe(branch_df, branch)
writing_dataframe(employee_df, employee)

# Process customer account data
customer_account = rawlayer_data["tb_customer_account"]
cus_accdf = dataframe_creation(customer_account)
customer_df = cus_accdf[['customer_skey', 'customer_id', 'branch_id', 'employee_id', 'age', 'age_level', 'occupation', 'income', 'income_level', 'record_created_date_time']].drop_duplicates(subset=['customer_id']).sort_values(by='customer_id')
account_df = cus_accdf[['account_skey', 'account_id', 'customer_id', 'account_type', 'balance', 'account_open_date', 'account_end_date', 'status', 'record_created_date_time']].sort_values(by='account_id')

writing_dataframe(customer_df, transformation_data["tb_customer_account"][0])
writing_dataframe(account_df, transformation_data["tb_customer_account"][1])
