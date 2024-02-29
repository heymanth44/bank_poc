
import pyspark
import os
import sys

import yaml
from pyspark.sql import SparkSession
from pyspark.sql.types import DateType, TimestampType, DecimalType
from datetime import datetime
from pyspark.sql.functions import lit
import json


spark = SparkSession.builder \
    .appName("Push DataFrame to PostgreSQL") \
    .config("spark.executor.memory", "2g") \
    .config("spark.driver.memory", "1g") \
    .config("spark.executor.instances", "2") \
    .config("spark.driver.extraClassPath", "postgresql-42.7.0.jar") \
    .config("spark.executor.extraClassPath", "postgresql-42.7.0.jar") \
    .getOrCreate()

sc = pyspark.SparkContext()



with open('resources\postgress.yaml', 'r') as file:
    yaml_data = yaml.load(file, Loader=yaml.FullLoader)
    
host= yaml_data["postgress_details"]["host"]
port = yaml_data["postgress_details"]["port_no"]
username = yaml_data["postgress_details"]["username"]
password = yaml_data["postgress_details"]["password"]
db_name= "poc_bank"
jdbc_url = f"jdbc:postgresql://{host}:{port}/{db_name}"

properties = {
    "user": username,
    "password": password,
}

with open('resources/rawlayer.json', 'r') as file:
    rawlayer_data = json.load(file)

with open('resources/transform_layer.json', 'r') as file:
    transformation_data = json.load(file)

def dataframe_creation(tablename):

    # Read data from PostgreSQL into DataFrame
    df = spark.read.format("jdbc") \
        .option("url", jdbc_url) \
        .option("dbtable", f"""(SELECT * FROM rawlayer."{tablename}") AS subquery""") \
        .options(**properties) \
        .load()
    df = df.withColumn("record_created_date_time", lit(datetime.now()).cast(TimestampType()))

    return df

def writing_dataframe(df, table_name):
    df.write.format("jdbc") \
    .option("url", jdbc_url) \
    .option("dbtable", f"transform_layer.{table_name}") \
    .options(**properties) \
    .mode("overwrite") \
    .save()

for table in rawlayer_data["table_name"]:
    df = dataframe_creation(table)
    df.printSchema()
    writing_dataframe(df,table)
branch_employee = rawlayer_data["branch_employee"]
df = dataframe_creation(branch_employee)
employee_df = df.select("Employee_ID","Branch_ID","Name")
employee_df = employee_df.orderBy("Employee_ID")
branch_df = df.select("Branch_ID","Branch_Name","City","Region","Division")
branch_df = branch_df.dropDuplicates(["Branch_ID"])
branch_df = branch_df.orderBy("Branch_ID")
branch = transformation_data["branch_employee"][0]
employee = transformation_data["branch_employee"][1]
writing_dataframe(branch_df,branch)
writing_dataframe(employee_df,employee)

customer_account = rawlayer_data["customer_account"]
cus_accdf = dataframe_creation(customer_account)
# cus_accdf.show(2)
# cus_accdf.printSchema()
customer_df = cus_accdf.select("Customer_Skey","Customer_ID","Branch_ID","Employee_ID","Age","Age_Level","Occupation","Income","Income_Level","record_created_date_time")
customer_df = customer_df.dropDuplicates(["Customer_ID"])
customer_df = customer_df.orderBy("Customer_ID")
writing_dataframe(customer_df,transformation_data["customer_account"][0])

account_df = cus_accdf.select("Account_SKey","Account_ID","Customer_ID","Account_Type","Balance","Account_open_date","Account_end_date","Status","record_created_date_time")
# account_df = account_df.orderBy("Account_ID")
writing_dataframe(account_df,transformation_data["customer_account"][1])
account_df.printSchema()



