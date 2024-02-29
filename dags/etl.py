from datetime import datetime, timedelta
from airflow import DAG
import subprocess
import pandas
from sqlalchemy import create_engine
import os
from airflow.operators.python_operator import PythonOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.bash_operator import BashOperator

try:
        # Execute pip install command to install PySpark
        subprocess.check_call(["pip", "install", "numpy"])
        print("PySpark installed successfully.")
except subprocess.CalledProcessError as e:
        print(f"Error installing PySpark: {e}")

# try:
#         # Execute pip install command to install PySpark
#         subprocess.check_call(["pip", "install", "sqlalchemy"])
#         print("PySpark installed successfully.")
# except subprocess.CalledProcessError as e:
#         print(f"Error installing PySpark: {e}")

# try:
#         # Execute pip install command to install PySpark
#         subprocess.check_call(["pip", "install", "findspark"])
#         print("PySpark installed successfully.")
# except subprocess.CalledProcessError as e:
#         print(f"Error installing PySpark: {e}")

# try:
#         # Execute pip install command to install PySpark
#         subprocess.check_call(["pip", "install", "pyyaml"])
#         print("pyyaml installed successfully.")
# except subprocess.CalledProcessError as e:
#         print(f"Error installing pyyaml: {e}")




def rl_staging():
    dag_directory = os.path.dirname(os.path.abspath('dags/etl.py'))
    # Specify the absolute path to the Python file
    python_file_path = os.path.join(dag_directory, 'rawlayer/rl_stage.py')
    # Run the Python file
    exec(open(python_file_path).read())

def tl_staging():
    dag_directory = os.path.dirname(os.path.abspath('dags/etl.py'))
    # Specify the absolute path to the Python file
    python_file_path = os.path.join(dag_directory, 'transformlayer/tl_staging.py')
    # Run the Python file
    exec(open(python_file_path).read())

default_args = {
    'owner': 'airflow',
}

dag = DAG(
    'etl_dag',
    start_date=datetime(2024, 2, 1),
    default_args=default_args,
    description='ETL DAG',
    schedule_interval=timedelta(days=1),
    catchup = False
)

start_pipeline = DummyOperator(
    task_id='start_pipeline',
    dag=dag,
)

extract_task = PythonOperator(
    task_id='extract_task',
    python_callable=rl_staging,
    dag=dag,
)

silver_task = PythonOperator(
    task_id='silver_task',
    python_callable=tl_staging,
    dag=dag,
)
# transform_task = BashOperator(
#     task_id='transform_data',
#     bash_command='python /path/to/your/transform_script.py',
#     dag=dag,
# )

# curate_task = BashOperator(
#     task_id='curate_data',
#     bash_command='python /path/to/your/curate_script.py',
#     dag=dag,
# )

end_pipeline = DummyOperator(
    task_id='end_pipeline',
    dag=dag,
)


start_pipeline >> extract_task >> silver_task >> end_pipeline
