from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.utils.dates import days_ago
from airflow.providers.google.cloud.operators.datafusion import CloudDataFusionStartPipelineOperator
default_args = {
    'owner': 'airflow',
    'start_date': datetime(2023, 12, 18),
    'depends_on_past': False,
    'email': ['pk924730@gmail.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG('employee_data',
          default_args=default_args,
          description='Runs an external Python script',
          schedule_interval='@daily',
          catchup=False)

with dag:
    run_script_task = BashOperator(  #This task will generate fake data using extract.py script and load into gcs
        task_id='extract_data',  
        bash_command='python /home/airflow/gcs/dags/scripts/extract.py', #Bucket Path
    )

    start_pipeline = CloudDataFusionStartPipelineOperator(   #This task will start the data fusion pipeline
    location="us-central1",
    pipeline_name="etl-pipeline",
    instance_name="datafusion-dev",
    task_id="start_datafusion_pipeline",
    )

    run_script_task >> start_pipeline    #This is used for dependecies means after successful completion of run_script_task then only start_pipeline task will start 