import airflow
from airflow import DAG
from airflow.operators.dummy import DummyOperator
from airflow.providers.google.cloud.operators.gcs import GCSCreateBucketOperator
from airflow.providers.google.cloud.transfers.gcs_to_gcs import GCSToGCSOperator
from airflow.providers.google.cloud.operators.bigquery import *
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import *
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta

default_args = {
    'depends_on_past': False,
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=10)
}
dag=DAG(
    'electric_vehicle_sales',
    default_args=default_args,
    description='A DAG to electric vehicle sales',
    schedule_interval=None,
    start_date=datetime(2024, 7, 17),
    catchup=False,
)

start = DummyOperator(
    task_id='start_task',
    dag=dag
)

create_bucket = GCSCreateBucketOperator(
    task_id='create_bucket',
    bucket_name='electric_vehicle_sales',
    dag=dag
)

copy_sales_by_state = GCSToGCSOperator(
    task_id='copy_sales_by_state',
    source_bucket='electric_automative',
    source_object='electric_vehicle_sales_by_state.csv',
    destination_bucket='electric_vehicle_sales',
    destination_object='electric_vehicle_sales_by_state.csv',
    dag=dag
)

copy_sales_by_makers = GCSToGCSOperator(
    task_id='copy_sales_by_makers',
    source_bucket='electric_automative',
    source_object='electric_vehicle_sales_by_makers.csv',
    destination_bucket='electric_vehicle_sales',
    destination_object='electric_vehicle_sales_by_makers.csv',
    dag=dag
)

copy_dim_date = GCSToGCSOperator(
    task_id='copy_dim_date',
    source_bucket='electric_automative',
    source_object='dim_date.csv',
    destination_bucket='electric_vehicle_sales',
    destination_object='dim_date.csv',
    dag=dag
)

electric_vehicle_sales_dataset_stage=BigQueryCreateEmptyDatasetOperator(
    task_id='electric_vehicle_sales_dataset_stage',
    dataset_id='electric_vehicle_sales_by_state',
    project_id='keshanna-123',
    dag=dag
)


elctric_vehicle_sales_dataproc_cluster=BashOperator(
    task_id='elctric_vehicle_sales_dataproc_cluster',
    bash_command='gcloud dataproc clusters create electric-vehicle-cluster\
         --region=us-central1 --num-masters=1\
             --master-machine-type=e2-standard-2 --master-boot-disk-size=30GB\
                 --num-workers=2 --worker-machine-type=e2-standard-2\
                     --worker-boot-disk-size=30GB --image-version=1.3-debian10 \
                     --network=default --optional-components=jupyter,anaconda',
    dag=dag              
)

electric_vehicle_sales_by_state_spark=BashOperator(
    task_id='electric_vehicle_sales_by_state_spark',
    bash_command='gcloud dataproc jobs submit pyspark \
        --cluster=electric-vehicle-cluster --region=us-central1 \
            --py-files gs://electric_automative/sales_by_state.py gs://electric_automative/sales_by_state.py\
                 --jars=gs://spark-lib/bigquery/spark-bigquery-with-dependencies_2.11-0.23.2.jar',
    dag=dag             
)

electric_vehicle_sales_by_makers_spark=BashOperator(
    task_id='electric_vehicle_sales_by_makers_spark',
    bash_command='gcloud dataproc jobs submit pyspark \
        --cluster=electric-vehicle-cluster --region=us-central1 \
            --py-files gs://electric_automative/sales_by_makers.py gs://electric_automative/sales_by_makers.py\
                 --jars=gs://spark-lib/bigquery/spark-bigquery-with-dependencies_2.11-0.23.2.jar',
    dag=dag    
)

electric_vehicle_dim_date=BashOperator(
    task_id='electric_vehicle_dim_date',
    bash_command='gcloud dataproc jobs submit pyspark \
        --cluster=electric-vehicle-cluster --region=us-central1 \
            --py-files gs://electric_automative/vehicles_dim_date.py gs://electric_automative/vehicles_dim_date.py\
                 --jars=gs://spark-lib/bigquery/spark-bigquery-with-dependencies_2.11-0.23.2.jar',

)


end = DummyOperator(
    task_id='end_task',
    dag=dag
)

start >> create_bucket >>\
    [copy_sales_by_state, copy_sales_by_makers, copy_dim_date] >> \
        electric_vehicle_sales_dataset_stage>>\
            elctric_vehicle_sales_dataproc_cluster>>\
                [electric_vehicle_sales_by_state_spark,electric_vehicle_sales_by_makers_spark,electric_vehicle_dim_date]>>\
            end