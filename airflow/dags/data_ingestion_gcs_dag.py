# Python Standard Library Imports 
import os
import fnmatch

# Imports for Airflow
from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

# Imports for google cloud storage 
from google.cloud import storage
from airflow.providers.google.cloud.operators.bigquery import BigQueryCreateExternalTableOperator
import pyarrow.csv as pv
import pyarrow.parquet as pq

# Imports for Pyspark
import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql import types

# Setting up variables
PROJECT_ID = os.environ.get("GCP_PROJECT_ID")
BUCKET = os.environ.get("GCP_GCS_BUCKET")
path_to_local_home = os.environ.get("AIRFLOW_HOME", "/opt/airflow/")
BIGQUERY_DATASET = os.environ.get("BIGQUERY_DATASET", 'ppp_data_all')

# Function that returns a link to the section of data determined by the run number 
def choose_link(ti):
   test_list = [    'https://data.sba.gov/dataset/8aa276e2-6cab-4f86-aca4-a7dde42adf24/resource/c84fa84d-c047-4b66-8056-5748f6a2bfca/download/public_150k_plus_220102.csv',
    'https://data.sba.gov/dataset/8aa276e2-6cab-4f86-aca4-a7dde42adf24/resource/af49f823-84da-4900-8a29-14d37783312c/download/public_up_to_150k_1_220102.csv',
    'https://data.sba.gov/dataset/8aa276e2-6cab-4f86-aca4-a7dde42adf24/resource/7dd5066e-587b-4e7f-97d6-aeeb8d43e95e/download/public_up_to_150k_2_220102.csv',
    'https://data.sba.gov/dataset/8aa276e2-6cab-4f86-aca4-a7dde42adf24/resource/87226a98-ab63-4d24-8c13-862dc4e231ce/download/public_up_to_150k_3_220102.csv',
    'https://data.sba.gov/dataset/8aa276e2-6cab-4f86-aca4-a7dde42adf24/resource/9fc51bda-fb7f-4e60-a768-9f872bc97e20/download/public_up_to_150k_4_220102.csv',
    'https://data.sba.gov/dataset/8aa276e2-6cab-4f86-aca4-a7dde42adf24/resource/3c6c0c0e-ed9f-478f-a6f4-56e0c7819a9d/download/public_up_to_150k_5_220102.csv',
    'https://data.sba.gov/dataset/8aa276e2-6cab-4f86-aca4-a7dde42adf24/resource/de046832-58bb-4239-ac46-e17d42352126/download/public_up_to_150k_6_220102.csv',
    'https://data.sba.gov/dataset/8aa276e2-6cab-4f86-aca4-a7dde42adf24/resource/77531198-8895-4bf2-ba25-cf0b74449545/download/public_up_to_150k_7_220102.csv',
    'https://data.sba.gov/dataset/8aa276e2-6cab-4f86-aca4-a7dde42adf24/resource/0ac12ffc-0502-4655-81ff-a479b4302ee7/download/public_up_to_150k_8_220102.csv',
    'https://data.sba.gov/dataset/8aa276e2-6cab-4f86-aca4-a7dde42adf24/resource/0b2cbc69-9a58-4b28-8fd6-3426243b8f90/download/public_up_to_150k_9_220102.csv',
    'https://data.sba.gov/dataset/8aa276e2-6cab-4f86-aca4-a7dde42adf24/resource/0e8a121f-3baa-4b7e-ba71-466b867dcae8/download/public_up_to_150k_10_220102.csv',
    'https://data.sba.gov/dataset/8aa276e2-6cab-4f86-aca4-a7dde42adf24/resource/23cbab5f-781d-498c-b7d7-7604d5cb6bde/download/public_up_to_150k_11_220102.csv',
    'https://data.sba.gov/dataset/8aa276e2-6cab-4f86-aca4-a7dde42adf24/resource/809345ac-f85d-487e-b07b-f9a34064564d/download/public_up_to_150k_12_220102.csv',
]
   value = ti.xcom_pull(key='return_value', task_ids = 'wget')
   print(value)
   ti.xcom_push(key = 'return_value', value=test_list[int(value)])


def pyspark_transform(src_file):
    """ Takes the downloaded csv file and makes necessaary transformations. It then merges the NAICS codes based on the file we have saved.
    Finally, it saves our data as a parquet file """

    print(src_file)

    # Sets up our spark session 
    spark = SparkSession.builder\
        .master("local[*]")\
        .appName('test')\
        .getOrCreate() 

    # The schema for our incoming data 
    schema = types.StructType([
        types.StructField('LoanNumber',types.StringType(),True),
        types.StructField('DateApproved',types.StringType(),True),
        types.StructField('SBAOfficeCode',types.StringType(),True),
        types.StructField('ProcessingMethod',types.StringType(),True),
        types.StructField('BorrowerName',types.StringType(),True),
        types.StructField('BorrowerAddress',types.StringType(),True),
        types.StructField('BorrowerCity',types.StringType(),True),
        types.StructField('BorrowerState',types.StringType(),True),
        types.StructField('BorrowerZip',types.StringType(),True),
        types.StructField('LoanStatusDate',types.DateType(),True),
        types.StructField('LoanStatus',types.StringType(),True),
        types.StructField('Term',types.IntegerType(),True),
        types.StructField('SBAGuarantyPercentage',types.IntegerType(),True),
        types.StructField('InitialApprovalAmount',types.DoubleType(),True),
        types.StructField('CurrentApprovalAmount',types.DoubleType(),True),
        types.StructField('UndisbursedAmount',types.IntegerType(),True),
        types.StructField('FranchiseName',types.StringType(),True),
        types.StructField('ServicingLenderLocationID',types.StringType(),True),
        types.StructField('ServicingLenderName',types.StringType(),True),
        types.StructField('ServicingLenderAddress',types.StringType(),True),
        types.StructField('ServicingLenderCity',types.StringType(),True),
        types.StructField('ServicingLenderState',types.StringType(),True),
        types.StructField('ServicingLenderZip',types.StringType(),True),
        types.StructField('RuralUrbanIndicator',types.StringType(),True),
        types.StructField('HubzoneIndicator',types.StringType(),True),
        types.StructField('LMIIndicator',types.StringType(),True),
        types.StructField('BusinessAgeDescription',types.StringType(),True),
        types.StructField('ProjectCity',types.StringType(),True),
        types.StructField('ProjectCountyName',types.StringType(),True),
        types.StructField('ProjectState',types.StringType(),True),
        types.StructField('ProjectZip',types.StringType(),True),
        types.StructField('CD',types.StringType(),True),
        types.StructField('JobsReported',types.IntegerType(),True),
        types.StructField('NAICSCode',types.DoubleType(),True),
        types.StructField('Race',types.StringType(),True),
        types.StructField('Ethnicity',types.StringType(),True),
        types.StructField('UTILITIES_PROCEED',types.FloatType(),True),
        types.StructField('PAYROLL_PROCEED',types.DoubleType(),True),
        types.StructField('MORTGAGE_INTEREST_PROCEED',types.DoubleType(),True),
        types.StructField('RENT_PROCEED',types.DoubleType(),True),
        types.StructField('REFINANCE_EIDL_PROCEED',types.DoubleType(),True),
        types.StructField('HEALTH_CARE_PROCEED',types.DoubleType(),True),
        types.StructField('DEBT_INTEREST_PROCEED',types.DoubleType(),True),
        types.StructField('BusinessType',types.StringType(),True),
        types.StructField('OriginatingLenderLocationID',types.StringType(),True),
        types.StructField('OriginatingLender',types.StringType(),True),
        types.StructField('OriginatingLenderCity',types.StringType(),True),
        types.StructField('OriginatingLenderState',types.StringType(),True),
        types.StructField('Gender',types.StringType(),True),
        types.StructField('Veteran',types.StringType(),True),
        types.StructField('NonProfit',types.StringType(),True),
        types.StructField('ForgivenessAmount',types.DoubleType(),True),
        types.StructField('ForgivenessDate',types.StringType(),True)
    ])

    # Schema for our NAICS code csv file 
    naics_schema = types.StructType([
        types.StructField('NAICS_code',types.DoubleType(),True),
        types.StructField('NAICS_Title',types.StringType(),True),
    ])

    # Converts the dates in our csv file to a format that pyspark can read 
    df = spark.read.option("header","true").schema(schema).csv(src_file)
    df.select(col("DateApproved"),to_date(col("DateApproved"), 'yyyy-MM-dd'))
    df.select(col("ForgivenessDate"),to_date(col("ForgivenessDate"), 'yyyy-MM-dd'))

    # Reads in our naics data
    naics_df = spark.read.option("header","true").schema(naics_schema).csv(f'{path_to_local_home}/addl_files/NAICS_codes.csv')

    # Merges our two pyspark dataframes, standardizes our zip code, and then saves down as parquet files 
    result_df = df.join(naics_df, df.NAICSCode == naics_df.NAICS_code, "left")
    result_df = result_df.withColumn("BorrowerZip",when(col("BorrowerZip").contains('9'),regexp_replace('BorrowerZip','(?=-).*','')).otherwise(col("BorrowerZip")))
    result_df = result_df.drop('NAICS_code','NAICSCode','ProcessingMethod','LoanStatusDate','ServicingLenderLocationID','ServicingLenderName','ServicingLenderAddress','ServicingLenderCity','ServicingLenderState','ServicingLenderZip')
    result_df.write.mode('overwrite').parquet(src_file.replace('.csv',''))

# NOTE: takes 20 mins, at an upload speed of 800kbps. Faster if your internet has a better upload speed
def upload_to_gcs(bucket, object_name, local_file):
    """
    Ref: https://cloud.google.com/storage/docs/uploading-objects#storage-upload-object-python
    :param bucket: GCS bucket name
    :param object_name: target path & file-name
    :param local_file: source path & file-name
    :return:
    """
    # WORKAROUND to prevent timeout for files > 6 MB on 800 kbps upload speed.
    # (Ref: https://github.com/googleapis/python-storage/issues/74)
    storage.blob._MAX_MULTIPART_SIZE = 5 * 1024 * 1024  # 5 MB
    storage.blob._DEFAULT_CHUNKSIZE = 5 * 1024 * 1024  # 5 MB
    # End of Workaround
    client = storage.Client()
    bucket = client.bucket(bucket)
    print(os.listdir(local_file))
    number = 0 
    for file in os.listdir(local_file):
        if fnmatch.fnmatch(file, 'part*.parquet'):
            blob = bucket.blob(f"raw/{object_name}/part_{number}.parquet")
            blob.upload_from_filename(f'{local_file}/{file}')
            number += 1

default_args = {
    "owner": "airflow",
    "start_date": days_ago(13),  # There are a total of 13 files that need to be downloaded
    "depends_on_past": False,
    "retries": 1,
}

# NOTE: DAG declaration - using a Context Manager (an implicit way)
with DAG(
    dag_id="data_ingestion_gcs_dag",
    schedule_interval="@daily",
    default_args=default_args,
    catchup=True,
    max_active_runs=1,
    tags=['PPP-Project'],
) as dag:

    wget_task = BashOperator(
      task_id = 'wget',
      bash_command = 'echo "{{  (data_interval_start - task.start_date).days }}"',
      do_xcom_push=True
   )

    choose_link = PythonOperator(
        task_id="choose_link",
        python_callable=choose_link
    )

    download_dataset_task = BashOperator(
        task_id="download_dataset_task",
        bash_command="curl -sSL {{ ti.xcom_pull(task_ids=[\'choose_link\'])[0] }} > " + f"{path_to_local_home}" + "/{{  (data_interval_start - task.start_date).days }}.csv" 
    )

    format_to_parquet_task = PythonOperator(
        task_id="format_to_parquet_task",
        python_callable=pyspark_transform,
        op_kwargs={
            "src_file": f"{path_to_local_home}/" + "{{  (data_interval_start - task.start_date).days }}.csv", 
        },
    )

    local_to_gcs_task = PythonOperator(
        task_id="local_to_gcs_task",
        python_callable=upload_to_gcs,
        op_kwargs={
            "bucket": BUCKET,
            "object_name": "{{  (data_interval_start - task.start_date).days }}",
            "local_file": f"{path_to_local_home}/" + "{{  (data_interval_start - task.start_date).days }}", 
        },
    )

    bigquery_external_table_task = BigQueryCreateExternalTableOperator(
        task_id="bigquery_external_table_task",
        table_resource={
            "tableReference": {
                "projectId": PROJECT_ID,
                "datasetId": BIGQUERY_DATASET,
                "tableId": "external_table",
            },
            "externalDataConfiguration": {
                "sourceFormat": "PARQUET",
                "sourceUris": [f"gs://{BUCKET}/raw/*.parquet"],
            },
        },
    )

    wget_task >> choose_link >> download_dataset_task >> format_to_parquet_task >> local_to_gcs_task >> bigquery_external_table_task