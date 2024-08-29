import sys
import boto3
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql.functions import col

## @params: [JOB_NAME]
args = getResolvedOptions(sys.argv, ['JOB_NAME'])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)
job.commit()

ssm_client = boto3.client('ssm', 'us-east-1')

def get_parameter_ssm(parameter_name:str) -> str:
    response = ssm_client.get_parameter(
        Name=parameter_name, 
        WithDecryption=True
    )
    return response['Parameter']['Value']

_PARAM_BUCKET_DATALAKE = f"s3://{get_parameter_ssm('BUCKET_DATALAKE')}"
_PARAM_DATALAKE_BRONZE_PREFIX = get_parameter_ssm('DATALAKE_BRONZE_PREFIX')
_PARAM_DATALAKE_SILVER_PREFIX = get_parameter_ssm('DATALAKE_SILVER_PREFIX')

# Leitura dos arquivos bronze
bronze_file_path = f"{_PARAM_BUCKET_DATALAKE}/{_PARAM_DATALAKE_BRONZE_PREFIX}/"
df_bronze = spark.read.json(bronze_file_path)

df_bronze = df_bronze.where(col('brewery_type')\
                .isin('micro','nano','regional','brewpub','large','planning','bar','contract','proprietor','closed'))

df_bronze\
    .write.partitionBy("country")\
    .format("parquet").mode("overwrite").save(f"{_PARAM_BUCKET_DATALAKE}/{_PARAM_DATALAKE_SILVER_PREFIX}/")