import sys
import boto3
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job

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
_PARAM_DATALAKE_SILVER_PREFIX = get_parameter_ssm('DATALAKE_SILVER_PREFIX')
_PARAM_DATALAKE_GOLD_PREFIX = get_parameter_ssm('DATALAKE_GOLD_PREFIX')

# Leitura dos arquivos silver
silver_file_path = f"{_PARAM_BUCKET_DATALAKE}/{_PARAM_DATALAKE_SILVER_PREFIX}/"
spark.read.parquet(silver_file_path)\
    .createOrReplaceTempView('silver')


# Agrupamento por tipo de cervejaria
df_by_type = spark.sql("""
    SELECT
        brewery_type AS BREWERY_TYPE,
        COUNT(1) AS QUANTITY
    FROM silver
    GROUP BY BREWERY_TYPE
    ORDER BY QUANTITY DESC
""")

## Escrita dos arquivos
df_by_type.write.format("parquet").mode("overwrite")\
    .save(f"{_PARAM_BUCKET_DATALAKE}/{_PARAM_DATALAKE_GOLD_PREFIX}/view_by_type/")

# Agrupamento por localização
df_by_location = spark.sql("""
    SELECT
        country AS COUNTRY,
        state AS STATE,
        COUNT(1) AS QUANTITY
    FROM silver
    GROUP BY country, state
    ORDER BY QUANTITY DESC
""")

## Escrita dos arquivos
df_by_location.write.format("parquet").mode("overwrite")\
    .save(f"{_PARAM_BUCKET_DATALAKE}/{_PARAM_DATALAKE_GOLD_PREFIX}/view_by_location/")