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

# Leitura dos arquivos silver
silver_file_path = f"{_PARAM_BUCKET_DATALAKE}/{_PARAM_DATALAKE_SILVER_PREFIX}/"
spark.read.parquet(silver_file_path)\
    .createOrReplaceTempView('silver')

try:
    # Verificação de Duplicidade
    df_validacao = spark.sql("""
        SELECT True AS validation 
        FROM silver 
        GROUP BY id 
        HAVING count(1) > 1
    """)

    if df_validacao.count() > 0:
        raise Exception(f'ERRO: {df_validacao.count()} registros duplicados encontrados')
        
    # Verificação de Nulicidade
    colunas_nao_nulas = ['id', 'brewery_type', 'country']
    
    for coluna in colunas_nao_nulas:
        df_validacao = spark.sql(f"""
            SELECT True AS validation 
            FROM silver 
            WHERE {coluna} IS NULL
        """)

        if df_validacao.count() > 0:
            raise Exception(f'ERRO: {coluna} possui registros nulos.')
        
except Exception as e:
    raise Exception(f"Erro na estrutura da tabela. {e}")