import json
import requests
import boto3
import os
import pytz
from itertools import count
from datetime import datetime

'''
instanciamento do client s3 com a SKD boto3, que 
viabiliza utilizar os serviços da AWS programaticamente
'''
client_s3 = boto3.client('s3')
ssm_client = boto3.client('ssm', 'us-east-1')

def get_parameter_ssm(parameter_name:str) -> str:
    response = ssm_client.get_parameter(
        Name=parameter_name, 
        WithDecryption=True
    )
    return response['Parameter']['Value']

def save_file_s3(bucket_name: str, bucket_prefix: str, api_data: list) -> str:
    '''
    Função responsável por salvar os dados retornados da API como um arquivo 
    .json unificado, no bucket/path definidos na configuração da Lambda.
    
    Args: 
        api_data: lista de dicionários contendo os dados retornados pela API.
        
    Returns:
        String com o nome do arquivo criado no bucket do S3.
    '''
    
    
    
    timestamp_now = datetime.timestamp(datetime.now(pytz.timezone('America/Sao_Paulo')))
    file_name = f"{timestamp_now}.json"   
    
    client_s3.put_object(
        Body=json.dumps(api_data, ensure_ascii=False).encode('utf8'), 
        Bucket=bucket_name, 
        Key=f'{bucket_prefix}/{file_name}'
    )
    return file_name

           
def _format_request_params(page=None, per_page=50):
    params = {}

    if page is not None:
        params['page'] = str(page)
        params['per_page'] = str(per_page)

    return params

def _get_request(api_endpoint:str, params=None):
    response = requests.get(
        api_endpoint, 
        params=params
    )
    return response

def _get_data(api_endpoint:str, params=None):
    r = _get_request(
        api_endpoint=api_endpoint,
        params=params
    )
    json = r.json()
    if json:
        return json
    else:
        return None
    
    
def lambda_handler(event: dict, context) -> dict:
    '''
    Função principal do arquivo, é acionada assim que a Lambda é executada.
    
    Returns:
        Dicionário contendo o statusCode do carregando de dados da API e o 
        body contendo o nome do arquivo gerado e armazenado no bucket do S3. 
    '''
    _API_ENDPOINT  = get_parameter_ssm('API_ENDPOINT')
    _BUCKET_NAME   = get_parameter_ssm('BUCKET_DATALAKE')
    _BUCKET_PREFIX = get_parameter_ssm('DATALAKE_BRONZE_PREFIX')
    try:
        data = []
        for page in count(start=1):
    
            params = _format_request_params(
                page=page,
                per_page=200
            )
            data = _get_data(
                api_endpoint=_API_ENDPOINT,
                params=params
            )

            if not data:
                break
            
            save_file_s3(
                _BUCKET_NAME,
                _BUCKET_PREFIX,
                data
            )
        
        return {
            'statusCode': 200,
            'body': json.dumps(f'Arquivo gerado!')
        }
    except Exception as e:
        raise Exception(f"Erro ao consultar endpoint. Error: {e}")