import json
import boto3
import urllib3

# Inicialização do cliente S3
s3_client = boto3.client('s3')

def lambda_handler(event, context):
    http = urllib3.PoolManager()
    s3_bucket = 's3://data-health/stream/'
    s3_key = 'arquivo.json'
    
    # Processamento dos registros do Kinesis Data Stream
    for record in event['Records']:
        # Decodificação dos dados do Kinesis
        payload = json.loads(record['kinesis']['data'])
        
        # Pegue o URL do site do payload (ou defina aqui diretamente)
        url = payload['https://datasus.saude.gov.br/']
        
        # Baixando o arquivo do URL
        response = http.request('GET', url)
        
        # Salvando no S3
        s3_client.put_object(Bucket=s3_bucket, Key=s3_key, Body=response.data)
        
        print(f"Arquivo salvo no S3: {s3_bucket}/{s3_key}")
    
    return {
        'statusCode': 200,
        'body': json.dumps('Processamento completo!')
    }
