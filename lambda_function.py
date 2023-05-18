import json
import boto3
from datetime import datetime

def lambda_handler(event, context):

    print(event)

    for record in event['Records']:
        body = record['body']
        body = json.loads(body)
        message = body['Message']
        message = json.loads(message)
        
        nome_cliente = message['nome_cliente']
        segmento = message['segmento']
        servico_contratado = message['servico_contratado']
        numero_contratado = message['numero_contratado']
        volumetria_maxima = message['volumetria_maxima_rota']
        
        agora = datetime.today()
        
        conteudo_arquivo = 'Data hora da contratação: ' + str(agora) + ' UTC\n' + \
                            'Número provisionado: ' + numero_contratado + '\n' + \
                            'Contratante: ' + nome_cliente
        encoded_string = conteudo_arquivo.encode('utf-8')
        bucket_name = 'demo-fanout-provisionamento'
        file_name = numero_contratado + '-numero-origem.txt'
        s3_path = nome_cliente + '/' + servico_contratado + \
                '/' + str(agora.year) + f'{agora.month:02d}' + '/' + file_name
        s3_path = s3_path.replace(' ', '_').lower()
    
        s3 = boto3.resource('s3')
        s3.Bucket(bucket_name).put_object(Key=s3_path, Body=encoded_string)
        
        print('Número de origem da rota criado com sucesso: ' + s3_path)

    return {
        'status_code': 200
    }