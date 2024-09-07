import json
import boto3
import pickle
from aws_lambda_powertools import Logger

class ClienteService():
    def __init__(self, logger: Logger) -> None:
        self.logger = logger
        self.s3_client = boto3.client('s3')

    def deletar_dados_cliente(self, body: dict):
        cpf = body.get('cpf', '')
        nome = body.get('nome', '')
        endereco = body.get('endereco', '')
        telefone = body.get('telefone', '')
        
        self.s3_client.delete_object(Bucket="bucket-clientes-fiap", Key=f"{cpf}.pkl")
        self.logger.info(f"Dados do cliente {nome} foram deletados.")
        return 200
    
    def inserir_dados_cliente(self, body):
        cpf = body.get('cpf', '')
        nome = body.get('nome', '')
        endereco = body.get('endereco', '')
        telefone = body.get('telefone', '')

        json_data = {
            'cpf': cpf,
            'nome': nome,
            'endereco': endereco, 
            'telefone': telefone
            }
        
        pickled_obj = pickle.dumps(json_data)

        self.s3_client.put_object(
            Bucket="bucket-clientes-fiap",
            Key=f"{cpf}.pkl",
            Body=pickled_obj
        )
        return 201