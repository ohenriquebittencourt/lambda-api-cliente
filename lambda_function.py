import json
from aws_lambda_powertools import Logger
from aws_lambda_powertools.utilities.data_classes import event_source, APIGatewayProxyEvent

from src.services.cliente_service import ClienteService

logger = Logger(service="api-clientes")
cliente_service = ClienteService(logger)

handlers = {
    ("POST", "/inserir_dados_cliente"): cliente_service.inserir_dados_cliente,
    ("POST", "/deletar_dados_cliente"): cliente_service.deletar_dados_cliente,
}

@event_source(data_class=APIGatewayProxyEvent)
def lambda_handler(event: APIGatewayProxyEvent, context) -> dict:
    try:
        request = (event.http_method, event.path)
        if request in handlers:
            logger.info(f"Event: {event.body}")
            method = handlers[request]
            response = method(json.loads(event.body))
            return response
        else:
            return {
                "status_code": 404,
                "body": "Método/Rota não suportado"
            }
    except Exception as e:
        logger.error(f"An error occurred: {str(e)}")
        return {
            "status_code": 500,
            "body": f"An error occurred: {str(e)}"
        }


# event = {
#     "httpMethod": "POST",
#     "path": "/deletar_dados_cliente",
#     "body": json.dumps({
#         "cpf": "48863078822",
#         "nome": "Henrique Bittencourt Severo",
#         "endereco": "Rua Francisco da Costa Machado 301",
#         "telefone": "11953331048"
#     })
# }

# try:
#     result = lambda_handler(event, None)
#     print(result)
# except Exception as e:
#     print(f"An error occurred: {str(e)}")
