import boto3
import json
import os
from dotenv import load_dotenv
from decimal import Decimal

# Carregar variáveis de ambiente
load_dotenv()

# Configurar cliente DynamoDB
dynamodb = boto3.resource(
    'dynamodb',
    region_name='us-east-1',  # Ajuste para sua região
    aws_access_key_id=os.getenv('AWS_ACCESS_KEY'),
    aws_secret_access_key=os.getenv('AWS_SECRET_KEY')
)

# Função auxiliar para converter tipos DynamoDB para tipos Python
def decimal_default(obj):
    if isinstance(obj, Decimal):
        return float(obj)
    raise TypeError

# Referenciar a tabela
table = dynamodb.Table('map_data')

# Scanear a tabela
print("Scaneando tabela...")
items = []
response = table.scan()
items.extend(response['Items'])

# Continuar scaneando se houver mais itens (paginação)
while 'LastEvaluatedKey' in response:
    print(f"Scaneados {len(items)} itens até agora...")
    response = table.scan(ExclusiveStartKey=response['LastEvaluatedKey'])
    items.extend(response['Items'])

print(f"Total de itens scaneados: {len(items)}")

# Salvar em JSON
with open('map_data.json', 'w', encoding='utf-8') as f:
    json.dump(items, f, ensure_ascii=False, indent=2, default=decimal_default)

print("Dados salvos em map_data.json")