from service.dynamodb import extract_dynamodb_list, DynamoClient
from service.google_api import extract_google_maps_names


def handler(event):

    dynamodb_list = extract_dynamodb_list()
    restaurants_names_list = extract_google_maps_names()

    # Lists all restaurant names in DynamoDB
    existing_names = [
        item.get('google_name', '')
        for item in dynamodb_list
    ]

    # Compares with the restaurants_names_list:
    # Restaurants that need to be updated;
    restaurants_to_update = [
        name for name in restaurants_names_list
        if any(name in existing_name for existing_name in existing_names)
    ]

    for google_name in restaurants_to_update:  # Update
        for restaurant in dynamodb_list:
            if restaurant.get('google_name') == google_name:
                print(f'Updating {google_name} record')

                google_id = restaurant.get('google_id')
                client = DynamoClient(google_name, google_id)
                client.save_to_dynamo()

                print('Updated record sucessfully!')


    ## TODO: Mudar o modelo de atualização: para os campos Essential e Pro, atualiza todos os campos; 
    # Para os campos entreprise, atualiza só os mais antigos