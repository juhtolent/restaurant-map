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
    # Restaurants that need a new row created in DynamoDB;

    restaurants_to_put = [
        name for name in restaurants_names_list
        if name not in existing_names
    ]

    for google_name in restaurants_to_put:  # Put
        print(f'Creating {google_name} record')

        client = DynamoClient(google_name)
        client.save_to_dynamo()

        print('Created record sucessfully!')

    # Once all rows are created and updated
    # Compares the restaurant lists to identify which need to be deleted from DynamoDB
    new_dynamodb_list = extract_dynamodb_list()

    for restaurant_dynamo in new_dynamodb_list:
        if restaurant_dynamo.get('google_name') not in restaurants_names_list:
            print(f'Deleting {google_name} record')

            client = DynamoClient(restaurant_dynamo.get(
                'google_name'), restaurant_dynamo.get('google_id'))
            client.delete_from_dynamo()

            print('Deleted record sucessfully!')
