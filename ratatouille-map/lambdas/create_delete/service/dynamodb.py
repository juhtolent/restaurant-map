import boto3
import requests
import os
from datetime import datetime
from dotenv import load_dotenv

load_dotenv(r"C:\Users\julia\Desktop\ratatouille-map\.gitignore\.env")


def extract_dynamodb_list(table: str = 'map_data') -> list:
    """
    Retrieves all items from a DynamoDB table.

    :param table: The name of the DynamoDB table. Defaults to 'map_data'.
    :type table: str
    :return: A list of dictionaries, where each dictionary represents an item.
    :rtype: List[Dict[str, Any]]
    """
    session = boto3.Session(
        aws_access_key_id=os.getenv('AWS_ACCESS_KEY'),
        aws_secret_access_key=os.getenv('AWS_SECRET_KEY')
    )

    dynamodb = session.resource('dynamodb', region_name='us-east-1')
    table = dynamodb.Table(table)

    response = table.scan()

    return response['Items']


class DynamoClient():
    def __init__(self, google_name, google_id=None, table='map_data'):
        """
        Initializes the PutUpdateDynamoClient.

        :param google_name: The name of the Google Place.
        :type google_name: str
        :param google_id: The Google Place ID. Optional.
        :type google_id: str or None
        :param table: The DynamoDB table name. Defaults to 'map_data'.
        :type table: str
        """
        self.table = table
        self.google_name = google_name
        self.google_id = google_id
        self.api_key = os.getenv('API_KEY')
        self.dynamodb = boto3.Session(
            aws_access_key_id=os.getenv('AWS_ACCESS_KEY'),
            aws_secret_access_key=os.getenv('AWS_SECRET_KEY')
        ).resource('dynamodb', region_name='us-east-1').Table(table)

    def get_google_id(self):
        """
        Retrieves Google Place ID, because it is needed to retrieve the other fields.
        If the restaurant data is going to be updated, then it uses teh param google_id.

        :return: The Google Place ID, or None if not found.
        :rtype: str or None
        """

        if self.google_id:
            return self.google_id

        url = 'https://places.googleapis.com/v1/places:searchText'

        headers = {
            'Content-Type': 'application/json',
            'X-Goog-Api-Key': self.api_key,
            'X-Goog-FieldMask': 'places.id,places.displayName'
        }

        data = {'textQuery': self.google_name}

        response = requests.post(url, headers=headers, json=data)

        if response.status_code == 200:
            response_data = response.json()
            google_id = response_data['places'][0].get('id', None)

            return google_id
        else:
            return print(f"Error {response.status_code}: {response.text}")

    def get_google_fields(self):
        google_id = self.get_google_id()

        # API URL
        url = f'https://places.googleapis.com/v1/places/{google_id}'

        # Fields as a list
        field_list = [
            "displayName",
            # "formattedAddress",
            # "location",
            # "googleMapsUri",
            # "types",
            # "primaryTypeDisplayName",
            # "websiteUri",
            # "regularOpeningHours",
            # "businessStatus",
            # "editorialSummary",
            # "priceLevel",
            # "rating",
            # "servesVegetarianFood"
        ]
        fields = ",".join(field_list)

        # Headers
        headers = {
            'Content-Type': 'application/json',
            'X-Goog-Api-Key': self.api_key,
            'X-Goog-FieldMask': fields,
            'Accept-Language': 'pt-BR'
        }

        # Make the POST request
        response = requests.get(url, headers=headers)

        # Check the response status and content
        if response.status_code == 200:
            return google_id, response.json()
        else:
            print(f"Error {response.status_code}: {response.text}")

    def convert_opening_hours_to_dynamodb_format(self, openinghours_periods):
        result = []

        if not openinghours_periods:
            return result

        for period in openinghours_periods:
            # Opening hours
            open_data = period.get('open', {})
            day_of_week = open_data.get('day', 0)
            open_hour = str(open_data.get('hour', 0)).zfill(2)
            open_minute = str(open_data.get('minute', 0)).zfill(2)
            opens_time = f'{open_hour}:{open_minute}'

            # Closing hours
            close_data = period.get('close', {})
            close_hour = str(close_data.get('hour', 0)).zfill(2)
            close_minute = str(close_data.get('minute', 0)).zfill(2)
            closes_time = f'{close_hour}:{close_minute}'

            # Create DynamoDB format
            period_entry = {
                'day_of_the_week': str(day_of_week),
                'opens': opens_time,
                'closes': closes_time,
                'is_opened': True
            }

            result.append(period_entry)

        opened_days = set(period['open'].get('day')
                          for period in openinghours_periods)

        # Closed days are not shown in the API response
        for day_of_week in range(7):
            if day_of_week not in opened_days:
                closed_day_entry = {
                    'day_of_the_week': str(day_of_week),
                    'is_opened': False
                }
                result.append(closed_day_entry)

        return result

    def process_google_fields(self):
        google_id, google_fields_data = self.get_google_fields()

        item_data = {
            'google_id': google_id,
            'google_name': self.google_name,
            'google_display_name': google_fields_data.get('displayName', ''),
            'google_url': google_fields_data.get('googleMapsUri', ''),
            'address': google_fields_data.get('formattedAddress', ''),
            'latitude': str(google_fields_data['location'].get('latitude', '')),
            'longitude': str(google_fields_data['location'].get('longitude', '')),
            'timestamp_insert': datetime.now(),
            'timestamp_update': datetime.now()
        }

        # Fields that might be None - store as NULL if they are None
        optional_fields = [
            ('restaurant_types', google_fields_data.get('types')
             if 'types' in google_fields_data else None),
            ('primary_restaurant_type', google_fields_data.get(
                'primaryTypeDisplayName', {}).get('text')),
            ('business_status', google_fields_data.get('businessStatus')),
            ('website', google_fields_data.get('websiteUri')
             if 'websiteUri' in google_fields_data else None),
            ('opening_hours_description', [description.replace('\u2009', '') for description in google_fields_data['regularOpeningHours'].get(
                'weekdayDescriptions')] if 'regularOpeningHours' in google_fields_data else None),
            ('opening_hours', self.convert_opening_hours_to_dynamodb_format(google_fields_data['regularOpeningHours'].get(
                'periods')) if 'regularOpeningHours' in google_fields_data else None),
            ('editorial_summary', google_fields_data.get('editorialSummary', {}).get(
                'text') if 'editorialSummary' in google_fields_data else None),
            ('ratings', str(google_fields_data.get('rating'))
             if 'rating' in google_fields_data else None),
            ('vegetarian_food', google_fields_data.get('servesVegetarianFood')
             if 'servesVegetarianFood' in google_fields_data else None)
        ]

        for field_name, field_value in optional_fields:
            if field_value is not None:
                item_data[field_name] = field_value
            else:
                item_data[field_name] = None

        return item_data

    def save_to_dynamo(self):
        item_data = self.process_google_fields()

        if self.google_id:  # update
            update_expression = ['set ']
            update_values = {}

            for key, value in item_data.items():
                if key != 'google_id' and key != 'google_name':
                    update_expression.append(f" {key} = :{key},")
                    update_values[f":{key}"] = value
            update_expression = "".join(update_expression)[:-1]

            self.dynamodb.update_item(
                Key={'google_id': item_data['google_id'],
                     'google_name': item_data['google_name']},
                UpdateExpression=update_expression,
                ExpressionAttributeValues=update_values,
                ReturnValues='UPDATED_NEW'
            )
        else:  # put
            self.dynamodb.put_item(Item=item_data)

    def delete_from_dynamo(self):
        self.dynamodb.update_item(
                Key={'google_id': self.google_id,
                     'google_name': self.google_name},
                ConditionExpression="attribute_exists(google_id) AND attribute_exists(google_name)"
        )