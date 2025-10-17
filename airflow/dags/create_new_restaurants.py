from airflow.sdk import dag, task, Variable
import datetime
import psycopg2

# Importar as funções do arquivo utils
from utils.task_functions import extract_google_maps_names, GooglePlacesAPIClient, DataFormatter, RestaurantDataProcessor

@dag(dag_id='create_new_restaurants', schedule="@daily", start_date=datetime.datetime(2021, 12, 1), catchup=False)
def taskflow():
    
    @task(task_id="extract_google_maps_list", retries=0)
    def extract_google_maps_list():
        return extract_google_maps_names()
    
    
    restaurants_names_list = extract_google_maps_list()
    @task(task_id="extract_google_maps_list", retries=0)
    def transform_task(resultado):
        # Establish database connection
        conn = psycopg2.connect(
            host="localhost",
            database=Variable.get('POSTGRES_DB'),
            user=Variable.get('POSTGRES_USER'),
            password=Variable.get('POSTGRES_PASSWORD'),
            port='5432'
        )
        
        try:
            # Initialize managers and clients
            api_client = GooglePlacesAPIClient()
            data_formatter = DataFormatter()
            processor = RestaurantDataProcessor(conn, api_client, data_formatter)
            
            # Identify restaurants that need to be inserted
            restaurants_to_insert = processor.get_restaurants_to_insert(restaurants_names_list)
            
            # Process each restaurant
            restaurants_to_insert = restaurants_to_insert[:1]
            
            for restaurant_name in restaurants_to_insert:
                print("restaurants_to_insert",restaurants_to_insert)
                processor.process_and_store(restaurant_name)
        finally:
            conn.close()

taskflow()

#TODO: ver como faz para uma task consumir a variável de outra 