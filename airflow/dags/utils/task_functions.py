import psycopg2
import re
import requests
import html
import os
from datetime import datetime
from typing import List, Dict, Optional, Tuple, Any
from airflow.sdk import Variable

def extract_google_maps_names(url_suffix: str = Variable.get("GOOGLE_LIST_ID")) -> str:
    ''' 
    Extract restaurant names from a Google Maps URL. 
    This extraction method is based on this https://gist.github.com/ByteSizedMarius/8c9df821ebb69b07f2d82de01e68387d

    :param url_suffix: The specific URL suffix for the Google Maps data.
    :type url_suffix: str, optional
    :default url_suffix: A predefined suffix for my restaurant list
    :returns: A list of extracted restaurant names
    '''
    # Fetch data from Google Maps list URL
    print("requests")
    response = requests.get(f"https://google.com/maps/@/data={url_suffix}")
    response.raise_for_status()
    raw_text = response.text
    print("raw_text",raw_text)
    
    # Select only the restaurant data section
    cleaned_text = raw_text.split(r")]}'\n")[2].split("]]\"],")[0] + "]]"
    unescaped_text = html.unescape(cleaned_text)
    
    print("unescaped_text",unescaped_text)
    
    # Extract only restaurant names using regex pattern
    restaurants_names_list = re.findall(
        r'(?:\\"/g/[^\\"]++\\"]|]]),\\"(.*?)\\",\\"', 
        unescaped_text
    )
    
    print(f"Restaurant Names List: {restaurants_names_list}")
    
    return restaurants_names_list


def add_quota_counter(conn: psycopg2.extensions.connection, 
                      api_services: List[str] = None) -> None:
    """
    Add quota counter for specified API services.
    Creates a new record if it doesn't exist for the current month,
    or increments the existing counter.
    
    :param api_services: List of API service tiers to update
    :type api_services: List[str], optional
    :default api_services: ['pro', 'essential', 'enterprise']
    """
    if api_services is None:
        api_services = ['pro', 'essential', 'enterprise']
    
    # API tier quota limits
    quota_limits_standard = {
        'essential': 10000,
        'pro': 5000,
        'enterprise': 1000
    }
        
    month_year = datetime.now().strftime('%Y-%m-01')
    cursor = conn.cursor()
    
    try:
        for api_service in api_services:
            # Check if quota record exists for this month and service
            check_query = """
                SELECT quota_used 
                FROM google_api_quota
                WHERE month_year = %s AND api_service = %s
            """
            cursor.execute(check_query, (month_year, api_service))
            result = cursor.fetchone()
            
            if result:
                # Record exists - increment quota usage
                update_query = """
                    UPDATE google_api_quota
                    SET quota_used = quota_used + 1 
                    WHERE month_year = %s AND api_service = %s
                """
                cursor.execute(update_query, (month_year, api_service))
            else:
                # Record doesn't exist - create new entry
                quota_limit = quota_limits_standard.get(api_service)
                
                if quota_limit is None:
                    print(f"Error: {api_service} is not a valid Google API tier")
                    continue
                
                insert_query = """
                    INSERT INTO google_api_quota(
                        month_year, api_service, quota_limit, quota_used
                    )
                    VALUES (%s, %s, %s, %s)
                """
                cursor.execute(insert_query, (month_year, api_service, quota_limit, 1))
        
        conn.commit()
    
    finally:
        cursor.close()


class GooglePlacesAPIClient:
    """
    Handles all interactions with the Google Places API.
    """
    
    def __init__(self, api_key: Optional[str] = None):
        """
        Initialize the Google Places API client.
        
        :param api_key: Google API key for authentication
        :type api_key: str, optional
        """
        self.api_key = api_key or Variable.get('GOOGLE_API_KEY')
        self.base_url = 'https://places.googleapis.com/v1/places'
    
    def get_place_id(self, restaurant_name: str) -> Optional[str]:
        """
        Retrieve Google Place ID for a restaurant.
        The Place ID is required to retrieve detailed place information.
        
        :param restaurant_name: Name of the restaurant to search for
        :type restaurant_name: str
        :returns: The Google Place ID, or None if not found
        :rtype: str or None
        """
        url = f'{self.base_url}:searchText'
        
        headers = {
            'Content-Type': 'application/json',
            'X-Goog-Api-Key': self.api_key,
            'X-Goog-FieldMask': 'places.id,places.displayName',
            'Accept-Language': 'pt-BR'
        }
        
        data = {'textQuery': f"{restaurant_name} in Brazil"}
        
        response = requests.post(url, headers=headers, json=data)
        
        if response.status_code == 200:
            response_data = response.json()
            google_id = response_data['places'][0].get('id', None)
            print(f"Google ID: {google_id}")
            return google_id
        else:
            print(f"Error {response.status_code}: {response.text}")
            return None
    
    def get_place_details(self, restaurant_name: str) -> Tuple[Optional[str], Optional[Dict]]:
        """
        Retrieve detailed information about a place from Google Places API.
        
        :param restaurant_name: Name of the restaurant
        :type restaurant_name: str
        :returns: Tuple of (google_id, place_details_dict)
        :rtype: Tuple[Optional[str], Optional[Dict]]
        """
        google_id = self.get_place_id(restaurant_name)
        
        if not google_id:
            return None, None
        
        url = f'{self.base_url}/{google_id}'
        
        # Define fields to retrieve from the API
        field_list = [
            "displayName",
            "formattedAddress",
            "location",
            "googleMapsUri",
            "types",
            "primaryTypeDisplayName",
            "websiteUri",
            "regularOpeningHours",
            "businessStatus",
            "editorialSummary",
            "priceLevel",
            "rating",
            "servesVegetarianFood",
        ]
        fields = ",".join(field_list)
        
        headers = {
            'Content-Type': 'application/json',
            'X-Goog-Api-Key': self.api_key,
            'X-Goog-FieldMask': fields,
            'Accept-Language': 'pt-BR'
        }
        
        response = requests.get(url, headers=headers)
        
        if response.status_code == 200:
            return google_id, response.json()
        else:
            print(f"Error {response.status_code}: {response.text}")
            return google_id, None


class DataFormatter:
    """
    Handles formatting of Google API data for db storage.
    """
    
    def __init__(self):
        pass
    
    def format_opening_hours_description(self, opening_hours_description: Optional[List[str]]) -> Optional[str]:
        """
        Format opening hours description text for storage.
        Joins list items and cleans up special characters.
        
        :param opening_hours_description: List of opening hours descriptions
        :type opening_hours_description: List[str], optional
        :returns: Formatted description string
        :rtype: str or None
        """
        if not opening_hours_description:
            return None
        
        joined = '\n'.join(opening_hours_description)
        
        # Remove thin spaces and replace newlines with commas
        cleaned = joined.replace('\u2009', '').replace('\n', ', ')
        
        return cleaned
    
    def format_day_name(self, day_number: int) -> Optional[str]:
        """
        Convert day number to Portuguese day name.
        
        :param day_number: Day number (0=Sunday, 6=Saturday)
        :type day_number: int
        :returns: Portuguese day name
        :rtype: str or None
        """
        
        # Mapping of weekday numbers to Portuguese day names
        weekday_names = {
            0: 'Domingo',
            1: 'Segunda-feira',
            2: 'Terça-feira',
            3: 'Quarta-feira',
            4: 'Quinta-feira',
            5: 'Sexta-feira',
            6: 'Sábado'
        }
        return weekday_names.get(day_number, None)
    
    def format_opening_hours_periods(
        self, 
        opening_hours_periods: Optional[List[Dict]], 
        restaurant_id: int
    ) -> List[Dict[str, Any]]:
        """
        Format opening hours periods data for database insertion.
        Includes both open and closed days.
        
        :param opening_hours_periods: List of period dictionaries from API
        :type opening_hours_periods: List[Dict], optional
        :param restaurant_id: Database ID of the restaurant
        :type restaurant_id: int
        :returns: List of formatted period entries for database
        :rtype: List[Dict[str, Any]]
        """
        result = []
        
        if not opening_hours_periods:
            return result
        
        # Process each opening period
        for period in opening_hours_periods:
            # Extract opening time
            open_data = period.get('open', {})
            day_of_week = open_data.get('day', 0)
            open_hour = str(open_data.get('hour', 0)).zfill(2)
            open_minute = str(open_data.get('minute', 0)).zfill(2)
            opens_time = f'{open_hour}:{open_minute}'
            
            # Extract closing time
            close_data = period.get('close', {})
            close_hour = str(close_data.get('hour', 0)).zfill(2)
            close_minute = str(close_data.get('minute', 0)).zfill(2)
            closes_time = f'{close_hour}:{close_minute}'
            
            # Create formatted entry
            period_entry = {
                'restaurant_id': restaurant_id,
                'day_of_the_week': self.format_day_name(day_of_week),
                'opens_at': opens_time,
                'closes_at': closes_time,
                'is_opened': True
            }
            
            result.append(period_entry)
        
        # Identify days that are closed (not in API response)
        opened_days = set(
            period.get('open', {}).get('day') 
            for period in opening_hours_periods
            if period.get('open') and period.get('open').get('day') is not None
        )
        
        # Add entries for closed days
        for day_of_week in range(7):
            if day_of_week not in opened_days:
                closed_day_entry = {
                    'restaurant_id': restaurant_id,
                    'day_of_the_week': self.format_day_name(day_of_week),
                    'opens_at': None,
                    'closes_at': None,
                    'is_opened': False
                }
                result.append(closed_day_entry)
        
        return result
    
    def parse_street_name(self, address: Optional[str]) -> Dict[str, Optional[str]]:
        """ Parse a street address string into its structured components."""
        
        # Initialize return dictionary
        result = {
            "street_type": None,
            "street_name": None,
            "street_number": None,
            "street_complement": None,
            "street_neighborhood": None,
            "postalcode": None,
            "city": None,
            "state": None,
            "country": None
        }
        
        if not address:
            return result
        
        # Remove extra spaces
        address = ' '.join(address.split())
        
        # Extract country (usually at the end)
        country_match = re.search(r',\s*([^,]+)$', address)
        if country_match:
            result["country"] = country_match.group(1).strip()
            address = address[:country_match.start()].strip()
        
        # Extract postal code (format: 00000-000 or 00000000)
        postalcode_match = re.search(r'\b(\d{5}-?\d{3})\b', address)
        if postalcode_match:
            result["postalcode"] = postalcode_match.group(1).replace('-', '')
            address = address.replace(postalcode_match.group(0), '').strip(' ,')
        
        # Extract state (format: - UF)
        state_match = re.search(r'-\s*([A-Z]{2})\s*,?\s*$', address)
        if state_match:
            result["state"] = state_match.group(1)
            address = address[:state_match.start()].strip()
        
        # Extract city (last part before state)
        if address:
            parts_temp = address.split(',')
            if len(parts_temp) > 0:
                result["city"] = parts_temp[-1].strip()
                address = ','.join(parts_temp[:-1]).strip()
        
        # Now 'address' contains: street type + name, number, [complements...], neighborhood
        if not address:
            return result
        
        # Split by commas
        parts = [p.strip() for p in address.split(',') if p.strip()]
        
        if len(parts) == 0:
            return result
        
        # First part: street type + street name
        first_part = parts[0]
        
        # Complete list of street types
        street_types = ['Avenida', 'Rua', 'Alameda', 'Travessa', 'Praça', 
                    'Rodovia', 'Estrada', 'Viela', 'Largo', 'Beco', 
                    'Av.', 'Al.', 'R.', 'Tv.', 'Pç.']
        
        for st_type in street_types:
            if first_part.startswith(st_type):
                result["street_type"] = st_type
                first_part = first_part[len(st_type):].strip()
                break
        
        result["street_name"] = first_part.strip()
        
        # Remove first part (already processed)
        parts = parts[1:]
        
        # Second part: should be the number
        if len(parts) >= 1:
            potential_number = parts[0].strip()
            # Remove dash if exists at the beginning
            potential_number = re.sub(r'^\s*-\s*', '', potential_number).strip()
            
            # Check if it's only number or number with letter
            if re.match(r'^\d+[A-Za-z]?$', potential_number):
                result["street_number"] = potential_number
                parts = parts[1:]  # Remove from list
            else:
                # Could be "275 - Pinheiros", extract the number
                number_match = re.match(r'^(\d+[A-Za-z]?)\s*-\s*(.+)$', potential_number)
                if number_match:
                    result["street_number"] = number_match.group(1)
                    # Put the rest back in parts
                    parts[0] = number_match.group(2)
                else:
                    # Try to extract only the number from the beginning
                    number_match = re.match(r'^(\d+[A-Za-z]?)\s+(.+)$', potential_number)
                    if number_match:
                        result["street_number"] = number_match.group(1)
                        parts[0] = number_match.group(2)
        
        # Remaining parts: last is neighborhood, middle ones are complement
        if len(parts) > 0:
            # Last part is neighborhood
            last_part = parts[-1].strip()
            # Remove dash at beginning if exists
            last_part = re.sub(r'^\s*-\s*', '', last_part).strip()
            
            # Could be "loja 14 - Consolação" where Consolação is neighborhood
            # Search for last dash
            last_dash = last_part.rfind(' - ')
            if last_dash != -1:
                result["street_neighborhood"] = last_part[last_dash + 3:].strip()
                # If there was content before last dash, add to complement
                before_dash = last_part[:last_dash].strip()
                if before_dash:
                    parts[-1] = before_dash
                else:
                    parts = parts[:-1]
            else:
                result["street_neighborhood"] = last_part
                parts = parts[:-1]
            
            # Everything left is complement
            if len(parts) > 0:
                complement_parts = []
                for part in parts:
                    # Remove dashes at beginning/end
                    part = re.sub(r'^\s*-\s*|\s*-\s*$', '', part).strip()
                    if part:
                        complement_parts.append(part)
                
                if complement_parts:
                    result["street_complement"] = ' - '.join(complement_parts)
        
        return result


class RestaurantDataProcessor:
    """
    Processes and stores restaurant data in the database.
    """
    
    def __init__(
        self, 
        connection: psycopg2.extensions.connection,
        api_client: GooglePlacesAPIClient,
        data_formatter: DataFormatter
    ):
        """
        Initialize the restaurant data processor.
        
        :param connection: PostgreSQL database connection
        :type connection: psycopg2.extensions.connection
        :param api_client: Google Places API client instance
        :type api_client: GooglePlacesAPIClient
        :param quota_manager: API quota manager instance
        :type quota_manager: GoogleAPIQuotaManager
        """
        self.conn = connection
        self.api_client = api_client
        self.data_formatter = data_formatter
    
    def process_and_store(self, restaurant_name: str) -> None:
        """
        Process restaurant data from Google API and store in database.
        Handles restaurants, opening hours, and restaurant types.
        
        :param restaurant_name: Name of the restaurant to process
        :type restaurant_name: str
        :raises Exception: If any database operation fails
        """
        # Fetch data from Google Places API
        google_id, google_fields_data = self.api_client.get_place_details(restaurant_name)
        
        if not google_id or not google_fields_data:
            print(f"Error: Could not retrieve data for {restaurant_name}")
            return
        
        cursor = self.conn.cursor()
        
        try:
            # Validate address data
            address = google_fields_data.get("formattedAddress", None)
            if not address:
                print("Error: No address in Google response")
                raise Exception("Missing address data")
            
            # Parse street address components
            street_parsed = self.data_formatter.parse_street_name(address)
            
            # 1. INSERT/UPDATE RESTAURANT DATA
            print('INSERT RESTAURANT')
            restaurant_id = self._insert_restaurant(
                cursor, 
                google_id, 
                restaurant_name, 
                google_fields_data, 
                street_parsed
            )
            
            # Update API quota counter
            add_quota_counter(self.conn)
            
            # 2. INSERT OPENING HOURS
            print('INSERT OPENING HOURS')
            self._insert_opening_hours(cursor, google_fields_data, restaurant_id)
            
            # 3. INSERT RESTAURANT TYPES
            print('INSERT RESTAURANT TYPES')
            self._insert_restaurant_types(cursor, google_fields_data, restaurant_id)
            
            self.conn.commit()
            print(f"✓ Restaurant {restaurant_name} inserted with ID: {restaurant_id}")
        
        except Exception as e:
            self.conn.rollback()
            print(f"Error inserting restaurant: {e}")
            raise
        
        finally:
            cursor.close()
    
    def _insert_restaurant(
        self, 
        cursor: psycopg2.extensions.cursor,
        google_id: str,
        restaurant_name: str,
        google_fields_data: Dict,
        street_parsed: Dict
    ) -> int:
        """
        Insert or update restaurant main data.
        
        :param cursor: Database cursor
        :param google_id: Google Place ID
        :param restaurant_name: Restaurant name
        :param google_fields_data: Complete data from Google API
        :param street_parsed: Parsed address components
        :returns: Restaurant database ID
        :rtype: int
        """
        restaurant_insert = """
            INSERT INTO restaurants (
                google_id, google_name, google_display_name, 
                street_type, street_name, street_number, street_complement,
                street_neighborhood, postalcode, city, state, country,
                business_status, editorial_summary, google_url,
                latitude, longitude, ratings, vegetarian_food, website,
                opening_hours_description
            ) VALUES (
                %(google_id)s, %(google_name)s, %(google_display_name)s,
                %(street_type)s, %(street_name)s, %(street_number)s, %(street_complement)s,
                %(street_neighborhood)s, %(postalcode)s, %(city)s, %(state)s, %(country)s,
                %(business_status)s, %(editorial_summary)s, %(google_url)s,
                %(latitude)s, %(longitude)s, %(ratings)s, %(vegetarian_food)s, %(website)s,
                %(opening_hours_description)s
            )
            ON CONFLICT (google_id) DO UPDATE SET
                google_name = EXCLUDED.google_name,
                google_display_name = EXCLUDED.google_display_name,
                street_type = EXCLUDED.street_type,
                street_name = EXCLUDED.street_name,
                street_number = EXCLUDED.street_number,
                street_complement = EXCLUDED.street_complement,
                street_neighborhood = EXCLUDED.street_neighborhood,
                postalcode = EXCLUDED.postalcode,
                city = EXCLUDED.city,
                state = EXCLUDED.state,
                country = EXCLUDED.country,
                business_status = EXCLUDED.business_status,
                editorial_summary = EXCLUDED.editorial_summary,
                google_url = EXCLUDED.google_url,
                latitude = EXCLUDED.latitude,
                longitude = EXCLUDED.longitude,
                ratings = EXCLUDED.ratings,
                vegetarian_food = EXCLUDED.vegetarian_food,
                website = EXCLUDED.website,
                opening_hours_description = EXCLUDED.opening_hours_description,
                updated_at = CURRENT_TIMESTAMP
            RETURNING id;
        """
        
        restaurant_params = {
            "google_id": google_id,
            "google_name": restaurant_name,
            "google_display_name": google_fields_data.get("displayName", {}).get("text", None),
            "street_type": street_parsed.get("street_type"),
            "street_name": street_parsed.get("street_name"),
            "street_number": street_parsed.get("street_number"),
            "street_complement": street_parsed.get("street_complement"),
            "street_neighborhood": street_parsed.get("street_neighborhood"),
            "postalcode": street_parsed.get("postalcode"),
            "city": street_parsed.get("city"),
            "state": street_parsed.get("state"),
            "country": street_parsed.get("country"),
            "business_status": google_fields_data.get("businessStatus", None),
            "editorial_summary": google_fields_data.get("editorialSummary", {}).get("text", None),
            "google_url": google_fields_data.get("googleMapsUri", None),
            "latitude": str(google_fields_data["location"].get("latitude", None)),
            "longitude": str(google_fields_data["location"].get("longitude", None)),
            "ratings": str(google_fields_data.get('rating')) if 'rating' in google_fields_data else None,
            "vegetarian_food": google_fields_data.get("servesVegetarianFood", False),
            "website": google_fields_data.get("websiteUri", None),
            "opening_hours_description": self.data_formatter.format_opening_hours_description(
                google_fields_data.get('regularOpeningHours', {}).get('weekdayDescriptions', None)
            )
        }
        
        print(restaurant_params)
        
        cursor.execute(restaurant_insert, restaurant_params)
        restaurant_id = cursor.fetchone()[0]
        
        return restaurant_id
    
    def _insert_opening_hours(
        self, 
        cursor: psycopg2.extensions.cursor,
        google_fields_data: Dict,
        restaurant_id: int
    ) -> None:
        """
        Insert opening hours data for a restaurant.
        
        :param cursor: Database cursor
        :param google_fields_data: Complete data from Google API
        :param restaurant_id: Restaurant database ID
        """
        opening_hours_periods = google_fields_data.get("regularOpeningHours", {}).get("periods", {})
        
        if not opening_hours_periods:
            return
        
        # Format periods for database insertion
        formatted_periods = self.data_formatter.format_opening_hours_periods(
            opening_hours_periods, 
            restaurant_id
        )
        
        opening_hours_insert = """
            INSERT INTO opening_hours (
                restaurant_id, day_of_week, opens_at, closes_at, is_opened
            ) VALUES (
                %(restaurant_id)s, %(day_of_the_week)s, %(opens_at)s, %(closes_at)s, %(is_opened)s
            );
        """
        
        for period_entry in formatted_periods:
            if period_entry:
                cursor.execute(opening_hours_insert, period_entry)
    
    def _insert_restaurant_types(
        self, 
        cursor: psycopg2.extensions.cursor,
        google_fields_data: Dict,
        restaurant_id: int
    ) -> None:
        """
        Insert restaurant type classifications.
        
        :param cursor: Database cursor
        :param google_fields_data: Complete data from Google API
        :param restaurant_id: Restaurant database ID
        """
        restaurant_types_list = google_fields_data.get("types", [])
        primary_type = google_fields_data.get("primaryTypeDisplayName", {}).get("text", None)
        
        restaurant_type_insert = """
            INSERT INTO restaurant_types (
                restaurant_id, restaurant_type, is_primary_type
            ) VALUES (
                %(restaurant_id)s, %(restaurant_type)s, %(is_primary_type)s
            )
            ON CONFLICT (restaurant_id, restaurant_type) DO NOTHING;
        """
        
        # Insert all restaurant types
        print('Processing restaurant types list')
        if restaurant_types_list:
            for restaurant_type in restaurant_types_list:
                if restaurant_type:
                    restaurant_types_params = {
                        'restaurant_id': restaurant_id,
                        'restaurant_type': restaurant_type,
                        'is_primary_type': False
                    }
                    cursor.execute(restaurant_type_insert, restaurant_types_params)
        
        # Insert primary type
        print('Processing primary type')
        if primary_type:
            primary_type_params = {
                'restaurant_id': restaurant_id,
                'restaurant_type': primary_type,
                'is_primary_type': True
            }
            cursor.execute(restaurant_type_insert, primary_type_params)
    
    def get_restaurants_to_insert(self, restaurants_names_list: List[str]) -> List[str]:
        """
        Identify restaurants that need to be inserted into the database.
        Filters out restaurants that already exist.
        
        :param restaurants_names_list: List of restaurant names to check
        :type restaurants_names_list: List[str]
        :returns: List of restaurant names not yet in database
        :rtype: List[str]
        """
        cursor = self.conn.cursor()
        
        try:
            # Create placeholders for SQL query
            placeholders = ','.join(['(%s)'] * len(restaurants_names_list))
            
            # Query to find restaurants not in database
            query = f"""
                SELECT nome
                FROM (
                    VALUES {placeholders}
                ) AS lista(nome)
                WHERE NOT EXISTS (
                    SELECT 1
                    FROM restaurants r
                    WHERE r.google_name = lista.nome
                );
            """
            
            cursor.execute(query, restaurants_names_list)
            restaurants_to_insert = [row[0] for row in cursor.fetchall()]
            
            return restaurants_to_insert
        
        finally:
            cursor.close()


def main():

    # Extract restaurant names from Google Maps
    restaurants_names_list = extract_google_maps_names()
    
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
