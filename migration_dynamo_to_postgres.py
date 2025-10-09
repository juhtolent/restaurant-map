import psycopg2
import json
import re
import os
from dotenv import load_dotenv


def parse_street_name(address):
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
                   'Av.', 'Al.', 'R.', 'Tv.']
    
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


def day_of_week_name(day_number):
    """ Convert day number to day name."""
    days = {
        '0': 'Domingo',
        '1': 'Segunda-feira',
        '2': 'Terça-feira',
        '3': 'Quarta-feira',
        '4': 'Quinta-feira',
        '5': 'Sexta-feira',
        '6': 'Sábado'
    }
    return days.get(str(day_number), None)


def insert_restaurants_to_db(data, conn):
    """ Insert restaurant data into PostgreSQL database."""
    cursor = conn.cursor()
    
    try:
        for restaurant_data in data:
            print(f"Processing: {restaurant_data.get('google_name')}")
            
            # Parse address
            street_parsed = parse_street_name(restaurant_data.get("address", ""))
            
            # 1. INSERT RESTAURANT
            print('INSERT RESTAURANT')
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
                "google_id": restaurant_data.get("google_id"),
                "google_name": restaurant_data.get("google_name"),
                "google_display_name": restaurant_data.get("google_display_name"),
                "street_type": street_parsed.get('street_type'),
                "street_name": street_parsed.get('street_name'),
                "street_number": street_parsed.get('street_number'),
                "street_complement": street_parsed.get('street_complement'),
                "street_neighborhood": street_parsed.get('street_neighborhood'),
                "postalcode": street_parsed.get('postalcode'),
                "city": street_parsed.get('city'),
                "state": street_parsed.get('state'),
                "country": street_parsed.get('country'),
                "business_status": restaurant_data.get("business_status"),
                "editorial_summary": restaurant_data.get("editorial_summary"),
                "google_url": restaurant_data.get("google_url"),
                "latitude": restaurant_data.get("latitude"),
                "longitude": restaurant_data.get("longitude"),
                "ratings": restaurant_data.get("ratings"),
                "vegetarian_food": restaurant_data.get("vegetarian_food", False),
                "website": restaurant_data.get("website"),
                "opening_hours_description": ', '.join(restaurant_data.get("opening_hours_description")) if restaurant_data.get("opening_hours_description") else None
            }
            
            cursor.execute(restaurant_insert, restaurant_params)
            restaurant_id = cursor.fetchone()[0]
            
            # 2. INSERT OPENING HOURS
            print('INSERT OPENING HOURS')
            opening_hours_list = restaurant_data.get("opening_hours", [])
            if opening_hours_list:
                for date_hour in opening_hours_list:
                    if date_hour:
                        opening_hours_params = {
                            'restaurant_id': restaurant_id,
                            'day_of_the_week': day_of_week_name(date_hour.get('day_of_the_week')),
                            'opens_at': date_hour.get('opens', None),
                            'closes_at': date_hour.get('closes', None),
                            'is_opened': date_hour.get('is_opened', False),
                        }
                        opening_hours_insert = """
                            INSERT INTO opening_hours (
                                restaurant_id, day_of_week, opens_at, closes_at, is_opened
                            ) VALUES (
                                %(restaurant_id)s, %(day_of_the_week)s, %(opens_at)s, %(closes_at)s, %(is_opened)s
                            );
                        """
                            
                        cursor.execute(opening_hours_insert, opening_hours_params)
            
            # 3. INSERT RESTAURANT TYPES
            print('INSERT RESTAURANT TYPES')
            restaurant_types_list = restaurant_data.get("restaurant_types", [])
            primary_type = restaurant_data.get("primary_restaurant_type")
            
            print('restaurant_types_list')
            if restaurant_types_list:
                for restaurant_type in restaurant_types_list:
                    if restaurant_type:
                        restaurant_types_params = {
                            'restaurant_id': restaurant_id,
                            'restaurant_type': restaurant_type,
                            'is_primary_type': False
                        }
                        
                        restaurant_type_insert = """
                            INSERT INTO restaurant_types (
                                restaurant_id, restaurant_type, is_primary_type
                            ) VALUES (
                                %(restaurant_id)s, %(restaurant_type)s, %(is_primary_type)s
                            )
                            ON CONFLICT (restaurant_id, restaurant_type) DO NOTHING;
                        """
                        
                        cursor.execute(restaurant_type_insert, restaurant_types_params)
            
            print('primary_type')
            if primary_type:
                primary_type_params = {
                            'restaurant_id': restaurant_id,
                            'restaurant_type': primary_type,
                            'is_primary_type': True
                        }
                
                cursor.execute(restaurant_type_insert, primary_type_params)
            
            conn.commit()
            print(f"✓ Restaurant {restaurant_data.get('google_name')} inserted with ID: {restaurant_id}")
            
    except Exception as e:
        conn.rollback()
        print(f"Error inserting restaurant: {e}")
        raise
    finally:
        cursor.close()


if __name__ == "__main__":
    # Load restaurant data from JSON file
    with open(r'archive/map_data.json') as json_file:
        data = json.load(json_file)
        
    load_dotenv()

    # Connect to database
    conn = psycopg2.connect(
        host="localhost",
        database=os.getenv('POSTGRES_DB'),
        user=os.getenv('POSTGRES_USER'),
        password=os.getenv('POSTGRES_PASSWORD'),
        port='5432'
    )

    try:
        insert_restaurants_to_db(data, conn)
        print("\n✓ All restaurants successfully inserted!")
    except Exception as e:
        print(f"\n✗ Error during insertion: {e}")
    finally:
        conn.close()