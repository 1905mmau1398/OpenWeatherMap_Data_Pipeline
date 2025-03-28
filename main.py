import requests
import pandas as pd
import psycopg2
from psycopg2.extras import execute_values
import logging
from datetime import datetime
import os
from dotenv import load_dotenv

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[logging.StreamHandler()]
)
logger = logging.getLogger('weather_etl_pipeline')

# Load environment variables from .env file
load_dotenv()

# Database connection parameters
DB_HOST = os.getenv('DB_HOST', 'localhost')
DB_PORT = os.getenv('DB_PORT', '5432')
DB_NAME = os.getenv('DB_NAME', 'weather_data')
DB_USER = os.getenv('DB_USER', 'postgres')
DB_PASSWORD = os.getenv('DB_PASSWORD', 'password')

# OpenWeatherMap API Configuration
API_KEY = os.getenv('WEATHER_API_KEY') # API key for OpenWeatherMap
CITIES = [
    "London, UK", 
    "New York, US", 
    "Tokyo, JP", 
    "Sydney, AU", 
    "Rio de Janeiro, BR"
    ]

# List of cities to get weather data for
WEATHER_API_URL = "https://api.openweathermap.org/data/2.5/weather"

def extract():
    """
    Extract weather data from OpenWeatherMap API for multiple cities
    Returns a pandas DataFrame with the extracted data
    """
    logger.info("Starting extraction phase...")
    
    if not API_KEY:
        logger.error("No API key found. Set WEATHER_API_KEY in your .env file")
        raise ValueError("Missing API key")
    
    all_weather_data = []
    
    try:
        for city in CITIES:
            # Set up parameters for the API request
            params = {
                'q': city,
                'appid': API_KEY,
                'units': 'metric'  # Use Celsius for temperature
            }
            
            # Make API request
            response = requests.get(WEATHER_API_URL, params=params)
            response.raise_for_status()  # Raise an exception for HTTP errors
            
            # Get the data as JSON
            weather_data = response.json()
            
            # Extract relevant fields (flattening the nested JSON)
            processed_data = {
                'city': city,
                'country': weather_data['sys']['country'],
                'temperature': weather_data['main']['temp'],
                'feels_like': weather_data['main']['feels_like'],
                'humidity': weather_data['main']['humidity'],
                'pressure': weather_data['main']['pressure'],
                'wind_speed': weather_data['wind']['speed'],
                'weather_main': weather_data['weather'][0]['main'],
                'weather_description': weather_data['weather'][0]['description'],
                'timestamp': datetime.fromtimestamp(weather_data['dt']),
                'sunrise': datetime.fromtimestamp(weather_data['sys']['sunrise']),
                'sunset': datetime.fromtimestamp(weather_data['sys']['sunset'])
            }
            
            all_weather_data.append(processed_data)
            logger.info(f"Extracted weather data for {city}")
        
        # Convert list of dictionaries to DataFrame
        df = pd.DataFrame(all_weather_data)
        
        logger.info(f"Extracted weather data for {len(df)} cities")
        return df
    
    except requests.exceptions.RequestException as e:
        logger.error(f"Error extracting data from Weather API: {e}")
        raise

def transform(df):
    """
    Transform the extracted weather data
    Returns a transformed DataFrame
    """
    logger.info("Starting transformation phase...")
    
    try:
        # Add ETL timestamp
        df['etl_timestamp'] = datetime.now()
        
        # Calculate day/night status
        def is_daytime(row):
            current_time = row['timestamp']
            sunrise = row['sunrise']
            sunset = row['sunset']
            return 'Day' if sunrise <= current_time <= sunset else 'Night'
        
        df['day_night'] = df.apply(is_daytime, axis=1)
        
        # Convert temperature to Fahrenheit for additional data
        df['temperature_f'] = df['temperature'] * 9/5 + 32
        """ 
        # Equivalent Excel transformation:  
        excel_df['Profit'] = excel_df['Revenue'] - excel_df['Cost']  
        """
        df['feels_like_f'] = df['feels_like'] * 9/5 + 32
        
        # Create temperature category
        def temp_category(temp):
            if temp < 0:
                return 'Very Cold'
            elif temp < 10:
                return 'Cold'
            elif temp < 20:
                return 'Mild'
            elif temp < 30:
                return 'Warm'
            else:
                return 'Hot'
        
        df['temp_category'] = df['temperature'].apply(temp_category)
        
        # Add weather data quality flag
        df['data_quality'] = 'Good'  # Default value
        
        # Mark suspicious data
        df.loc[df['temperature'] > 50, 'data_quality'] = 'Suspicious'  # Suspicious if over 50°C
        df.loc[df['temperature'] < -50, 'data_quality'] = 'Suspicious'  # Suspicious if below -50°C
        
        logger.info("Data transformation completed")
        return df
    
    except Exception as e:
        logger.error(f"Error transforming data: {e}")
        raise

def create_table():
    """
    Create the target table in PostgreSQL if it doesn't exist
    """
    create_table_query = """
    CREATE TABLE IF NOT EXISTS current_weather (
        id SERIAL PRIMARY KEY,
        city TEXT,
        country TEXT,
        temperature NUMERIC,
        temperature_f NUMERIC,
        feels_like NUMERIC,
        feels_like_f NUMERIC,
        humidity INTEGER,
        pressure INTEGER,
        wind_speed NUMERIC,
        weather_main TEXT,
        weather_description TEXT,
        timestamp TIMESTAMP,
        sunrise TIMESTAMP,
        sunset TIMESTAMP,
        day_night TEXT,
        temp_category TEXT,
        data_quality TEXT,
        etl_timestamp TIMESTAMP
    );
    """
    
    try:
        # Connect to the PostgreSQL database
        conn = psycopg2.connect(
            host=DB_HOST,
            port=DB_PORT,
            database=DB_NAME,
            user=DB_USER,
            password=DB_PASSWORD
        )
        
        # Create a cursor
        with conn.cursor() as cur:
            cur.execute(create_table_query)
            conn.commit()
            logger.info("Target table created or already exists")
        
        conn.close()
    
    except Exception as e:
        logger.error(f"Error creating table: {e}")
        raise

def load(df):
    """
    Load the transformed weather data into PostgreSQL
    """
    logger.info("Starting loading phase...")
    
    try:
        # Connect to the PostgreSQL database
        conn = psycopg2.connect(
            host=DB_HOST,
            port=DB_PORT,
            database=DB_NAME,
            user=DB_USER,
            password=DB_PASSWORD
        )
        
        # Create table if it doesn't exist
        create_table()
        
        # Prepare data for insertion
        columns = df.columns.tolist()
        
        # Convert DataFrame to list of tuples
        data_tuples = [tuple(row) for row in df.itertuples(index=False)]
        
        # Insert data into the database
        with conn.cursor() as cur:
            # Construct the INSERT query
            columns_str = ', '.join(columns)
            
            insert_query = f"""
            INSERT INTO current_weather ({columns_str})
            VALUES %s
            """
            
            execute_values(cur, insert_query, data_tuples)
            conn.commit()
        
        # Close the database connection
        conn.close()
        
        logger.info(f"Successfully loaded {len(df)} weather records into PostgreSQL")
    
    except Exception as e:
        logger.error(f"Error loading data: {e}")
        raise

def run_etl_pipeline():
    """
    Run the complete ETL pipeline for weather data
    """
    logger.info("Starting Weather ETL pipeline...")
    
    try:
        # Extract
        raw_data = extract()
        
        # Transform
        transformed_data = transform(raw_data)
        
        # Load
        load(transformed_data)
        
        logger.info("Weather ETL pipeline completed successfully")
    
    except Exception as e:
        logger.error(f"ETL pipeline failed: {e}")
        raise

if __name__ == "__main__":
    run_etl_pipeline()