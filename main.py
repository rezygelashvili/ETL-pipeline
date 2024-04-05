import os
from dotenv import load_dotenv
from ETLPipeline import ETLPipeline

load_dotenv()

# create a .env file for your specific credentials

api_key = os.getenv("API_KEY")

pg_connection_dict = {
    'dbname': os.getenv("DBNAME"),
    'user': os.getenv("USER"),
    'password': os.getenv("PASSWORD"),
    'host': os.getenv("HOST"),
    'port': os.getenv("PORT"),
}

etlppl = ETLPipeline(**pg_connection_dict)

# uncomment the functions below if you're running for the first time

# etlppl.extract_from_api(api_key)
# etlppl.transform_data()
# etlppl.load_data('collection')
