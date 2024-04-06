import os
from dotenv import load_dotenv
from ETLPipeline import ETLPipeline

load_dotenv()

if __name__ == "__main__":
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

    pages = 10

    # uncomment the functions below if you're running for the first time

    # etlppl.extract_from_api(api_key, pages)
    # etlppl.transform_data()
    # etlppl.load_data('collection')
