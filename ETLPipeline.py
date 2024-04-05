import numpy as np
import requests
import pandas as pd
import json
import os
from CustomORM import CustomORM


class ETLPipeline:

    def __init__(self):
        self.url = "https://api.opensea.io/api/v2/collections?chain=ethereum"
        self.api_key = None
        self.df = None
        self.data_lake = None
        self.orm = None

    def extract_from_api(self, api_key):

        self.api_key = api_key

        headers = {
            "accept": "application/json",
            "x-api-key": self.api_key
        }

        try:
            response = requests.get(self.url, headers=headers)
            response.raise_for_status()
            self.data_lake = json.loads(response.text)
        except requests.RequestException as e:
            print(f"Error fetching data from OpenSea Collections API: {e}")
            return None
        except KeyError:
            print("Unexpected response format from OpenSea Collections API")
            return None

    def save_raw_data(self, file_path):
        with open(file_path, "w") as json_file:
            json.dump(self.data_lake, json_file, indent=4)

    def transform_data(self):
        self.df = pd.DataFrame(data=self.data_lake['collections'])
        self.df.replace('', np.nan, inplace=True)
        self.df['address'] = self.df['contracts'].map(lambda x: np.nan if len(x) == 0 else x[0]['address'])

    def load_data(self, dbname, user, password, host, port, table_name):
        self.orm = CustomORM(dbname, user, password, host, port)
        self.orm.connect()
        self.orm.create_table(table_name, {'id': 'INTEGER PRIMARY KEY', 'collection': 'TEXT', 'name': 'TEXT',
                                           'description': 'TEXT', 'image_url': 'TEXT', 'owner': 'TEXT',
                                           'twitter_username': 'TEXT', 'address': 'TEXT'})

        if not self.orm.select(table_name):
            for i in range(len(self.df.index)):
                self.orm.insert(table_name, (i + 1, self.df['collection'].loc[i], self.df['name'].loc[i],
                                             self.df['description'].loc[i], self.df['image_url'].loc[i],
                                             self.df['owner'].loc[i],
                                             self.df['twitter_username'].loc[i], self.df['address'].loc[i]))
