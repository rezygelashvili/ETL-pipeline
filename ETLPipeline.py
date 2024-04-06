import requests
import pandas as pd
import json

from CustomORM import CustomORM


class ETLPipeline:

    def __init__(self, dbname: str, user: str, password: str, host: str, port: int):
        self.url: str = "https://api.opensea.io/api/v2/collections?chain=ethereum&next="
        self.api_key: str = ''
        self.df: pd.DataFrame = pd.DataFrame()
        self.data: list[dict[str, any]] = []
        self.raw_data: str = ''
        self.orm: CustomORM = CustomORM(dbname, user, password, host, port)
        self.orm.connect()

    def extract_from_api(self, api_key: str, pages: int) -> None:

        self.api_key = api_key

        headers = {
            "accept": "application/json",
            "x-api-key": self.api_key
        }

        url_next = ''

        for i in range(pages):
            try:
                print(f"extracting page {i + 1}")
                response = requests.get(self.url + url_next, headers=headers)
                response.raise_for_status()
                json_data = json.loads(response.text)
                self.raw_data += f", {response.text}"
                url_next = json_data['next']
                self.data.extend(json_data['collections'])
            except requests.RequestException as e:
                print(f"Error fetching data from OpenSea Collections API: {e}")
                return None
            except KeyError:
                print("Unexpected response format from OpenSea Collections API")
                return None

    def save_raw_data(self, file_path: str):
        with open(file_path, "w") as json_file:
            json.dump(self.data, json_file, indent=4)

    def transform_data(self):
        self.df = pd.DataFrame(data=self.data)
        self.df.replace('', None, inplace=True)
        self.df['address'] = self.df['contracts'].map(lambda x: None if len(x) == 0 else x[0]['address'])

    def load_data(self, table_name: str):
        self.orm.create_table(table_name, {'id': 'INTEGER PRIMARY KEY', 'collection': 'TEXT', 'name': 'TEXT',
                                           'description': 'TEXT', 'image_url': 'TEXT', 'owner': 'TEXT',
                                           'twitter_username': 'TEXT', 'address': 'TEXT'})

        if not self.orm.select(table_name):
            for i in range(len(self.df.index)):
                self.orm.insert(table_name, (i + 1, self.df['collection'].loc[i], self.df['name'].loc[i],
                                             self.df['description'].loc[i], self.df['image_url'].loc[i],
                                             self.df['owner'].loc[i],
                                             self.df['twitter_username'].loc[i], self.df['address'].loc[i]))
