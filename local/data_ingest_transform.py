import os
import sys
import json
import requests
from datetime import datetime
import pandas as pd

from handling.logger import logging
from handling.exceptions import CustomException


bronzepath = os.path.join(os.getcwd(),'local','bronze')
goldpath = os.path.join(os.getcwd(),'local','gold')

def data_ingest():
    try:
        url = "https://api.nasa.gov/neo/rest/v1/feed?start_date=2026-04-15&end_date=2026-04-16&api_key=DEMO_KEY"
        response = requests.get(url)
        logging.info('Data Fetched.')
        data = response.json()
        filename = os.path.join(bronzepath,f'{datetime.now().strftime('%d-%m-%Y %H h %M m %S s')}.json')
        with open(filename,'w') as f:
            json.dump(data,f)
        logging.info('Data saved locally.')
        return data
    except Exception as e:
        raise CustomException(e,sys)

def data_transform(data=None):
    try:
        transformed = []
        if not data:
            with open(r'local/bronze/near_earth_raw.json','r') as f:
                data = json.load(f)
        logging.info('')
        for date in data['near_earth_objects']:
            for neo in data['near_earth_objects'][date]:
                transformed.append({
                    'id': neo['id'],
                    'name' : neo['name'],
                    'est_diameter_km_min_avg': (float(neo['estimated_diameter']['kilometers']['estimated_diameter_min'])+float(neo['estimated_diameter']['kilometers']['estimated_diameter_max']))/2,
                    'is_hazardous': neo['is_potentially_hazardous_asteroid'],
                    'close_approach_date' : neo['close_approach_data'][0]['close_approach_date'],
                    'velocity_km_s' : float(neo['close_approach_data'][0]['relative_velocity']['kilometers_per_second']),
                    'orbiting_body': neo['close_approach_data'][0]['orbiting_body']
                })
        df = pd.DataFrame(transformed)
        filepath = os.path.join(goldpath,f'{datetime.now().strftime('%d-%m-%Y %H h %M m %S s')}.csv')
        df.to_csv(filepath,index=False)
    except Exception as e:
        raise CustomException(e,sys)




if __name__ == '__main__':
    # data = data_ingest()
    print(data_transform())