import os
import sys
import json
import requests
import pandas as pd
from dotenv import load_dotenv
load_dotenv()

from handling.logger import logging
from handling.exceptions import CustomException


bronzepath = os.path.join(os.getcwd(),'local','bronze')
goldpath = os.path.join(os.getcwd(),'local','gold')
API_KEY = os.environ['NASA_API']
def data_ingest():
    try:
        url = f"https://api.nasa.gov/neo/rest/v1/feed?start_date=2026-04-09&end_date=2026-04-16&api_key={API_KEY}"
        response = requests.get(url)
        logging.info('Data Fetched.')
        data = response.json()
        filename = os.path.join(bronzepath,'near_earth_raw.json')
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
        logging.info('Extracting useful information.')
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
        logging.info('Data converted to dataframe')
        filepath = os.path.join(goldpath,'.csv')
        risk_summary = df.groupby('is_hazardous').agg({
            'velocity_km_s': 'mean',
            'id': 'count'
        }).reset_index()
    
        # daily_trends = df.groupby('approach_date').size().reset_index(name='object_count')
        # daily_trends.to_csv(os.path.join(goldpath,'daily_trends.csv'), index=False)
        summary_stats = {
        'total_objects': len(df),
        'hazardous_count': df['is_hazardous'].sum(),
        'avg_velocity': df['velocity_km_s'].mean(),
        'closest_miss_km': df['miss_distance_km'].min()
        }
        
        

        df.to_parquet(os.path.join(goldpath,'table_parquet.parquet'))
        risk_summary.to_csv(os.path.join(goldpath,'risk_summary.csv'), index=False)
        df.to_csv(os.path.join(goldpath,'processed.csv'),index=False)
        logging.info('Data saved as csv file.')
        return df
    except Exception as e:
        raise CustomException(e,sys)




if __name__ == '__main__':
    data = data_ingest()
    print(data_transform(data))