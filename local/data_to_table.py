import os
import sys
import pandas as pd

from handling.exceptions import CustomException
from handling.logger import logging
from sqlalchemy import create_engine, Table,MetaData,text
from sqlalchemy.dialects.postgresql import insert

engine = create_engine("postgresql://admin:admin@localhost:5432/nasa_db")


def create_table(engine=engine):
    try:
        query = """
        CREATE TABLE IF NOT EXISTS nasaneo(
        id BIGINT PRIMARY KEY,
        name TEXT UNIQUE,
        absolute_magnitude FLOAT,
        est_diameter_km_min_avg FLOAT,
        is_hazardous BOOLEAN,
        close_approach_date DATE,
        miss_distance_km FLOAT,
        velocity_km_s FLOAT,
        orbiting_body TEXT
        );
        """
        with engine.begin() as conn:
            conn.execute(text(query))
        return engine
    except Exception as e:
        raise CustomException(e,sys)

def insert_data(data:pd.DataFrame,engine=engine,table = 'nasaneo'):
    try:
        data.to_sql(table, engine,if_exists='append',index=False)
    except Exception as e:
        raise CustomException(e,sys)
    logging.info('Written data to table')