import configparser
import logging

import pandas as pd
import sqlalchemy

config = configparser.ConfigParser()
config.read("/opt/airflow/dags/pand/creds/creds.ini")

logging.basicConfig(level=logging.INFO, filemode='w', filename='/opt/airflow/dags/pand/files_worker/logs/df_creator',
                    format='%(name)s - %(levelname)s - %(message)s')

ENGINE = sqlalchemy.create_engine(
    f'postgresql://{config.get("postgres", "user")}:{config.get("postgres","password")}@host.docker.internal:{config.get("postgres","port")}/{config.get("postgres","database")}')


def ins_data_to_db(df):
    logging.info(f'Start db injection')

    df = pd.read_json(df)
    for i in ['model', 'year', 'mileage', 'fuelType', 'engineSize', 'price']:
        logging.info(f'deleting Null from column {i}')

        df = df[df[i].notna()]

    df.to_sql('cars', ENGINE, if_exists='append', index=False)

    logging.info(f'db injection finished successfully!')



