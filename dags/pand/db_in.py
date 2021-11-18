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


def create_db():
    logging.info(f'Creating new db!')

    try:
        q = 'CREATE TABLE cars (\
                                id SERIAL NOT NULL PRIMARY KEY ,\
                                model TEXT Not NULL,\
                                year INT Not NULL,\
                                price INT Not NULL,\
                                transmission TEXT,\
                                mileage INT Not NULL,\
                                fuelType TEXT Not NULL,\
                                tax TEXT,\
                                mpg FLOAT,\
                                engineSize FLOAT Not NULL,\
                                manufacturer TEXT,\
                                date_added timestamp NOT NULL DEFAULT NOW()\
                                ) ;'
        ENGINE.execute(q)

        logging.info(f'db created successfully!')

    except Exception as e:
        error = str(e.__dict__['orig'])

        logging.warning(f'Failed: {error}')
