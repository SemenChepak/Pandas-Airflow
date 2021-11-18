import logging
import configparser

import pandas as pd

from pand.files_worker.file_cleaner import file_cleaner


config = configparser.ConfigParser()
config.read("/opt/airflow/dags/pand/creds/creds.ini")

logging.basicConfig(level=logging.INFO, filemode='w', filename='/opt/airflow/dags/pand/files_worker/logs/df_creator',
                    format='%(name)s - %(levelname)s - %(message)s')


def df_generator(file_list):
    """read files: if schema is incorrect fix file"""

    logging.info(f'Start reading the files')

    for file in file_list:

        logging.info(f'Reading file: {file}')

        try:
            df = pd.read_csv(f'{config.get("local_import", "PATH")}/{file}',
                             dtype={'model': pd.StringDtype(),
                                    'year': pd.Int32Dtype(),
                                    'price': pd.Int32Dtype(),
                                    'transmission': pd.StringDtype(),
                                    'mileage': pd.Int32Dtype(),
                                    'fuelType': pd.StringDtype(),
                                    'tax': pd.Int32Dtype(),
                                    'mpg': pd.Float32Dtype(),
                                    'engineSize': pd.Float32Dtype(),
                                    }
                             )
        except Exception as err:

            logging.warning(f'Exception: {err}! Called file_cleaner')

            df = file_cleaner(file)

        df['manufacturer'] = file.split('.')[0]
        df = rm_tax_chars(df)

        logging.info(f'File ready! File:{file}')

        yield df


def collect_df(files):
    """collect all files_df into one"""

    logging.info(f'Creating empty DataFrame!')

    d_faa = pd.DataFrame()
    for i in df_generator(files):
        logging.info(f'Appended to DataFrame:{i}!')

        d_faa = d_faa.append(i)

    logging.info(f'DataFrame is ready:{d_faa.info}!')

    return d_faa.reset_index(drop=True).to_json(orient='records')


def rm_tax_chars(df):
    """fix column name"""
    try:

        logging.info(f'fixing column header tax!')

        df.rename(columns={'tax(£)': 'tax'}, inplace=True)
    except Exception as err:

        logging.info(f'Exception: {err}')

    return df
