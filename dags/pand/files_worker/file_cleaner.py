import logging
import configparser
import pandas as pd

config = configparser.ConfigParser()
config.read("/opt/airflow/dags/pand/creds/creds.ini")

logging.basicConfig(level=logging.INFO, filemode='w', filename='/opt/airflow/dags/pand/files_worker/logs/df_creator',
                    format='%(name)s - %(levelname)s - %(message)s')


def file_cleaner(file: str) -> pd.DataFrame:
    """read csv file and fix them to the right format"""

    logging.info(f'Start cleaning file:{file}')

    dfa = pd.read_csv(f'{config["local_import"]["PATH"]}/{file}', dtype={'model': pd.StringDtype(),
                                               'year': pd.Int32Dtype(),
                                               })

    df = delete_empty_rows(dfa)
    df = remove_separators(df, 'price')
    df = fix_columns(df, 'mileage', 'mileage2')
    df = fix_columns(df, 'fuel type', 'fuel type2')
    df = fix_columns(df, 'engine size', 'engine size2')
    df = remove_separators(df, 'mileage')
    df = delete_columns(df, ['engine size2', 'fuel type2', 'mileage2', 'reference'])
    df = engine_values_fix(df)
    df = rename_columns(df, 'engine size')
    df = rename_columns(df, 'fuel type')
    df = df.drop(df[df['mileage'] == 'Unknown'].index)
    return df


def delete_empty_rows(df: pd.DataFrame) -> pd.DataFrame:
    """delete empty rows from df and replace all Nan or <NA> with -1"""
    try:
        df = df[df['year'].notna()]
        df.reset_index(drop=True)
        df = df.replace(pd.NA, -1)

        logging.info(f'Deleting empty rows')

    except KeyError as err:

        logging.warning(f'Exception:{err}')

    return df


def remove_separators(df: pd.DataFrame, coll: str) -> pd.DataFrame:
    """remove separators and chars from columns"""
    df[coll] = df[coll].str.replace(' £', '')
    df[coll] = df[coll].str.replace(',', '')
    df[coll] = df[coll].str.replace('.', '')

    logging.info(f'Renaming columns')

    return df


def fix_columns(df: pd.DataFrame, column_to_change: str, column_with_info: str) -> pd.DataFrame:
    """concat column mileage and  mileage2 if mileage2 is not null"""
    try:

        logging.info(f'fixing column: {column_to_change}')

        df[column_to_change] = df[column_to_change].fillna(df[column_with_info])

    except KeyError as err:

        logging.warning(f'Exception:{err}')

    return df


def delete_columns(df: pd.DataFrame, columns: list) -> pd.DataFrame:
    """delete useless columns"""
    try:
        df = df.drop(columns, 1)

        logging.info(f'Dropped column:{columns}')

    except KeyError as err:

        logging.warning(f'Exception:{err}')

    return df


def rename_columns(df: pd.DataFrame, column_for_rename: str) -> pd.DataFrame:
    """rename column as in the top files"""
    try:
        logging.info(f'Renaming column:{column_for_rename}')
        new_name = column_for_rename.split(" ")
        new_name = new_name[0] + new_name[1].capitalize()
        df.rename(columns={column_for_rename: new_name}, inplace=True)

    except KeyError as err:

        logging.warning(f'Exception:{err}')

    return df


def engine_values_fix(df: pd.DataFrame) -> pd.DataFrame:
    """fix all values to top format"""
    for index, row in df.iterrows():

        if len(str(df['engine size'][index])) >= 3 and "." not in str(df['engine size'][index]):
            df['engine size'][index] = str(df['engine size'][index])[1] + "." + str(df['engine size'][index])[2]
        else:
            df['engine size'][index] = str(df['engine size'][index])[:3]

    df['engine size'] = pd.to_numeric(df['engine size'], errors='coerce')
    return df
