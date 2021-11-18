import configparser
import logging
from io import BytesIO
from os import listdir
from shutil import rmtree
from urllib.request import urlopen
from zipfile import ZipFile


config = configparser.ConfigParser()
config.read("/opt/airflow/dags/pand/creds/creds.ini")

logging.basicConfig(level=logging.INFO, filemode='w', filename='/opt/airflow/dags/pand/files_worker/logs/df_creator',
                    format='%(name)s - %(levelname)s - %(message)s')


def extract_from_site() -> None:
    """get data from URL"""

    logging.info(f'Extracting files from {config.get("local_import", "URL")}')
    print(f'_____________________________________________________________________________________________________________{config.get("local_import", "URL")}')

    http_response = urlopen(config.get("local_import", "URL"))
    zipfile = ZipFile(BytesIO(http_response.read()))

    logging.info(f'Extracting files from zip.file')

    zipfile.extractall(path=config.get("local_import", "PATH"))


def delete_directory() -> None:
    """get directory with data"""
    rmtree(config.get("local_import", "PATH"))

    logging.warning(f'Directory deleted successfully')


def files() -> list:
    """return list of downloaded files"""
    return [file for file in listdir(config.get("local_import", "PATH"))]
