""" importing modules """
import configparser
import logging
import sqlalchemy
import pandas as pd
from fastparquet import ParquetFile
import json

def initiate_logging(project : str,log_loc: str) -> bool:
    """Function to initiate log file which will be used across framework"""
    try:
        log_file_nm= project + '.log'
        new_file1 = log_loc + log_file_nm
        format_str = '%(asctime)s | %(name)-10s | %(processName)-12s | %(funcName)s\
           |%(levelname)-5s | %(message)s'
        formatter = logging.Formatter(format_str)
        # create formatter & how we want ours logs to be formatted
        logging.basicConfig(filename=new_file1, filemode='w', level=logging.INFO, format=format_str)
        st_handler = logging.StreamHandler() # create handler and set the log level
        st_handler.setLevel(logging.INFO)
        st_handler.setFormatter(formatter)
        logging.getLogger().addHandler(st_handler)
        return True
    except Exception as ex:
        logging.error('UDF Failed: initiate_logging failed')
        raise ex


# reading the cofig.ini file and passing the connection details as dictionary
def get_config_section(config_path : str, conn_nm : str) -> dict:
    """ function for establishing connection """
    try:
        config = configparser.ConfigParser()
        config.read(config_path)
        if not config.has_section(conn_nm):
            logging.error("Invalid section %s.", str(conn_nm))
            raise Exception(f"Invalid Section {conn_nm}")
        return dict(config.items(conn_nm))
    except Exception as error:
        logging.exception("get_config_section() is %s.", str(error))
        raise Exception("get_config_section(): " + str(error)) from error


# code to read csv and ingest in to table
def ingest_csv_to_postgres(json_data : dict, postgres_details : dict) -> bool:
    """ function for ingesting data from csv to postgres """
    try:
        logging.info("file based ingestion started")
        conn = sqlalchemy.create_engine(f'postgresql://{postgres_details["user"]}'
        f':{postgres_details["password"]}@{postgres_details["host"]}'
        f':{int(postgres_details["port"])}/{postgres_details["database"]}', encoding='utf-8')
        # conn = sqlalchemy.create_engine(f'postgresql://{user}:{password}@{host}:\
        # {port}/{data_base}', encoding='latin1')
        # print(conn)
        file = pd.read_csv( json_data["task"]["source"]["source_file_path"],
            sep = json_data["task"]["source"]["file_delimiter"], chunksize = 100000,encoding='latin1')
        for chunk in file:
            print(json_data["task"]["target"]["table_name"])
            chunk.to_sql(json_data["task"]["target"]["table_name"], conn, json_data["task"]["target"]["schema"],
                index=False,if_exists=json_data["task"]["target"]["if_exists"] )
        logging.info("csv to postgres ingestion completed")
        conn.dispose()
        return True
    except Exception as error:
        logging.exception("csv_to_postgres() is %s", str(error))
        raise Exception("csv_to_postgres(): " + str(error)) from error


if __name__ == "__main__":
    initiate_logging('logging_injestion',r"C:/Users/ParulShrikhande/Desktop/logging/")
    logging.info('logging initiated')

    try:
        with open(r"C:\\Users\\ParulShrikhande\\Desktop\\dq_checks_file\\task_id_1.json","r", encoding='utf-8') as jsonfile:
            logging.info("reading json data")
            json_data = json.load(jsonfile)
            logging.info("reading json data completed")
            print(json_data["task"]["task_id"])
            print("hellooo")
            print(json_data["task"]["source"]["source_type"])
    except Exception as error:
        logging.error("error in reading json %s.", str(error))
        raise Exception("error in reading json: " + str(error)) from error
    
    # main script starts here
    if json_data["task"]["task_type"]=="csv_ingestion":
        logging.info("entered  in to ingestion")
        if json_data["task"]["source"]["source_type"] == "csv" and json_data["task"]["target"]["target_type"] == "postgres" :
            # set your parameters for the source database connection URI
            # using the keys from the configfile.ini
            conn_details = get_config_section(json_data["task"]["config_file"],\
            json_data["task"]["conn_nm"])
            ingest_csv_to_postgres(json_data, conn_details)           
        else:
            logging.warning("input proper source and target")
    else:
        logging.info("only ingestion available currently")

