""" importing modules """
import configparser
import logging
import sqlalchemy
import pandas as pd
from fastparquet import ParquetFile

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
        file = pd.read_csv( json_data["csv_loc"],
            sep = json_data["delimiter"], chunksize = 100000,encoding='latin1')
        for chunk in file:
            chunk.to_sql(json_data["table"], conn, json_data["schema"],
                index=False,if_exists=json_data["if_exists"] )
        logging.info("csv to postgres ingestion completed")
        conn.dispose()
        return True
    except Exception as error:
        logging.exception("csv_to_postgres() is %s", str(error))
        raise Exception("csv_to_postgres(): " + str(error)) from error


# # code to ingest data from one table in postgres to other table in postgres
def ingest_postgres_to_postgres(json_data : dict, source_details : dict,
 target_details : dict) -> pd.DataFrame:
    """ function for ingesting data from postgres to postgres """
    try:
        logging.info("postgres to postgres ingestion started")
        db1 = sqlalchemy.create_engine(f'postgresql://{source_details["user"]}'
        f':{source_details["password"]}@{source_details["host"]}'
        f':{int(source_details["port"])}/{source_details["database"]}', encoding='utf-8')

        db2 = sqlalchemy.create_engine(f'postgresql://{target_details["user"]}'
        f':{target_details["password"]}@{target_details["host"]}'
        f':{int(target_details["port"])}/{target_details["database"]}', encoding='utf-8')
        logging.info('Reading data from source..')
        query = pd.read_sql_table(json_data["source_table"], db1, json_data["source_schema"])
        query.to_sql(json_data["target_table"], db2, json_data["target_schema"], index=False,\
        if_exists=json_data["if_exists"])
        logging.info('data copied in to postgres')
        db1.dispose()
        db2.dispose()
        return pd.DataFrame (json_data , source_details , target_details )
    except Exception as error:
        logging.exception("postgres_to_postgres() is %s", str(error))
        raise Exception("postgres_to_postgres(): " + str(error)) from error


def convert_csv_to_parquet(json_data : dict) -> bool:
    """ function for ingesting data from csv to parquet """
    try:
        logging.info("csv to parquet conversion started")    
        file = pd.read_csv(json_data["src_loc"],sep = json_data["delimiter"], chunksize = 100000, encoding = "latin 1")
        for chunk in file:
            chunk.to_parquet(json_data["target_loc"],index=False)
        logging.info("csv to parquet conversion completed")
        return True
    except Exception as error:
        logging.exception("convert_csv_to_parquet() is %s", str(error))
        raise Exception("convert_csv_to_parquet(): " + str(error)) from error
    

def ingest_parquet_to_csv(json_data : dict) -> bool:
    """ function for ingesting data from parquet to csv """
    try:
        logging.info("parquet based ingestion started")    
        # Reading the data from Parquet File
        file = ParquetFile(json_data["src_loc"],sep = json_data["delimiter"], chunksize = 100000, encoding = "utf-8")

        # Converting data in to pandas dataFrame
        df = file.to_pandas()
        print(df)
        # Converting to CSV
        for chunk in df:
            chunk.to_csv(json_data["target_loc"],index=False)
        logging.info(" parquet_to_csv ingestion completed")
        return True
    except Exception as error:
        logging.exception("parquet_to_csv() is %s", str(error))
        raise Exception("parquet_to_csv(): " + str(error)) from error    
    
def ingest_excel_to_parquet(json_data : dict) -> bool:
    """ function for ingesting data from excel to parquet """
    try:
        logging.info("excel based ingestion started") 
        file = pd.DataFrame(pd.read_excel(json_data["src_loc"],sep = json_data["delimiter"], chunksize = 100000, encoding = "utf-8"))
        # Write the Parquet file
        for chunk in file:
            chunk.to_parquet(json_data["target_loc"],index=False)
   
        # file = pd.read_csv(json_data["src_loc"],sep = json_data["delimiter"], chunksize = 100000, encoding = "latin 1")
        # for chunk in file:
        #     chunk.to_parquet(json_data["target_loc"],index=False)
        logging.info("excel to parquet ingestion completed")
        return True
    except Exception as error:
        logging.exception("excel_to_parquet() is %s", str(error))
        raise Exception("excel_to_parquet(): " + str(error)) from error    