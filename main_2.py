""" importing modules """
import json
import definitions_2

definitions_2.initiate_logging('logging_injestion',r"C:/Users/ParulShrikhande/Desktop/logging/")
definitions_2.logging.info('logging initiated')

# reading the json file
"""
try:
    with open(r"C:\\Users\\ParulShrikhande\\Desktop\\ingestion_file\\shilpi\\config.json","r", encoding='utf-8') as jsonfile:
        definitions_2.logging.info("reading json data")
        json_data = json.load(jsonfile)
        definitions_2.logging.info("reading json data completed")
except Exception as error:
    definitions_2.logging.error("error in reading json %s.", str(error))
    raise Exception("error in reading json: " + str(error)) from error

# main script starts here
if json_data["type"]=="ingestion":
    definitions_2.logging.info("entered  in to ingestion")
    if json_data["source"] == "file" and json_data["target"] == "postgres" :
        # set your parameters for the source database connection URI
        # using the keys from the configfile.ini
        conn_details = definitions_2.get_config_section(json_data["config_path"],\
        json_data["conn_nm"])
        definitions_2.ingest_csv_to_postgres(json_data, conn_details)
    elif json_data["source"]=="postgres" and json_data["target"]=="postgres":
        definitions_2.logging.info("entered in postgres ingestion")
        source_conn_details=definitions_2.get_config_section(json_data["config_path"],\
        json_data["conn_nm1"])
        target_conn_details=definitions_2.get_config_section(json_data["config_path"],\
        json_data["conn_nm2"])
        definitions_2.ingest_postgres_to_postgres(json_data,
            source_conn_details, target_conn_details)
    elif json_data["source"] == "csvfile" and json_data["target"] == "parquet" :
        definitions_2.logging.info("entered in converting_csv_to_parquet")
        definitions_2.convert_csv_to_parquet(json_data )    
    elif json_data["source"] == "file" and json_data["target"] == "result" :
        definitions_2.logging.info("entered in parquet to csv ingestion")
        definitions_2.ingest_parquet_to_csv(json_data )
    elif json_data["source"] == "file" and json_data["target"] == "result" :
        definitions_2.logging.info("entered in  excel to parquet ingestion")
        definitions_2.ingest_excel_to_parquet(json_data )              
    else:
        definitions_2.logging.warning("input proper source and target")
else:
    definitions_2.logging.info("only ingestion available currently")
"""


def csv_ingestion(job_id):
    try:
        with open(r"C:\\Users\\ParulShrikhande\\Desktop\\ingestion_file\\shilpi\\config2.json","r", encoding='utf-8') as jsonfile:
            definitions_2.logging.info("reading json data")
            json_data = json.load(jsonfile)
            definitions_2.logging.info("reading json data completed")
    except Exception as error:
        definitions_2.logging.error("error in reading json %s.", str(error))
        raise Exception("error in reading json: " + str(error)) from error
    
    # main script starts here
    if json_data["type"]=="ingestion":
        definitions_2.logging.info("entered  in to ingestion")
        if json_data["source"] == "file" and json_data["target"] == "postgres" :
            # set your parameters for the source database connection URI
            # using the keys from the configfile.ini
            conn_details = definitions_2.get_config_section(json_data["config_path"],\
            json_data["conn_nm"])
            definitions_2.ingest_csv_to_postgres(json_data, conn_details)
        elif json_data["source"]=="postgres" and json_data["target"]=="postgres":
            definitions_2.logging.info("entered in postgres ingestion")
            source_conn_details=definitions_2.get_config_section(json_data["config_path"],\
            json_data["conn_nm1"])
            target_conn_details=definitions_2.get_config_section(json_data["config_path"],\
            json_data["conn_nm2"])
            definitions_2.ingest_postgres_to_postgres(json_data,
                source_conn_details, target_conn_details)
        elif json_data["source"] == "csvfile" and json_data["target"] == "parquet" :
            definitions_2.logging.info("entered in converting_csv_to_parquet")
            definitions_2.convert_csv_to_parquet(json_data )    
        elif json_data["source"] == "file" and json_data["target"] == "result" :
            definitions_2.logging.info("entered in parquet to csv ingestion")
            definitions_2.ingest_parquet_to_csv(json_data )
        elif json_data["source"] == "file" and json_data["target"] == "result" :
            definitions_2.logging.info("entered in  excel to parquet ingestion")
            definitions_2.ingest_excel_to_parquet(json_data )              
        else:
            definitions_2.logging.warning("input proper source and target")
    else:
        definitions_2.logging.info("only ingestion available currently")
    
