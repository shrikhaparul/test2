""" importing modules """
from configparser import ConfigParser
from datetime import datetime
import multiprocessing
from multiprocessing.pool import ThreadPool
import great_expectations as ge
import sqlalchemy
import numpy as np
import pandas as pd


def read_config(filename, section):
    """reading the cofigure.ini file and passing the connection""" 
    parser = ConfigParser()
    parser.read(filename)
    datb = {}
    if parser.has_section(section):
        items = parser.items(section)
        for item in items:
            datb[item[0]] = item[1]
    else:
        raise Exception(f'{0} not found in the {1} file'.format(section, filename))
    return datb


def run_checks_in_parallel(index, cols, control_table_df, checks_mapping_df, ge_df):
    """Running all the checks specified in control table in parallel"""
    output_df = pd.DataFrame(
        columns=cols + [
            'unexpected_index_list', 'threshold_voilated_flag', 'run_flag',
             'result', 'output_reference', 'start_time', 'end_time', 'good_records_file', 'bad_records_file'])
    output_df.at[index,'threshold_voilated_flag'] = 'N'
    output_df.at[index, 'run_flag'] = 'N'
    for col in cols:
        output_df.at[index, col] = control_table_df.at[index, col]
    if control_table_df.at[index, 'active'] == 'Y':
        output_df.at[index, 'run_flag'] = 'Y'
        output_df.at[index, 'start_time'] = datetime.now().strftime("%d/%m/%Y %H:%M:%S")
        checks_nm = control_table_df.at[index, 'check']
        inputs_required = control_table_df.at[index, 'parameters'].split('|')
        #This section covers all the great expectations QA checks
        func_name = 'expect_' + checks_nm
        default_parameters_dtypes = checks_mapping_df[
            checks_mapping_df['func_name'] == func_name]['parameters'].item().split('|')
        ge_df_func = "ge_df." + func_name + '('
        for i, ele in enumerate(default_parameters_dtypes):
            if ele == 'string':
                ge_df_func = ge_df_func + "'{" + f"{i}" + "}',"
            else:
                ge_df_func = ge_df_func + "{" + f"{i}" + "},"
        ge_df_func = ge_df_func + "result_format = 'COMPLETE')"
        ge_df_func = ge_df_func.format(*inputs_required)
        res = eval(ge_df_func)
        output_df.at[index, 'result'] = 'PASS' if res['success'] is True else 'FAIL'
        if not res['success'] and control_table_df.at[index, 'ignore_bad_records'] == 'Y':
            output_df.at[index, 'output_reference'] = res['result']
            if 'unexpected_index_list' in res['result']:
                output_df.at[
                    index, 'unexpected_index_list'] = res['result']['unexpected_index_list']
                if control_table_df.at[
                    index, 'threshold_bad_records'] < res['result']['unexpected_percent']:
                    output_df.at[index,'threshold_voilated_flag'] = 'Y'
            output_df.at[index, 'end_time'] = datetime.now().strftime("%d/%m/%Y %H:%M:%S")
    return output_df    
    

def qc_check(control_table_df, checks_mapping_df, check_type, ing_type, ing_loc, conn_str):
    """Running all the checks specified in control table in parallel"""
    print(control_table_df['type'])
    print(check_type)
    control_table_df = control_table_df[control_table_df['type'] == check_type]
    print("heloo")
    cols = control_table_df.columns.tolist()
    resultset = pd.DataFrame(
        columns=cols + [
            'unexpected_index_list', 'threshold_voilated_flag', 'run_flag', 'result',
             'output_reference', 'start_time', 'end_time', 'good_records_file', 'bad_records_file'])
    row_val = control_table_df.index.values.tolist()
   
    pool = ThreadPool(multiprocessing.cpu_count())
    if ing_type == 'csv':
        ge_df = ge.read_csv(ing_loc, encoding='utf-8')
    elif ing_type == 'postgres':
        conn = sqlalchemy.create_engine(f'postgresql://{conn_str["user"]}'
            f':{conn_str["password"].replace("@", "%40")}@{conn_str["host"]}')
        pd_df = pd.read_sql(f'select * from {ing_loc}', conn)
        ge_df = ge.from_pandas(pd_df)
    else:
        raise Exception("Not a valid ingestion type")
    datasets = pool.map(
        lambda x:run_checks_in_parallel(
            x, cols, control_table_df, checks_mapping_df, ge_df), row_val)
    pool.close()
    for x in datasets:
        resultset = pd.concat([resultset, x])       
    resultset['unexpected_index_list'] = resultset['unexpected_index_list'].replace(np.nan,'')
    bad_records_indexes = list(set([
        item for sublist in resultset['unexpected_index_list'].tolist() for item in sublist]))
    output_loc = ing_type.strip('.csv') #+ "_"
    if check_type == 'pre_check':
        if 'FAIL' in resultset.result.values:
            indexes = list(set(bad_records_indexes))
            bad_records_df = ge_df[ge_df.index.isin(indexes)]
            bad_records_df.to_csv(
                output_loc + 'rejected_records_' + datetime.now().strftime(
                    "%d_%m_%Y_%H_%M_%S") + '.csv', index=False)
            if 'Y' in resultset['threshold_voilated_flag']:
                good_records_df = pd.DataFrame(columns=ge_df.columns.tolist())
                good_records_df.to_csv(
                    output_loc + 'accepted_records_'  + '.csv', index=False)
            else:
                good_records_df = ge_df[~ge_df.index.isin(indexes)]
                good_records_df.to_csv(
                    output_loc + 'accepted_records_'  + '.csv', index=False)
        else:
            good_records_df = ge_df
            good_records_df.to_csv(
                output_loc + 'accepted_records_'  + '.csv', index=False)
        resultset[
            'good_records_file'] = output_loc + 'accepted_records_'+ '.csv' #+ datetime.now().strftime(
            #"%d_%m_%Y_%H_%M_%S") + '.csv'
        resultset[
            'bad_records_file'] = output_loc + 'rejected_records_' + datetime.now().strftime(
            "%d_%m_%Y_%H_%M_%S") + '.csv'
    else:
        resultset['good_records_file'] = ""
        resultset['bad_records_file'] = ""
    resultset = resultset.drop(columns = ['unexpected_index_list', 'threshold_voilated_flag'])
    #resultset.to_csv('log_summary_' + datetime.now().strftime(
    # "%d_%m_%Y_%H_%M_%S") +'.csv',index=False)
    return resultset
    
