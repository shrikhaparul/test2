""" importing modules """
import json
import sqlalchemy
import pandas as pd
import definitions_qc as dq


def QC_pre_check(job_id):
    #To read configure json file to extract important deatils
    with open(r"C:\\Users\\ParulShrikhande\\Desktop\\dq_checks_file\\configure.json","r", encoding='utf-8') as jsonfile:
        config_json = json.load(jsonfile)
    control_table = pd.read_json(config_json['control_table'])
    #Creating connection to postgresql by using sqlalchemy
    conn_str = dq.read_config(config_json['config_path'], config_json['config_section_nm'])
    conn = sqlalchemy.create_engine(f'postgresql://{conn_str["user"]}'
            f':{conn_str["password"].replace("@", "%40")}@{conn_str["host"]}')
    #Reading postgresql checks_mapping table
    checks_mapping = pd.read_sql("select * from checks_mapping", conn)
    src_conn_str =  dq.read_config(
        config_json['config_path'], config_json[
            'src_section_nm']) if config_json['src_section_nm'] != '' else ''
    tgt_conn_str =  dq.read_config(
        config_json['config_path'], config_json[
            'tgt_section_nm']) if config_json['tgt_section_nm'] != '' else ''
    print("ooooo")
    pre_check_result = dq.qc_check(
        control_table, checks_mapping, 'pre_check', config_json[
            'source'], config_json['src_loc'], src_conn_str)
    print("ooooo")


def QC_post_check(job_id):
    #To read configure json file to extract important deatils
    with open(r"C:\\Users\\ParulShrikhande\\Desktop\\dq_checks_file\\configure.json","r", encoding='utf-8') as jsonfile:
        config_json = json.load(jsonfile)
    control_table = pd.read_json(config_json['control_table'])
    #Creating connection to postgresql by using sqlalchemy
    conn_str = dq.read_config(config_json['config_path'], config_json['config_section_nm'])
    conn = sqlalchemy.create_engine(f'postgresql://{conn_str["user"]}'
            f':{conn_str["password"].replace("@", "%40")}@{conn_str["host"]}')
    #Reading postgresql checks_mapping table
    checks_mapping = pd.read_sql("select * from checks_mapping", conn)
    src_conn_str =  dq.read_config(
        config_json['config_path'], config_json[
            'src_section_nm']) if config_json['src_section_nm'] != '' else ''
    tgt_conn_str =  dq.read_config(
        config_json['config_path'], config_json[
            'tgt_section_nm']) if config_json['tgt_section_nm'] != '' else ''
    print("ooooo")
    post_check_result = dq.qc_check(
        control_table, checks_mapping, 'post_check', config_json[
            'target'], config_json['tgt_loc'], tgt_conn_str)
    print("ooooo")
    """ post_check_result = dq.qc_check(
        control_table, checks_mapping, 'post_check', config_json[
            'target'], config_json['tgt_loc'], tgt_conn_str)

    #Appending the pre_check and post_check results into final_check_result
    final_check_result = pd.concat([pre_check_result, post_check_result], axis=0)
    final_check_result = final_check_result.reset_index(drop=True)
    print(final_check_result)
    """
