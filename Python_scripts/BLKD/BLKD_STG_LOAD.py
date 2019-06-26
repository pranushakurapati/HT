import datetime
import pytz
import pandas as pd
import os
import csv

from snowflake.sqlalchemy import URL
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

import sys
sys.path.append(".")

import json


config_engine = {
    'user':'ABServiceAccountSnowflake', #'ABServiceAccountSnowflake',
    'password': 'Hightower2019!',
    'account': 'hightoweradvisors.us-east-1', #'saggezzapartner.us-east-1',
    'warehouse': 'HT_SOURCE_WH',#'HT_WH',# Needs to be changed accordingly with respect to High Tower Instance
    'database': 'HT_SOURCE_DB',# Needs to be changed accordingly with respect to High Tower Instance
    'schema': 'CONFIG',# Needs to be changed accordingly with respect to High Tower Instance
    'role': 'DEVELOPER', #'HT_DEVELOPER',
    'numpy': True
}



def create_connection(engine_name):
    engine = create_engine(URL(
                                user=engine_name['user'],
                                password=engine_name['password'],
                                account=engine_name['account'],
                                warehouse=engine_name['warehouse'],
                                database=engine_name['database'],
                                schema=engine_name['schema'],
                                role= engine_name['role'],
                                numpy=engine_name['numpy']
                                ))
    return engine.connect()


def is_file_loaded(process_id, process_name, file, connection, load_type):
    get_run_id = connection.execute(''' select run_id from etl_process where process_id = {0} 
                                          and process_name = '{1}' and
                                          file_name= '{2}' and load_type='{3}' '''.format(process_id,process_name, file, load_type))
    run_id = get_run_id.fetchall()
    if len(run_id) > 0:
        file_previously_loaded_check = True
        run_id = run_id[0][0]
    else:
        file_previously_loaded_check = False
        run_id = 0
    return run_id, file_previously_loaded_check


def delete_existing_rows(table_name, run_id, connection):
    print('File was already loaded, deleting records from the table {0}'.format(table_name))
    ### delete records from table with that run_id
    delete_records = connection.execute(''' delete from {0} where run_id = {1}'''.format(
        table_name, run_id))
    deleted_records = delete_records.fetchall()[0][0]
    print('Records deleted from: ' + table_name + " Count: " + deleted_records.astype(str))\


def fetch_file_details(file):
    fname = file.split(".")[0] + "." + file.split(".")[1]
    fdate = datetime.datetime.now(pytz.timezone('America/Chicago')).strftime("%Y-%m-%d %H:%M:%S.%f %z")
    ftype = file.split(".")[2].upper()
    return fname, fdate, ftype


def insert_into_etl_process(connection, *args):
    rec = args[0]
    connection.execute(''' insert into etl_process(
                       PROCESS_ID,PROCESS_NAME,CUSTODIAN_NAME,SRC_FILE_TABLE_NAME,FILE_TYPE,
                       TGT_TABLE,LOAD_TYPE,FILE_NAME,RECORDS_LOADED,STATUS,FILE_DATE,
                       MODIFIED_DATE,TOTAL_RECORD_COUNT,RECORDS_REJECTED) 
                       values ({0},'{1}','{1}','{2}','{3}','{4}','{5}','{6}',{7},'{8}','{9}','{10}',{11},{12})'''.format(
                       rec[0], rec[1], rec[2], rec[3], rec[4], rec[5], rec[6], rec[7], rec[8], rec[9], rec[10],
                       rec[11], rec[12]))


def fetch_run_id(connection, process_id, process_name, file, load_type):
    get_newrun_id = connection.execute(''' select run_id from etl_process where process_id = {0} 
                                          and process_name = '{1}' and
                                          file_name= '{2}' and load_type='{3}' '''.format(process_id, process_name, file, load_type))
    newrun_id = get_newrun_id.fetchall()
    run_id = newrun_id[0][0]
    return run_id


def update_run_id(connection, table_name, run_id):
    connection.execute(''' Update {0} set run_id = {1} where run_id = 0'''.format(table_name, run_id))


def update_etl_process(connection, *args):
    rec = args[0]
    connection.execute(''' update etl_process set MODIFIED_DATE = '{10}',RECORDS_LOADED = {7}, 
    TOTAL_RECORD_COUNT = {11}, TGT_TABLE = '{4}', RECORDS_REJECTED = {12}, STATUS='{8}', FILE_DATE='{9}',
    FILE_TYPE='{3}', LOAD_TYPE='{5}', SRC_FILE_TABLE_NAME = '{2}' where process_id = {0} 
    and process_name = '{1}' and file_name= '{6}' and run_id = {13} '''.format(rec[0], rec[1], rec[2], rec[3], rec[4],
                                                                               rec[5], rec[6], rec[7], rec[8], rec[9],
                                                                               rec[10], rec[11], rec[12], rec[13]))

def process_files(path):
    print('File names before processing files')
    files = os.listdir(path)
    print(files)

    ##NEWLY ADDED CODE TO AVOID SPACES IN FILENAMES
    for filename in files:
        if ' ' in filename:
            NewName = filename.replace(" ", "")
            os.rename(os.path.join(path, filename), os.path.join(path, NewName))
            print(NewName)


    print('File names after processing files')
    files = os.listdir(path)
    print(files)


file_previously_loaded_check = False

config_connection = create_connection(config_engine)

staging_engine = json.loads(config_connection.execute('''select connection_str from connectiondata 
                                              where connection_id=1''').fetchone()[0])
print('staging Conection: {}'.format(staging_engine))
stage_connection = create_connection(staging_engine)


#stage_connection.execute('''Create or replace table BLKD_ACCOUNT_STG(Data Variant, PROCESS_ID NUMBER(10,0), 
    #CUSTODIAN_NAME VARCHAR(255), RUN_ID NUMBER(11,0),CREATEDATE TIMESTAMP_LTZ)''')


def main(argv):

    path = argv[0]
    process_id = int(argv[1])
    process_name = argv[2]

    blkd_table_mapping = json.loads(config_connection.execute('''select mapping_dict from table_mapping where process_name='{0}' '''.format(process_name)).fetchone()[0])
    print('blkd_table_mapping : {}'.format(blkd_table_mapping))

    process_files(path)
    files = os.listdir(path)

    for file in files:

        data = []

        file_details = fetch_file_details(file)
        fname = file_details[0]
        file_date = file_details[1]
        file_type = file_details[2]
        
        for key, val in blkd_table_mapping.items():
            if key in fname:
                table_name = val
                break;
        
        get_run_id = is_file_loaded(process_id, process_name, file, config_connection, load_type='FILE TO STG')
        print('get_run_id: {}'.format(get_run_id))

        run_id = get_run_id[0]
        file_previously_loaded_check = get_run_id[1]
        
        if (run_id > 0):
            delete_existing_rows(table_name, run_id, stage_connection)
            #delete_existing_rows("ETL_ERROR", run_id, config_connection)
            
        
        stage_connection.execute('''CREATE OR REPLACE STAGE temp_stage''')
        stage_connection.execute(r"put file:///{path}\{file_name} @temp_stage".format(path=path,file_name=file))
        stage_connection.execute('''Create OR REPLACE table temp (Data Variant)''')
        stage_connection.execute('''COPY INTO TEMP FROM @temp_stage FILE_FORMAT=(TYPE='json' STRIP_OUTER_ARRAY=true) ON_ERROR = 'continue';''')
        
        errors = pd.read_sql_query(''' select * from table(validate({0}, job_id => '_last'))'''.format('temp'),
                                       stage_connection)
        
        stage_connection.execute('''ALTER TABLE Temp ADD PROCESS_ID NUMBER(10,0), 
        CUSTODIAN_NAME VARCHAR(255), RUN_ID NUMBER(11,0),CREATEDATE TIMESTAMP_LTZ;''')
        
        stage_connection.execute("UPDATE Temp SET PROCESS_ID = {}, CUSTODIAN_NAME = '{}',RUN_ID = {}, CREATEDATE = '{}';"
                                 .format(process_id, process_name, run_id, file_date))
        
        var = stage_connection.execute("INSERT INTO {} SELECT * FROM TEMP".format(table_name))
        

        stage_connection.execute('''DROP table TEMP''')
        
        
        record_details = [process_id, process_name, file, file_type, table_name, 'FILE TO STG', file,
                              var.rowcount, 'In Progress', file_date,
                              datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
                              var.rowcount, -3]

        
        
        if (file_previously_loaded_check == False):
            insert_into_etl_process(config_connection, record_details)
            run_id = fetch_run_id(config_connection, process_id, process_name, file, load_type='FILE TO STG')
        else:
            record_details.append(run_id)
            update_etl_process(config_connection, record_details)

        update_run_id(stage_connection, table_name, run_id)

    config_connection.close()
    stage_connection.close()
    
    

if __name__ == '__main__':
    main(sys.argv[1:])

