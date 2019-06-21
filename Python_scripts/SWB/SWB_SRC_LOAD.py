import csv
import sys
sys.path.append(".")

import os
import datetime
import pandas as pd

from common_utils.stg_common_utils import *
from common_utils import initial_config
import json

time_of_load = datetime.datetime.now()
file_previously_loaded_check = False

config_connection = create_connection(initial_config.config_engine)

staging_engine = json.loads(config_connection.execute('''select connection_str from connectiondata 
                                                    where connection_id=1''').fetchone()[0])

swb_table_mapping = json.loads(config_connection.execute('''select mapping_dict from table_mapping where process_name='SWB' 
                                                        and load_type='STG_TO_SRC' ''').fetchone()[0])

stage_connection = create_connection(staging_engine)

source_engine = json.loads(config_connection.execute('''select connection_str from connectiondata 
                                                    where connection_id=2''').fetchone()[0])
source_connection = create_connection(source_engine)


def main(argv):

    path = argv[0]
    process_id = int(argv[1])
    process_name = argv[2]
    files = os.listdir(path)

    for file in files:

        file_details = fetch_file_details(file)
        fname =file_details[0]
        file_date = file_details[1]
        file_type = file_details[2]

        try:

            stg_table_name = pd.read_sql_query(''' 
            select tgt_table from etl_process where src_file_table_name ='{0}' and (status = 'SUCCESS') 
            '''.format(file), config_connection)
            stg_table_name = stg_table_name['tgt_table'][0]
            table_name = swb_table_mapping[stg_table_name]
        except IndexError as e:
            print('''Staging table contains Errors''')
            stg_table_name = pd.read_sql_query(''' select tgt_table from etl_process where src_file_table_name ='{0}' 
            and load_type='FILE TO STG' '''.format(file), config_connection)
            stg_table_name = stg_table_name['tgt_table'][0]
            table_name = swb_table_mapping[stg_table_name]
            continue

        column_details = fetch_col_specifications(process_id, process_name, fname, config_connection)
        column_specifications = column_details[0]
        column_name = column_details[1]

        source_details = stage_to_source(process_id, process_name, fname, stg_table_name, table_name, file, column_specifications,
                        column_name, stage_connection, config_connection)
        working_folder = source_details[0]
        data = source_details[1]

        get_run_id = is_file_loaded(process_id, process_name, file, config_connection, load_type='STG TO SRC')
        run_id = get_run_id[0]
        file_previously_loaded_check = get_run_id[1]

        if (run_id > 0):
            delete_existing_rows(table_name, run_id, source_connection)
            delete_existing_rows("ETL_ERROR", run_id, config_connection)

        stage_name='swb_source'

        put_and_copy_file(working_folder, data, source_connection, table_name, stage_name, load_type = 'STG TO SRC')

        errors = pd.read_sql_query(''' select * from table(validate({0}, job_id => '_last'))'''.format(table_name),
                                   source_connection)

        pd.read_sql_query(''' remove @{0}'''.format(stage_name), source_connection)

        print('Number of Records Loaded: ' + str(data.shape[0] - errors.shape[0]))

        record_details = [process_id, process_name, stg_table_name, file_type, table_name, 'STG TO SRC', file,
                          data.shape[0] - errors.shape[0], 'In Progress', file_date,
                          datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
                          data.shape[0], errors.shape[0]]

        if (file_previously_loaded_check == False):
            insert_into_etl_process(config_connection, record_details)
            run_id = fetch_run_id(config_connection, process_id, process_name, file, load_type='STG TO SRC')
        else:
            record_details.append(run_id)
            update_etl_process(config_connection, record_details)

        update_run_id(source_connection, table_name, run_id)

        if (errors.shape[0] > 0):
            print("ERRORS PRESENT WHILE LOADING........count:", errors.shape[0])
            for index, error in errors.iterrows():
                config_connection.execute(''' insert into ETL_ERROR( RUN_ID,PROCESS_ID,ERROR_DESC, ERROR_LINE,ERROR_CODE,ERROR_COL,CREATED_DATE) 
                        values ({0},{1},'{2}','{3}','{4}','{5}','{6}')'''.format(run_id, process_id, error['error'].replace("'",""),
                                                                                 error['line'], error['code'], error['column_name'],
                                                                                 time_of_load))
                #                             (errors[ERROR][0], errors[LINE][0],errors[CODE][0],errors[COLUMN_NAME][0]))
            config_connection.execute(
                ''' Update ETL_PROCESS set  status = 'FAILED' where run_id = {0}'''.format(run_id))
        else:
            print('''No ERRORS Updating 'ETL_PROCESS' table:''')
            config_connection.execute(
                ''' Update ETL_PROCESS set  status = 'SUCCESS' where run_id = {0}'''.format(run_id))

    config_connection.close()
    stage_connection.close()
    source_connection.close()


if __name__ == '__main__':
    main(sys.argv[1:])