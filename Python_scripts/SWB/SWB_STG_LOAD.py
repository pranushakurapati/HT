import sys
sys.path.append(".")

import datetime
import os
import pandas as pd
import json
import csv

from common_utils.stg_common_utils import *
from common_utils import initial_config

time_of_load = datetime.datetime.now()
file_previously_loaded_check = False

config_connection = create_connection(initial_config.config_engine)

staging_engine = json.loads(config_connection.execute('''select connection_str from connectiondata 
                                              where connection_id=1''').fetchone()[0])

stage_connection = create_connection(staging_engine)


def main(argv):

    path = argv[0]
    process_id = int(argv[1])
    process_name = argv[2]

    swb_table_mapping = json.loads(config_connection.execute('''select mapping_dict from table_mapping where 
                                                            process_name='{0}' 
                                                            and load_type='FILE_TO_STG' '''.format(process_name)).fetchone()[0])

    files = os.listdir(path)

    for file in files:

        data = []

        file_details = fetch_file_details(file)
        fname = file_details[0]
        file_date = file_details[1]
        file_type = file_details[2]

        try:
            table_name = swb_table_mapping[fname]
            if(fname=="SWB_CRS.TRN" or fname=="SWB_CRS.ACC" or fname=="SWB_CRS.RPS" or fname=="SWB_CRS.SEC"):
                index1 = 0
                index2 = 0
                ignore_index = 0
                ignore_top = 2
                compareString = ""
                data = transform_file(path + "/" + file, index1, index2, compareString, ignore_index, ignore_top)
        except KeyError as e:
            print("Error in fetching table for File: " + fname)
            break

        data = create_staged_data(process_id, process_name, data)
        print(data.head())

        get_run_id = is_file_loaded(process_id, process_name, file, config_connection, load_type='FILE TO STG')
        run_id = get_run_id[0]
        file_previously_loaded_check = get_run_id[1]

        if (run_id > 0):
            delete_existing_rows(table_name, run_id, stage_connection)
            delete_existing_rows("ETL_ERROR", run_id, config_connection)

        print("Inserting " + fname + ".........")

        working_folder = pd.read_sql_query('''select local_folder from filelocation where process_id={0}'''
                                           .format(process_id), config_connection)['local_folder'][0]
        working_folder = working_folder.strip('"')

        if not os.path.exists(working_folder+'\csv'):
            os.mkdir(working_folder+'\csv')
        working_folder = working_folder+'\csv'

        stage_name='swb_stage'

        data.to_csv(working_folder+ '\Test_CSV_file_to_stage.csv', sep='`', header=False, index=False, na_rep='', quoting=csv.QUOTE_NONE)

        put_and_copy_file(working_folder, data, stage_connection, table_name, stage_name)

        errors = pd.read_sql_query(''' select * from table(validate({0}, job_id => '_last'))'''.format(table_name),
                                   stage_connection)

        print('Number of Errors found: ' + str(errors.shape[0]))

        print('Number of Records Loaded: ' + str(data.shape[0] - errors.shape[0]))

        print('Logging the file status')

        record_details = [process_id, process_name, file, file_type, table_name, 'FILE TO STG', file,
                          data.shape[0] - errors.shape[0], 'In Progress', file_date,
                          datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
                          data.shape[0], errors.shape[0]]

        if (file_previously_loaded_check == False):
            insert_into_etl_process(config_connection, record_details)
            run_id = fetch_run_id(config_connection, process_id, process_name, file, load_type='FILE TO STG')
        else:
            record_details.append(run_id)
            update_etl_process(config_connection, record_details)

        update_run_id(stage_connection, table_name, run_id)

        stage_connection.execute('''insert into HT_SOURCE_DB.CONFIG.ETL_error( RUN_ID,PROCESS_ID,ERROR_DESC, ERROR_LINE,ERROR_CODE,ERROR_COL,CREATED_DATE, MODIFIED_DATE) 
                select {1}, {2}, error, line, code, column_name, '{3}','{3}' from table(validate({0}, job_id => '_last'))'''.format(table_name,run_id,process_id,time_of_load))

        if (errors.shape[0] > 0):

            print("ERRORS PRESENT WHILE LOADING........count:", errors.shape[0])
            config_connection.execute(
                ''' Update ETL_PROCESS set  status = 'FAILED' where run_id = {0}'''.format(run_id))
        else:
            print('''No ERRORS Updating 'ETL_PROCESS' table:''')
            config_connection.execute(
                ''' Update ETL_PROCESS set  status = 'SUCCESS' where run_id = {0}'''.format(run_id))

    config_connection.close()
    stage_connection.close()

if __name__ == '__main__':
    main(sys.argv[1:])