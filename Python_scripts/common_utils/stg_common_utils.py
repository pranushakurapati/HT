import datetime
import pandas as pd
import os
import csv

from snowflake.sqlalchemy import URL
from sqlalchemy import create_engine
# from fsplit.filesplit import FileSplit


def create_connection(engine_name):
    engine = create_engine(URL(user=engine_name['user'],
                               password=engine_name['password'],
                               account=engine_name['account'],
                               warehouse=engine_name['warehouse'],
                               database=engine_name['database'],
                               schema=engine_name['schema'],
                               role=engine_name['role'],
                               numpy=engine_name['numpy']))
    return engine.connect()


def is_file_loaded(process_id, process_name, file, connection, load_type):
    get_run_id = connection.execute(''' select run_id from etl_process where process_id = {0}
                                          and process_name = '{1}' and
                                          file_name= '{2}' and load_type='{3}' '''.format(process_id,
                                                                                          process_name,
                                                                                          file, load_type))
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
    # delete records from table with that run_id
    delete_records = connection.execute(''' delete from {0} where run_id = {1}'''.format(
        table_name, run_id))
    deleted_records = delete_records.fetchall()[0][0]
    print('Records deleted from: ' + table_name + " Count: " + deleted_records.astype(str))


def transform_file(fname, index1, index2, compareString, ignore_index, ignore_top=0):
    file = open(fname, "r")
    strings = []
    each_string = ''
    nol = len(file.readlines())
    file = open(fname, "r")
    for index, line in enumerate(file):
        if ((index > ignore_top) & (index != nol - 1)):  # to skip 1st and last line of file
            if ((line[index1:index2] == compareString) & (index != 1)):  # check if string starts with 1
                if (each_string != ''):
                    strings.append(each_string)  # push to list as a new record
                each_string = line.replace("\n", "")[ignore_index:]  # initialize a new record
            else:
                each_string += line.replace("\n", "")[ignore_index:]  # skip 1st letter and append to same record

    strings.append(each_string)
    return strings


def fetch_file_details(file):
    fname = file.split("_")[0] + "_" + file.split("_")[1]
    fdate = datetime.datetime.strptime(file.split("_")[2].split(".")[0], '%Y%m%d')
    fdate = datetime.datetime.strftime(fdate, '%Y-%m-%d')
    ftype = file.split(".")[-1].upper()
    return fname, fdate, ftype


def create_staged_data(process_id, process_name, filedata, filename):
    staged_data = pd.DataFrame(data=filedata, columns=["data"])
    staged_data["processid"] = process_id
    staged_data["custodian_name"] = process_name
    staged_data["run_id"] = None
    staged_data["createdate"] = datetime.datetime.now()
    staged_data["file_name"] = filename
    staged_data = staged_data[['processid', 'custodian_name', 'run_id', "data", "createdate", "file_name"]]

    return staged_data


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


def update_run_id(connection, table_name, run_id, file):
    connection.execute(''' Update {0} set run_id = {1} where run_id is null and file_name='{2}' '''.format(table_name, run_id, file))


def update_etl_process(connection, *args):
    rec = args[0]
    connection.execute(''' update etl_process set MODIFIED_DATE = '{10}',RECORDS_LOADED = {7},
    TOTAL_RECORD_COUNT = {11}, TGT_TABLE = '{4}', RECORDS_REJECTED = {12}, STATUS='{8}', FILE_DATE='{9}',
    FILE_TYPE='{3}', LOAD_TYPE='{5}', SRC_FILE_TABLE_NAME = '{2}' where process_id = {0}
    and process_name = '{1}' and file_name= '{6}' and run_id = {13} '''.format(rec[0], rec[1], rec[2], rec[3], rec[4],
                                                                               rec[5], rec[6], rec[7], rec[8], rec[9],
                                                                               rec[10], rec[11], rec[12], rec[13]))


def fetch_col_specifications(pid, pname, fname_pattern, connection):

    col_specs = pd.read_sql_query(''' select col_specs from columnspecification where
                                          process_id = {0} and process_name = '{1}' and
                                          file_name_pattern= '{2}' '''.format(pid, pname, fname_pattern),
                                  connection)

    header_str = col_specs['col_specs'][0].split(';')
    index = []
    for item in header_str:
        index.append((item.split(',')[0], item.split(',')[1]))

    result = list(tuple((int(x[0]), int(x[1])) for x in index))

    column_names = pd.read_sql_query(''' select column_names from columnspecification
                                             where process_id = {0} and process_name = '{1}' and
                                             file_name_pattern= '{2}' '''.format(pid, pname, fname_pattern),
                                     connection)
    column_names_list = column_names['column_names'][0].split(',')
    column_names_list = [x.upper() for x in column_names_list]

    return result, column_names_list


def put_and_copy_file(folder_path, data_frame, connection, table_name, stage_name, file_name):
    pd.read_sql_query(''' remove @{0}'''.format(stage_name), connection)

    # file_size = os.path.getsize(folder_path+ '\Test_CSV_file_to_stage.csv')
    # if file_size > 200000000:  # Check if file size is greater than 200MB
    # fs = FileSplit(file=folder_path+ '\Test_CSV_file_to_stage.csv', splitsize=50000000,
    # output_dir=folder_path)  # Split each file into 50MB
    # fs.split()
    # os.remove(folder_path+ '\Test_CSV_file_to_stage.csv')  # Need to check if we have to delete the file

    pd.read_sql_query("put file://" + folder_path + "\\" + file_name + " @{0}".format(stage_name),
                      connection)

    pd.read_sql_query("copy into {0} from @{1}/{2}.gz force = true on_error = 'continue'".format(table_name, stage_name, file_name), connection)

    os.remove(folder_path + "\\" + file_name)


def stage_to_source(process_id, process_name, file_name_pattern, stage_table, source_table, file,
                    col_specs, col_names, stg_connection, conf_connection):
    print("Transferring: ", str(file_name_pattern), "id", process_id, "name", str(process_name))
    run_id = pd.read_sql_query(''' select run_id from etl_process where file_name = '{0}' and load_type = 'FILE TO STG' '''.format(file), conf_connection)
    run_id = run_id['run_id'][0]

    data_from_stage = pd.read_sql_query(''' select data from {0} where run_id = {1}'''.format(stage_table,
                                                                                              run_id), stg_connection)
    print("data_from_stage ----- ", data_from_stage.head())

    working_folder = pd.read_sql_query('''select local_folder from filelocation where process_id={0}'''.format(process_id),
                                       conf_connection)['local_folder'][0]
    working_folder = working_folder.strip('"')

    if not os.path.exists(working_folder + '\csv'):
        os.mkdir(working_folder + '\csv')
    working_folder = working_folder + '\csv'

    file_name = 'Test_{0}.csv'.format(os.path.splitext(file)[0])

    data_from_stage.to_csv(working_folder + '\\' + file_name, sep='{', header=False, index=False, na_rep='',
                           quoting=csv.QUOTE_NONE)
    data = pd.read_fwf(working_folder + '\\' + file_name, colspecs=col_specs, header=None, names=col_names, dtype=str,
                       na_values=' ', keep_default_na=False, encoding='utf-8')
    data['file_name'] = file

    print("data_from_stage ----- ", data.head())
    os.remove(working_folder + "\\" + file_name)

    return working_folder, data


def fetch_col_specifications_PRSH(process_id, process_name, fname, config_connection):
    print(fname)
    master_col_specs = pd.read_sql_query(''' select col_specs from columnspecification where
                                          process_id = {0} and process_name = '{1}' and
                                          file_name_pattern= '{2}' '''.format(process_id, process_name, fname),
                                         config_connection)
    master_column_names = pd.read_sql_query(''' select column_names from columnspecification
                                                 where process_id = {0} and process_name = '{1}' and
                                                 file_name_pattern= '{2}' '''.format(process_id, process_name, fname),
                                            config_connection)

    col_specs_list = master_col_specs['col_specs'][0].split('|')
    column_names_list = master_column_names['column_names'][0].split('|')
    final_specs_list = []
    final_column_names_list = []
    for col_specs, column_names in zip(col_specs_list, column_names_list):

        header_str = col_specs.split(';')
        index = []

        for item in header_str:
            if(len(item) > 0):
                index.append((item.split(',')[0], item.split(',')[1]))

        result = list(tuple((int(x[0]), int(x[1])) for x in index))

        result2 = column_names.split(',')
        result2 = [x.upper() for x in result2]
        final_specs_list.append(result)
        final_column_names_list.append(result2)

    return final_specs_list, final_column_names_list


def stage_to_source_PRSH(process_id, process_name, fname, stg_table_name, table_name, file, column_specs_map,
                         column_name_map, stage_connection, config_connection):
    print("Transferring: ", str(fname), "id", process_id, "name", str(process_name))

    run_id = pd.read_sql_query(''' select run_id from etl_process where file_name = '{0}' and load_type = 'FILE TO STG' '''.format(file), config_connection)
    run_id = run_id['run_id'][0]

    data_from_stage = pd.read_sql_query(''' select data from {0} where run_id = {1}'''.format(stg_table_name, run_id),
                                        stage_connection)
    print("data_from_stage ----- ", data_from_stage.head())

    working_folder = pd.read_sql_query('''select local_folder from filelocation where process_id={0}'''
                                       .format(process_id), config_connection)['local_folder'][0]
    working_folder = working_folder.strip('"')

    if not os.path.exists(working_folder + '\csv'):
        os.mkdir(working_folder + '\csv')
    working_folder = working_folder + '\csv'
    if fname == 'PRSH_ACCT' or fname == 'PRSH_ACCF':
        recordTypes = ['A', 'B', 'C', 'D', 'E', 'W', 'F']
    elif fname == 'PRSH_GTDE' or fname == 'PRSH_GSDE':
        recordTypes = ['A', 'B']
    elif fname == 'PRSH_ISCA':
        recordTypes = ['A', 'C', 'D', 'E', 'F', 'G']

    column_master_list = []
    for recordType in recordTypes:
        for x in column_name_map[recordType]:
            if len(x) > 0:
                column_master_list.append(x.strip())

    column_master_list = list(dict.fromkeys(column_master_list))
    data = pd.DataFrame(columns=column_master_list)
    file_name = 'Test_{0}.csv'.format(file.split('.')[0])
    for recordType in recordTypes:
        print(recordType)
        if fname == 'PRSH_ISCA':
            data_from_stage[data_from_stage['data'].apply(lambda x: x[0:1] == recordType)].to_csv(working_folder + '\\' + file_name,
                                                                                                  sep='`',
                                                                                                  header=False,
                                                                                                  index=False, na_rep='',
                                                                                                  quoting=csv.QUOTE_NONE)
        else:
            data_from_stage[data_from_stage['data'].apply(lambda x: x[2:3] == recordType)].to_csv(working_folder + '\\' + file_name,
                                                                                                  sep='`',
                                                                                                  header=False,
                                                                                                  index=False,
                                                                                                  na_rep='',
                                                                                                  quoting=csv.QUOTE_NONE)

        data_tmp = pd.read_fwf(working_folder + '\\' + file_name, colspecs=column_specs_map[recordType], header=None,
                               names=column_name_map[recordType], dtype=str, na_values=' ', keep_default_na=False,
                               encoding='utf-8')
        data = data.append(data_tmp, ignore_index=True, sort=False)
        print(data.shape)
    data['file_name'] = file

    print("data_from_stage ----- ", data.head())
    os.remove(working_folder + "\\" + file_name)

    return working_folder, data
