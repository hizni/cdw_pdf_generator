import pyodbc
import pandas as pd
import polars as pl
import base64
import math 
from datetime import datetime
import yaml

import os, sys


def get_db_connection(server, database):
   return pyodbc.connect('Driver={ODBC Driver 18 for SQL Server}' + \
                          ';SERVER=' + server + \
                          ';DATABASE=' + database + \
                          ';TrustServerCertificate=Yes;Trusted_Connection=Yes;MultipleActiveResultSets=True' )   

def get_data_from_database(db_connection, schema_table_name):
    query = 'SELECT * FROM ' + schema_table_name
    # df = pd.read_sql(query, db_connection)
    df = pl.read_database(query, db_connection)

    return df

def replace_templated_string(inputString):
    now = datetime.now() # current date and time

    # datestamp
    current_datestamp = now.strftime("%Y%m%d")
    inputString = inputString.replace("{{datestamp}}", current_datestamp)

    # timestamp
    current_timestamp = now.strftime("%Y%m%d%H%M")
    inputString = inputString.replace("{{timestamp}}", current_timestamp)    

    # add other templates to this function as needed

    return inputString


def save_to_delimited_file(dataframe, 
                           target_dir, 
                           filename, 
                           filename_prefix=None, 
                           columns_list = None, 
                           max_file_size_mb = None, 
                           delimiter = ",", 
                           sub_dir_by_date=False):
    
    # filepath = pathlib.Path(target_dir + filename)
    now = datetime.now() # current date and time
    current_datestamp = now.strftime("%Y%m%d")
    # print(current_datestamp)

    
    # Check whether the specified target path exists or not
    isExist = os.path.exists(f'{target_dir}/')
    if not isExist:
        raise NotADirectoryError
    else:
        if (sub_dir_by_date == True):
            # Check whether a subdir with todays timestamp already exists or not. If not create it
            isExist = os.path.exists(f'{target_dir}/{current_datestamp}')
            if not isExist:
                os.makedirs(f'{target_dir}/{current_datestamp}')

            target_dir = f'{target_dir}/{current_datestamp}'

    # get subset of original dataframe based on list of column names passed. 
    # if no column names are passed just process original dataframe
    if (columns_list != None):
        # if (set(list(columns_list)).issubset(set(dataframe.columns))):
        if (all(item in list(dataframe.columns) for item in list(columns_list))):
            # check if list of passed column names exist in columns in dataframe. If they do not then we have an issue
            output_df = dataframe[columns_list]
        else:
            raise ValueError('Columns in columnlist do not exist in dataframe ')
    else:
        output_df = dataframe

    filename = replace_templated_string(filename)

    # getting info to help make decisions
    df_row_count = len(output_df)
    print('row count: ' + str(df_row_count))
    if (df_row_count == 0):
        print(f"dataframe for {filename} is empty. No file to produce here!")
        return 0
    
    df_size_in_bytes = sys.getsizeof(output_df)
    df_size_in_mb = (df_size_in_bytes / 1000000)
    print('original dataframe size in MB: ' + str(df_size_in_mb))

    # if file size is not an issue, just output to csv
    if(max_file_size_mb == None):
        # output_df.write_csv(f'{target_dir}/{current_datestamp}/{filename}.csv', has_header=True, batch_size=5000)
        output_df.write_csv(f'{target_dir}/{filename}.csv', has_header=True, batch_size=5000)
        
    else:
        df_row_count = len(output_df)
        iteration = math.ceil(df_size_in_mb / max_file_size_mb)
        number_of_rows_in_chunk = int(df_row_count / iteration)

        print('row count: ' + str(df_row_count))
        print('iteration: ' + str(iteration))
        print('number of rows in chunk: ' + str(number_of_rows_in_chunk))

        if iteration == 1:
            # output_df.to_csv(f'{target_dir}/{current_datestamp}/{filename}.csv', header=True, chunksize=5000, index=False)
            output_df.to_csv(f'{target_dir}/{filename}.csv', header=True, chunksize=5000, index=False)
        else:
            for i, start in enumerate(range(0, df_row_count, number_of_rows_in_chunk)):
                # output_df[start:start+number_of_rows_in_chunk].to_csv(f'{target_dir}/{current_datestamp}/{i}_{filename}_{i}.csv', chunksize=5000)
                output_df[start:start+number_of_rows_in_chunk].to_csv(f'{target_dir}/{i}_{filename}_{i}.csv', chunksize=5000)

    return

def generate_dataset_extract(connection, schema_table_name):
    df = get_data_from_database(connection, schema_table_name)

    return df

def populate_dataset(connection, schema_table_name):
    cursor = connection.cursor()
    sql = 'exec data.load_data_' + schema_table_name
    cursor.execute(sql)


def read_in_csv_data(file1):
    df = pl.read_csv(file1, try_parse_dates=False)
    print("first df shape")
    print(df.shape)


def read_in_data(file1, file2):

    if file2 is not None:
        df2 = pl.read_csv(file2, try_parse_dates=False)
        print("second df shape")
        print(df2.shape)

    # print(df2.head(5))
    if file1 is not None:
        df1 = pl.read_csv(file1, try_parse_dates=False)
        print("first df shape")
        print(df1.shape)
        # print(df1.head(5))
    else:
        df1 = df2.clear(n=0)



    # concat dataframes 
    result_df = pl.concat(
        [
            # df1.with_columns(pl.col("admission_method").cast(str)),
            # df2.with_columns(pl.col("admission_method").cast(str)),
            df1.with_columns(),
            df2.with_columns()
        ],
        how="vertical",
    )

    print("joined df")
    print(result_df.shape)

    # Remove duplicates from concatenated. only shows rows being submitted in second file
    print("diffed df")
    dep_dup_df = result_df.unique(keep='none')
    print(dep_dup_df.shape)


    dep_dup_df.group_by("salted_master_patient_id").agg(pl.struct(['source_spell_id']).n_unique().alias('result'))
    print(dep_dup_df)


    # print(dep_dup_df.group_by("salted_master_patient_id").agg(pl.struct(['source_spell_id']).n_unique().alias('result')))

    # diff manifest 
    new_patient_records = dep_dup_df.group_by("salted_master_patient_id").agg(pl.struct(['source_spell_id']).n_unique().alias('result')).join(df1, on="salted_master_patient_id", how='anti').select(['salted_master_patient_id','result']).with_columns (patient = pl.lit ('new'),finding = pl.lit('new'))
    additional_records =  dep_dup_df.group_by("salted_master_patient_id").agg(pl.struct(['source_spell_id']).n_unique().alias('result')).join(df1, on="salted_master_patient_id", how='inner').select(['salted_master_patient_id','result']).with_columns (patient = pl.lit ('existing'),finding = pl.lit('new'))
    
    manifest_df = pl.concat([new_patient_records, additional_records], rechunk=True)
    print(manifest_df)
    
    return dep_dup_df, manifest_df

    # print(dep_dup_df.head(5))

    # df1.join(df2, on='source_spell_id', suffix='_df2').filter(pl.any([pl.col(x)!=pl.col(f"{x}_df2") for x in df1.columns if x!='source_spell_id']))
    # df2 == df1

if __name__ == '__main__':
    # server = 'oxnetdwp04'
    # database = 'cig_101_test'

    # # create connection
    # conn = get_db_connection(server , database )

    # # print('=== output -1 : populate inpat spells dataset ====')
    # populate_dataset(conn, 'inpat_spells')

    # # print('=== output 0 : create csv for inpat spells ====')
    # save_to_delimited_file(generate_dataset_extract(conn, 'data.inpat_spells'), './generated/inpat_spells', '{{timestamp}}._inpat_spells')

    # print('=== output 1: compare dataframes ====')
    # diff_data, manifest = read_in_data('./generated/compare/inpat_spells/202310131259._inpat_spells.csv', './generated/compare/inpat_spells/202310131528._inpat_spells.csv')

    # print("=== final output ===")
    # print("diff inpat data")
    # print(diff_data)

    # print("manifest inpat data")
    # print(manifest)

    # save_to_delimited_file( diff_data, './generated/diff', '{{datestamp}}._inpat_spells')
    # save_to_delimited_file( manifest, './generated/diff', '{{datestamp}}._manifest_inpat_spells')

    server = 'oxnetdwp04'
    database = 'cig_101_test'

    # create connection
    conn = get_db_connection(server , database )
    cursor = conn.cursor()
    
    # # iterating over list of datasets in yaml file to generate output to CSVs
    # with open("dp_cig_101.yaml", "r") as stream:
    #     try:
    #         # getting data from yaml config
    #         config = yaml.safe_load(stream)
    #         print(config['name'])
    #         data_from = config['data_from'] 
    #         data_till = config['data_till']
    #         datasets = config['datasets']
    #         # generating data product
    #         print("Generating data product with following parameters")
    #         for d in datasets:
    #             print("Loading..." + d)

    #             # # print('=== output 0 : populate inpat spells dataset ====')
    #             sql = f'exec data.load_data_{d} ?,?'
    #             params = (data_from, data_till)
    #             cursor.execute(sql, params)
    #             conn.commit()
    #             # cursor.cancel()
    #             # # print('=== output 0 : save dataset for latest run ====')
    #             save_to_delimited_file(generate_dataset_extract(conn, f'data.{d}'), f'./generated/{d}' , f'{{{{timestamp}}}}._{d}', sub_dir_by_date=False)
        # except yaml.YAMLError as exc:
        #     print(exc)

    # comparing the current and last exported CSV to perform diff. 
    # diff_data, manifest = read_in_data(None, './generated/inpat_spells/202310231109._inpat_spells.csv')

    # print(diff_data)

    # print(manifest)
    # diff_data, manifest = read_in_data('./generated/compare/inpat_spells/202310231109._inpat_spells.csv', './generated/compare/inpat_spells/202310202252._inpat_spells.csv')

    df1 = pl.read_csv('./diff_exported/bmi_measurements/20231024143202_manifest._bmi_measurements.csv', try_parse_dates=False)

    print("manifest bmi measurement")
    print(df1.head())

    df2 = pl.read_csv('./diff_exported/inpat_spells/20231023161711_manifest._inpat_spells.csv', try_parse_dates=False)
    print("manifest inpat spells")
    print(df2.head())

    df3 = pl.read_csv('./diff_exported/emergency_investigations/20231023161750_manifest._emergency_investigations.csv', try_parse_dates=False)
    print("manifest emergency investigations spells")
    print(df3.head())


    concat_df = pl.concat(
        [
            # df1.with_columns(pl.col("admission_method").cast(str)),
            # df2.with_columns(pl.col("admission_method").cast(str)),
            df1.with_columns(),
            df2.with_columns(),
            df3.with_columns()
        ],
        how="vertical",
    )

    print(concat_df.head(10))

    dedup_df = concat_df.unique(keep='none')
    print(dedup_df.head(10))

    subset_df = concat_df.select(['salted_master_patient_id', 'patient', 'finding' ])
    qq = subset_df.unique(keep='first')
    print(qq.head(10))
    

                
                


