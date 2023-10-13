import pyodbc
import pandas as pd
import polars as pl
import base64
import math 
from datetime import datetime

import os, sys


def get_db_connection(server, database):
   return pyodbc.connect('Driver={ODBC Driver 18 for SQL Server}' + \
                          ';SERVER=' + server + \
                          ';DATABASE=' + database + \
                          ';TrustServerCertificate=Yes;Trusted_Connection=Yes;' )   

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


def save_to_delimited_file(dataframe, target_dir, filename, filename_prefix=None, columns_list = None, max_file_size_mb = None, delimiter = ",", timestamp_file=False):
    
    # filepath = pathlib.Path(target_dir + filename)
    now = datetime.now() # current date and time
    current_datestamp = now.strftime("%Y%m%d")
    # print(current_datestamp)

    # Check whether the specified target path exists or not
    isExist = os.path.exists(f'{target_dir}/')
    if not isExist:
        raise NotADirectoryError
    else:
        # Check whether a subdir with todays timestamp already exists or not. If not create it
        isExist = os.path.exists(f'{target_dir}/{current_datestamp}')
        if not isExist:
            os.makedirs(f'{target_dir}/{current_datestamp}')

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
        output_df.write_csv(f'{target_dir}/{current_datestamp}/{filename}.csv', has_header=True, batch_size=5000)
        
    else:
        df_row_count = len(output_df)
        iteration = math.ceil(df_size_in_mb / max_file_size_mb)
        number_of_rows_in_chunk = int(df_row_count / iteration)

        print('row count: ' + str(df_row_count))
        print('iteration: ' + str(iteration))
        print('number of rows in chunk: ' + str(number_of_rows_in_chunk))

        if iteration == 1:
            output_df.to_csv(f'{target_dir}/{current_datestamp}/{filename}.csv', header=True, chunksize=5000, index=False)
        else:
            for i, start in enumerate(range(0, df_row_count, number_of_rows_in_chunk)):
                output_df[start:start+number_of_rows_in_chunk].to_csv(f'{target_dir}/{current_datestamp}/{i}_{filename}_{i}.csv', chunksize=5000)

    return

def generate_dataset_extract(connection, schema_table_name):
    df = get_data_from_database(connection, schema_table_name)

    return df

def read_in_data(file1, file2):
    df1 = pl.read_csv(file1, try_parse_dates=False)
    print(df1.shape)
    # print(df1.head(5))
    df2 = pl.read_csv(file2, try_parse_dates=False)
    print(df2.shape)
    # print(df2.head(5))

    result_df = pl.concat(
        [
            df1,
            df2,
        ],
        how="vertical",
    )

    print(result_df.shape)

    # Reading dataset
    dep_dup_df = result_df.unique(keep='first')
    print(dep_dup_df.shape)

    print(dep_dup_df.head(5))

    # df1.join(df2, on='source_spell_id', suffix='_df2').filter(pl.any([pl.col(x)!=pl.col(f"{x}_df2") for x in df1.columns if x!='source_spell_id']))
    # df2 == df1

if __name__ == '__main__':
    server = 'oxnetdwp04'
    database = 'cig_101_test'

    # create connection
    conn = get_db_connection(server , database )

    # print('=== output 0 : create csv for inpat spells ====')
    # save_to_delimited_file(generate_dataset_extract(conn, 'data.inpat_spells'), './generated/compare', '{{timestamp}}._inpat_spells')

    print('=== output 1: compare dataframes ====')
    read_in_data('./generated/compare/20231013/202310131259._inpat_spells.csv', './generated/compare/20231013/202310131528._inpat_spells.csv')
