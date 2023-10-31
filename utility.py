
import pyodbc
import polars as pl
from datetime import datetime
import os, sys
import math
import sqlalchemy  as sa

def get_db_connection(server, database):
   return pyodbc.connect('Driver={ODBC Driver 18 for SQL Server}' + \
                          ';SERVER=' + server + \
                          ';DATABASE=' + database + \
                          ';TrustServerCertificate=Yes;Trusted_Connection=Yes;MultipleActiveResultSets=True' )  

def get_db_connection_string(server, database):
    connectionString = f"DRIVER={{ODBC Driver 18 for SQL Server}};SERVER='{server}';DATABASE='{database}';TrustServerCertificate=Yes;Trusted_Connection=Yes;MultipleActiveResultSets=True"
    return connectionString

def get_sql_alchemy_engine(server, database, driver):

    url = f"mssql+pyodbc://{server}/{database}?TrustServerCertificate=yes&driver={driver}"
    return sa.create_engine(url)

def get_sql_alchemy_url(server, database, driver):

    url = f"mssql+pyodbc://{server}/{database}?TrustServerCertificate=yes&driver={driver}"
    return url


def get_data_from_database(db_connection, schema_table_name):
    query = 'SELECT * FROM ' + schema_table_name
    df = pl.read_database(query, db_connection)

    return df

# def get_latest_build_data(db_connection, dataset):
    
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
    
    now = datetime.now() # current date and time
    current_datestamp = now.strftime("%Y%m%d")

    # Check whether the specified target path exists or not
    isExist = os.path.exists(f'{target_dir}/')
    if not isExist:
        os.makedirs(f'{target_dir}')
        # raise NotADirectoryError
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
        output_df.write_csv(f'{target_dir}/{filename}.csv', has_header=True, batch_size=5000)
        
    else:
        df_row_count = len(output_df)
        iteration = math.ceil(df_size_in_mb / max_file_size_mb)
        number_of_rows_in_chunk = int(df_row_count / iteration)

        print('row count: ' + str(df_row_count))
        print('iteration: ' + str(iteration))
        print('number of rows in chunk: ' + str(number_of_rows_in_chunk))

        if iteration == 1:
            output_df.to_csv(f'{target_dir}/{filename}.csv', header=True, chunksize=5000, index=False)
        else:
            for i, start in enumerate(range(0, df_row_count, number_of_rows_in_chunk)):
                output_df[start:start+number_of_rows_in_chunk].to_csv(f'{target_dir}/{i}_{filename}_{i}.csv', chunksize=5000)

    return

def compute_dataset_diff_data(earlierExtractFile, latestExtractFile, groupByCol, groupOnCol):

    df2 = None

    if not os.path.exists(latestExtractFile):
        raise FileNotFoundError("The latest extract file was not found")
    else:
        df2 = pl.read_csv(latestExtractFile, try_parse_dates=False)

    if earlierExtractFile is None:
        earlierPath = ''      
    else:
        earlierPath = latestExtractFile  
    if not os.path.exists(earlierPath):
        if df2.is_empty():
            raise ValueError("No data for latest extract was found")
        else:
            df1 = df2.clear(n=0)
    else:
        df1 = pl.read_csv(earlierExtractFile, try_parse_dates=False)

    print("Latest dataframe contains " + df2.select(pl.count()).collect() + " rows")
    print("Earlier dataframe contains " + df1.select(pl.count()).collect() + " rows")

    concat_df = pl.concat(
        [
            # df1.with_columns(pl.col("admission_method").cast(str)),
            # df2.with_columns(pl.col("admission_method").cast(str)),
            df1.with_columns(),
            df2.with_columns()
        ],
        how="vertical",
    )

    dedup_df = concat_df.unique(keep='none')
    dedup_df.group_by(groupByCol).agg(pl.struct([groupOnCol]).n_unique().alias('result'))
    print("Dedup dataframe contains " + dedup_df.select(pl.count()).collect() + " rows")

    # produce diff manifest 
    new_patient_records = dedup_df.group_by(f'{groupByCol}').agg(pl.struct([f'{groupOnCol}']).n_unique().alias('result')).join(df1, on=f'{groupByCol}', how='anti').select([f'{groupByCol}','result']).with_columns (patient = pl.lit ('new'),finding = pl.lit('new'))
    additional_records =  dedup_df.group_by(f'{groupByCol}').agg(pl.struct([f'{groupOnCol}']).n_unique().alias('result')).join(df1, on=f'{groupByCol}', how='inner').select([f'{groupByCol}','result']).with_columns (patient = pl.lit ('existing'),finding = pl.lit('new'))

    manifest_df = pl.concat([new_patient_records, additional_records], rechunk=True)

    return dedup_df, manifest_df
    
