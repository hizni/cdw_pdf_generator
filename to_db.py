import pyodbc
import polars as pl
from datetime import datetime
import base64
import pdfkit
import utility
import sqlalchemy  as sa
import os

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
def load_FMI_to_database(conn):

    cursor = conn.cursor()
    f = open("/Users/hizni/Projects/cdw_extracts/FMI/ORD-1714641-01.pdf", mode="rb")
    data = f.read()
    f.close()
    sql = f'insert into oxpos_live_patients_redacted.fmi_report_pdf(oxpos_id, file_name, report_date, test_id, pdf_data) values (?,?,?,?,?)'
    params = (1, 'ORD-1714641-01.pdf', '2023-10-06', 'ORD-1714641-01', data)
    cursor.execute(sql, params)
    conn.commit()

    f = open("/Users/hizni/Projects/cdw_extracts/FMI/ORD-1714853-01.pdf", mode="rb")
    data = f.read()
    f.close()
    sql = f'insert into oxpos_live_patients_redacted.fmi_report_pdf(oxpos_id, file_name, report_date, test_id, pdf_data) values (?,?,?,?,?)'
    params = (7, 'ORD-1714853-01.pdf', '2023-10-09', 'ORD-1714853-01', data)
    cursor.execute(sql, params)
    conn.commit()

    f = open("/Users/hizni/Projects/cdw_extracts/FMI/ORD-1714855-01.pdf", mode="rb")
    data = f.read()
    f.close()
    sql = f'insert into oxpos_live_patients_redacted.fmi_report_pdf(oxpos_id, file_name, report_date, test_id, pdf_data) values (?,?,?,?,?)'
    params = (6, 'ORD-1714855-01.pdf', '2023-10-09', 'ORD-1714855-01', data)
    cursor.execute(sql, params)
    conn.commit()  

    f = open("/Users/hizni/Projects/cdw_extracts/FMI/ORD-1714856-01.pdf", mode="rb")
    data = f.read()
    f.close()
    sql = f'insert into oxpos_live_patients_redacted.fmi_report_pdf(oxpos_id, file_name, report_date, test_id, pdf_data) values (?,?,?,?,?)'
    params = (8, 'ORD-1714856-01.pdf', '2023-10-06', 'ORD-1714856-01', data)
    cursor.execute(sql, params)
    conn.commit()  

    f = open("/Users/hizni/Projects/cdw_extracts/FMI/ORD-1714952-01.pdf", mode="rb")
    data = f.read()
    f.close()
    sql = f'insert into oxpos_live_patients_redacted.fmi_report_pdf(oxpos_id, file_name, report_date, test_id, pdf_data) values (?,?,?,?,?)'
    params = (23, 'ORD-1714952-01.pdf', '2023-10-06', 'ORD-1714952-01', data)
    cursor.execute(sql, params)
    conn.commit()  

    

    f = open("/Users/hizni/Projects/cdw_extracts/FMI/ORD-1725895-01.pdf", mode="rb")
    data = f.read()
    f.close()
    sql = f'insert into oxpos_live_patients_redacted.fmi_report_pdf(oxpos_id, file_name, report_date, test_id, pdf_data) values (?,?,?,?,?)'
    params = (3, 'ORD-1725895-01.pdf', '2023-10-09', 'ORD-1725895-01', data)
    cursor.execute(sql, params)
    conn.commit()  


if __name__ == '__main__':
    server = 'oxnetdwp04'
    database = 'cig_101_test'

    # conn = get_db_connection(server , database )
    # cursor = conn.cursor()

    # e = create_engine('mssql+pyodbc://' + server + ':1433/' + database + '?driver={ODBC+Driver+17+for+SQL+Server}?TrustedConnection=yes')
    # with e.begin() as conn:
        
        # load_FMI_to_database(conn)

        # # check database that has been loaded into oxpos_live_patients_redacted.fmi_report_pdf 
        # df = get_data_from_database(conn, "oxpos_live_patients_redacted.fmi_report_pdf")
        # print(df)

    data = pl.read_csv('./diff_exported/emergency_investigations/20231023161750_diff._emergency_investigations.csv')
    print(data.head())    # creating column list for insertion 

    
    conn_str = utility.get_db_connection_string('oxnetdwp04', 'cig_101_test')
    print(conn_str)

    # engine = create_engine("mssql+pyodbc://<username>:<password>@<dsnname>")
    # # Driver={ODBC Driver 17 for SQL Server};Server=myServerAddress;Database=myDataBase;Trusted_Connection=yes;
    # data.write_database('emergency_investigations', f'mssql+pyodbc://{server}:1433/{database}?driver={{ODBC+Driver+18+for+SQL+Server}}?TrustedConnection=yes', if_exists='replace', engine='sqlalchemy')
    #     # data.to_pandas().to_sql('emergency_investigations', e, schema='diff', if_exists='replace', chunksize=5000, index=False)

    print(sa.__version__)
    server = "oxnetdwp04"
    database = "cig_101_test"
    driver = "ODBC+Driver+18+for+SQL+Server"
    url = f"mssql+pyodbc://{server}/{database}?TrustServerCertificate=yes&driver={driver}"
    
    engine = sa.create_engine(url)
    with engine.begin() as conn:
        data.write_database(table_name='diff.emergency_investigations',connection=url, if_exists='replace')
        # stmt = sa.sql.text("SELECT top 10 * from build.run_history")
        # conn.execute(stmt)
    print("Write to database successful.")





