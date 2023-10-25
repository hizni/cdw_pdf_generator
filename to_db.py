import pyodbc
import polars as pl
from datetime import datetime
import base64
import pdfkit
import utility

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
    database = 'data_products__oxpos_cohort_3'

    conn = get_db_connection(server , database )
    cursor = conn.cursor()
    # load_FMI_to_database(conn)

    # # check database that has been loaded into oxpos_live_patients_redacted.fmi_report_pdf 
    # df = get_data_from_database(conn, "oxpos_live_patients_redacted.fmi_report_pdf")
    # print(df)

    data = pl.read_csv('./diff_exported/emergency_investigations/20231023161750_diff._emergency_investigations.csv')

    print(data.head())
    # creating column list for insertion 
    cols = data.columns
    print(cols)
    
    conn_str = utility.get_db_connection_string('oxnetdwp04', 'cig_101_test')
    print(conn_str)


    data.write_database('diff.emergency_investigations', conn_str, if_exists='replace', engine="adbc")

    # # Insert DataFrame records one by one. 
    # for i,row in data.iterrows():
    #     sql = f'INSERT INTO diff.emergency_investigations ({cols}) VALUES (%s,*(len({row})-1) + %s)'
    #     cursor.execute(sql, tuple(row)) 
    #     # the connection is not autocommitted by default, so we must commit to save our # changes 
    #     conn.commit()


