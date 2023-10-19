import pyodbc
import polars as pl
from datetime import datetime
import base64
import pdfkit

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

if __name__ == '__main__':
    server = 'oxnetdwp04'
    database = 'data_products__oxpos_cohort_3'

    conn = get_db_connection(server , database )
    cursor = conn.cursor()

    # sql = f'insert into oxpos_live_patients_redacted.fmi_report_pdf(oxpos_id, file_name, report_date, test_id, pdf_data) values (?,?,?,?,?)'
    # params = (999, 'testfile.pdf', '2023-05-23', 'ORD-122-312', None)
    # cursor.execute(sql, params)

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

    df = get_data_from_database(conn, "oxpos_live_patients_redacted.fmi_report_pdf")
    print(df)


