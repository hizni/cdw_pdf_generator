import pyodbc
# import pymssql
import subprocess
import getpass
import argparse


def has_kerberos_ticket():
    return True if subprocess.call(['klist', '-v']) == 0 else False

def create_kerberos_ticket(user_name, domain_name, user_password):

    ssh = subprocess.Popen(["kinit", f'{user_name}@{domain_name}'],
                        stdin =subprocess.PIPE,
                        stdout=subprocess.PIPE,
                        stderr=subprocess.PIPE,
                        universal_newlines=True,
                        bufsize=0)

    ssh.stdin.write(f"{user_password}\n")
    ssh.stdin.write("exit\n")
    ssh.stdin.close()

def list_kerberos_ticket():
    ssh = subprocess.Popen(["klist"],
                        stdin =subprocess.PIPE,
                        stdout=subprocess.PIPE,
                        stderr=subprocess.PIPE,
                        universal_newlines=True,
                        bufsize=0)
    ssh.stdin.close()

def sqlserver_connection(server, database):
    # Trusted_Connection allows connection to be made to server with existing Kerberos ticket
    return pyodbc.connect('Driver={ODBC Driver 18 for SQL Server}' + \
                          ';SERVER=' + server + \
                          ';DATABASE=' + database + \
                          ';TrustServerCertificate=Yes;Trusted_Connection=Yes;' )

def sqlserver_freetds_connection(server, username, password):
    return pyodbc.connect(f"DSN={0};UID={1};PWD={2}".format(server,username,password))


# pyodbc.connect(driver=driver, server=server, database=database, trusted_connection='yes')



if __name__ == '__main__':

    # server = 'oxnetdwp02.oxnet.nhs.uk'
    # database = 'data_products__oxpos_cohort_3'

    server = 'oxnetdwp01.oxnet.nhs.uk'
    database = 'BADGERNET'

    # print(has_kerberos_ticket())
# # -- SQL server authentication credentials
#     # user_name = 'py_login'
#     # user_password = 'H3bQZf!UmLsG'

#     # Create database connection string - using SQL authentication
#     # conn = pymssql.connect(server='oxnetdwp02.oxnet.nhs.uk', user='py_login', password='H3bQZf!UmLsG', database='data_product__oxpos_cohort_3')  
#     # cursor = conn.cursor()
#     # cursor.execute("""SELECT 1;""")
# # -- MY CODE
# # Connecting using SQL Server authentication
#     # conn = pyodbc.connect('DRIVER={ODBC Driver 18 for SQL Server};SERVER=' + server + ';DATABASE=' + database + ';UID=' + username + ';PWD=' + password + ';Encrypt=Yes;TrustServerCertificate=Yes;')
    
#     # conn = pyodbc.connect('DRIVER={ODBC Driver 18 for SQL Server};SERVER=' + server + ';DATABASE=' + database + ';UID=adm_hiznis;DOMAIN=OXNET;Encrypt=Yes;TrustServerCertificate=Yes;')
    
#     # cursor = conn.cursor()
#     # cursor.execute("SELECT * FROM oxpos_cohort_3.oxpos_headline_diagnosis_list;")
#     # for row in cursor:
#     #     print(row)
#     # conn.close()

# # -- example code
    user_name = 'adm_hiznis'
    user_password = getpass.getpass(f'Enter {user_name} Windows password: ')
    domain_name = 'OXNET.NHS.UK'

    # create_kerberos_ticket(user_name, domain_name, user_password)

    # list_kerberos_ticket()

    
    cursor = sqlserver_connection(server, database).cursor()

    cursor.execute("select * from raw.tblEntities")
    row = cursor.fetchone()
    if row:
        print(row)

