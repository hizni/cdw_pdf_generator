import main
import re
import pyodbc
import math 
import pdfkit
import jinja2
import pandas as pd
from datetime import datetime 
import errno, sys 
import os 
import base64
import utility


# def insert_text_where_match_regex(original_text, pattern, insert_text):
#     matches =  re.finditer(pattern, test_text)
#     for match in matches:
#         # print(match.start())
#         index = match.start()
        
#         original_text = original_text[:index] + insert_text + original_text[index + 1:]

#     return original_text

# generate pdf_content
def create_pdf_content(template_vars, templates_dir, template_file):

    env = jinja2.Environment(loader=jinja2.FileSystemLoader(templates_dir))
    template = env.get_template(template_file)

    #template vars are passed as a dictionary from a row in the retrieved record set
    html_out = template.render(template_vars)

    return html_out

def save_pdf(file_content, target_dir, filename):
    try:
        # with open('your_pdf_file_here.pdf', 'wb+') as file:
        #     file.write(file_content)

        config = pdfkit.configuration(wkhtmltopdf='/usr/local/bin/wkhtmltopdf')

        output_pdf_path_and_file = target_dir + filename + '.pdf'
        
        pdfkit.from_string(file_content, output_pdf_path_and_file, configuration=config)

    except Exception as error:
        # logging.error(f'Error saving file to disc. Error: {error}')
        print(f'Error saving file to disc. Error: {error}')
        raise error
    
def manual_cleaning_step(text):
    """
    .replace("Roberts-Gant","[REDACTED]")
    .replace("Dr Eve","[REDACTED]")
    .replace("Dr Mark","[REDACTED]")
    .replace("Dr [REDACTED] Brown","[REDACTED]")
    """
    new_text = ''
    if(text != None):
        # print("original: " + text)
        new_text = str(text).replace("Roberts-Gant","[REDACTED]").replace("Dr Eve","[REDACTED]").replace("Dr Mark","[REDACTED]").replace("Dr [REDACTED] Brown","[REDACTED]")

    return new_text

# def get_data_from_database(db_connection, schema_table_name):
#     cursor = db_connection.cursor()  
#     cursor.execute('SELECT * FROM ' + schema_table_name)

#     # get columns returned
#     columns = [ x[0] for x in cursor.description]
#     # get rows returned
#     rows = cursor.fetchall()

#     # create dataframe
#     return pd.DataFrame(rows, columns=columns)

def additional_report_fields_cleaning(df):
    for i, row in enumerate(df.to_dict('records')):

        # check if report freetext columns exist in dataframe and perform additional redaction on them
        if 'DiagnosticReportText' in df.columns:
            df.at[i,'DiagnosticReportText'] = manual_cleaning_step(row['DiagnosticReportText'])

        if 'ConclusionCodeDisplay' in df.columns:
            df.at[i,'ConclusionCodeDisplay'] = manual_cleaning_step(row['ConclusionCodeDisplay'])

        if 'ProcedureNote' in df.columns:
            df.at[i,'ProcedureNote'] = manual_cleaning_step(row['ProcedureNote'])

        if 'Findings' in df.columns:
            df.at[i,'Findings'] = manual_cleaning_step(row['Findings'])

        if 'Symptoms' in df.columns:
            df.at[i,'Symptoms'] = manual_cleaning_step(row['Symptoms'])

        if 'Pathological' in df.columns:
            df.at[i,'Pathological'] = manual_cleaning_step(row['Pathological'])

        if 'Radiology' in df.columns:
            df.at[i,'Radiology'] = manual_cleaning_step(row['Radiology'])

        if 'DiagnosticReportText' in df.columns:
            df.at[i,'DiagnosticReportText'] = manual_cleaning_step(row['DiagnosticReportText'])

def generate_and_store_pdf_data_in_df(df, template_dir, template_file):
    for i, row in enumerate(df.to_dict('records')):
        pdf_content = create_pdf_content(row, template_dir, template_file)

        # insert PDF content into dataframe col in row (if exists in template)
        if 'AttachmentName' in df.columns:
            df.at[i,'AttachmentName'] = str(row['DiagnosticReportIdentifier']) + '.pdf'

        if 'AttachmentContent' in df.columns:
            df.at[i,'AttachmentContent'] = base64.b64encode(str.encode(pdf_content)).decode()
        
        if 'AttachmentType' in df.columns:
            df.at[i,'AttachmentType'] = 'application/pdf'

        if 'ProcedureIdentifier_list' in df.columns:
            df.at[i,'AttachmentContent'] = base64.b64encode(str.encode(pdf_content)).decode()

def get_pdf_content(file_content):
    try:
        config = pdfkit.configuration(wkhtmltopdf='/usr/local/bin/wkhtmltopdf')
        return pdfkit.from_string(file_content, configuration=config)
    
    except Exception as error:
        # logging.error(f'Error saving file to disc. Error: {error}')
        print(f'Error getting PDF content. Error: {error}')
        raise error

# def save_to_delimited_file(dataframe, target_dir, filename, columns_list = None, max_file_size_mb = None, delimiter = ","):
    
#     # filepath = pathlib.Path(target_dir + filename)
#     now = datetime.now() # current date and time
#     current_datestamp = now.strftime("%Y%m%d")
#     print(current_datestamp)

#     # Create a subdir below the target dir for todays date
#     isExist = os.path.exists(f'{target_dir}/{current_datestamp}')
#     if not isExist:
#         os.makedirs(f'{target_dir}/{current_datestamp}')

#     # output_df = dataframe

#     if (columns_list != None):
#         output_df = dataframe[columns_list]
#     else:
#         output_df = dataframe

#     # getting info to help make decisions regarding sizing
#     df_size_in_bytes = sys.getsizeof(output_df)
#     df_size_in_mb = (df_size_in_bytes / 1000000)

#     # if file size is not an issue, just output to csv
#     if(max_file_size_mb == None):
#         output_df.to_csv(f'{target_dir}/{current_datestamp}/{filename}.csv', header=True, chunksize=5000)

#     else:
#         df_row_count = len(output_df)

#         iteration = math.ceil(df_size_in_mb / max_file_size_mb)
#         number_of_chunks = int(df_row_count / iteration)

#         for i, start in enumerate(range(0, df_row_count, number_of_chunks)):
#             output_df[start:start+number_of_chunks].to_csv(f'{target_dir}/{filename}_{i}.csv', chunksize=5000)

#     return

def generate_pathology_oxpos_submission(connection):
    schema_table_name = 'diff.oxpos_diagnostic_report_pathology'
    template_dir = './templates'
    template_file = 'pathology-report-template.html'

    df = utility.get_data_from_database(connection, 'diff.oxpos_diagnostic_report_pathology')

    df = manual_cleaning_step(df)

    for i, row in enumerate(df.to_dict('records')):
        # generate PDF content
        pdf_content = create_pdf_content(row, template_dir, template_file)

        # # saving PDFs to disk to check
        # save_pdf(pdf_content, target_dir='./final_generated/pdf/pathology/', filename=str(row['DiagnosticReportIdentifier']))

        # insert PDF content into dataframe row
        df.at[i,'AttachmentName'] = str(row['DiagnosticReportIdentifier']) + '.pdf'
        # df.at[i,'AttachmentContent'] = base64.b64encode(str.encode(pdf_content)).decode()

        df.at[i,'AttachmentContent'] = base64.b64encode(get_pdf_content(pdf_content)).decode()
        df.at[i,'AttachmentContentMimeType'] = 'application/pdf'


    # renaming columns from extracted dataset. Should be pushed back to data product generation as will save us having to do this here.
    # any name changes have to be reflected in templates as well
    #  Old column name                  | New column name
    #  DiagnosticReportIdentifier       | DiagnosticPrimaryIdentifier
    #  DiagnosticReportIdentifierSystem | DiagnosticPrimaryIdentifierSystem
    #  DiagnosticReportStatus           | PrimaryReportStatus
    df = df.rename(columns={ 'DiagnosticReportIdentifier': 'DiagnosticPrimaryIdentifier',
                             'DiagnosticReportIdentifierSystem': 'DiagnosticPrimaryIdentifierSystem',
                             'DiagnosticReportStatus' : 'PrimaryReportStatus'
                       })

    return df

if __name__ == '__main__':

    # generate PDFs for reports in navify 
    server = 'oxnetdwp04'
    database = 'cig_101_test'
    driver = "ODBC+Driver+18+for+SQL+Server"

    url = utility.get_sql_alchemy_url(server, database, driver)

    # create connection
    conn = utility.get_db_connection(server , database )
    cursor = conn.cursor()
 
    engine = utility.get_sql_alchemy_engine(server, database, driver)
    with engine.begin() as conn:
        # if not diff_data.is:
        # test_join.write_database(table_name=f'diff.manifest',connection=url, if_exists='replace')
                
        # conn.commit()
        # print('finished writing commit')
        columns_list=[      'SourceOrgIdentifier',
                            'SourceSystemIdentifier',
                            'PatientPrimaryIdentifier',
                            'PatientPrimaryIdentifierSystem',
                            'DiagnosticReportIdentifier',
                            'DiagnosticReportIdentifierSystem',
                            'ConditionIdentifier',
                            'ConditionIdentifierSystem',
                            'ProcedureIdentifier',
                            'ProcedureIdentifierSystem',
                            'PrimaryReportStatus',
                            'DiagnosticReportCode',
                            'DiagnosticReportCodeSystem',
                            'DiagnosticReportDisplay',
                            'EffectiveDateTime',
                            'DiagnosisCategory',
                            'DiagnosisCategorySystem',
                            'DiagnosisCategoryDisplay',
                            'DiagnosticReportCategoryText',
                            'ProviderIdentifier',
                            'ProviderIdentifierSystem',
                            'ProviderFullName',
                            'ResultIdentifier',
                            'ResultIdentifierSystem',

                            'ConclusionCode' ,
                            'ConclusionCodeSystem',
                            'ConclusionCodeDisplay',
                            'ConclusionText' 

                            'AttachmentName',
                            
                            'AttachmentContentMimeType',
                            'AttachmentContent',
                            'PresentedFormSize',
                            'PresentedFormPointer'
                            
                            ]
        
        # pathology
        utility.save_to_delimited_file(generate_pathology_oxpos_submission(conn), './final_generated', filename='{{datestamp}}_pathology._navify_diagnostic_report' ,columns_list=columns_list, max_file_size_mb=25, sub_dir_by_date=True)
        conn.commit


    
    
    # Create database connection string (PYODBC) using SQL authentication
    conn = pyodbc.connect('Driver={ODBC Driver 18 for SQL Server}' + \
                          ';SERVER=' + server_name + \
                          ';DATABASE=' + database_name + \
                          ';TrustServerCertificate=Yes;Trusted_Connection=Yes;' )
    
    #TODO - change DB connection to use SQLAlchemy - see if speed up interaction
    #TODO - add feedback . Generating PDF can be quite slow
    
    # create dataframe from data extracted from table
    df = get_data_from_database(conn, schema_table_name)
    additional_report_fields_cleaning(df)

    """
    renaming columns from extracted dataset. Should be pushed back to data product generation as will save us having to do this here.
    any name changes have to be reflected in templates as well
     Old column name                  | New column name
     DiagnosticReportIdentifier       | DiagnosticPrimaryIdentifier
     DiagnosticReportIdentifierSystem | DiagnosticPrimaryIdentifierSystem
     DiagnosticReportStatus           | PrimaryReportStatus
     """
    df = df.rename(columns={ 'DiagnosticReportIdentifier': 'DiagnosticPrimaryIdentifier',
                             'DiagnosticReportIdentifierSystem': 'DiagnosticPrimaryIdentifierSystem',
                             'DiagnosticReportStatus' : 'PrimaryReportStatus'})

    generate_and_store_pdf_data_in_df(df, template_dir, template_file)
    save_to_delimited_file(df, './generated', str(template_file).removesuffix('-template.html') ,columns_list=columns_list, max_file_size_mb=25)
    # test_text = '2021-03-17:Higher risk drinking;2023-02-12:Myxoid liposarcoma;2023-02-12:Pulmonary embolism;2023-02-12:Epigastric hernia;2023-02-16:Pulmonary embolism;2023-02-16:Bladder problem;2023-02-16:Deep vein thrombosis;2023-02-16:Urinary problem'
    # print(test_text)

# formatting of text
    # headline_dx_list_pattern = r';[0-9][0-9][0-9][0-9]-[0-9][0-9]-[0-9][0-9]'
    # insert_text_where_match_regex(test_text, headline_dx_list_pattern, '\n')

    # mdt_pattern = r' [0-3][0-9].[0-1][0-9].[0-9][0-9]'
    # insert_text_where_match_regex(test_text, mdt_pattern, '\n')

