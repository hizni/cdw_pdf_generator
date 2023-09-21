import pyodbc
import pandas as pd
import base64
import math 
from datetime import datetime
import jinja2
import os, sys
import pdfkit

def get_db_connection(server, database):
   return pyodbc.connect('Driver={ODBC Driver 18 for SQL Server}' + \
                          ';SERVER=' + server + \
                          ';DATABASE=' + database + \
                          ';TrustServerCertificate=Yes;Trusted_Connection=Yes;' )   

def get_data_from_database(db_connection, schema_table_name):
    query = 'SELECT * FROM ' + schema_table_name
    df = pd.read_sql(query, db_connection)

    return df

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
    
def get_pdf_content(file_content):
    try:
        config = pdfkit.configuration(wkhtmltopdf='/usr/local/bin/wkhtmltopdf')
        return pdfkit.from_string(file_content, configuration=config)
    
    except Exception as error:
        # logging.error(f'Error saving file to disc. Error: {error}')
        print(f'Error getting PDF content. Error: {error}')
        raise error
    
def manual_cleaning_regex(text):
    """
    .replace("Roberts-Gant","[REDACTED]")
    .replace("Dr Eve","[REDACTED]")
    .replace("Dr Mark","[REDACTED]")
    .replace("Dr [REDACTED] Brown","[REDACTED]")
    """
    new_text = ''
    if(text != None):

        new_text = str(text).replace("Roberts-Gant","[REDACTED]").replace("Dr Eve","[REDACTED]").replace("Dr Mark","[REDACTED]").replace("Dr [REDACTED] Brown","[REDACTED]")

    return new_text

def create_pdf_content(template_vars, templates_dir, template_file):

    env = jinja2.Environment(loader=jinja2.FileSystemLoader(templates_dir))
    template = env.get_template(template_file)

    #template vars are passed as a dictionary from a row in the retrieved record set
    html_out = template.render(template_vars)

    return html_out

def save_to_delimited_file(dataframe, target_dir, filename, columns_list = None, max_file_size_mb = None, delimiter = ",", timestamp_file=False):
    
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

    # getting info to help make decisions
    df_row_count = len(output_df)
    print('row count: ' + str(df_row_count))
    if (df_row_count == 0):
        print(f"dataframe for {filename} is empty. Nothing file to produce here!")
        return 0
    
    df_size_in_bytes = sys.getsizeof(output_df)
    df_size_in_mb = (df_size_in_bytes / 1000000)
    print('original dataframe size in MB: ' + str(df_size_in_mb))

    # if file size is not an issue, just output to csv
    if(max_file_size_mb == None):
        if(timestamp_file == True):
            output_df.to_csv(f'{target_dir}/{current_datestamp}/{current_datestamp}._{filename}.csv', header=True, chunksize=5000, index=False)
        else:
            output_df.to_csv(f'{target_dir}/{current_datestamp}/{filename}.csv', header=True, chunksize=5000)
        
    else:
        df_row_count = len(output_df)
        iteration = math.ceil(df_size_in_mb / max_file_size_mb)
        number_of_rows_in_chunk = int(df_row_count / iteration)

        print('row count: ' + str(df_row_count))
        print('iteration: ' + str(iteration))
        print('number of rows in chunk: ' + str(number_of_rows_in_chunk))

        if iteration == 1:
            if(timestamp_file == True):
                output_df.to_csv(f'{target_dir}/{current_datestamp}/{current_datestamp}._{filename}.csv', header=True, chunksize=5000, index=False)
            else:
                output_df.to_csv(f'{target_dir}/{current_datestamp}/{filename}.csv', header=True, chunksize=5000, index=False)
        else:
            for i, start in enumerate(range(0, df_row_count, number_of_rows_in_chunk)):
                # output_df[start:start+number_of_rows_in_chunk].to_csv(f'{target_dir}/{current_datestamp}/{filename}_{i}.csv', chunksize=5000)
                if(timestamp_file == True):
                    output_df[start:start+number_of_rows_in_chunk].to_csv(f'{target_dir}/{current_datestamp}/{current_datestamp}_{i}._{filename}.csv', header=True, chunksize=5000, index=False)
                else:
                    output_df[start:start+number_of_rows_in_chunk].to_csv(f'{target_dir}/{current_datestamp}/{i}_{filename}.csv', header=True, chunksize=5000, index=False)

    return

def manual_clean_df(df):
    
    for i, row in enumerate(df.to_dict('records')):
    # check if report freetext columns exist in dataframe and perform additional redaction on them
        if 'DiagnosticReportText' in df.columns:
            df.at[i,'DiagnosticReportText'] = manual_cleaning_regex(row['DiagnosticReportText'])

        if 'ConclusionCodeDisplay' in df.columns:
            df.at[i,'ConclusionCodeDisplay'] = manual_cleaning_regex(row['ConclusionCodeDisplay'])

        if 'ProcedureNote' in df.columns:
            df.at[i,'ProcedureNote'] = manual_cleaning_regex(row['ProcedureNote'])

        if 'Findings' in df.columns:
            df.at[i,'Findings'] = manual_cleaning_regex(row['Findings'])

        if 'Symptoms' in df.columns:
            df.at[i,'Symptoms'] = manual_cleaning_regex(row['Symptoms'])

        if 'Pathological' in df.columns:
            df.at[i,'Pathological'] = manual_cleaning_regex(row['Pathological'])

        if 'Radiology' in df.columns:
            df.at[i,'Radiology'] = manual_cleaning_regex(row['Radiology'])
         
    return df

def generate_pathology_oxpos_submission(connection):
    schema_table_name = 'oxpos_cohort_3.oxpos_diagnostic_report_pathology'
    template_dir = './templates'
    template_file = 'pathology-report-template.html'

    df = get_data_from_database(connection, 'oxpos_cohort_3.oxpos_diagnostic_report_pathology')

    df = manual_clean_df(df)

    for i, row in enumerate(df.to_dict('records')):
        # generate PDF content
        pdf_content = create_pdf_content(row, template_dir, template_file)

        # # saving PDFs to disk to check
        # save_pdf(pdf_content, target_dir='./generated/pdf/pathology/', filename=str(row['DiagnosticReportIdentifier']))

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
 
def generate_radiology_oxpos_submission(connection):
    schema_table_name = 'oxpos_cohort_3.oxpos_diagostic_report_radiology'
    template_dir = './templates'
    template_file = 'radiology-report-template.html'

    df = get_data_from_database(connection, 'oxpos_cohort_3.oxpos_diagnostic_report_radiology')

    df = manual_clean_df(df)
 
    for i, row in enumerate(df.to_dict('records')):
        # generate PDF content
        pdf_content = create_pdf_content(row, template_dir, template_file)
    
        # insert PDF content into dataframe row
        df.at[i,'AttachmentName'] = str(row['DiagnosticReportIdentifier']) + '.pdf'
        df.at[i,'AttachmentContent'] = base64.b64encode(get_pdf_content(pdf_content)).decode()
        df.at[i,'AttachmentContentMimeType'] = 'application/pdf'

        # saving PDFs to disk to check
        # save_pdf(pdf_content, target_dir='./generated/pdf/radiology/', filename=str(row['DiagnosticReportIdentifier']))


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
   
def generate_surgical_reports(connection):
    schema_table_name = 'oxpos_cohort_3.oxpos_surgical_report'
    template_dir = './templates'
    template_file = 'surgical-report-template.html'

    df = get_data_from_database(connection, 'oxpos_cohort_3.oxpos_surgical_report')

    df = manual_clean_df(df)
 
    for i, row in enumerate(df.to_dict('records')):
        # generate PDF content
        pdf_content = create_pdf_content(row, template_dir, template_file)
    
        # insert PDF content into dataframe row
        df.at[i,'AttachmentName'] = str(row['DiagnosticReportIdentifier']) + '.pdf'
        df.at[i,'AttachmentContent'] = base64.b64encode(get_pdf_content(pdf_content)).decode()
        df.at[i,'AttachmentContentMimeType'] = 'application/pdf'

        # saving PDFs to disk to check
        # save_pdf(pdf_content, target_dir='./generated/pdf/surgical/', filename=str(row['DiagnosticReportIdentifier']))

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

    columns_list=[   'SourceOrgIdentifier','SourceSystemIdentifier','PatientPrimaryIdentifier','PatientPrimaryIdentifierSystem'
                    ,'DiagnosticPrimaryIdentifier' ,'DiagnosticPrimaryIdentifierSystem','PrimaryReportStatus','DiagnosticReportCode'
                    ,'DiagnosticReportCodeSystem','DiagnosticReportDisplay','EffectiveDateTime','DiagnosisCategory','DiagnosisCategorySystem'
                    ,'DiagnosisCategoryDisplay','ProviderIdentifier','ProviderIdentifierSystem','AttachmentName','AttachmentContent'
                    ,'AttachmentContentMimeType','ResultIdentifier','ResultIdentifierSystem','ConditionIdentifier','ConditionIdentifierSystem'
                    ,'ProcedureIdentifier','ProcedureIdentifierSystem','DiagnosticReportCategoryText','ProviderFullName','ConclusionCode'
                    ,'ConclusionCodeSystem','ConclusionCodeDisplay','ConclusionText' ]

    return df

def generate_mdt_reports(connection):
    schema_table_name = 'oxpos_cohort_3.oxpos_diagnostic_mdt_report'
    template_dir = './templates'
    template_file = 'mdt-report-template.html'

    df = get_data_from_database(connection, 'oxpos_cohort_3.oxpos_diagnostic_mdt_report')

    df = manual_clean_df(df)
 
    for i, row in enumerate(df.to_dict('records')):
        # generate PDF content
        pdf_content = create_pdf_content(row, template_dir, template_file)
    
        # insert PDF content into dataframe row
        df.at[i,'AttachmentName'] = str(row['DiagnosticReportIdentifier']) + '.pdf'
        df.at[i,'AttachmentContent'] = base64.b64encode(get_pdf_content(pdf_content)).decode()
        df.at[i,'AttachmentContentMimeType'] = 'application/pdf'

        # saving PDFs to disk to check
        # save_pdf(pdf_content, target_dir='./generated/pdf/mdt/', filename=str(row['DiagnosticReportIdentifier']))


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

    # columns_list=[   'SourceOrgIdentifier','SourceSystemIdentifier','PatientPrimaryIdentifier','PatientPrimaryIdentifierSystem'
    #                 ,'DiagnosticPrimaryIdentifier' ,'DiagnosticPrimaryIdentifierSystem','PrimaryReportStatus','DiagnosticReportCode'
    #                 ,'DiagnosticReportCodeSystem','DiagnosticReportDisplay','EffectiveDateTime','DiagnosisCategory','DiagnosisCategorySystem'
    #                 ,'DiagnosisCategoryDisplay','ProviderIdentifier','ProviderIdentifierSystem','AttachmentName','AttachmentContent'
    #                 ,'AttachmentContentMimeType','ResultIdentifier','ResultIdentifierSystem','ConditionIdentifier','ConditionIdentifierSystem'
    #                 ,'ProcedureIdentifier','ProcedureIdentifierSystem','DiagnosticReportCategoryText','ProviderFullName','ConclusionCode'
    #                 ,'ConclusionCodeSystem','ConclusionCodeDisplay','ConclusionText' ]
    
    return df

def generate_headline_diagnosis(connection):
    df = get_data_from_database(connection, 'oxpos_cohort_3.oxpos_headline_diagnosis')

    for i, row in enumerate(df.to_dict('records')):
        # generate PDF content
        # pdf_content = create_pdf_content(row, template_dir, template_file)
    
        # insert PDF content into dataframe row
        df.at[i,'EffectiveDateTime'] = 'Date: ' + str(row['EffectiveDateTime'])

    # renaming cols
    df = df.rename(columns={ 'DiagnosticReportIdentifier' : 'PMHIdentifier',
                             'DiagnosticReportIdentifierSystem' : 'PMHIdentifierSystem',
                             'ProcedureIdentifier' : 'PMHTitle',
                             'EffectiveDateTime' : 'PMHDescription'
                       })

    return df

def generate_observation_grade_extract(connection):
    df = get_data_from_database(connection, 'oxpos_cohort_3.oxpos_observation_grade')

    # renaming cols
    df = df.rename(columns={ 'DiagnosticReportIdentifier': 'DiagnosticPrimaryIdentifier',
                             'DiagnosticReportIdentifierSystem': 'DiagnosticPrimaryIdentifierSystem',
                             'DiagnosticReportStatus' : 'PrimaryReportStatus'
                       })
    
    return df

def generate_icdo_extract(connection):
    df = get_data_from_database(connection, 'oxpos_cohort_3.oxpos_observation_icdo')

    # renaming cols
    df = df.rename(columns={ 'DiagnosticReportIdentifier': 'DiagnosticPrimaryIdentifier',
                             'DiagnosticReportIdentifierSystem': 'DiagnosticPrimaryIdentifierSystem',
                             'DiagnosticReportStatus' : 'PrimaryReportStatus'
                       })
    
    return df

def generate_tnm_extract(connection):
    df = get_data_from_database(connection, 'oxpos_cohort_3.oxpos_observation_tnm')

    # renaming cols
    df = df.rename(columns={ 'DiagnosticReportIdentifier': 'DiagnosticPrimaryIdentifier',
                             'DiagnosticReportIdentifierSystem': 'DiagnosticPrimaryIdentifierSystem',
                             'DiagnosticReportStatus' : 'PrimaryReportStatus'
                       })
    return df

def generate_patient_extract(connection):
    df = get_data_from_database(connection, 'oxpos_cohort_3.oxpos_patient')

    # renaming cols
    df = df.rename(columns={ 'DiagnosticReportIdentifier': 'DiagnosticPrimaryIdentifier',
                             'DiagnosticReportIdentifierSystem': 'DiagnosticPrimaryIdentifierSystem',
                             'DiagnosticReportStatus' : 'PrimaryReportStatus'
                       })
    
    return df

def generate_condition_extract(connection):
    df = get_data_from_database(connection, 'oxpos_cohort_3.oxpos_condition')

    # renaming cols
    df = df.rename(columns={ 'DiagnosticReportIdentifier': 'DiagnosticPrimaryIdentifier',
                             'DiagnosticReportIdentifierSystem': 'DiagnosticPrimaryIdentifierSystem',
                             'DiagnosticReportStatus' : 'PrimaryReportStatus'
                       })
    return df

def generate_body_structure_extract(connection):
    df = get_data_from_database(connection, 'oxpos_cohort_3.oxpos_body_structure')

    # renaming cols
    df = df.rename(columns={ 'DiagnosticReportIdentifier': 'DiagnosticPrimaryIdentifier',
                             'DiagnosticReportIdentifierSystem': 'DiagnosticPrimaryIdentifierSystem',
                             'DiagnosticReportStatus' : 'PrimaryReportStatus'
                       })
    return df

def generate_procedure_extract(connection):
    df = get_data_from_database(connection, 'oxpos_cohort_3.oxpos_procedure')

    # renaming cols
    df = df.rename(columns={ 'DiagnosticReportIdentifier': 'DiagnosticPrimaryIdentifier',
                             'DiagnosticReportIdentifierSystem': 'DiagnosticPrimaryIdentifierSystem',
                             'DiagnosticReportStatus' : 'PrimaryReportStatus'
                       })
    return df

if __name__ == '__main__':
    server = 'oxnetdwp04'
    database = 'data_products__oxpos_cohort_3'

    # create connection
    conn = get_db_connection(server , database )
    print('=== output 0 : patient ====')
    save_to_delimited_file(generate_patient_extract(conn), './generated', 'navify_patient', timestamp_file=True)
   

    print('=== output 1 : pathology report ====')
    # columns_list=[   'SourceOrgIdentifier','SourceSystemIdentifier','PatientPrimaryIdentifier','PatientPrimaryIdentifierSystem'
    #                 ,'DiagnosticPrimaryIdentifier' ,'DiagnosticPrimaryIdentifierSystem','PrimaryReportStatus','DiagnosticReportCode'
    #                 ,'DiagnosticReportCodeSystem','DiagnosticReportDisplay','EffectiveDateTime','DiagnosisCategory','DiagnosisCategorySystem'
    #                 ,'DiagnosisCategoryDisplay','ProviderIdentifier','ProviderIdentifierSystem','AttachmentName','AttachmentContent'
    #                 ,'AttachmentContentMimeType','ResultIdentifier','ResultIdentifierSystem','ConditionIdentifier','ConditionIdentifierSystem'
    #                 ,'ProcedureIdentifier','ProcedureIdentifierSystem','DiagnosticReportCategoryText','ProviderFullName','ConclusionCode'
    #                 ,'ConclusionCodeSystem','ConclusionCodeDisplay','ConclusionText' ]

    
    diagnostic_reports_df = generate_pathology_oxpos_submission(conn)
    # save_to_delimited_file(df, './generated', str(template_file).removesuffix('-template.html') ,columns_list=columns_list, max_file_size_mb=25, timestamp_file=True)

    print('=== output 2 : radiology report ====')
    # columns_list=[   'SourceOrgIdentifier','SourceSystemIdentifier','PatientPrimaryIdentifier','PatientPrimaryIdentifierSystem'
    #                 ,'DiagnosticPrimaryIdentifier' ,'DiagnosticPrimaryIdentifierSystem','PrimaryReportStatus','DiagnosticReportCode'
    #                 ,'DiagnosticReportCodeSystem','DiagnosticReportDisplay','EffectiveDateTime','DiagnosisCategory','DiagnosisCategorySystem'
    #                 ,'DiagnosisCategoryDisplay','ProviderIdentifier','ProviderIdentifierSystem','AttachmentName','AttachmentContent'
    #                 ,'AttachmentContentMimeType','ResultIdentifier','ResultIdentifierSystem','ConditionIdentifier','ConditionIdentifierSystem'
    #                 ,'ProcedureIdentifier','ProcedureIdentifierSystem','DiagnosticReportCategoryText','ProviderFullName','ConclusionCode'
    #                 ,'ConclusionCodeSystem','ConclusionCodeDisplay','ConclusionText' ]

    diagnostic_reports_df = pd.concat([diagnostic_reports_df, generate_radiology_oxpos_submission(conn)])
    # save_to_delimited_file(df, './generated', str(template_file).removesuffix('-template.html') ,columns_list=columns_list, max_file_size_mb=25, timestamp_file=True)

    print('=== output 3 : surgical report ====')
   
    # columns_list=[   'SourceOrgIdentifier','SourceSystemIdentifier','PatientPrimaryIdentifier','PatientPrimaryIdentifierSystem'
    #                 ,'DiagnosticPrimaryIdentifier' ,'DiagnosticPrimaryIdentifierSystem','PrimaryReportStatus','DiagnosticReportCode'
    #                 ,'DiagnosticReportCodeSystem','DiagnosticReportDisplay','EffectiveDateTime','DiagnosisCategory','DiagnosisCategorySystem'
    #                 ,'DiagnosisCategoryDisplay','ProviderIdentifier','ProviderIdentifierSystem','AttachmentName','AttachmentContent'
    #                 ,'AttachmentContentMimeType','ResultIdentifier','ResultIdentifierSystem','ConditionIdentifier','ConditionIdentifierSystem'
    #                 ,'ProcedureIdentifier','ProcedureIdentifierSystem','DiagnosticReportCategoryText','ProviderFullName','ConclusionCode'
    #                 ,'ConclusionCodeSystem','ConclusionCodeDisplay','ConclusionText' ]

    diagnostic_reports_df = pd.concat([diagnostic_reports_df, generate_surgical_reports(conn)])
    # save_to_delimited_file(df, './generated', str(template_file).removesuffix('-template.html') ,columns_list=columns_list, max_file_size_mb=25, timestamp_file=True)

    print('=== output 4 : MDT report ====')
   
    # columns_list=[   'SourceOrgIdentifier','SourceSystemIdentifier','PatientPrimaryIdentifier','PatientPrimaryIdentifierSystem'
    #                 ,'DiagnosticPrimaryIdentifier' ,'DiagnosticPrimaryIdentifierSystem','PrimaryReportStatus','DiagnosticReportCode'
    #                 ,'DiagnosticReportCodeSystem','DiagnosticReportDisplay','EffectiveDateTime','DiagnosisCategory','DiagnosisCategorySystem'
    #                 ,'DiagnosisCategoryDisplay','ProviderIdentifier','ProviderIdentifierSystem','AttachmentName','AttachmentContent'
    #                 ,'AttachmentContentMimeType','ResultIdentifier','ResultIdentifierSystem','ConditionIdentifier','ConditionIdentifierSystem'
    #                 ,'ProcedureIdentifier','ProcedureIdentifierSystem','DiagnosticReportCategoryText','ProviderFullName','ConclusionCode'
    #                 ,'ConclusionCodeSystem','ConclusionCodeDisplay','ConclusionText' ]

    diagnostic_reports_df = pd.concat([diagnostic_reports_df, generate_mdt_reports(conn)])
    # save_to_delimited_file(df, './generated', str(template_file).removesuffix('-template.html') ,columns_list=columns_list, max_file_size_mb=25, timestamp_file=True)



    print('==== output 1 to 4 : diagnostic reports in one file =====')

    # diagnostic_reports_df
    columns_list=[   'SourceOrgIdentifier','SourceSystemIdentifier','PatientPrimaryIdentifier','PatientPrimaryIdentifierSystem'
                    ,'DiagnosticPrimaryIdentifier' ,'DiagnosticPrimaryIdentifierSystem','PrimaryReportStatus','DiagnosticReportCode'
                    ,'DiagnosticReportCodeSystem','DiagnosticReportDisplay','EffectiveDateTime','DiagnosisCategory','DiagnosisCategorySystem'
                    ,'DiagnosisCategoryDisplay','ProviderIdentifier','ProviderIdentifierSystem','AttachmentName','AttachmentContent'
                    ,'AttachmentContentMimeType','ResultIdentifier','ResultIdentifierSystem','ConditionIdentifier','ConditionIdentifierSystem'
                    ,'ProcedureIdentifier','ProcedureIdentifierSystem','DiagnosticReportCategoryText','ProviderFullName','ConclusionCode'
                    ,'ConclusionCodeSystem','ConclusionCodeDisplay','ConclusionText' ]
    
    save_to_delimited_file(diagnostic_reports_df, './generated', 'navify_diagnostic_report' ,columns_list=columns_list, max_file_size_mb=25, timestamp_file=True)

    print('=== output 5 : headline diagnosis ====')
    
    columns_list=[   'SourceOrgIdentifier','SourceSystemIdentifier','PatientPrimaryIdentifier','PatientPrimaryIdentifierSystem'
                    ,'PMHIdentifier','PMHIdentifierSystem','PMHTitle','PMHDescription']

                # ,'DiagnosticPrimaryIdentifier' ,'DiagnosticPrimaryIdentifierSystem','PrimaryReportStatus','DiagnosticReportCode'
                # ,'DiagnosticReportCodeSystem','DiagnosticReportDisplay','EffectiveDateTime','DiagnosisCategory','DiagnosisCategorySystem'
                # ,'DiagnosisCategoryDisplay','ProviderIdentifier','ProviderIdentifierSystem','AttachmentName','AttachmentContent'
                # ,'AttachmentContentMimeType','ResultIdentifier','ResultIdentifierSystem','ConditionIdentifier','ConditionIdentifierSystem'
                # ,'ProcedureIdentifier','ProcedureIdentifierSystem','DiagnosticReportCategoryText','ProviderFullName','ConclusionCode'
                # ,'ConclusionCodeSystem','ConclusionCodeDisplay','ConclusionText' 
                


    save_to_delimited_file(generate_headline_diagnosis(conn), './generated', 'navify_PMHcondition' ,columns_list=columns_list, max_file_size_mb=25, timestamp_file=True)

    print('=== output 6 : observation grade ====')

    save_to_delimited_file(generate_observation_grade_extract(conn), './generated', 'navify_observation_grade', timestamp_file=True)

    print('=== output 7 : ICDO grade ====')
        
    # columns_list=[   'SourceOrgIdentifier','SourceSystemIdentifier','PatientPrimaryIdentifier','PatientPrimaryIdentifierSystem'
    #             ,'DiagnosticPrimaryIdentifier' ,'DiagnosticPrimaryIdentifierSystem','PrimaryReportStatus','DiagnosticReportCode'
    #             ,'DiagnosticReportCodeSystem','DiagnosticReportDisplay','EffectiveDateTime','DiagnosisCategory','DiagnosisCategorySystem'
    #             ,'DiagnosisCategoryDisplay','ProviderIdentifier','ProviderIdentifierSystem','AttachmentName','AttachmentContent'
    #             ,'AttachmentContentMimeType','ResultIdentifier','ResultIdentifierSystem','ConditionIdentifier','ConditionIdentifierSystem'
    #             ,'ProcedureIdentifier','ProcedureIdentifierSystem','DiagnosticReportCategoryText','ProviderFullName','ConclusionCode'
    #             ,'ConclusionCodeSystem','ConclusionCodeDisplay','ConclusionText' ]


    save_to_delimited_file(generate_icdo_extract(conn), './generated', 'navify_observation_icdo', timestamp_file=True)

    print('=== output 8 : TNM grade ====')
   
    # columns_list=[   'SourceOrgIdentifier','SourceSystemIdentifier','PatientPrimaryIdentifier','PatientPrimaryIdentifierSystem'
    #             ,'DiagnosticPrimaryIdentifier' ,'DiagnosticPrimaryIdentifierSystem','PrimaryReportStatus','DiagnosticReportCode'
    #             ,'DiagnosticReportCodeSystem','DiagnosticReportDisplay','EffectiveDateTime','DiagnosisCategory','DiagnosisCategorySystem'
    #             ,'DiagnosisCategoryDisplay','ProviderIdentifier','ProviderIdentifierSystem','AttachmentName','AttachmentContent'
    #             ,'AttachmentContentMimeType','ResultIdentifier','ResultIdentifierSystem','ConditionIdentifier','ConditionIdentifierSystem'
    #             ,'ProcedureIdentifier','ProcedureIdentifierSystem','DiagnosticReportCategoryText','ProviderFullName','ConclusionCode'
    #             ,'ConclusionCodeSystem','ConclusionCodeDisplay','ConclusionText' ]


    save_to_delimited_file(generate_tnm_extract(conn), './generated', 'navify_observation_tnm', timestamp_file=True)

   

    print('=== output 10 : condition ====')
   
    # columns_list=[   'SourceOrgIdentifier','SourceSystemIdentifier','PatientPrimaryIdentifier','PatientPrimaryIdentifierSystem'
    #             ,'DiagnosticPrimaryIdentifier' ,'DiagnosticPrimaryIdentifierSystem','PrimaryReportStatus','DiagnosticReportCode'
    #             ,'DiagnosticReportCodeSystem','DiagnosticReportDisplay','EffectiveDateTime','DiagnosisCategory','DiagnosisCategorySystem'
    #             ,'DiagnosisCategoryDisplay','ProviderIdentifier','ProviderIdentifierSystem','AttachmentName','AttachmentContent'
    #             ,'AttachmentContentMimeType','ResultIdentifier','ResultIdentifierSystem','ConditionIdentifier','ConditionIdentifierSystem'
    #             ,'ProcedureIdentifier','ProcedureIdentifierSystem','DiagnosticReportCategoryText','ProviderFullName','ConclusionCode'
    #             ,'ConclusionCodeSystem','ConclusionCodeDisplay','ConclusionText' ]


    save_to_delimited_file(generate_condition_extract(conn), './generated', 'navify_condition', timestamp_file=True)
      
    print('=== output 11 : body structure ====')
   
    # columns_list=[   'SourceOrgIdentifier','SourceSystemIdentifier','PatientPrimaryIdentifier','PatientPrimaryIdentifierSystem'
    #             ,'DiagnosticPrimaryIdentifier' ,'DiagnosticPrimaryIdentifierSystem','PrimaryReportStatus','DiagnosticReportCode'
    #             ,'DiagnosticReportCodeSystem','DiagnosticReportDisplay','EffectiveDateTime','DiagnosisCategory','DiagnosisCategorySystem'
    #             ,'DiagnosisCategoryDisplay','ProviderIdentifier','ProviderIdentifierSystem','AttachmentName','AttachmentContent'
    #             ,'AttachmentContentMimeType','ResultIdentifier','ResultIdentifierSystem','ConditionIdentifier','ConditionIdentifierSystem'
    #             ,'ProcedureIdentifier','ProcedureIdentifierSystem','DiagnosticReportCategoryText','ProviderFullName','ConclusionCode'
    #             ,'ConclusionCodeSystem','ConclusionCodeDisplay','ConclusionText' ]


    save_to_delimited_file(generate_body_structure_extract(conn), './generated', 'navify_body_structure', timestamp_file=True)

    print('=== output 12 : procedure ====')
    
    # columns_list=[   'SourceOrgIdentifier','SourceSystemIdentifier','PatientPrimaryIdentifier','PatientPrimaryIdentifierSystem'
    #             ,'DiagnosticPrimaryIdentifier' ,'DiagnosticPrimaryIdentifierSystem','PrimaryReportStatus','DiagnosticReportCode'
    #             ,'DiagnosticReportCodeSystem','DiagnosticReportDisplay','EffectiveDateTime','DiagnosisCategory','DiagnosisCategorySystem'
    #             ,'DiagnosisCategoryDisplay','ProviderIdentifier','ProviderIdentifierSystem','AttachmentName','AttachmentContent'
    #             ,'AttachmentContentMimeType','ResultIdentifier','ResultIdentifierSystem','ConditionIdentifier','ConditionIdentifierSystem'
    #             ,'ProcedureIdentifier','ProcedureIdentifierSystem','DiagnosticReportCategoryText','ProviderFullName','ConclusionCode'
    #             ,'ConclusionCodeSystem','ConclusionCodeDisplay','ConclusionText' ]


    save_to_delimited_file(generate_procedure_extract(conn), './generated', 'navify_procedure', timestamp_file=True)
    conn.close()
