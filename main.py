import pymssql
import pandas as pd
import jinja2
import pdfkit
from tqdm import tqdm
import argparse
import errno, sys 
import pathlib
import base64
import numpy as np
from datetime import datetime 
import os 
import math


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
        
def get_data_from_database(db_connection, schema_table_name):
    cursor = db_connection.cursor()  
    cursor.execute('SELECT * FROM ' + schema_table_name)

    # get columns returned
    columns = [ x[0] for x in cursor.description]
    # get rows returned
    rows = cursor.fetchall()

    # create dataframe
    return pd.DataFrame(rows, columns=columns)



def save_to_delimited_file(dataframe, target_dir, filename, columns_list = None, max_file_size_mb = None, delimiter = ","):
    
    # filepath = pathlib.Path(target_dir + filename)
    now = datetime.now() # current date and time
    current_datestamp = now.strftime("%Y%m%d")
    print(current_datestamp)

    # Check whether the specified path exists or not
    isExist = os.path.exists(f'{target_dir}/{current_datestamp}')
    if not isExist:
        os.makedirs(f'{target_dir}/{current_datestamp}')

    # output_df = dataframe

    if (columns_list != None):
        output_df = dataframe[columns_list]
    else:
        output_df = dataframe

    # getting info to help make decisions
    df_size_in_bytes = sys.getsizeof(output_df)
    df_size_in_mb = (df_size_in_bytes / 1000000)

    # if file size is not an issue, just output to csv
    if(max_file_size_mb == None):
        output_df.to_csv(f'{target_dir}/{current_datestamp}/{filename}.csv', header=True, chunksize=5000)

        
    else:
        df_row_count = len(output_df)

        # print("dataframe_size_in_b calc: " + str(df_size_in_bytes))
        # print("dataframe_size_in_mb calc: " + str(df_size_in_mb))

        iteration = math.ceil(df_size_in_mb / max_file_size_mb)
        number_of_chunks = int(df_row_count / iteration)

        # print("max_file_size: " + str(max_file_size_mb))
        # print("rows in dataframe: " + str(df_row_count))
        # print("iterations: "+ str(iteration))
        # print("rows per iteration: " + str(number_of_chunks))

        # for idx, chunk in enumerate(np.array_split(output_df, number_of_chunks)):
        #     chunk.to_csv(f'{target_dir}{filename}_{idx}.csv')

        for i, start in enumerate(range(0, df_row_count, number_of_chunks)):
            output_df[start:start+number_of_chunks].to_csv(f'{target_dir}/{filename}_{i}.csv', chunksize=5000)

    return


        
if __name__ == '__main__':

    # get data from data source

    # database_name = 'data_products__oxpos_cohort_3'
    # schema_table_name = '[oxpos_cohort_3].[oxpos_diagnostic_mdt_report]'
    # template_dir = './templates/'
    # template_file = 'mdt-report-template.html'

    # generated_pdf_dir = './generated_pdf/mdt_report/'

    # parsing command line args
    parser = argparse.ArgumentParser()
    parser.add_argument("--db", help="database that data will be extracted from",type=str, required=True)
    parser.add_argument("--schema", help="schema that data will be extracted from",type=str, required=True)
    parser.add_argument("--table", help="table that data will be extracted from", type=str, required=True)
    parser.add_argument("--template_dir", help="path to dir holding template(s)", type=str, required=True)
    parser.add_argument("--template_file", help="template file name", type=str, required=True)
    parser.add_argument("--generate_to_dir", help="dir to generate output file to. Type will depend on function called ", type=str, required=True)

    args = parser.parse_args()
    
    database_name = args.db
    schema_table_name = args.schema + "." + args.table
    template_dir = args.template_dir
    template_file = args.template_file
    generate_to_dir = args.generate_to_dir

    #check if template dir
    
    # Create database connection string - using SQL authentication
    conn = pymssql.connect(server='oxnetdwp02.oxnet.nhs.uk', user='py_login', password='H3bQZf!UmLsG', database=database_name)  

    # create dataframe from data extracted from table
    df = get_data_from_database(conn, schema_table_name)
   
    # # # iterate over dataframe rows presented as a dictionary
    # # added TQDM progress bar
    # for row in tqdm(df.to_dict('records'), desc="Creating PDF: "):
        
    #     # create pdf file content
    #     pdf_content = create_pdf(row, template_dir, template_file)
    #     filename = row['DiagnosticReportIdentifier']

    #     # encode pdf data to base64 string

    #     # write file to disk
    #     save_to_pdf(pdf_content, generated_pdf_dir, filename)

    # data = {'diagnostic_report_identifier':[], 'pdf_data':[]}
    # lst = []


    
    # for col in df.columns:
    #     print(col)

 
    # check if report freetext columns exist in dataframe and perform additional redaction on them
    for i, row in enumerate(df.to_dict('records')):

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
        
        # # perform formatting changes on content for headline_diagnosis table
        #     if 'ProcedureIdentifier_list' in df.columns:
        #         df.at[i, 'ProcedureIdentifier_list'] = regex_reformat(row['ProcedureIdentifier_list')
        # generate PDF content
        pdf_content = create_pdf_content(row, template_dir, template_file)

        # row['AttachmentName'].replace(row['DiagnosticReportIdentifier'] + '.pdf')
        # row['AttachmentContent'].replace(base64.b64encode(str.encode(pdf_content)))
        # row['AttachmentType'].replace('application/pdf')

        # insert PDF content into dataframe col in row (if exists in template)
        if 'AttachmentName' in df.columns:
            df.at[i,'AttachmentName'] = str(row['DiagnosticReportIdentifier']) + '.pdf'

        if 'AttachmentContent' in df.columns:
            df.at[i,'AttachmentContent'] = base64.b64encode(str.encode(pdf_content)).decode()
        
        if 'AttachmentType' in df.columns:
            df.at[i,'AttachmentType'] = 'application/pdf'

        if 'ProcedureIdentifier_list' in df.columns:
            df.at[i,'AttachmentContent'] = base64.b64encode(str.encode(pdf_content)).decode()

        # save_pdf() - generate PDF for each record
        # save_pdf(pdf_content, generate_to_dir, str(row['EffectiveDateTime']))

        # new_row = {'diagnostic_report_identifier': row['DiagnosticReportIdentifier'], 'pdf_data': base64.b64encode(str.encode(pdf_content))}
        # lst.append(base64.b64encode(str.encode(pdf_content)))      


    # df_extended = pd.DataFrame(lst, columns=['pdf_data'])
    # out = pd.concat([df, df_extended])
    # write dataframe to csv

    # renaming columns from extracted dataset. Should be pushed back to data product generation as will save us having to do this here.
    # any name changes have to be reflected in templates as well
    #  Old column name                  | New column name
    #  DiagnosticReportIdentifier       | DiagnosticPrimaryIdentifier
    #  DiagnosticReportIdentifierSystem | DiagnosticPrimaryIdentifierSystem
    #  DiagnosticReportStatus           | PrimaryReportStatus
    df = df.rename(columns={ 'DiagnosticReportIdentifier': 'DiagnosticPrimaryIdentifier',
                             'DiagnosticReportIdentifierSystem': 'DiagnosticPrimaryIdentifierSystem',
                             'DiagnosticReportStatus' : 'PrimaryReportStatus'})

    # print(df['AttachmentContent'])
    # if (schema_table_name == 'oxpos_headline_diagnosis_list'):
    #     for i, row in enumerate(df.to_dict('records')):
            
    # filepath = pathlib.Path('./generated_csv/test3.csv')
    # df.to_csv(filepath, header=True, chunksize=5000 , columns=['SourceOrgIdentifier','SourceSystemIdentifier','PatientPrimaryIdentifier','PatientPrimaryIdentifierSystem'
    #                                                            ,'DiagnosticPrimaryIdentifier' ,'DiagnosticPrimaryIdentifierSystem','PrimaryReportStatus','DiagnosticReportCode',
    #                                                            'DiagnosticReportCodeSystem','DiagnosticReportDisplay','EffectiveDateTime','DiagnosisCategory','DiagnosisCategorySystem',
    #                                                            'DiagnosisCategoryDisplay','ProviderIdentifier','ProviderIdentifierSystem',
    #                                                            'AttachmentName','AttachmentContent',
    #                                                             'AttachmentContentMimeType',
    #                                                            'ResultIdentifier','ResultIdentifierSystem','ConditionIdentifier','ConditionIdentifierSystem',
    #                                                            'ProcedureIdentifier','ProcedureIdentifierSystem','DiagnosticReportCategoryText','ProviderFullName','ConclusionCode','ConclusionCodeSystem',
    #                                                            'ConclusionCodeDisplay','ConclusionText'
    # ])

    columns_list= [  'SourceOrgIdentifier','SourceSystemIdentifier','PatientPrimaryIdentifier','PatientPrimaryIdentifierSystem'
                    ,'DiagnosticPrimaryIdentifier' ,'DiagnosticPrimaryIdentifierSystem','PrimaryReportStatus','DiagnosticReportCode'
                    ,'DiagnosticReportCodeSystem','DiagnosticReportDisplay','EffectiveDateTime','DiagnosisCategory','DiagnosisCategorySystem'
                    ,'DiagnosisCategoryDisplay','ProviderIdentifier','ProviderIdentifierSystem','AttachmentName','AttachmentContent'
                    ,'AttachmentContentMimeType','ResultIdentifier','ResultIdentifierSystem','ConditionIdentifier','ConditionIdentifierSystem'
                    ,'ProcedureIdentifier','ProcedureIdentifierSystem','DiagnosticReportCategoryText','ProviderFullName','ConclusionCode'
                    ,'ConclusionCodeSystem','ConclusionCodeDisplay','ConclusionText' ]


    # monitoring stats
    # print("dataframe memory usage (bytes): " + str(df.memory_usage(deep=True).sum()))
    # b = sys.getsizeof(df)
    # kb = b / 1000
    # KB = kb * 0.976562
    # print("dataframe size of(bytes): " + str(b))
    # print("dataframe size of(kb): " + str(kb))
    # print("dataframe size of(KB): " + str(KB))
    # #convert kilobyte to kibibyte
    # df.info(memory_usage='deep')

    # saving data extracted from database to file(s)
    save_to_delimited_file(df, './generated_csv', str(template_file).removesuffix('-template.html') ,columns_list=columns_list, max_file_size_mb=25)


    # df_subset = df[columns_list]
    # b = sys.getsizeof(df_subset)
    # kb = b / 1000
    # KB = kb * 0.976562
    # print("subset dataframe size of(bytes): " + str(b))
    # print("subset dataframe size of(kb): " + str(kb))
    # print("subset dataframe size of(KB): " + str(KB))
    # #convert kilobyte to kibibyte
    # df_subset.info(memory_usage='deep')


    """
    dataframe occupy more space in memory vs when persisted on disk. 
    attempting to do sizing to help chunk up a large dataset using it's in-mem size value rather than the size on disk. 
    however this will mean that the exported files will always be smaller than if in memory
    """
    # df_huge = pd.DataFrame(np.random.randint(0, 100, size=(10000000, 5)))
    # df_huge = df_huge.rename(columns={i:f"x_{i}" for i in range(5)})
    # df_huge["category"] = ["A", "B", "C", "D"] * 2500000

    # b = sys.getsizeof(df_huge)
    # kb = b / 1000
    # KB = kb * 0.976562
    # print("df_huge dataframe size of(bytes): " + str(b))
    # print("df_huge dataframe size of(kb): " + str(kb))
    # print("df_huge dataframe size of(KB): " + str(KB))
    # #convert kilobyte to kibibyte
    # df_huge.info(memory_usage='deep')
          


    # df_huge.to_csv(f'huge_df_test.csv')


