import main

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
        # if new_text != text:
            
            # print("corrected: " + new_text)
            # print("manual cleaning triggered")
    return new_text

def manual_report_fields(df):
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

if __name__ == '__main__':

    # generate PDFs for reports in navify 

    # Create database connection string - using SQL authentication
    conn = pymssql.connect(server='oxnetdwp02.oxnet.nhs.uk', user='py_login', password='H3bQZf!UmLsG', database=database_name)  

    # pathology
    database_name = 'data_products__oxpos_cohort_3'
    schema_table_name = 'oxpos_cohort_3.vw_oxpos_headline.list'
    template_dir = './templates'
    template_file = 'headline-diagnosis-template.html'
    generate_to_dir = './generated_pdf'


    columns_list=[   'SourceOrgIdentifier','SourceSystemIdentifier','PatientPrimaryIdentifier','PatientPrimaryIdentifierSystem'
                    ,'DiagnosticPrimaryIdentifier' ,'DiagnosticPrimaryIdentifierSystem','PrimaryReportStatus','DiagnosticReportCode'
                    ,'DiagnosticReportCodeSystem','DiagnosticReportDisplay','EffectiveDateTime','DiagnosisCategory','DiagnosisCategorySystem'
                    ,'DiagnosisCategoryDisplay','ProviderIdentifier','ProviderIdentifierSystem','AttachmentName','AttachmentContent'
                    ,'AttachmentContentMimeType','ResultIdentifier','ResultIdentifierSystem','ConditionIdentifier','ConditionIdentifierSystem'
                    ,'ProcedureIdentifier','ProcedureIdentifierSystem','DiagnosticReportCategoryText','ProviderFullName','ConclusionCode'
                    ,'ConclusionCodeSystem','ConclusionCodeDisplay','ConclusionText' ]
    


    # create dataframe from data extracted from table
    df = main.get_data_from_database(conn, schema_table_name)
    manual_report_fields(df)

    
