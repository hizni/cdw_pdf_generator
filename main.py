import pymssql
import pandas as pd
import jinja2
import pdfkit
from tqdm import tqdm

# generate pdf
def create_pdf(template_vars, templates_dir, template_file):

    env = jinja2.Environment(loader=jinja2.FileSystemLoader(templates_dir))
    template = env.get_template(template_file)

    #template vars are passed as a dictionary from a row in the retrieved record set
    html_out = template.render(template_vars)

    return html_out


def save_pdf(file_content, path, filename):
    try:
        # with open('your_pdf_file_here.pdf', 'wb+') as file:
        #     file.write(file_content)

        config = pdfkit.configuration(wkhtmltopdf='/usr/local/bin/wkhtmltopdf')

        output_pdf_path_and_file = path + filename + '.pdf'
        
        pdfkit.from_string(file_content, output_pdf_path_and_file, configuration=config)

    except Exception as error:
        # logging.error(f'Error saving file to disc. Error: {error}')
        print(f'Error saving file to disc. Error: {error}')
        raise error
        
        
if __name__ == '__main__':

    # get data from data source

    database_name = 'data_products__oxpos_cohort_3'
    schema_table_name = '[oxpos_cohort_3].[oxpos_diagnostic_mdt_report]'
    template_dir = './templates/'
    template_file = 'mdt-report-template.html'

    generated_pdf_dir = './generated_pdf/mdt_report/'

    # Create database connection string - using SQL authentication
    conn = pymssql.connect(server='oxnetdwp02.oxnet.nhs.uk', user='py_login', password='H3bQZf!UmLsG', database=database_name)  

    cursor = conn.cursor()  
    cursor.execute('SELECT * FROM ' + schema_table_name)

    # get columns returned
    columns = [ x[0] for x in cursor.description]
    # get rows returned
    rows = cursor.fetchall()

    # create dataframe
    df = pd.DataFrame(rows, columns=columns)
   
    # # iterate over dataframe rows presented as a dictionary
    # added TQDM progress bar
    for row in tqdm(df.to_dict('records'), desc="Creating PDF: "):
        
        pdf_file = create_pdf(row, template_dir, template_file)
        filename = row['DiagnosticReportIdentifier']

        save_pdf(pdf_file, generated_pdf_dir, filename)
        