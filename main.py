import pymssql
import pandas as pd
import jinja2
import pdfkit

# generate pdf
def create_pdf(template_vars, templates_dir, template_file):

    env = jinja2.Environment(loader=jinja2.FileSystemLoader(templates_dir))
    template = env.get_template(template_file)

    #template vars are passed as a dictionary from a row in the retrieved record set
    html_out = template.render(template_vars)

    # env = jinja2.Environment(loader=jinja2.FileSystemLoader('./templates/'))
    # template = env.get_template('name.txt')
    # html_out = template.render(name='hizni')

    # file_content = pdfkit.from_string(
    #     html_out,
    #     False,
    #     options='here_a_dict_with_special_page_properties',
    #     css='here_your_css_file_path' # its a list e.g ['my_css.css', 'my_other_css.css']
    # )
    # print(html_out)
    return html_out


def save_pdf(file_content, path, filename):
    try:
        # with open('your_pdf_file_here.pdf', 'wb+') as file:
        #     file.write(file_content)

        config = pdfkit.configuration(wkhtmltopdf='/usr/local/bin/wkhtmltopdf')

        output_pdf_path_and_file = path + '/' + filename + '.pdf'
        pdfkit.from_string(file_content, output_pdf_path_and_file, configuration=config)

    except Exception as error:
        # logging.error(f'Error saving file to disc. Error: {error}')
        print(f'Error saving file to disc. Error: {error}')
        raise error
        
        
if __name__ == '__main__':

    # get data from data source
    conn = pymssql.connect(server='oxnetdwp02.oxnet.nhs.uk', user='py_login', password='H3bQZf!UmLsG', database='data_products__oxpos_cohort_3')  
    cursor = conn.cursor()  
    cursor.execute('SELECT * FROM [oxpos_cohort_3].[oxpos_diagnostic_mdt_report]')

    template_dir = './templates/'
    template_file = 'mdt-report-template.html'
    names = [ x[0] for x in cursor.description]
    rows = cursor.fetchall()
    df = pd.DataFrame(rows, columns=names)
   
    recs = df.to_dict('records')

    count = 0
    for row in df.to_dict('records'):
        
        pdf_file = create_pdf(row, template_dir, template_file)
        filename = row['DiagnosticReportIdentifier']
        print(f"creating PDF: {filename}.pdf")
        save_pdf(pdf_file, './generated_pdf', filename)
        count=count+1