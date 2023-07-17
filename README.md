# cdw_pdf_generator
PDF generator for datasets from CDW

A simple tool that creates PDFs from data retrieved by querying named tables in the data product.

The templates for the PDF are defined in HTML and make use of the Jinja templating engine. The PDFs are generated using the command line utility `wkhtmltopdf`.

This can be installed using pre-built packages from https://wkhtmltopdf.org/downloads.html
Or it can be installed using the Brew package: `brew install wkhtmltopdf`

