import utility
import yaml
import polars as pl

if __name__ == '__main__':
    server = 'oxnetdwp04'
    database = 'cig_101_test'

    # create connection
    conn = utility.get_db_connection(server , database )
    cursor = conn.cursor()

    yaml_config = './dp_cig_101.yaml'

# iterating over list of datasets in yaml file to generate output to CSVs
    with open(yaml_config, "r") as stream:
        try:
            # getting data from data prduct yaml config
            config = yaml.safe_load(stream)
            data_from = config['data_from'] 
            data_till = config['data_till']
            datasets = config['datasets']

#         print("Generating data product with following parameters")
            
            # getting current config details
            selected_datasets = ','.join(datasets)
            current_config = 'data_from:' + str(data_from) + ';data_till:' + str(data_till) + 'datasets:' + selected_datasets
            print(current_config)

            for d in datasets:
                print("Exporting latest dataset for..." + d)        

                # print('==== step 1 : export data ====')
                sql = f'exec build.get_latest_run_details ?'
                params = (d)
                build_details = cursor.execute(sql, params)
                df = pl.DataFrame(build_details)
                # print('=== output 0 : populate inpat spells dataset ====')
                utility.save_to_delimited_file(utility.get_data_from_database(conn, f'data.{d}'), f'./generated/{d}' , f'{{{{timestamp}}}}._{d}', sub_dir_by_date=False)
        except yaml.YAMLError as exc:
            print(exc)

