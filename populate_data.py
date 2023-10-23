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

            # getting current config details
            selected_datasets = ','.join(datasets)
            current_config = 'data_from:' + str(data_from) + ';data_till:' + str(data_till) + 'datasets:' + selected_datasets
            print(current_config)
            
            for d in datasets:
                print("Loading..." + d)
                # # print('=== step 1 : populate dataset ====')
                # sql = f'exec data.load_data_{d} ?,?'
                # params = (data_from, data_till)
                # cursor.execute(sql, params)
                # conn.commit()

                # cursor.cancel()  
        except yaml.YAMLError as exc:
            print(exc)