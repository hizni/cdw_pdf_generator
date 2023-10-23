import utility
import yaml

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
            datasets = config['datasets']

            for d in datasets:
                print("Diffing last 2 exports (if available) for..." + d)    
                    

        except yaml.YAMLError as exc:
            print(exc)