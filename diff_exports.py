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
            datasets = config['datasets']

            for d in datasets:
                print("Diffing last 2 exports (if available) for {d} dataset");

                sql = f'exec build.get_prev_successful_run_details ?'
                params = (d)
                build_details = cursor.execute(sql, params) 

                # get column names from description
                columns = [column[0] for column in build_details.description]
                
                results = []
                for row in build_details.fetchall():
                    results.append(dict(zip(columns, row)))

                cursor.commit()
                row_count = len(results)

                if row_count == 0:
                   raise Exception(f"Dataset {d} has not been previously exported");
                elif row_count == 1:
                    # only one export has been taken - no diff can be performed
                    dataset_name = results[0].get('dataset')
                    build_id = results[0].get('build_id')
                    run_id = results[0].get('run_id')
                    was_exported = results[0].get('was_exported')

                    latest_file = f'./raw_exported/{dataset_name}/{run_id}._{dataset_name}.csv'
                    print(f'Latest export for {d} dataset is: {latest_file}')

                    # diff_data, manifest = utility.compute_dataset_diff_data(None, latest_file, 'salted_master_patient_id', 'source_spell_id', 'result')
                    # print(diff_data.head())
                    # print(manifest.head())
                    diff_data = utility.compute_dataset_diff_data(None, latest_file, 'salted_master_patient_id', 'source_spell_id', 'result')
                    print(diff_data.head())
                    
                    # diff_data, manifest = perform_extract_diff(None, latest_file)
                    utility.save_to_delimited_file(diff_data, f'./diff_exported/{d}' , f'{run_id}_diff._{d}', sub_dir_by_date=False)
                    # utility.save_to_delimited_file(diff_data, f'./diff_exported/{d}' , f'{run_id}_manifest._{d}', sub_dir_by_date=False)

                elif row_count == 2:
                    dataset_name = results[0].get('dataset')
                    build_id = results[0].get('build_id')
                    run_id = results[0].get('run_id')
                    was_exported = results[0].get('was_exported')

                    latest_file = f'./raw_exported/{dataset_name}/{run_id}._{dataset_name}.csv'

                    dataset_name = results[1].get('dataset')
                    build_id = results[1].get('build_id')
                    run_id = results[1].get('run_id')
                    was_exported = results[1].get('was_exported')

                    previous_file = f'./raw_exported/{dataset_name}/{run_id}._{dataset_name}.csv'     

                    print(f'Latest export for {d} dataset is: {latest_file}')
                    print(f'Previous export for {d} dataset is: {previous_file}')

                    # diff_data, manifest = utility.compute_dataset_diff_data(previous_file, latest_file, 'salted_master_patient_id', 'source_spell_id', 'result')
                    # print(diff_data.head())
                    # print(manifest.head())

                    diff_data = utility.compute_dataset_diff_data(previous_file, latest_file, 'salted_master_patient_id', 'source_spell_id', 'result')
                    print(diff_data.head())


                    utility.save_to_delimited_file(diff_data, f'./diff_exported/{d}' , f'{run_id}_diff._{d}', sub_dir_by_date=False)
                    # utility.save_to_delimited_file(diff_data, f'./diff_exported/{d}' , f'{run_id}_manifest._{d}', sub_dir_by_date=False)
                else:
                    raise Exception(f"Too many rows returned by build.get_prev_successful_run_details for {d} ");
                    

                    

        except yaml.YAMLError as exc:
            print(exc)