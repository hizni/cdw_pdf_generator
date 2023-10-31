import utility
import yaml
import polars as pl


if __name__ == '__main__':
    server = 'oxnetdwp04'
    database = 'cig_101_test'
    driver = "ODBC+Driver+18+for+SQL+Server"

    url = utility.get_sql_alchemy_url(server, database, driver)

    # create connection
    conn = utility.get_db_connection(server , database )
    cursor = conn.cursor()

    yaml_config = './dp_cig_101_diff.yaml'
    # yaml_config = './dp_cig_101_manual_diff.yaml'

    # iterating over list of datasets in yaml file to generate output to CSVs
    with open(yaml_config, "r") as stream:
        try:
            # getting data from data prduct yaml config
            config = yaml.safe_load(stream)
            data_from = config['data_from'] 
            data_till = config['data_till']
            datasets = config['datasets']

            # building diff datasets and manifest by dataset
            # YAML produces a dictionary of dictionaries of the data held in the dataset node
            for dict_item in datasets:
                for key in dict_item:
                    dataset = key
                    group_by_col = dict_item[key]["group_by_col"]
                    group_on_col = dict_item[key]["group_on_col"]
                    

            # for d in datasets:
            #     print(f"Diffing last 2 exports (if available) for {d} dataset");
        
                    sql = f'exec build.get_prev_successful_exported_run_details ?'
                    params = (dataset)
                    build_details = cursor.execute(sql, params) 

                    # get column names from description
                    columns = [column[0] for column in build_details.description]
                
                    results = []
                    for row in build_details.fetchall():
                        results.append(dict(zip(columns, row)))

                    cursor.commit()
                    row_count = len(results)

                    if row_count == 0:
                        raise Exception(f'Dataset {dataset} has not been previously exported');
                    elif row_count == 1:
                        # only one export has been taken - no diff can be performed
                        dataset_name = results[0].get('dataset')
                        build_id = results[0].get('build_id')
                        run_id = results[0].get('run_id')
                        was_exported = results[0].get('was_exported')

                        latest_file = f'./raw_exported/{dataset_name}/{run_id}._{dataset_name}.csv'
                        print(f'Latest export for {dataset} dataset is: {latest_file}')

                        # diff_data, manifest = utility.compute_dataset_diff_data(None, latest_file, 'salted_master_patient_id', 'source_spell_id', 'result')
                        # print(diff_data.head())
                        # print(manifest.head())
                        # diff_data = utility.compute_dataset_diff_data(None, latest_file, 'salted_master_patient_id', 'source_spell_id', 'result')
                        diff_data, manifest = utility.compute_dataset_diff_data(None, latest_file, group_by_col, group_on_col)
                        
                    #     # diff_data, manifest = perform_extract_diff(None, latest_file)
                        utility.save_to_delimited_file(diff_data, f'./diff_exported/{dataset_name}' , f'{run_id}_diff._{dataset_name}', sub_dir_by_date=False)
                        utility.save_to_delimited_file(manifest, f'./diff_exported/{dataset_name}' , f'{run_id}_manifest._{dataset_name}', sub_dir_by_date=False)

                        engine = utility.get_sql_alchemy_engine(server, database, driver)
                        with engine.begin() as conn:
                            # if not diff_data.is:
                            diff_data.write_database(table_name=f'diff.{dataset_name}',connection=url, if_exists='replace')

                    elif row_count == 2:
                        dataset_name = results[0].get('dataset')
                        latest_build_id = results[0].get('build_id')
                        latest_run_id = results[0].get('run_id')
                        latest_was_exported = results[0].get('was_exported')

                        latest_file = f'./raw_exported/{dataset_name}/{latest_run_id}._{dataset_name}.csv'

                        dataset_name = results[1].get('dataset')
                        earlier_build_id = results[1].get('build_id')
                        earlier_run_id = results[1].get('run_id')
                        earlier_was_exported = results[1].get('was_exported')

                        previous_file = f'./raw_exported/{dataset_name}/{earlier_run_id}._{dataset_name}.csv'     

                        print(f'Latest export for {dataset_name} dataset is: {latest_file}')
                        print(f'Previous export for {dataset_name} dataset is: {previous_file}')

                        diff_data, manifest = utility.compute_dataset_diff_data(previous_file, latest_file, group_by_col, group_on_col)

                        utility.save_to_delimited_file(diff_data, f'./diff_exported/{dataset_name}' , f'{latest_run_id}_diff._{dataset_name}', sub_dir_by_date=False)
                        utility.save_to_delimited_file(manifest, f'./diff_exported/{dataset_name}' , f'{latest_run_id}_manifest._{dataset_name}', sub_dir_by_date=False)

                    else:
                        raise Exception(f"Too many rows returned by sproc build.get_prev_successful_run_details() for {dataset_name} ");
                    

            # engine = utility.get_sql_alchemy_engine(server, database, driver)
            # with engine.begin() as conn:
            #     for dict_item in datasets:
            #         for key in dict_item:
            #             dataset = key      
            
            conn.close

        except yaml.YAMLError as exc:
            print(exc)