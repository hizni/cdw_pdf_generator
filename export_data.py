import utility
import yaml
import polars as pl

if __name__ == '__main__':
    server = 'oxnetdwp04'
    database = 'cig_101_test'

    # create connection
    conn = utility.get_db_connection(server , database )
    cursor = conn.cursor()

    # yaml_config = './dp_cig_101.yaml'
    yaml_config = './dp_cig_101_manual.yaml'
    # yaml_config = './oxpos_export.yaml'

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
            #TODO - store config somewhere.Possibly in build.record_run_details??
            print(current_config)

            for d in datasets:
                print(f'Exporting latest dataset for {d}')        

                # print('=== output 2 : get latest run details ====')
                sql = f'exec build.get_latest_run_details ?'
                params = (d)
                build_details = cursor.execute(sql, params)

                # get column names from description
                columns = [column[0] for column in build_details.description]
                
                results = []
                for row in build_details.fetchall():
                    results.append(dict(zip(columns, row)))
               
                # 0 to 1 rows will be returned
                # 0 rows - no data has been populated for this dataset before. This will throw and error
                # 1 row - check if dataset has been previously exported. No need to repeat the action
                cursor.commit()
                row_count = len(results)
                # print(f'{row_count} previous exports found for {d} dataset')

                # print(df.head())
                
                if row_count == 0:
                   raise Exception(f"Dataset {d} has not been previously populated");
                elif row_count == 1:
                    # row = df.get
                    dataset_name = results[0].get('dataset')
                    build_id = results[0].get('build_id')
                    run_id = results[0].get('run_id')
                    was_exported = results[0].get('was_exported')

                    #TODO - check if file exists already. could have been exported by someone else on a different machine!!!
                    # if was_exported == 0:
                    utility.save_to_delimited_file(utility.get_data_from_database(conn, f'data.{d}'), f'./raw_exported/{d}' , f'{run_id}._{d}', sub_dir_by_date=False)

                    sql = f'exec build.update_export_for_run ?,?,?'
                    params = (build_id, run_id, dataset_name)
                    build_details = cursor.execute(sql, params)
                    
                    cursor.commit()
                    # else:
                    #     print(f"Dataset {d} has been previously exported for run {run_id}")     
                else:
                    #unknown case. too many rows have been returned
                    raise Exception(f"Too many rows returned by build.get_latest_run_details for {d} ");
                                             
        except yaml.YAMLError as exc:
            print(exc)

