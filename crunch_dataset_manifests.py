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

    yaml_config = './dp_cig_101.yaml'
    # yaml_config = './dp_cig_101_manual.yaml'

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
            print('Crunching manifests...')


            common_df = None

            for d in datasets:
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

                    print(run_id)
                    print(f'{d}') 
                    # TODO - add in some handling if file doesn't exist. Export might not have happened since dataset was empty
                    # right now manually ignoring file in config yaml file
                    df = pl.read_csv(f'./diff_exported/{d}/{run_id}_manifest._{d}.csv', try_parse_dates=False)
                    # commom_df.join(df, 'salted_master_patient_id', "")
                    if common_df is None:
                        common_df = df
                    else:
                        common_df.extend(df)
                    
                    # cursor.commit()
                    # else:
                    #     print(f"Dataset {d} has been previously exported for run {run_id}")     
                else:
                    #unknown case. too many rows have been returned
                    raise Exception(f"Too many rows returned by build.get_latest_run_details for {d} ");

            # crunched manifests
            subset = common_df.unique(subset=["salted_master_patient_id","patient"])   
            print(subset)

            # existing patients subset
            existing_pt = subset.filter(pl.col('patient') == 'existing').select('salted_master_patient_id', 'patient')   
            print(existing_pt)

            # new patients subject   
            new_pt = subset.filter(pl.col('patient') == 'new').select('salted_master_patient_id', 'patient')                 
            print( new_pt)

            # OUTER joining the two subsets
            test_join = existing_pt.join(new_pt, 'salted_master_patient_id', how='outer')
            print(test_join)

            # loading crunched manifest into diff schema of database
            engine = utility.get_sql_alchemy_engine(server, database, driver)
            with engine.begin() as conn:
                # if not diff_data.is:
                test_join.write_database(table_name=f'diff.manifest',connection=url, if_exists='replace')
                
                conn.commit()
                print('finished writing commit')

        except yaml.YAMLError as exc:
            print(exc)