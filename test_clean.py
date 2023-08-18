from unittest import TestCase
from clean import get_db_connection, save_to_delimited_file
import pyodbc
import pandas as pd
import numpy as np
import sys
class TryTesting(TestCase):
    # def test_always_passes(self):
    #     self.assertTrue(True)

    # def test_always_fails(self):
    #     self.assertTrue(False)

    # test gettng db_connection
    # this will fail if cannot create a database connection object. Can fail due to:
    #   - a kerberos ticket hasn't been created before running the test
    #   - cannot resolve server
    def test_get_db_connection(self):
        try:
            server = 'oxnetdwp02'
            database = 'data_products__oxpos_cohort_3'

            self.assertRaises(pyodbc.OperationalError, get_db_connection(server , database))
            #clean.get_db_connection
        except pyodbc.OperationalError as odbcErr:
            assert True , f'Exception {odbcErr} when creating db connection'

    # test creating a delimited file from a data frame, where target dir does not exist
    def test_create_delimited_file_target_not_exist(self):
        try:
            # create a dummy dataframe. This would normally be created by a pyodbc query on the database
            df_huge = pd.DataFrame(np.random.randint(0, 100, size=(100, 5)))
            
            self.assertIsNotNone(df_huge)

            columns_list=[   'SourceOrgIdentifier','SourceSystemIdentifier','PatientPrimaryIdentifier', ]
            save_to_delimited_file(df_huge, './test-generated', 'test' ,columns_list=columns_list, max_file_size_mb=25, timestamp_file=True)


        except NotADirectoryError as ex:
            assert True, f'target dir does not exist: {ex}'

    def test_create_delimited_file_target_exist_no_column_list_check(self):
        try:
            # create a dummy dataframe. This would normally be created by a pyodbc query on the database
            df_huge = pd.DataFrame(np.random.randint(0, 100, size=(100, 5)))
            
            self.assertIsNotNone(df_huge)

            save_to_delimited_file(df_huge, './generated', 'test' , max_file_size_mb=25, timestamp_file=True)

        except NotADirectoryError as ex:
            assert False, f'target dir does not exist: {ex}'

    def test_create_delimited_file_target_exist_no_column_list_not_exist(self):
        try:
            # create a dummy dataframe. This would normally be created by a pyodbc query on the database
            df_huge = pd.DataFrame(np.random.randint(0, 100, size=(100, 5)))
            
            self.assertIsNotNone(df_huge)

            columns_list=[ 'A','B','C' ]
            save_to_delimited_file(df_huge, './generated', 'test' , columns_list=columns_list, max_file_size_mb=25, timestamp_file=True)

        except ValueError as ex:
            assert True, f'value error: {ex}'

    def test_create_delimited_file_multiple_files(self):
        try:
            # create a dummy dataframe. This would normally be created by a pyodbc query on the database
            df_huge = pd.DataFrame(np.random.randint(0, 10000000, size=(1000000, 5)))
            df_size_in_bytes = sys.getsizeof(df_huge)
            df_size_in_mb = (df_size_in_bytes / 1000000)

            
            print(f'df_size_in_mb {df_size_in_mb}')

            self.assertIsNotNone(df_huge)

            save_to_delimited_file(df_huge, './generated', 'test2' , max_file_size_mb=2, timestamp_file=True)

        except NotADirectoryError as ex:
            assert False, f'target dir does not exist: {ex}'
