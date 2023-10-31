
import utility
import yaml
import polars as pl

if __name__ == '__main__':
    server = 'oxnetdwp04'
    database = 'cig_101_test'
    #TODO - add UI feedback to users 
    #TODO - collect opt-out statistics
    # create connection
    conn = utility.get_db_connection(server , database )
    cursor = conn.cursor()

    sql = f'exec cohort.load_all_cohort'
    cursor.execute(sql)
    cursor.commit()