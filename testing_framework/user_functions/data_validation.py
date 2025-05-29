import pyspark
from pyspark.sql import SparkSession

def data_check(spark: SparkSession, src_df, tgt_df):
    #  excluded columns if any 
    # excluded_columns = ['']
    
    #  subtract() do not consider duplicates because it evaluates using a set(). so if there are duplicates in src or tgt, subtract would not result in a difference. 
    # Just like except in SQL.
    
    # data in src but not in target
    df_missing_tgt = src_df.subtract(tgt_df)
    df_missing_src = tgt_df.subtract(src_df)
    
    missing_values = {
        'Missing in Target': df_missing_tgt,
        'Missing in Source': df_missing_src
    }
    
    for name, df in missing_values.items():
        if df.count() > 0:
            print(f'{name} records found. {df.count()} records are missing. Sample of missing records are displayed below:')
            df.show(5)
        else:
            print('No records missing')
    