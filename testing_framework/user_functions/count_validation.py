import pyspark
from pyspark.sql import SparkSession

def count_check(spark: SparkSession, src_df, tgt_df):
    src_count = src_df.count()
    tgt_count = tgt_df.count()
    
    if src_count == tgt_count:
        print('Data count is a match')
    else:
        print("Data count mismatch. Further investigation required")
        print(f"Data count in Source: {src_count} and data count in Target: {tgt_count}")
        
    # to check the entire rows that are missing from tgt and not in source and also vice versa
    # mismatched_complete = src_df.subtract(tgt_df).union(
    #     tgt_df.subtract(src_df)
    # )
    
    # # to get the count of these mismatched rows
    # if mismatched_complete.count() > 0:
    #     print(f'The number of complete rows that are mismatched are: {mismatched_complete.count()}')
        
        