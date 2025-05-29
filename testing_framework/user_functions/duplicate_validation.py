import pyspark
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

def duplicate_check(spark: SparkSession, src_df, tgt_df, key_column):
    duplicate_src = src_df.groupBy(
        key_column
        ).agg(
            F.count('*').alias('Rec_Count')
            ).filter(
                F.col('Rec_Count') > 1
                )
            
    duplicate_tgt = tgt_df.groupBy(
        key_column
        ).agg(
            F.count('*').alias('Rec_Count')
            ).filter(
                F.col('Rec_Count') > 1
                )
            
    if duplicate_src.count() > 0:
        print(f'{duplicate_src.count()} duplicate records fround in Source. A sample of duplicate records are displayed below:')
        duplicate_src.show(5)
    else:
        print('No duplicates found in Source')
        
    if duplicate_tgt.count() > 0:
        print(f'{duplicate_tgt.count()} duplicate records fround in Target. A sample of duplicate records are displayed below:')
        duplicate_tgt.show(5)
    else:
        print('No duplicates found in Target')