import pyspark
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

def null_check(spark: SparkSession, src_df, tgt_df, null_col):
    # here we are looping through the columns from null_col and then doing a case statement and assigning 1 to where the condition matches amd then taking the sum.
    # we are also giving the alias to thie sum() as the same name as the column, c, from the loop so when the data is displayed, it is readable. since
    # we are returning only one row here, we could use collect()[0] because we are looping through out the src df
    src_null_counts_dict = src_df.select(
        [
            F.sum(
                F.when(F.col(c).isNull(), 1).otherwise(0)
                ).alias(c)
            for c in null_col   
        ]
    ).collect()[0].asDict()
    
    tgt_null_counts_dict = tgt_df.select(
        [
            F.sum(
                F.when(F.col(c).isNull(), 1).otherwise(0)
                ).alias(c)
            for c in null_col   
        ]
    ).collect()[0].asDict()
    
    #  filtering only those columns which actually have null values
    src_null_filter = {col_name:count for col_name, count in src_null_counts_dict.items() if count > 0}
    tgt_null_filter = {col_name:count for col_name, count in tgt_null_counts_dict.items() if count > 0}
    
    if src_null_filter:
        print('Source has NULL values in the not null columns. Below is the column name and the respective null value counts')
        print(src_null_filter)
    else:
        print('Source do not have any NULL values in the not null columns')
        
    
    if tgt_null_filter:
        print('Target has NULL values in the not null columns. Below is the column name and the respective null value counts')
        print(tgt_null_filter)
    else:
        print('Target do not have any NULL values in the not null columns')
    