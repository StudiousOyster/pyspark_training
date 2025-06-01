
import os, sys 

BASE = os.path.dirname(os.path.abspath(__file__))
parent_folder = os.path.join(BASE, "..")

sys.path.append(parent_folder)


from user_functions.config_reader import read_config
from user_functions.column_matching import match_columns
from user_functions.count_validation import count_check
from user_functions.data_validation import data_check
from user_functions.duplicate_validation import duplicate_check
from user_functions.null_validation import null_check

import pyspark
from pyspark.sql import SparkSession
import os
import re


os.environ['JAVA_HOME'] = "C:/Program Files/Java/jdk-11"

config_folder = r"C:\Users\chipp\OneDrive\Desktop\Training Folder\Pyspark_Training\pyspark_training\testing_framework\config"
config_file = 'config.csv'

spark = SparkSession.builder.appName('validation').master("local").getOrCreate()


configs = read_config(spark, config_folder, config_file)

for row in configs:
    src_path = row['src_folder_path']
    src_file_name = row['src_file_name']
    tgt_path = row['tgt_folder_path']
    tgt_file_name = row['tgt_file_name']
    key_column = row['key_column']
    null_col = [col.strip() for col in row['null_check_col'].split(',')]
    
    src_full_path = f'{src_path}\{src_file_name}'
    tgt_full_path = f'{tgt_path}\{tgt_file_name}'
    
    src_df = spark.read.format('csv') \
                .option('header', True)\
                .option('inferSchema', True)\
                .load(src_full_path)
                
    tgt_df = spark.read.format('csv') \
                .option('header', True)\
                .option('inferSchema', True)\
                .load(tgt_full_path)
    
    # parquet file --> tgt_df = spark.read.parquet('filepath')
    
    # creating a dictionary to store the old and new column names
    src_clean_cols = {}
    tgt_clean_cols = {}
    
    # cleaning the column names of both dfs so that they would be the same
    for col in src_df.columns:
        clean_col = re.sub(r'[^a-zA-Z0-9]', '', col.strip().lower())
        src_clean_cols[col] = clean_col
        
    for col in tgt_df.columns:
        clean_col = re.sub(r'[^a-zA-Z0-9]', '', col.strip().lower())
        tgt_clean_cols[col] = clean_col
        
    # renaming the df with the cleaned col names
    for old_col, new_col in src_clean_cols.items():
        src_df = src_df.withColumnRenamed(
            old_col,
            new_col
        )
        
    for old_col, new_col in tgt_clean_cols.items():
        tgt_df = tgt_df.withColumnRenamed(
            old_col,
            new_col
        )
    
    print('\n')
    print(f'Column mismatch testing between the src - {src_file_name} and the tgt - {tgt_file_name}')
    match_columns(spark, src_df, tgt_df)
    print("*****************************************************************************************")
    
    print('\n')
    print(f'Null check validation results between the source: {src_file_name} and the target: {tgt_file_name}')
    null_check(spark, src_df, tgt_df, null_col)
    print("*****************************************************************************************")
    
    print('\n')
    print(f'Count validation results between the source: {src_file_name} and the target: {tgt_file_name}')
    count_check(spark, src_df, tgt_df)
    print("*****************************************************************************************")
    
    print('\n')
    print(f'Duplcate records check between the source: {src_file_name} and the target: {tgt_file_name}')
    duplicate_check(spark, src_df, tgt_df, key_column)
    print("*****************************************************************************************")
    
    print('\n')
    print(f'Data validation results between the source: {src_file_name} and the target: {tgt_file_name}')
    data_check(spark, src_df, tgt_df)
    print("*****************************************************************************************")