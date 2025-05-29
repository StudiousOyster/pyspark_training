
# import os, sys 
# sys.path.append('./')
from user_functions.config_reader import read_config
from user_functions.column_matching import match_columns
from user_functions.count_validation import count_check

import pyspark
from pyspark.sql import SparkSession
import os


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
    
    print(f'Column mismatch testing between the src - {src_file_name} and the tgt - {tgt_file_name}')
    match_columns(spark, src_df, tgt_df)
    
    print(f'Count validation results between the source: {src_file_name} and the target: {tgt_file_name}')
    count_check(spark, src_df, tgt_df)
    
    print(f'Data validation results between the source: {src_file_name} and the target: {tgt_file_name}')
    
    
