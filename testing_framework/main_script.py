
# import os, sys 
# sys.path.append('./')
from user_functions.config_reader import read_config

import pyspark
from pyspark.sql import SparkSession
import os


os.environ['JAVA_HOME'] = "C:/Program Files/Java/jdk-11"

config_folder = r"C:\Users\chipp\OneDrive\Desktop\Training Folder\Pyspark_Training\pyspark_training\testing_framework\config"
config_file = 'config.csv'

spark = SparkSession.builder.appName('validation').getOrCreate()


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
                
    tgt_df = spark.read.parquet(tgt_full_path)
    
    



# for row in configs:
#     source_folder_name = row['source_folder_path']
#     source_table_name = row['src_file_name']
#     tgt_folder_name = row['tgt_folder_path']
#     tgt_table_name = row['tgt_file_name']
#     primary_key = row['key_column']
    

#     source_path = f'{source_folder_name}/{source_table_name}'
#     tgt_path = f'{tgt_folder_name}/{tgt_table_name}'
    
    
    
    
