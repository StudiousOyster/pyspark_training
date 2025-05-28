
import pyspark
from pyspark.sql import SparkSession
from user_functions import config_reader 

config_folder = r"C:\Users\chipp\OneDrive\Desktop\Training Folder\Pyspark_Training\pyspark_training\testing_framework\config"
config_file = 'config.csv'

spark = SparkSession.builder.appName('validation').getOrCreate()

configs = config_reader.read_config(spark, config_folder, config_file)

print(configs)

# for row in configs:
#     source_folder_name = row['source_folder_path']
#     source_table_name = row['src_file_name']
#     tgt_folder_name = row['tgt_folder_path']
#     tgt_table_name = row['tgt_file_name']
#     primary_key = row['key_column']
    

#     source_path = f'{source_folder_name}/{source_table_name}'
#     tgt_path = f'{tgt_folder_name}/{tgt_table_name}'
    
    
    
    
