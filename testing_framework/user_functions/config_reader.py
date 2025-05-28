import pyspark
from pyspark.sql import SparkSession
import os

def read_config (spark: SparkSession, config_folder, config_file):
    full_path = os.path.join(config_folder, config_file)
    
    if config_file.endswith('.csv'):
        df_config = spark.read.format('csv') \
                .option('header', True) \
                .load(full_path)
                
    # config_rows = [row.asDict() for row in df_config.collect()]
    
    return df_config.collect()
    
    
