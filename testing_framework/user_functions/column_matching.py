
import pyspark
from pyspark.sql import SparkSession

def match_columns(spark: SparkSession, src_df, tgt_df):
    # to create a list of columns that may be excluded from testing - like newid() columns or hashkeys
    excluded_cols_src = ['TransactionDt']
    excluded_cols_tgt = ['TransactionDt']
    
    #  to make sure that everything is in lower case before comparison
    excluded_cols_src = [col.strip().lower() for col in excluded_cols_src]
    excluded_cols_tgt = [col.strip().lower() for col in excluded_cols_tgt]
    
    # create the final list of columns for both src and tgt
    src_cols = [col.strip().lower() for col in src_df.columns if col.strip().lower() not in excluded_cols_src]
    # tgt_cols = [col.strip().lower() for col in tgt_df.columns if col.strip().lower() not in excluded_cols_tgt]
    tgt_cols = ['cinemacode', 'movieid', 'cinemaname', 'ticketcount', 'ticketprice', 'transactionid', 'transactiondt']
    
   
    if src_cols == tgt_cols:
        print('The number and the position of columns are a match between the Src and the Tgt')
        # return True # this can be done if we are using pytest or other framework and for assertions in the main file
    else:
        print('The number or the position of the columns are a mismatch between the Src and the Tgt. Needs further checking.')
        
    mismatched = []
    
    for idx, (src_col, tgt_col) in enumerate(zip(src_cols, tgt_cols)):
        if src_col != tgt_col:
            mismatched.append((idx, src_col, tgt_col))
            
    print(mismatched)
            
    if mismatched:
        print('Mismatched columns by position/index:')
        for idx, src, tgt in mismatched:
            print(f'At index {idx}: source_col = {src}, target_col = {tgt}')
                
    # for future testing frameworks
    # return {
    #     "match": src_cols == tgt_cols,
    #     "mismatched": mismatched
    # }
                