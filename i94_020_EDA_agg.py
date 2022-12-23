""" 
Run simple group by aggregation analysis of all columns of the raw i94 datalake 
and output result to CSV files for ease of insight gathering.
"""

import os
import configparser
import pandas as pd
from pyspark.sql import SparkSession



def simple_groupby(spark, df, colname: str, ordered: bool = True):
    """ Given a Spark Session, Spark DataFrame and a group-by field, 
        do a simple count / groupby (one field only)
        Return a Spark DataFrame
    """
    df.createOrReplaceTempView("_tmp_df")
    sql_str = f"""\
        select {colname}, count(*) as records
        from _tmp_df
        group by 1
    """
    df2 = spark.sql(sql_str)
    if ordered:
        df2=df2.orderBy(colname)       
    return df2


def write_groupby_stats_csv(
        df_spark,
        groupby_field_name,
        out_filename,
        out_dir,
        n_rows=1000):
    """ given one spark dataframe (in a specific schema) save into a CSV file """
    
    print(f'writing: {out_filename} to {out_dir}')
    pd.DataFrame(df_spark.take(n_rows), columns=[groupby_field_name, 'count'])\
        .to_csv(f"{out_dir}/{out_filename}", index=False)


def bulk_write_groupby_stats_csv(
    spark,
    in_dir,
    out_dir,
    n_rows=1000):
    """ bulk run write_groupby_stats_csv """
    
    df = spark.read.parquet(in_dir)
    
    for colname in df.columns:
        
        # for the i94 column, do a simeple group-by analysis
        df_agg = simple_groupby(spark, df, colname)
    
        out_filename = f"df_{colname}_agg_1.csv"
        
        # write the aggregated Spark Dataframe into a CSV file
        write_groupby_stats_csv(
            df_agg,
            colname,
            out_filename,
            out_dir=out_dir,
            n_rows=n_rows,
        )
        

def main():
    print("Start Script")
    config_dev = configparser.ConfigParser()
    config_dev.read_file(open('aws_dev.cfg'))
 
    # Directory if the i94 raw datalake to (df_i94).
    PAR_I94_DIR_BY_I94YR_I94MON = config_dev.get('DATA_PATHS_LOCAL', 'PAR_I94_DIR_BY_I94YR_I94MON')
    print(f"PAR_I94_DIR_BY_I94YR_I94MON: {PAR_I94_DIR_BY_I94YR_I94MON}")

    # Directory where we wish the group-by summary stats go to
    RAW_I94_DIR_SAMPLE_GROUPBY_STATS_CSV = config_dev.get('DATA_PATHS_LOCAL', 'RAW_I94_DIR_SAMPLE_GROUPBY_STATS_CSV')
    print(f"RAW_I94_DIR_SAMPLE_GROUPBY_STATS_CSV: {RAW_I94_DIR_SAMPLE_GROUPBY_STATS_CSV}")   
    
    # Start spark session.
    spark = SparkSession.builder.\
        config("spark.jars.repositories", "https://repos.spark-packages.org/").\
        config("spark.jars.packages", "saurfang:spark-sas7bdat:2.0.0-s_2.11").\
        enableHiveSupport().getOrCreate()

    # for each column of the i94 datalake, do a simple group by and 
    # write first 1000 rows to CSV files for ease of referenes
    bulk_write_groupby_stats_csv(
        spark,
        PAR_I94_DIR_BY_I94YR_I94MON,
        RAW_I94_DIR_SAMPLE_GROUPBY_STATS_CSV,
        n_rows=1000,
    )
    
    spark.stop()
    print("Script Finishes.")
    
    
if __name__ == "__main__":
    main()