""" Given a spark session, parse i94 SAS datasets (via Spark) into parquet directly """

import configparser
from pyspark.sql import SparkSession
from i94_utils import rmtree_if_exists


def i94_sas7bdat_to_parquet(
    spark,
    sas7bdat_base_path: str,
    parquet_base_path: str,
    yy_list: list,
    mmm_list: list,
    partition_by_list: list = None):
    """ Given a spark session handle and parameters, parse i94 SAS datasets into parquet directly """
    
    for yy in yy_list:
        for mmm in mmm_list:
            input_dir = f"{sas7bdat_base_path}"
            input_file = f"i94_{mmm}{yy}_sub.sas7bdat"
            input_path = f"{input_dir}/{input_file}"
            print(f"Reading (input_path): {input_path}")
            df_spark_i94 = spark.read.format('com.github.saurfang.sas.spark').load(input_path)
            print(f"Writing to (parquet_base_path): {parquet_base_path}")
            if partition_by_list:
                df_spark_i94.write.mode("append").partitionBy(*partition_by_list).parquet(parquet_base_path) 
            else:
                df_spark_i94.write.mode("append").parquet(parquet_base_path)             
    print(f"Done. i94 dataset written to: {parquet_base_path}")
    

def data_quality_checks(
        spark,
        i94yr_list: list,
        i94mon_list: list,
        print_schema: False):
    """ Data Quality Check """
    
    for y in i94yr_list:
        for m in i94mon_list:
            print(f"Partition: i94yr={y} and i94mm={m}")
            # Read the primary i94 parquet (for a partition)
            df_i94 = spark.read.parquet(PAR_I94_DIR_BY_I94YR_I94MON).where(f"i94yr={y} and i94mon={m}")
            if print_schema:
                df_i94.printSchema()
            
            # Test 1: there must be 28 columns in total as expected
            cols = len(df_i94.columns)
            print(f"Total Columns: {cols}")
            assert len(df_i94.columns) == 28  
            
            # Test 2: total row counts must be greater than 0
            rows = df_i94.count()
            print(f"Total Rows: {rows}")
            assert rows > 0
            
            # Test 3: cicid must be unique for each partition (`i94yr`, `i94mon`)
            assert df_i94.select("cicid").count() == df_i94.select("cicid").distinct().count()
            
            # We can add more tests here if we want.
            # Otherwise, let's call it a day.
            print("Partition Pass!")

    print("Overall: Passed!")  
    
    
def main():
    print("Start Script")
    config_dev = configparser.ConfigParser()
    config_dev.read_file(open('aws_dev.cfg'))

    # Input: Directory where the SAS7BDAT files are stored.
    RAW_I94_MTHLY_SAS7BDAT_DIR = config_dev.get('DATA_PATHS_UDACITY', 'RAW_I94_MTHLY_SAS7BDAT_DIR')
    print(f"RAW_I94_MTHLY_SAS7BDAT_DIR: {RAW_I94_MTHLY_SAS7BDAT_DIR}")
    
    # Output: Directory where we wish to write our partitioned raw datalake to (df_i94).
    PAR_I94_DIR_BY_I94YR_I94MON = config_dev.get('DATA_PATHS_LOCAL', 'PAR_I94_DIR_BY_I94YR_I94MON')
    print(f"PAR_I94_DIR_BY_I94YR_I94MON: {PAR_I94_DIR_BY_I94YR_I94MON}")    
    
    # Start spark session.
    spark = SparkSession.builder.\
        config("spark.jars.repositories", "https://repos.spark-packages.org/").\
        config("spark.jars.packages", "saurfang:spark-sas7bdat:2.0.0-s_2.11").\
        enableHiveSupport().getOrCreate()
    
    # Reset output directory if safe_mode is set to False
    # (If safe_mode is set to True, this setep would do nothing)
    rmtree_if_exists(PAR_I94_DIR_BY_I94YR_I94MON, safe_mode=False)
    
    # Read the 12 monthly (2016) SAS7BDAT files into Spark DataFrame and write to parquet immediately
    i94_sas7bdat_to_parquet(
        spark,
        RAW_I94_MTHLY_SAS7BDAT_DIR,
        PAR_I94_DIR_BY_I94YR_I94MON,
        ['16'],
        ['jan', 'feb', 'mar', 'apr', 'may', 'jun', 'jul', 'aug', 'sep', 'oct', 'nov', 'dec'],
        partition_by_list=["i94yr", "i94mon"]
    )
      
    # Data Quality checks to make sure our raw parquets are ok.
    data_quality_checks(
        spark,
        [2016],
        [1,2,3,4,5,6,7,8,9,10,11,12],
        print_schema=False,
    )
    
    spark.stop()
    print("Script Finishes.")

    
if __name__ == "__main__":
    main()
