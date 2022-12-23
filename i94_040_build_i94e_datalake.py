"""
Given the raw i94 datalake, and the lookup CSVs, this module combine these and 
create the i94e (i94 Enhanced) Datalake
"""

import configparser
import pyspark.sql.functions as F
import pyspark.sql.types as T
from pyspark.sql import SparkSession
from i94_utils import rmtree_if_exists, convert_datetime
from datetime import datetime, timedelta
from pyspark.sql import SparkSession


def i94e_build_write_in_chunks(
    spark,
    i94yr_list: list,
    i94mon_list: list,
    out_dir: str,
    path_i94: str,
    path_lkup_addr: str,
    path_lkup_cntyl: str,
    path_lkup_port: str,
    path_lkup_mode: str,
    path_lkup_visa: str,
    partition_by_list: list = None
    ):
    """ Create i94e partiion by partition, and append to base """

    # Read the primary i94 parquet
    df_lkup_addr = spark.read.csv(path_lkup_addr, header=True)
    df_lkup_cntyl = spark.read.csv(path_lkup_cntyl, header=True)
    df_lkup_port = spark.read.csv(path_lkup_port, header=True)
    df_lkup_mode = spark.read.csv(path_lkup_mode, header=True)
    df_lkup_visa = spark.read.csv(path_lkup_visa, header=True)
    
    df_lkup_addr.createOrReplaceTempView('addr')
    df_lkup_cntyl.createOrReplaceTempView('cntyl')
    df_lkup_mode.createOrReplaceTempView('mode')
    df_lkup_port.createOrReplaceTempView('port')
    df_lkup_visa.createOrReplaceTempView('visa')
    
    print(f"Build and write i94e in chunks (by: {partition_by_list})")
    for y in i94yr_list:
        
        for m in i94mon_list:
            print(f"Partition: i94yr={y} and i94mm={m}")
            # Read the primary i94 parquet (for a partition)
            df_i94 = spark.read.parquet(path_i94).where(f"i94yr={y} and i94mon={m}")

            # Create the python datetime here (we might be able to do it within spark SQL however)
            udf_datetime_from_sas = F.udf(lambda x: convert_datetime(x), T.DateType())
            df_i94 = df_i94.withColumn('arrdate_pydt', udf_datetime_from_sas(df_i94.arrdate))
            df_i94 = df_i94.withColumn('depdate_pydt', udf_datetime_from_sas(df_i94.depdate))

            # From i94 left join lookup tables (for that particular partition)
            df_i94.createOrReplaceTempView('main')
            df_i94e = spark.sql("""
            select
                concat(
                    cast(cast(main.i94yr as int) as string),
                    '-',
                    lpad(cast(cast(main.i94mon as int) as string), 2, '0'),
                    '-',
                    cast(cast(main.cicid as int) as string)
                ) as guid,

                main.i94yr,
                main.i94mon,
                main.cicid,

                main.arrdate_pydt as arrival_date,
                main.depdate_pydt as departure_date,

                main.i94addr,
                addr.value as i94addr_value,

                main.i94cit,
                cntyl_cit.value as i94cit_value,

                main.i94res,
                cntyl_res.value as i94res_value,

                main.i94mode,
                mode.value as i94mode_value,

                main.i94port,
                port.value as i94port_value,    

                main.i94visa,
                visa.value as i94visa_value,
                main.visatype,
                main.visapost,
               
                main.gender,
                main.biryear,
                main.i94bir,
                main.occup,

                main.admnum,
                main.insnum,    
                main.entdepa,
                main.entdepd,
                main.entdepu,
                main.matflag,
                main.dtadfile,
                main.dtaddto,
                main.airline,
                main.fltno,
                main.arrdate,
                main.depdate

            from main
            left join addr on main.i94addr = addr.key
            left join cntyl as cntyl_cit on main.i94cit = cntyl_cit.key
            left join cntyl as cntyl_res on main.i94res = cntyl_res.key
            left join mode on main.i94mode = mode.key
            left join port on main.i94port = port.key
            left join visa on main.i94visa = visa.key
            """)

            # append to base
            df_i94e.write.mode("append").partitionBy(*partition_by_list).parquet(out_dir)
            
            
def main():
    print("Start Script")
    config_dev = configparser.ConfigParser()
    config_dev.read_file(open('aws_dev.cfg'))
    
    # Input 1 (i94) + Input 2 (lookup tables) => Input 3 (i94e)
    
    #  Input 1: Directory of the i94 raw partitioned datalake to (df_i94).
    PAR_I94_DIR_BY_I94YR_I94MON = config_dev.get('DATA_PATHS_LOCAL', 'PAR_I94_DIR_BY_I94YR_I94MON')
    print(f"PAR_I94_DIR_BY_I94YR_I94MON: {PAR_I94_DIR_BY_I94YR_I94MON}")
    
    # Input 2: the CSV Lookup files that we created from the SAS Program text file
    RAW_I94_LOOKUP_CSV_FILE_I94ADDR=config_dev.get('DATA_PATHS_LOCAL', 'RAW_I94_LOOKUP_CSV_FILE_I94ADDR')
    RAW_I94_LOOKUP_CSV_FILE_I94CNTYL=config_dev.get('DATA_PATHS_LOCAL', 'RAW_I94_LOOKUP_CSV_FILE_I94CNTYL')
    RAW_I94_LOOKUP_CSV_FILE_I94PORT=config_dev.get('DATA_PATHS_LOCAL', 'RAW_I94_LOOKUP_CSV_FILE_I94PORT')
    RAW_I94_LOOKUP_CSV_FILE_I94MODE=config_dev.get('DATA_PATHS_LOCAL', 'RAW_I94_LOOKUP_CSV_FILE_I94MODE')
    RAW_I94_LOOKUP_CSV_FILE_I94VISA=config_dev.get('DATA_PATHS_LOCAL', 'RAW_I94_LOOKUP_CSV_FILE_I94VISA')
    print(f"RAW_I94_LOOKUP_CSV_FILE_I94ADDR: {RAW_I94_LOOKUP_CSV_FILE_I94ADDR}")
    print(f"RAW_I94_LOOKUP_CSV_FILE_I94CNTYL: {RAW_I94_LOOKUP_CSV_FILE_I94CNTYL}")
    print(f"RAW_I94_LOOKUP_CSV_FILE_I94PORT: {RAW_I94_LOOKUP_CSV_FILE_I94PORT}")
    print(f"RAW_I94_LOOKUP_CSV_FILE_I94MODE: {RAW_I94_LOOKUP_CSV_FILE_I94MODE}")
    print(f"RAW_I94_LOOKUP_CSV_FILE_I94VISA: {RAW_I94_LOOKUP_CSV_FILE_I94VISA}")
    
    # Output: Directory where we wish the consolidated i94 Enhanced (i94e) datalake goes to
    PAR_I94E_DIR = config_dev.get('DATA_PATHS_LOCAL', 'PAR_I94E_DIR')
    print(f"PAR_I94E_DIR: {PAR_I94E_DIR}")
    
    # Start spark session.
    spark = SparkSession.builder.\
        config("spark.jars.repositories", "https://repos.spark-packages.org/").\
        config("spark.jars.packages", "saurfang:spark-sas7bdat:2.0.0-s_2.11").\
        enableHiveSupport().getOrCreate()

    # Reset output directory if safe_mode is set to False
    # (If safe_mode is set to True, this setep would do nothing)
    rmtree_if_exists(PAR_I94E_DIR, safe_mode=False)    
    
    # Build consolidated i94e datalake here, partitioned by i94yr and i94mon
    i94e_build_write_in_chunks(
        spark,
        i94yr_list=[2016],
        i94mon_list=[1,2,3,4,5,6,7,8,9,10,11,12],
        out_dir=PAR_I94E_DIR,
        path_i94=PAR_I94_DIR_BY_I94YR_I94MON,
        path_lkup_addr=RAW_I94_LOOKUP_CSV_FILE_I94ADDR,
        path_lkup_cntyl=RAW_I94_LOOKUP_CSV_FILE_I94CNTYL,
        path_lkup_port=RAW_I94_LOOKUP_CSV_FILE_I94PORT,
        path_lkup_mode=RAW_I94_LOOKUP_CSV_FILE_I94MODE,
        path_lkup_visa=RAW_I94_LOOKUP_CSV_FILE_I94VISA,
        partition_by_list=["i94yr", "i94mon"]
    )
    
    spark.stop()
    print("Script Finishes.")
    
    
if __name__ == "__main__":
    main()
