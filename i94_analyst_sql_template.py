import configparser
from pyspark.sql import SparkSession


def main():
    print("Start Script")
    
    # Get the path of the i94e datalake
    config_dev = configparser.ConfigParser()
    config_dev.read_file(open('aws_dev.cfg'))
    PAR_I94E_DIR = config_dev.get('DATA_PATHS_LOCAL', 'PAR_I94E_DIR')
    
    # Start a spark session
    spark = SparkSession.builder.enableHiveSupport().getOrCreate()

    # Read in only month 6, 7, 8 in 2016
    df_i94e = spark.read.parquet(PAR_I94E_DIR).where(f"i94yr=2016 and i94mon in (6,7,8)")
   
    # alternatively (commented out): read in the whole datalake (more intensive but doable)
    #df_i94e = spark.read.parquet(PAR_I94E_DIR)

    # Peek the schema
    df_i94e.printSchema()
    
    # Run a SQL queries
    top_x = 100
    df_i94e.createOrReplaceTempView("df_i94e")
    sql_query_1 = """
    select
        i94yr,
        i94mon,
        i94visa_value,
        visatype,
        count(*) as records
    from df_i94e
    group by 1,2,3,4
    order by 1,2,3,4
    """
    print(f"sql_query_1 (show {top_x} rows):")
    print(sql_query_1)
    spark.sql(sql_query_1).show(top_x)
    
    # Run a SQL queries
    sql_query_2 = """
    select
        i94yr,
        i94mon,
        count(*) as records
    from df_i94e
    group by 1,2
    order by 1,2
    """
    print(f"sql_query_2 (show {top_x} rows):")
    print(sql_query_2)
    spark.sql(sql_query_2).show(top_x)

    print("Script Finishes.")
    
    
if __name__ == "__main__":
    main()
