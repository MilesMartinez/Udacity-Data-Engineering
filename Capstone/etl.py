import pandas as pd
import configparser, os
from datetime import datetime
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col, monotonically_increasing_id, split
from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear, date_format, dayofweek, to_date, from_unixtime
from pyspark.sql.types import StructType as R, StructField as Fld, DoubleType as Dbl, StringType as Str, IntegerType as Int, LongType as Long, DateType as Date

# import AWS credentials
config = configparser.ConfigParser()
config.read('dl.cfg')
os.environ['AWS_ACCESS_KEY_ID']=config['CREDS']['AWS_ACCESS_KEY_ID']
os.environ['AWS_SECRET_ACCESS_KEY']=config['CREDS']['AWS_SECRET_ACCESS_KEY']

def create_spark_session():
    """
    Description: Creates a Spark Session
    
    Returns: Spark session object
    """
    spark = SparkSession \
            .builder \
            .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.2") \
            .getOrCreate()
    return spark

def process_i94_data(spark, input_data, output_data):
    """
    Description:
        Reads in i94 records data from S3,
        Formats data into a tables for i94 records, residents, residents' visas, and time,
        and then stores those data tables back into S3 as parquet files
        
    Parameters:
        spark: Spark session object
        input_data: S3 object where the i94 data is stored
        output_data: S3 object to save newly formatted data tables to
    
    """
    
    # import i94 data
    im_df = spark.read.parquet(input_data + "sas_data/*.snappy.parquet")
    im_df.createOrReplaceTempView('im_data')
    
    # create resident table
    resident_table = spark.sql("""
    SELECT admnum, i94bir, occup, biryear, gender
        FROM im_data
    WHERE cicid IS NOT NULL
    """).dropDuplicates()
    resident_table.write.mode('overwrite').parquet(output_data + 'resident_table.parquet')

    # create visa table
    visas_table = spark.sql("""
    SELECT cicid, i94visa, visapost, visatype
        FROM im_data
    WHERE cicid IS NOT NULL
    """).dropDuplicates()
    visas_table.write.mode('overwrite').parquet(output_data + 'visas_table.parquet')

    # create i94 table
    i94_table = spark.sql("""
    SELECT cicid, i94yr, i94mon, i94cit, i94res, i94port, arrdate, i94mode, i94addr, depdate, dtadfile, entdepa, entdepd, entdepu, matflag, dtaddto, airline, admnum, fltno
        FROM im_data
    WHERE cicid IS NOT NULL
    """).dropDuplicates()
    i94_table.write.mode('overwrite').parquet(output_data + 'i94_table.parquet')

    # create datetime column from original timestamp column
        # convert date columns to date types
    im_df = im_df.withColumn('dtadfile', to_date(col("dtadfile"),"yyyyMMdd").cast(Date()))
    im_df = im_df.withColumn('dtaddto', to_date(col("dtaddto"),"MMddyyyy").cast(Date()))
    im_df.createOrReplaceTempView('im_data')

        # extract columns to create time table
    t1 = im_df.select(col('dtadfile').alias('datetime'), 
                           dayofmonth('dtadfile').alias('day'), 
                           weekofyear('dtadfile').alias('week'), 
                           month('dtadfile').alias('month'), 
                           year('dtadfile').alias('year'), 
                           dayofweek('dtadfile').alias('weekday'))

    t2 = im_df.select(col('dtaddto').alias('datetime'), 
                           dayofmonth('dtaddto').alias('day'), 
                           weekofyear('dtaddto').alias('week'), 
                           month('dtaddto').alias('month'), 
                           year('dtaddto').alias('year'), 
                           dayofweek('dtaddto').alias('weekday'))

    time_table = t1.union(t2).dropDuplicates()
    time_table.createOrReplaceTempView('time_table')
    time_table.write.mode('overwrite').partitionBy('year', 'month').parquet(output_data + 'time_table.parquet')

    
def process_demographic_data(spark, input_data, output_data):
    """
    Description:
        Reads in US Cities Demographics data from S3,
        Formats data into a table for data at the city level and another table for the data rolled up at the state level,
        and then stores those data tables back into S3 as parquet files
        
    Parameters:
        spark: Spark session object
        input_data: S3 object where US cities demographic data is stored
        output_data: S3 object to save newly formatted data tables to
    
    """
    
    # import US Cities demographic data
    cityDataSchema = R([
        Fld('City', Str()),
        Fld('State', Str()),
        Fld('Median_Age', Dbl()),
        Fld('Male_Pop', Int()),
        Fld('Female_Pop', Int()),
        Fld('Total_Pop', Int()),
        Fld('Num_Veterans', Int()),
        Fld('Num_Foreign_born', Int()),
        Fld('Avg_Household_Size', Dbl()),
        Fld('State_Code', Str()),
        Fld('Race', Str()),
        Fld('Count', Int())
    ])
    city_df = spark.read.csv(input_data + "us-cities-demographics.csv", sep=';', schema=cityDataSchema)
    city_df.createOrReplaceTempView('city_data')
    
    # create cities table
    cities_table = spark.sql("""
    SELECT *
        FROM city_data
        WHERE City IS NOT NULL
    """).dropDuplicates()
    cities_table.write.mode('overwrite').parquet(output_data + 'cities_table.parquet')

    # create states table
    states_table = spark.sql("""
    SELECT State,
        AVG(Median_Age) as Median_Age,
        SUM(Male_Pop) as Male_Pop,
        SUM(Female_Pop) as Female_Pop,
        SUM(Total_Pop) as Total_Pop,
        SUM(Num_Veterans) as Num_Veterans,
        SUM(Num_Foreign_born) as Num_Foreign_born,
        AVG(Avg_Household_Size) as Avg_Household_Size,
        State_Code,
        Race,
        SUM(Count) as Count
    FROM city_data
    GROUP BY State, State_Code, Race
    """).dropDuplicates()
    states_table.write.mode('overwrite').parquet(output_data + 'states_table.parquet')
    
def process_airport_data(spark, input_data, output_data):
    """
    Description:
        Reads in airpot data from S3,
        Formats data into a table,
        and then stores that data table bac into S3 as parquet files
        
    Parameters:
        spark: Spark session object
        input_data: S3 object where the airport data is stored
        output_data: S3 object to save newly formatted data tables to
    
    """
    
    # import airport data
    airportDataSchema = R([
        Fld('ident', Str()),
        Fld('type', Str()),
        Fld('name', Str()),
        Fld('elevation_ft', Int()),
        Fld('continent', Str()),
        Fld('iso_country', Str()),
        Fld('iso_region', Str()),
        Fld('municipality', Str()),
        Fld('gps_code', Str()),
        Fld('iata_code', Str()),
        Fld('local_code', Str()),
        Fld('coordinates', Str())
    ])
    airport_df = spark.read.csv(input_data + "airport-codes_csv.csv", schema=airportDataSchema)
    split_col = split(airport_df['coordinates'], ', ')
    airport_df = airport_df.withColumn('latitidue', split_col.getItem(0))
    airport_df = airport_df.withColumn('longitude', split_col.getItem(1))
    airport_df = airport_df.drop('coordinates')
    airport_df.createOrReplaceTempView('airport_codes')

    airports_table = spark.sql("""
    SELECT *
        FROM airport_codes
        WHERE ident IS NOT NULL
    """).dropDuplicates()
    airports_table.write.mode('overwrite').parquet(output_data + 'airports_table.parquet')


# data quality checks
def checkIfEmpty(spark, df, table_name):
    """
    Description:
        Tests if a given spark dataframe is empty
        
    Parameters:
        spark: a Spark session object
        df: a spark dataframe
        table_name: the name of the table being tested, for logging purposes
        
    Returns:
        True if the test passed
        False if the test failed
        
    """
    
    x = spark.sql("""
    SELECT COUNT(*) AS cnt FROM df
    """).toPandas()['cnt'][0]
    if(int(x) > 0):
        print('PASSED - Completeness Check for table ' + table_name)
        return True
    else:
        print('FAILED - Completeness Check for table ' + table_name)
        return False
    
def checkUnique(df, table_name):
    """
    Description:
        Tests if a given spark dataframe is empty
        
    Parameters:
        df: a spark dataframe
        table_name: the name of the table being tested, for logging purposes
        
    Returns:
        True if the test passed
        False if the test failed
        
    """
    
    if df.count() > df.dropDuplicates().count():
        print('FAILED - Unique Row Check for table ' + table_name)
        return True
    else:
        print('PASSED - Unique Row Check for table ' + table_name)
        return False

def main():
    # initialize
    input_data = '<my input s3 bucket>'
    output_data = "<my output s3 bucket>" 
    spark = create_spark_session()

    # process data
    process_i94_data(spark, input_data, output_data)
    process_demographic_data(spark, input_data, output_data)
    process_airport_data(spark, input_data, output_data)
    
    # Run Tests
    tables = ['airports_table.parquet', \
              'cities_table.parquet', \
              'i94_table.parquet', \
              'resident_table.parquet', \
              'time_table.parquet', \
              'visas_table.parquet']
    for t in tables:
        df = spark.read.parquet(output_data + t)
        df.createOrReplaceTempView('df')
        checkIfEmpty(spark, df, t)
        checkUnique(df, t)
        
if __name__ == "__main__":
    main()