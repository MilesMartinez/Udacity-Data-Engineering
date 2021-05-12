import configparser
from datetime import datetime
import os
os.chdir('/Users/mz965x/Desktop/DEND/Project-4-Datalake')
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col, monotonically_increasing_id
from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear, date_format, dayofweek, to_date, from_unixtime
from pyspark.sql.types import StructType as R, StructField as Fld, DoubleType as Dbl, StringType as Str, IntegerType as Int, LongType as Long, DateType as Date


config = configparser.ConfigParser()
config.read('dl.cfg')
os.environ['AWS_ACCESS_KEY_ID']=config['CREDS']['AWS_ACCESS_KEY_ID']
os.environ['AWS_SECRET_ACCESS_KEY']=config['CREDS']['AWS_SECRET_ACCESS_KEY']


def create_spark_session():
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.2") \
        .getOrCreate()
    return spark


def process_song_data(spark, input_data, output_data):
    """
    Input:
        spark: Spark session object
        input_data: S3 object where user song data is stored
        output_data: S3 object to save newly formatted data tables to
        
    Desc:
        Reads in user song data from S3,
        formats data into a table for songs and a table for artists,
        and then stores those data tables back into S3 as parquet files
    """
    
    # get filepath to song data file
    song_data = input_data + "song_data/*/*/*/*.json"

    # read song data file
    songDataSchema = R([
        Fld('artist_id', Str()),
        Fld('artist_latitude', Dbl()),
        Fld('artist_longitude', Dbl()),
        Fld('artist_location', Str()),
        Fld('artist_name', Str()),
        Fld('song_id', Str()),
        Fld('title', Str()),
        Fld('duration', Dbl()),
        Fld('year', Int())
    ])
    df = spark.read.json(song_data, schema=songDataSchema)
    df.createOrReplaceTempView('song_data')

    # extract columns to create songs table
    songs_table = spark.sql("""
    SELECT song_id, title, artist_id, year, duration
        FROM song_data
    WHERE song_id IS NOT NULL
    """).dropDuplicates()
    
    # write songs table to parquet files partitioned by year and artist
    songs_table.write.mode('overwrite').partitionBy('year', 'artist_id').parquet(output_data + 'songs_table.parquet')

    # extract columns to create artists table
    artists_table = spark.sql("""
    SELECT artist_id, artist_name AS name, artist_location AS location, artist_latitude AS latitude, artist_longitude AS longitude
        FROM song_data
    WHERE artist_id IS NOT NULL
    """).dropDuplicates()
    
    # write artists table to parquet files
    artists_table.write.mode('overwrite').parquet(output_data + 'artists_table.parquet')

def process_log_data(spark, input_data, output_data):
    """
    Input:
        spark: Spark session object
        input_data: S3 object where user log data is stored
        output_data: S3 object to save newly formatted data tables to
        
    Desc:
        Reads in user log data from S3,
        formats data into tables for users, songplays, and time,
        and then stores those data tables back into S3 as parquet files
    """
        
    # get filepath to log data file
    log_data = input_data + 'log_data/*/*/*.json'
    
    # read log data file
    logDataSchema = R([
        Fld('artist', Str()),
        Fld('auth', Str()),
        Fld('firstName', Str()),
        Fld('gender', Str()),
        Fld('itemInSession', Long()),
        Fld('lastName', Str()),
        Fld('length', Dbl()),
        Fld('level', Str()),
        Fld('location', Str()),
        Fld('method', Str()),
        Fld('page', Str()),
        Fld('registration', Dbl()),
        Fld('sessionId', Long()),
        Fld('song', Str()),
        Fld('status', Long()),
        Fld('ts', Long()),
        Fld('userAgent', Str()),
        Fld('userId', Str())
    ])
    
    df = spark.read.json(log_data, schema=logDataSchema)
    
    # filter by actions for song plays
    df = df.filter("page == 'NextSong' and userId IS NOT NULL")

    # extract columns for users table 
    user_table = df.select(col('userId').alias('user_id'),
                          col('firstName').alias('first_name'),
                          col('lastName').alias('last_name'),
                          'gender',
                          'level').dropDuplicates()
    
    # write users table to parquet files
    user_table.write.mode('overwrite').parquet(output_data + 'users_table.parquet')
    
    # create datetime column from original timestamp column
    get_datetime = udf(lambda x: datetime.fromtimestamp(x/1000).strftime('%Y-%m-%d %H:%M:%S'))
    df = df.withColumn('datetime', get_datetime(df.ts))
    df.createOrReplaceTempView('log_data')
    
    # extract columns to create time table
    time_table = df.select(col('datetime').alias('start_time'),
                           hour('datetime').alias('hour'), 
                           dayofmonth('datetime').alias('day'), 
                           weekofyear('datetime').alias('week'), 
                           month('datetime').alias('month'), 
                           year('datetime').alias('year'), 
                           dayofweek('datetime').alias('weekday')).dropDuplicates()
    time_table.createOrReplaceTempView('time_table')
    
    # write time table to parquet files partitioned by year and month
    time_table.write.mode('overwrite').partitionBy('year', 'month').parquet(output_data + 'time_table.parquet')

    # read in song data to use for songplays table
    song_data = input_data + "song_data/*/*/*/*.json"
    songDataSchema = R([
        Fld('artist_id', Str()),
        Fld('artist_latitude', Dbl()),
        Fld('artist_longitude', Dbl()),
        Fld('artist_location', Str()),
        Fld('artist_name', Str()),
        Fld('song_id', Str()),
        Fld('title', Str()),
        Fld('duration', Dbl()),
        Fld('year', Int())
    ])
    df = spark.read.json(song_data, schema=songDataSchema)
    df.createOrReplaceTempView('song_data')

    # extract columns from joined song and log datasets to create songplays table 
    songplays_table = spark.sql("""
    SELECT t.start_time, t.year, t.month, userId AS user_id, level, song_id, artist_id, sessionId AS session_id, location, userAgent AS user_agent
        FROM ( log_data l JOIN song_data s ON (l.artist = s.artist_name AND l.song = s.title) )
        JOIN time_table t ON (t.start_time = l.datetime)
    """).withColumn('songplay_id', monotonically_increasing_id())

    # write songplays table to parquet files partitioned by year and month
    songplays_table.write.mode('overwrite').partitionBy('year', 'month').parquet(output_data + 'songplays.parquet')
    
    

def main():
    spark = create_spark_session()
    input_data = "s3a://udacity-dend/"
    output_data = "<my bucket>"
    
    process_song_data(spark, input_data, output_data)    
    process_log_data(spark, input_data, output_data)


if __name__ == "__main__":
    main()
