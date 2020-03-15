import configparser
from datetime import datetime
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf
from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear, date_format, to_timestamp


config = configparser.ConfigParser()
config.read_file(open('dl.cfg'))

os.environ['AWS_ACCESS_KEY_ID'] = config.get('AWS', 'AWS_ACCESS_KEY_ID')
os.environ['AWS_SECRET_ACCESS_KEY'] = config.get('AWS', 'AWS_SECRET_ACCESS_KEY')


def create_spark_session():
    """Instantiates and returns Spark object to be used for data processing."""
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .getOrCreate()
    spark.sparkContext.setLogLevel("OFF")
    return spark


def process_song_data(spark, input_data, output_data):
    """
    Reads JSON song files from S3, performs necessary transformations, and writes output data back into S3 in the approriate output folder. The output data is stored in a parquet file format.
    
    Keyword Arguments:
    spark -- spark session object initialized in create_spark_session() function
    input_data -- S3 URL for input data
    output_data -- S3 URL and directory for output data
    """
    print('Processing Song Data:')
    # get filepath to song data file
    song_data = input_data + 'song_data/*/*/*/*.json'
    
    # read song data file
    df = spark.read.json(song_data)

    # extract columns to create songs table
    df.createOrReplaceTempView('songs')
    
    print('\t Creating song table')
    songs_table = spark.sql("""
    SELECT DISTINCT
        song_id, 
        title, 
        artist_id, 
        year, 
        duration
    FROM songs
    """)
    
    # write songs table to parquet files partitioned by year and artist
    songs_table.write.partitionBy('year', 'artist_id').parquet(output_data + 'songs_tables/', mode='overwrite')

    # extract columns to create artists table
    print('\t Creating artists table')
    artists_table = spark.sql("""
    SELECT DISTINCT
        artist_id,
        artist_name, 
        artist_location, 
        artist_latitude, 
        artist_longitude
    FROM songs
        """)
    
    # write artists table to parquet files
    artists_table.write.parquet(output_data + 'artists_tables/', mode='overwrite')


def process_log_data(spark, input_data, output_data):
    """
    Reads JSON user log files from S3, performs necessary transformations, and writes output data back into S3 in the approriate output folder. The output data is stored in a parquet file format.
    
    Keyword Arguments:
    spark -- spark session object initialized in create_spark_session() function
    input_data -- S3 URL for input data
    output_data -- S3 URL and directory for output data
    """

    print('Processing log table: ')
    # get filepath to log data file
    log_data = input_data + 'log_data/*.json'

    # read log data file
    df = spark.read.json(log_data)
    
    # filter by actions for song plays
    df = df.filter(df.page == 'NextSong')

    df.createOrReplaceTempView('log')

    print('\t Creating users table')
    # extract columns for users table    
    users_table = spark.sql("""
    SELECT DISTINCT
        userId, 
        firstName,
        lastName,
        gender, 
        level
    FROM log
        """)
    
    # write users table to parquet files
    users_table.write.parquet(output_data + 'users_tables/', mode='overwrite')
    
    print('\t Creating time table')   
    # create timestamp column, datetime columns and extract necessary columns to create time table
    time_table = spark.sql("""
    SELECT 
        start_time,
        hour(T.start_time)        as hour, 
        dayofweek(T.start_time)   as day,
        weekofyear(T.start_time)  as week, 
        month(T.start_time)       as month,
        year(T.start_time)        as year,
        dayofweek(T.start_time)   as weekday
    FROM 
        (SELECT to_timestamp(ts/1000) as start_time FROM log) T
    WHERE start_time IS NOT NULL
    
    """) 
    
    # write time table to parquet files partitioned by year and month
    # time_table.write.parquet('s3a://spakify_analytics/time/', mode='overwrite')
    time_table.write.parquet(output_data + 'time_tables/', mode='overwrite')

    # read in song data to use for songplays table
    song_df = spark.read.json(input_data + 'song_data/*/*/*/*.json')
    song_df.createOrReplaceTempView('song')

    print('\t Creating sonplays table')
    # extract columns from joined song and log datasets to create songplays table 
    songplays_table = spark.sql("""
    SELECT DISTINCT
        monotonically_increasing_id() as songplay_id, 
        to_timestamp(log.ts/1000) as start_time,
        log.userId, 
        log.level, 
        song.song_id,
        song.artist_id,
        log.sessionId,
        song.artist_location,
        log.userAgent
    FROM log
    JOIN song 
    ON (log.song = song.title)
        AND (log.artist = song.artist_name)""")

    # write songplays table to parquet files partitioned by year and month
    songplays_table.write.parquet(output_data + 'songplays_tables/', mode='overwrite')


def main(local_data=True):
    """
    Main function of etl.py
    
    Keyword Arguments:
    local_data -- Specifies the input and output data location. If the data is stored locally, local_data should be set to True. 
    """
    spark = create_spark_session()
    
    if local_data == True:
        input_data = "data/"
        output_data = "sparkify_data_lake/"
    else: 
        input_data = "s3a://udacity-dend/"
        output_data = "s3a://udacity-dend/sparkify_data_lake/"

    process_song_data(spark, input_data, output_data)    
    process_log_data(spark, input_data, output_data)


if __name__ == "__main__":
    main(local_data=True)
