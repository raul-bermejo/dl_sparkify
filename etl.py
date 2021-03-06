import configparser
from datetime import datetime
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col, monotonically_increasing_id 
from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear, dayofweek, date_format, to_timestamp 


# Read AWS credentials from .cfg file
config = configparser.ConfigParser()
config.read_file(open('dl.cfg'))

AWS_KEY_ID = config.get('AWS','AWS_KEY_ID')
AWS_SECRET = config.get('AWS','AWS_SECRET')

# Set credentials as environment variables
os.environ['AWS_KEY_ID']=config['AWS']['AWS_KEY_ID']
os.environ['AWS_SECRET']=config['AWS']['AWS_SECRET']

def create_spark_session():
    """
    Creates a spark session

    Parameters:
    -----------
    None

    Returns:
    --------
    spark: pyspark.sql.session.SparkSession
           Spark session to create dataframes
    """
    
    spark = SparkSession \
    .builder \
    .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
    .config("spark.hadoop.fs.s3a.impl","org.apache.hadoop.fs.s3a.S3AFileSystem") \
    .config("spark.hadoop.fs.s3a.awsAccessKeyId", os.environ['AWS_KEY_ID']) \
    .config("spark.hadoop.fs.s3a.awsSecretAccessKey", os.environ['AWS_SECRET']) \
    .getOrCreate()

    return spark


def process_song_data(spark, input_data, output_data):
    """
    Extracts objects in S3 buckets, loads song data into 
    pyspark dataframe. Extracts relevant data and loads it
    into artust and songs tables, and saves dataframes as
    .parquet files.

    Parameters:
    -----------
    spark: pyspark.sql.session.SparkSession
           Pyspark session
    input_data: str
                Path to input .json data (local or S3 url)
    output_data: str
                 Path to output .json data (local or S3 url)
    """

    # get filepath to song data file
    path_songdata = os.path.join(input_data, 'song-data/*/*/*/*.json')
    print(path_songdata)
    
    # read song data file
    df = spark.read.json(path_songdata)

    # extract columns to create songs table
    songs_table = df.select("song_id", "title", "artist_id", "year", "duration")
    
    # write songs table to parquet files partitioned by year and artist
    songs_table.write \
            .partitionBy("year", "artist_id") \
            .parquet(os.path.join(output_data, "songs_table.parquet"))

    # extract columns to create artists table
    artists_table = df.select('artist_id', 'artist_latitude', 'artist_location', 
                              'artist_longitude', 'artist_name',)
    
    # write artists table to parquet files
    artists_table.write \
            .parquet(os.path.join(output_data, "artists_table.parquet"))


def process_log_data(spark, input_data, output_data):
    """
    Extracts objects in S3 buckets, loads song data into 
    pyspark dataframe. Extracts relevant data and loads it
    into artust and songs tables, and saves dataframes as
    .parquet files.

    Parameters:
    -----------
    spark: pyspark.sql.session.SparkSession
           Pyspark session
    input_data: str
                Path to input .json data (local or S3 url)
    output_data: str
                 Path to output .json data (local or S3 url)
    """
    
    # get filepath to log file
    path_logdata = os.path.join(input_data, 'log-data/*.json')
    path_songdata = os.path.join(input_data, 'song-data/*/*/*.json')

    # read log data file
    df_log = spark.read.json(path_logdata)
    
    # filter by actions for song plays
    df_songplay = df_log.filter(df_log.page == "NextSong")
    
    # create datetime column from original timestamp column
    get_datetime = udf(lambda ts: datetime.utcfromtimestamp(int(ts)/1000).strftime('%Y-%m-%d %H:%M:%S'))
    df_log = df_log.withColumn("datetime",  get_datetime("ts"))
    
    # extract columns to create time table
    time_table = df_log.withColumn("hour",  hour("datetime")).withColumn("day",  dayofmonth("datetime")).withColumn("week",  weekofyear("datetime")).withColumn("month",  month("datetime")).withColumn("year", year("datetime")).withColumn("weekday",  dayofweek("datetime")).select("ts","hour","day","week","month","year","weekday")
    
#     time_table = df_log.withColumn("hour",  hour("datetime")) \
#        .withColumn("day",  dayofmonth("datetime")) \ 
#        .withColumn("week",  weekofyear("datetime")) \ 
#        .withColumn("month",  month("datetime")) \ 
#        .withColumn("year", year("datetime")) \
#        .withColumn("weekday",  dayofweek("datetime")) \
#        .select("ts","hour","day","week","month","year","weekday")
    
    # write time table to parquet files partitioned by year and month
    time_table.write \
            .parquet(os.path.join(output_data, "time_table.parquet"))

    # read in song data to use for songplays table
    df_song = spark.read.json(path_songdata)
    df_master = df_song.join(df_log, (df_song.artist_name == df_log.artist) & \
                                    (df_song.title == df_log.song) & \
                                    (df_song.duration == df_log.length))
    
    # add sequential id, year and month (for partitioning) to df_master
    df_master = df_master.withColumn("songplay_id", monotonically_increasing_id()).withColumn("month",  month("datetime")).withColumn("year", year("datetime")) \
    # Might need to filter non null values
    # df_master.filter

    # extract columns from joined song and log datasets to create songplays table     
    songplays_table = df_master.select("songplay_id", "datetime", "userId", "song_id", 
                                       "artist_id", "sessionId", "location", "userAgent",
                                       "year", "month")

    # write songplays table to parquet files partitioned by year and month
    songplays_table.write \
            .partitionBy("year", "month") \
            .parquet(os.path.join(output_data, "songs_table.parquet"))


def main():
    # define test and prod input/output
    input_s3 = "s3a://dl-sparkify/input/"
    output_s3 = "s3a://dl-sparkify/output/"
    
    # input_local = "./data/input-test/"
    # output_local = "./data/output-test/"
    
    spark = create_spark_session()
    
    process_song_data(spark, input_s3, output_s3)    
    process_log_data(spark, input_s3, output_s3)


if __name__ == "__main__":
    main()
