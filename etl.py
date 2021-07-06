import configparser
from datetime import datetime
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col
from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear, date_format


# Read AWS credentials from .cfg file
config = configparser.ConfigParser()
config.read_file(open('dl.cfg'))

AWS_KEY_ID = config.get('AWS','AWS_KEY_ID')
AWS_SECRET = config.get('AWS','AWS_SECRET')

def create_spark_session():
    spark = SparkSession \
            .builder \
            .appName("Data Wrangling with Spark SQL") \
            .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
            .getOrCreate()

    return spark


def process_song_data(spark, input_data, output_data):
    # get filepath to song data file
    song_data = os.path.join(input_data, 'song-data-test.json')
    
    # read song data file
    df = spark.read.json(song_data)

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
    
    # get filepath to log data file
    log_data = os.path.join(input_data, 'log-data-test.json')

    # read log data file
    df_log = spark.read.json(log_data)
    
    # filter by actions for song plays
    df_songplay = df_log.filter(df_log.page == "NextSong")
    
    # create datetime column from original timestamp column
    get_datetime = udf(lambda ts: datetime.utcfromtimestamp(int(ts)/1000).strftime('%Y-%m-%d %H:%M:%S'))
    df_log = df.withColumn("datetime",  get_datetime("ts"))
    
    # extract columns to create time table
    time_table = 
    
    # write time table to parquet files partitioned by year and month
    time_table

    # read in song data to use for songplays table
    df_song = 

    # extract columns from joined song and log datasets to create songplays table 
    songplays_table = 

    # write songplays table to parquet files partitioned by year and month
    songplays_table


def main():
    # define test and prod input/output
    input_bucket = "s3a://dl-sparkify/"
    output_bucket = ""
    
    input_local = "./data/input-test/"
    output_local = "./data/output-test/"
    
    spark = create_spark_session()
    
    process_song_data(spark, input_local, output_local)    
#     process_log_data(spark, input_data, output_data)


if __name__ == "__main__":
    main()