import configparser
from datetime import datetime
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear, to_timestamp, dayofweek


config = configparser.ConfigParser()
config.read('dl.cfg')

os.environ['AWS_ACCESS_KEY_ID']="{}".format(*config['AWS_ACCESS_KEY_ID'].values())
os.environ['AWS_SECRET_ACCESS_KEY']="{}".format(*config['AWS_SECRET_ACCESS_KEY'].values())


def create_spark_session():
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .getOrCreate()
    return spark


def process_song_data(spark, input_data_dir, output_data_dir):
    # get filepath to song data file
    song_data = input_data_dir + "song_data"
    
    # read song data file
    songs_df = spark.read.json(song_data)

    # extract columns to create songs table
    songs_table = songs_df.select('song_id', 'title', 'artist_id', 'year', 'duration')
    
    # write songs table to parquet files partitioned by year and artist
    songs_table.write.parquet(output_data_dir + 'songs_table.parquet')

    # extract columns to create artists table
    artists_table = songs_df.select('artist_id', 'name', 'location', 'latitude', 'longitude')
    
    # write artists table to parquet files
    artists_table.parquet(output_data_dir + 'artists_table.parquet')


def process_log_data(spark, input_data_dir, output_data_dir):
    # get filepath to log data file
    log_data = input_data_dir + "/log_data"

    # read log data file
    logs_df = spark.read.json(log_data)
    
    # filter by actions for song plays
    logs_by_actions_df = logs_df[logs_df.page == 'NextSong']

    # extract columns for users table    
    users_table = logs_by_actions_df.select('user_id', 'first_name', 'last_name', 'gender', 'level')
    
    # write users table to parquet files
    users_table.write.parquet(output_data_dir + 'users_table.parquet')

    # create timestamp column from original timestamp column
    logs_by_actions_df = logs_by_actions_df.withColumn('start_time', to_timestamp('ts'))
    
    # create datetime column from original timestamp column
    logs_by_actions_df = logs_by_actions_df.withColumn('year', year('time_stamp')) \
                         .withColumn('month', month('time_stamp')) \
                         .withColumn('day_of_month', dayofmonth('time_stamp')) \
                         .withColumn('day_of_week', dayofweek('time_stamp')) \
                         .withColumn('week_of_year', weekofyear('time_stamp')) \
                         .withColumn('hour', hour('time_stamp'))
    
    # extract columns to create time table
    time_table = logs_by_actions_df.select(
                                        'start_time',
                                        'hour',
                                        'day_of_month',
                                        'day_of_week',
                                        'week_of_year',
                                        'month',
                                        'year')
    
    # write time table to parquet files partitioned by year and month
    time_table.write.parquet(output_data_dir + 'time_table.parquet')

    # read in song data to use for songplays table
    songs_df = spark.read.load(input_data_dir + 'song_data')

    # extract columns from joined song and log datasets to create songplays table 
    songplays_table = logs_by_actions_df.join(songs_df, 'length') \
    .select(
        'start_time',
        'user_id',
        'level',
        'song_id',
        'artist_id',
        'session_id',
        'location',
        'user_agent')

    # write songplays table to parquet files partitioned by year and month
    songplays_table.write.parquet(output_data_dir + 'songplays_table.parquet')


def main():
    spark = create_spark_session()
    input_data_dir = "s3a://udacity-dend/"
    output_data_dir = "/user/"
    
    process_song_data(spark, input_data_dir, output_data_dir)    
    process_log_data(spark, input_data_dir, output_data_dir)


if __name__ == "__main__":
    main()