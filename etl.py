import configparser
from datetime import datetime
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col
from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear, date_format


config = configparser.ConfigParser()
config.read_file(open('dl.cfg'))

os.environ["AWS_ACCESS_KEY_ID"] = config['AWS']['AWS_ACCESS_KEY_ID']
os.environ["AWS_SECRET_ACCESS_KEY"] = config['AWS']['AWS_SECRET_ACCESS_KEY']


def create_spark_session():
    """
    Creates or return local Spark session
    """
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .getOrCreate()
    return spark


def process_song_data(spark, input_data, output_data):
    """
    Process song data according to the input_data and output_data given.
    """
    # get filepath to song data file
    song_data = input_data

    # read song data file
    df = spark.read.json(song_data)

    # extract columns to create songs table
    songs_table = df.select(
        ['song_id', 'title', 'artist_id', 'year', 'duration'])

    # write songs table to parquet files partitioned by year and artist
    songs_table.write.partitionBy(['year', 'artist_id']).parquet(
        "{}/songs.parquet".format(output_data), mode="overwrite")

    # extract columns to create artists table
    artists_table = df.selectExpr("artist_id", "artist_name as name", "artist_location as location",
                                  "artist_latitude as latitude", "artist_longitude as longitude")

    # write artists table to parquet files
    artists_table.write.parquet(
        "{}/artists.parquet".format(output_data), mode="overwrite")


def process_log_data(spark, input_data, output_data):
    """
    Process logs data according to the input_data and output_data given.
    """
    # get filepath to log data file
    log_data = input_data

    # read log data file
    df = spark.read.json(log_data)

    # filter by actions for song plays
    df = df.filter(df.page == 'NextSong')

    # extract columns for users table
    users_table = df.selectExpr('userId as user_id', 'firstName as first_name',
                                'lastName as last_name', 'gender', 'level').distinct()

    # write users table to parquet files
    users_table.write.parquet(
        "{}/users.parquet".format(output_data), mode="overwrite")

    # extract columns to create time table
    df.createOrReplaceTempView("logs")
    time_table = spark.sql('select t.start_time, \
           hour(t.start_time) as hour, \
           day(t.start_time) as day, \
           weekofyear(t.start_time) as week, \
           month(t.start_time) as month, \
           year(t.start_time) as year, \
           dayofweek(t.start_time) as weekday\
           from \
           (select from_unixtime(ts/1000) as start_time from logs group by start_time) t')

    # write time table to parquet files partitioned by year and month
    time_table.write.partitionBy(['year', 'month']).parquet(
        "{}/times.parquet".format(output_data), mode="overwrite")

    # read in song data to use for songplays table
    song_df = spark.read.parquet("{}/songs.parquet".format(output_data))

    # extract columns from joined song and log datasets to create songplays table
    song_df.createOrReplaceTempView('songs')
    songplays_table = spark.sql('select \
          monotonically_increasing_id() as songplay_id,  \
          from_unixtime(l.ts/1000) as start_time, \
          userId as user_id,\
          l.level,\
          s.song_id,\
          s.artist_id,\
          l.sessionId as session_id,\
          l.location,\
          l.userAgent as user_agent\
          from \
          logs l \
          left join songs s on l.song = s.title')

    songplays_table = songplays_table.withColumn('year', year(
        songplays_table.start_time)).withColumn('month', month(songplays_table.start_time))

    # write songplays table to parquet files partitioned by year and month
    songplays_table.write.partitionBy(['year', 'month']).parquet(
        "{}/songplays.parquet".format(output_data), mode="overwrite")


def main():

    spark = create_spark_session()
    input_data_sd = config['LOCAL']['INPUT_DATA_SD_LOCAL']
    input_data_ld = config['LOCAL']['INPUT_DATA_LD_LOCAL']
    output_data = config['LOCAL']['OUTPUT_DATA_LOCAL']
    process_song_data(spark, input_data_sd, output_data)
    process_log_data(spark, input_data_ld, output_data)


if __name__ == "__main__":
    main()
