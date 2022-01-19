import configparser
from datetime import datetime
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf
from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear, dayofweek, date_format

#config = configparser.ConfigParser()
#config.read('dl.cfg')

#os.environ['AWS_ACCESS_KEY_ID']=config['AWS']['AWS_ACCESS_KEY_ID']
#os.environ['AWS_SECRET_ACCESS_KEY']=config['AWS']['AWS_SECRET_ACCESS_KEY']

def create_spark_session():
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .getOrCreate()
    return spark


def process_song_data(spark, input_data, output_data):
    # get filepath to song data file
    song_data = input_data + 'song_data/A/A/*/*.json'
    
    # read song data file
    df = spark.read.json(song_data)
    df.createOrReplaceTempView('df')

    # extract columns to create songs table
    songs_table = spark.sql('''
            select song_id, 
                title, artist_id, 
                year, 
                duration
            from df''').dropDuplicates()
    
    # write songs table to parquet files partitioned by year and artist
    songs_table.write.partitionBy('year', 'artist_id').parquet(os.path.join(output_data, 'songs/songs.parquet'), 'overwrite')

    # extract columns to create artists table
    artists_table = spark.sql('''
            select artist_id, 
                artist_name, 
                artist_location, 
                artist_latitude, 
                artist_longitude
            from df''').dropDuplicates()
    
    # write artists table to parquet files
    artists_table.write.parquet(os.path.join(output_data, 'artists/artists.parquet'), 'overwrite')


def process_log_data(spark, input_data, output_data):
    # get filepath to log data file
    log_data = input_data + 'log_data/*/*/*.json'

    # read log data file
    df = spark.read.json(log_data)
    df.createOrReplaceTempView('df')

    # filter by actions for song plays
    df_actions = spark.sql("""
            select *
            from df
            where page == 'NextSong'""")
    df_actions.createOrReplaceTempView('df_actions')

    # extract columns for users table    
    users_table = spark.sql("""
        select userId, 
            firstName, 
            lastName, 
            gender, 
            level
        from df_actions""").dropDuplicates()

     # write users table to parquet files
    users_table.write.parquet(os.path.join(output_data, 'users/users.parquet'), 'overwrite')

    # create datetime column from original timestamp column
    get_datetime = udf(lambda x: datetime.fromtimestamp(int(x/1000)).isoformat())

    df_actions = df_actions.withColumn('start_time', get_datetime(df_actions.ts)) \
        .withColumn('year', year(get_datetime(df_actions.ts))) \
        .withColumn('month', month(get_datetime(df_actions.ts))) \
        .withColumn('hour', hour(get_datetime(df_actions.ts))) \
        .withColumn('week', weekofyear(get_datetime(df_actions.ts))) \
        .withColumn('day', dayofmonth(get_datetime(df_actions.ts))) \
        .withColumn('weekday', dayofweek(get_datetime(df_actions.ts)))
    df_actions.createOrReplaceTempView('df_actions')

    time_table = spark.sql("""
        select start_time, 
            hour, 
            day, 
            week, 
            month, 
            year, 
            weekday
        from df_actions""").dropDuplicates()

    # write time table to parquet files partitioned by year and month
    time_table.write.partitionBy('year', 'month').parquet(os.path.join(output_data, 'times/times.parquet'), 'overwrite')

    # read in song data to use for songplays table
    song_data = input_data + 'song_data/A/A/*/*.json'
    df_songs = spark.read.json(song_data)
    df_songs.createOrReplaceTempView('df_songs')

    # extract columns from joined song and log datasets to create songplays table
    songplays_table = spark.sql("""
        select  ac.start_time,
            ac.userId, 
            ac.level, 
            so.song_id, 
            so.artist_id, 
            ac.sessionId, 
            so.artist_location, 
            ac.userAgent
        from df_actions ac
        inner join df_songs so on
            ac.song = so.title and
            ac.artist = so.artist_name""")

    # write songplays table to parquet files partitioned by year and month
    songplays_table.write.parquet(os.path.join(output_data, 'songplays/songplays.parquet'), 'overwrite')


def main():
    spark = create_spark_session()
    input_data = "s3a://udacity-dend/"
    output_data = "s3a://sparkify-rico"
    
    process_song_data(spark, input_data, output_data)    
    process_log_data(spark, input_data, output_data)


if __name__ == "__main__":
    main()
