import configparser
from datetime import datetime
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col
from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear, date_format
from pyspark.sql import functions as F
from pyspark.sql import types as Ty
#import org.apache.spark.sql.functions._
from pyspark.sql.functions import monotonically_increasing_id
from pyspark.sql.types import StructType as ST, StructField as SF, DoubleType as Db, IntegerType as INT,\
StringType as S, DateType as DT, TimestampType as T, LongType as L


config = configparser.ConfigParser()
config.read('dl.cfg')

os.environ['AWS_ACCESS_KEY_ID']=config['KEYS']['AWS_ACCESS_KEY_ID']
os.environ['AWS_SECRET_ACCESS_KEY']=config['KEYS']['AWS_SECRET_ACCESS_KEY']


def create_spark_session():
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .getOrCreate()
    return spark

# Define a Schema for the song_data dataset to be read properly
songSchema = ST([
    SF("num_songs", L()),
    SF("artist_id", S()),
    SF("artist_latitude", Db()),
    SF("artist_longitude", Db()),
    SF("artist_location", S()),
    SF("artist_name", S()),
    SF("song_id", S()),
    SF("title", S()),
    SF("duration", Db()),
    SF("year", INT())
])

# Define a function that processes the song dataset¶
def process_song_data(spark, input_data, output_data):
    """
    Function to read song_data dataset, extract song_table and artists_table from it and save them in parquet format
    INPUTS/ARGUMENTS:
        * spark: spark session/context
        * input_data: defines an S3 bucket directory storing the dataset
        * output_data: defines an S3 bucket for saving the extracted data/tables in parquet format
    RETURNS:
        * song_data dataframe to be used in another function    
    """
    # get filepath to song data file
    song_data_path = os.path.join(input_data, "song_data/*/*/*/*.json")
    # read song data file
    song_data = spark.read.json(song_data_path, schema=songSchema, mode="PERMISSIVE")
    
    # save song_data in parquet format
    #song_data.write.mode("overwrite").parquet(os.path.join(output_data, "song_data1"))
    
    # Extract songs_table
    songs_table = song_data.select("song_id", "title", "artist_id", "year", "duration")
        
    # write songs table to parquet files partitioned by year and artist
    creation_time = datetime.now().strftime("%Y-%m-%d-%H-%M-%S-%f")
    song_table_output_path1 = output_data + "songs_table" + creation_time
    song_table_output_path = os.path.join(output_data, "songs_table")
    #songs_table.write.partitionBy("year", "artist_id").parquet(song_table_output_path1)
    songs_table.write.mode("overwrite").partitionBy("year", "artist_id").parquet(song_table_output_path)

    # extract columns to create artists table
    artists_table = song_data.select("artist_id", "artist_name" , "artist_location", "artist_latitude", "artist_longitude")
    # write artists table to parquet files
    artists_table_output_path = os.path.join(output_data, "artists_table") 
    artists_table.write.mode("overwrite").parquet(artists_table_output_path)
    
    return song_data 

# Define a function to process log data
def process_log_data(spark, input_data, output_data, process_song_data):
    """
    Procedure to read log_data dataset in S3, extracts users_table, time_table and save them in parquet format.
    It also processes the log_data and song_data datasets to create songplays_table and writes it in parquet format save to 
    to an S3 directory.
    INPUTS/ARGUMENTS:
        * spark: spark session/context
        * input_data: defines an S3 bucket directory storing the dataset
        * output_data: defines an S3 bucket for saving the extracted data/tables in parquet format
        * process_song_data: a function that returns song_data dataframe previously defined above
    RETURNS:
        * None   
    """
    # get filepath to log data file
    log_data_path = os.path.join(input_data, "log_data/*/*/*.json")

    # read log data file
    log_data = spark.read.json(log_data_path)
    
    # Save log data in parquet format
    #log_data.write.mode("overwrite").parquet(os.path.join(output_data, "log_data1"))
    # filter by actions for songplays
    log_data = log_data.filter(log_data.page=="NextSong")
    
    # extract columns for users table    
    users_table = log_data.select("userId", "firstName", "lastName", "gender", "level")
    
    # Save users_table in parquet format
    users_table_output_path = os.path.join(output_data, "users_table")
    users_table.write.mode("overwrite").parquet(users_table_output_path)


    # create timestamp column from original timestamp column
    get_timestamp = udf(lambda x:  (x/1000.0) , Ty.DoubleType()) 
    log_data = log_data.withColumn("start_time", get_timestamp(log_data.ts))
    
    
    # create datetime column from original timestamp column
    get_datetime = udf(lambda x: datetime.fromtimestamp(x), Ty.TimestampType())
    log_data = log_data.withColumn('datetime', get_datetime('start_time'))
    
    # create a temporary view for the log_data
    log_data.createOrReplaceTempView("log")
         
    # extract columns to create time table
    time_table = spark.sql("""
            SELECT start_time, hour(datetime) as hour, 
            dayofmonth(datetime) as day, weekofyear(datetime) as week_of_the_year,
            month(datetime) as month, year(datetime) as year
            
            FROM log
    """)
  
    # write time table to parquet files partitioned by year and month
    time_output = os.path.join(output_data, "time_table") 
    time_table.write.mode("overwrite").partitionBy("year", "month").parquet(time_output)

    # call the process_song_data function previously defined
    song_df = process_song_data(spark, input_data, output_data)
    
    # Create a temporary view of the song_df 
    song_df.createOrReplaceTempView("songs")
    
    # Add songplay_id to the log_data 
    log_data = log_data.withColumn("songplay_id", monotonically_increasing_id())
    
    # extract columns from joined song and log datasets to create songplays table 
    #songplay_id, start_time, user_id, level, song_id, artist_id, session_id, location, user_agent
    songplays_table = log_data.join(song_df, (log_data.song == song_df.title) & 
                              (log_data.artist == song_df.artist_name) & (log_data.length == song_df.duration), 
                              'left_outer')\
        .select(
            log_data.songplay_id,
            log_data.datetime.alias("start_time"),
            col("userId").alias('user_id'),
            log_data.level,
            song_df.song_id,
            song_df.artist_id,
            col("sessionId").alias("session_id"),
            log_data.location,
            col("useragent").alias("user_agent"),
            year('datetime').alias('year'),
            month('datetime').alias('month'))    

    # write songplays table to parquet files partitioned by year and month
    songplays_table_output_path = os.path.join(output_data, "songplays_table")
    songplays_table.write.mode("overwrite").partitionBy("year", "month").parquet(songplays_table_output_path)
    

def main():
    spark = create_spark_session()
    input_data = "s3a://udacity-dend/"
    
    #input_data = "s3a://data-lake-ernest/"
    output_data = "s3a://data-lake-ernest/"
    #output_data = "offor_data/output/"
    
    process_song_data(spark, input_data, output_data)    
    process_log_data(spark, input_data, output_data, process_song_data)


if __name__ == "__main__":
    main()