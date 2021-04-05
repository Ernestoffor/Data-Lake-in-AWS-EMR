# Data Lake in AWS Elastic MapReduce (EMR)
This project is a follow-up to [an earlier project](https://gitlab.com/offor20/data_modeling_with_postgreSQL). The music streaming company has grown its users base and decided to migrate the company's data to data lake in AWS EMR cluster. Their datasets reside in two directories inside an AWS s3 bucket. The directories are : 
* s3://udacity-dend/song_data
* s3://udacity-dend/log_data
In the project, Extraction, Loading and Transformation (ELT) data pipeline is built. First and foremost, spark is used to read data from the aforementioned two urls. Data is extracted to realise five tables, namely:
* **songplays_table (a Fact-Table)**- a table computed from the log records where page = NextSong. These are songs that can be played. The table has the following fields: 
    * songplay_id of IntegerType
    * start_time of DoubleType
    * user_id  of IntegerType
    * level of StringType
    * song_id of StringType
    * artist_id of StringType
    * session_id of IntegerType
    * location of StringType
    * user_agent of StringType
* **users_table (a dimensional table)** - a table of users of the company's app, comprising the following fields:
    * user_id of IntegerType
    * first_name of StringType
    * last_name of StringType
    * gender of StringType
    * level of StingType
* **songs_table (a dimensional table)** - a table of songs in the music database with the following fields:
    * song_id of StringType
    * title of StringType
    * artist_id of StringType
    * year of IntegerType
    * duration of DoubleType
* **artists_table (a dimensional table)** - is a table of artists with songs' titles in their name in the music database. It consists of the following fields:
    * artist_id of StringType
    * name of StringType
    * location of StringType
    * lattitude of DoubleType
    * longitude of DoubleType
* **time_table (a dimensional_table)** -is a table of timestamps of records in songplays broken down into the following specific units:
    * start_time of TimeStampType
    * hour of IntegerType
    * day of IntegerType
    * week of IntegerType
    * month of IntegerType
    * year of IntegerType
    * weekday of IntegerType
The diagram below illustrates the star schema represented by the five tables above.

![star schema](/images/star_schema.png)
Format: ![Alt Picture of the star schema](url)

