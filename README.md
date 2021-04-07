# Data Lake in AWS Elastic MapReduce (EMR)
This project is a follow-up to [an earlier project](https://gitlab.com/offor20/data_modeling_with_postgreSQL). The music streaming company has grown its users base and decided to migrate the company's data to data lake in AWS EMR cluster. Their datasets reside in two directories inside an AWS s3 bucket. The directories are : 
* s3://udacity-dend/song_data
* s3://udacity-dend/log_data
<p>In the project, Extraction, Loading and Transformation (ELT) data pipeline is built. First and foremost, spark is used to read data from the aforementioned two urls. The loaded data is then processed in the EMR cluster using spark. Subsequently, the results are saved back to S3 storage in parquet format. The result can either be queried directly from the EMR cluster or the S3 storage in Business Intelligence (BI) and analytic apps. The architectural design of the project is as shown in the diagram below. Emphasis of this project is on the green region.  </p>

![EMR Cluster Architecture](/images/architecture.png)

Data is processed and extracted to realise five tables, namely:
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
<p>
The diagram below illustrates the star schema represented by the five tables above.
</p>

![star schema](/images/star_schema.png)

## Getting Started with the Project
The following instructions describe what are needed to run or implement the codes implemented in the project. The project is implemented on the cloud but with a smaller datasets, it can also be implemented locally.
### Prerequisites
The following software packages and account are needed to have the project up and running for both testing and development purposes in local machine if so desired:
* Python 3
* Apache Spark
* Pyspark
* Amazon Account with I AM User credentials

### Installation
* Python can be installed by following the instructions in the links below:
    * [Python 3 on MacOS.](https://docs.python-guide.org/starting/install3/osx/#install3-osx)
    * [Python 3 on Linux.](https://docs.python-guide.org/starting/install3/linux/#install3-linux)
    * [Python 3 on Windows.](https://docs.python-guide.org/starting/install3/win/#install3-windows)
* [Installing Spark on Linux.](https://phoenixnap.com/kb/install-spark-on-ubuntu)
* [Installing Spark on Windows.](https://phoenixnap.com/kb/install-spark-on-windows-10)
* Once Python and Apache Spark are installed, Pyspark can be installed in terminal or command prompt by using pip as shown below.
```
pip install pyspark
```
### Sign up or Login to AWS Account
* [Create or sign in to an AWS account](https://signin.aws.amazon.com/signin?redirect_uri=https%3A%2F%2Fconsole.aws.amazon.com%2Fconsole%2Fhome%3Fstate%3DhashArgs%2523%26isauthcode%3Dtrue&client_id=arn%3Aaws%3Aiam%3A%3A015428540659%3Auser%2Fhomepage&forceMobileApp=0&code_challenge=m5zL3DNQLwKaLrLui1qIRag_AjJ-uxrKiSAqSioXp40&code_challenge_method=SHA-256)
* [Add an I AM User](https://console.aws.amazon.com/iam/home?region=us-west-2#/users)
### Set up an AWS EMR Cluster 
The diagram below demonstrated how to set up an EMR Cluster to run and test the project.
![EMR Cluster set up](/images/configuring-emr-cluster.png)



