# Data Lake in AWS Elastic MapReduce (EMR)
This project is a follow-up to [an earlier project](https://gitlab.com/offor20/data_modeling_with_postgreSQL). The music streaming company has grown its users base and decided to migrate the company's data to data lake in AWS EMR cluster. Their datasets reside in two directories inside an AWS s3 bucket. The directories are : 
* s3://udacity-dend/song_data
* s3://udacity-dend/log_data
<p>In the project, Extraction, Loading and Transformation (ELT) data pipeline is built. First and foremost, spark is used to read data from the aforementioned two urls. The loaded data is then processed in the EMR cluster using spark. Subsequently, the results are saved back to S3 storage in parquet format. The result can either be queried directly from the EMR cluster or the S3 storage in Business Intelligence (BI) and analytic apps. The architectural design of the project is as shown in the diagram below. Emphasis of this project is on the green region.  </p>

![EMR Cluster Architecture](/images/architecture.png)

Data is processed and extracted to realise five tables in a star schema, namely:
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
The following instructions describe what are needed to run or implement the codes implemented in the project. The project is implemented on the cloud but with a smaller datasets or very high CPU core, it can also be implemented locally. This implies that setting up an AWS Elastic Map Reduce (EMR) cluster is sufficient to run the codes and query the data. However, local installation guidelines and EMR cluster set up are given below.
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

### Create Notebook.
In order to run spark in the EMR cluster created above, a notebook needs to be created and configured in the cluster. In EMR cluster dashboard, select "Notebook" on the left menu list and click <span style="color:red">"Create Notebook"</span>. [Udacity](https://classroom.udacity.com/nanodegrees/nd027/parts/19ef4e55-151f-4510-8b5c-cb590ac52df2/modules/f268ecf3-99fa-4f44-8587-dfa0945b8a7f/lessons/1f8f1b41-f5aa-4276-93f7-ec4916a74ed5/concepts/eac5c2be-645d-4d58-b7ac-a2dc02268e7e) provides a template for configuring the Notebook as shown in the diagram below. However, you are free to choose Notebook Name of your choice. 
![Configure Notebook](/images/configure-notebook.png)

<p>Wait for the notebook status to be <span style="color: green">Ready</span> before proceeding with running the code and querying the data.</p>

## Implementation Steps
* Two similar python scripts or files namely: etl.ipynb and etl.py were created for development and deployment(production) respectively.
* In the scripts, three functions are used to process the song and log datasets. Here, all the necessary extraction, loading and transformations are carried out. See the functions' docStrings for more details.
* The datasets domiciled in Amazon S3 bucket directories are first read into song_data and log_data dataframes in the cluster, five tables are extracted from them and loaded back to another S3 bucket directories in Parquet format. Parquet format is highly compact, efficient and perfomant for data retrieval and analysis. The tables can be queried directly from the S3 storage. The song_data dataset was read using our own-defined schema called songSchema for conformance purposes in data types. 
* The process_song_data function:  Function to read song_data dataset, extract song_table and artists_table from it and save them in parquet format
    INPUTS/ARGUMENTS:
        * spark: spark session/context
        * input_data: defines an S3 bucket directory storing the dataset
        * output_data: defines an S3 bucket for saving the extracted data/tables in parquet format
    RETURNS:
        * song_data dataframe to be used in another function  
* The process_log_data function:  Procedure to read log_data dataset in S3, extracts users_table, time_table and save them in parquet format.
    It also processes the log_data and song_data datasets to create songplays_table and writes it in parquet format save to 
    to an S3 directory.
    INPUTS/ARGUMENTS:
        * spark: spark session/context
        * input_data: defines an S3 bucket directory storing the dataset
        * output_data: defines an S3 bucket for saving the extracted data/tables in parquet format
        * process_song_data: a function that returns song_data dataframe previously defined above
    RETURNS:
        * None   
* The main function: A procedure that defines arguments/inputs in the other two functions and performs the actual ELT.
    *  RETURNS:
        * None


## Running the code
Copy the the etl.ipynb file into the notebook.Starting from the first cell, click the Run/play button on the top toolbar to run every cell. Additional queries can be created to test the code. Also, etl.py is to be run in a terminal using SSH connection to the cluster by typing:
```
python3 etl.py 
```
and hiting the enter button.

## Authors
Ernest Offor Ugwoke -previous work [Data Modeling](https://gitlab.com/offor20/data_modeling_with_postgreSQL)

## Acknowledgement
The author acknowledges the [Udacity Data Engineering Team](www.udacity.com) who supervised and guided the implementation of the project.  