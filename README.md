# Making-A-Data-Lake

<img src='https://upload.wikimedia.org/wikipedia/commons/thumb/f/f3/Apache_Spark_logo.svg/1200px-Apache_Spark_logo.svg.png'>

<br>

## Project Walkthrough


- The aim of this project is to load data from an S3 Bucket into spark,
- Create the required tables,
- And then write these tables into HDFS.

<br>

## To run this project run the following command

<br>

<code> pip install -r requirements.txt </code>

## The etl.py script

- It contains two main functions (<code>process_song_data()</code>, <code> process_log_data()</code>)

- <code>process_song_data()</code> loads songs data from json files stored on an S3 bucket into a spark dataframe and then loads its data to two dimention tables(songs_table, and artists_table) then it loads the tables into an HDFS.
-  <code>process_log_data()</code> loads logs data from json files stored on an S3 bucket into a spark dataframe and then loads its data to two dimention tables and one fact table (users_table, time_table, and songplays_table) then it loads the tables into an HDFS.

