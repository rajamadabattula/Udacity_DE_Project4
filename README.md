# Data Engineering Nanodegree - Data Lake

On this project, we were requested to build a data ingestion pipeline for a number of JSON files provided on a S3 bucket along with the processing in Spark and the data loading into S3 parquet files. Some data manipulation was requested in order to create the parquet files with the same structure we've seen on the previous sessions.

## About Database

Sparkify analytics database (called here sparkifydb) schema has a star design. Start design means that it has one Fact Table having business data, and supporting Dimension Tables. Star DB design is maybe the most common schema used in ETL pipelines since it separates Dimension data into their own tables in a clean way and collects business critical data into the Fact table allowing flexible queries.

### Raw JSON data structures

- **log_data**: log_data contains data about what users have done (columns: event_id, artist, auth, firstName, gender, itemInSession, lastName, length, level, location, method, page, registration, sessionId, song, status, ts, userAgent, userId)
- **song_data**: song_data contains data about songs and artists (columns: num_songs, artist_id, artist_latitude, artist_longitude, artist_location, artist_name, song_id, title, duration, year)

Findings:

- Input data was available first offline during the development phase and later from s3://udacity-dend/song_data and s3://udacity-dend/log_data
- As expected, reading and especially writing data (song_data, log_data) to and from S3 is very slow process due to large amount of input data and slow network connection. See above for figures with 1 song_data file and 1 log_data file.

### Fact Table

- **songplays**: song play data together with user, artist, and song info (songplay_id, start_time, user_id, level, song_id, artist_id, session_id, location, user_agent)

### Dimension Tables

- **users**: user info (columns: user_id, first_name, last_name, gender, level)
- **songs**: song info (columns: song_id, title, artist_id, year, duration)
- **artists**: artist info (columns: artist_id, name, location, latitude, longitude)
- **time**: detailed time info about song plays (columns: start_time, hour, day, week, month, year, weekday)

## Project Execution

The right order to the execute this project is:

1. `python3 etl.py`

The scripts must be executed on this order in order to correctly finish the processing. The script `etl.py` reads the JSON files, executes the data manipulation using Spark and save the data back on S3 as parquet files.

## Files Structure

### `etl.py`

This is the script that executes the ETL job logic itself. Its job is to load the JSON files into staging tables and then process the data into dimension and fact tables. The following functions can be found on the script.

`create_spark_session()` creates the Spark Session where the data will be processed.

`process_song_data(spark, input_data, output_data)` reads the song data from S3 bucket and process the data in order to create the tables `songs` and `artists` according to the requirements. Once the processing is finished the tables are uploaded back to S3 but as parquet files.

`process_log_data(spark, input_data, output_data)` reads logs data from S3 and process the data to create the tables `users`, `times` and `songplays`. It also saves the tables as parquet files back to S3.

`main()` is entry function and sets how the process occurs when the script is executed. The scope here is to connect to the the cluster according to the configuration defined on `aws/credentials.cfg`, set `input_data` and `output_data` variables and call the functions `process_song_data` and `process_log_data`.
