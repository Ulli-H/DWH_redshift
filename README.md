# DWH in AWS Redshift

DataWarehousing in the cloud, with Amazon Redshift


*An Udacity Data Engineer Nanodegree project*

## Content
- [Description](#description)
- [Data](#data)
- [Redshift](#Redshift)
- [Workflow ETL-process](#workflow-etl-process)
- [Links](#links)

## Description  

The subject of the project is the fictional music streaming provider "Sparkify", which is in need of a data warehouse to analyze their data generated from the application.  The client requests for a service hosted in the AWS cloud. An Amazon Redshift DWH, a cluster with 4 nodes and raw data stored in s3 buckets. The tables in the database 'sparkifydb' are build in a 5 table star schema. The table and schema design are given in the [Redshift DWH](#Redshift-DWH) section. 



## Data  

The data used for this project consists of a song dataset and log dataset, which are both stored in s3 buckets. Both datasets are multiple files in JSON format. 
The song data files are partitioned by the first 3 letters of each song's track ID and contain information on each song and the artist of the song. 

The logdata files contain event data for every event of user activity that happended in the app during one day. The files are partitioned by year and date. 

All data was provided by Udacity as part of the data engineer nanodegree. 



## Redshift DWH  

The database is named "sparkifydb" and consists out of the following tables:

### - staging table for events data:
A staging table for the log dataset. 

### - staging table for songs data:
A staging table for the song dataset.


### - fact table: songplays (records in the log data associated with song plays)  
columns: *songplay_id, start_time, user_id, level, song_id, artist_id, session_id, location, user_agent*

### - dimension table: users (data on app users)  
columns: *user_id, first_name, last_name, gender, level*

### - dimension table: songs (data on all songs in library/app)  
columns: *song_id, title, artist_id, year, duration*

### - dimension table: artists (data on all artists in library/app)  
columns: *artist_id, name, location, latitude, longitude*

### - dimension table: time (timestamps of songplays in log data brocken down in different units)  
columns: *start_time, hour, day, week, month, year, weekday*



## Workflow ETL-process
  
1) I started out by writing the SQL statements for creating the tables and data ingestion in <code>sql_queries.py</code>

2) Afterwards I created the infrastructe in redshift. 

3) The <code>create_tables.py</code> contains the logic for dropping the tables (if they already exist) and creating the 2 staging tables as well as the 5 fact- and dimension tables.
  
4) In <code>etl.py</code> is the logic for loading the data from s3 to the staging tables and ingesting the data from staging to the fact and dimension tables. 

5) For confirmation of the process run the files in the following order:
- create_tables.py
- etl.py



## Links

[Repository](https://github.com/Ulli-H/DWH_Redshift)  
