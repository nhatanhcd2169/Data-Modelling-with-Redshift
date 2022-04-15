# SPARKIFY ETL Pipeline

## Project description

A startup named Sparkify released their music streaming app not long ago, and they now want to analyze the data they have been collecting on songs and user activity. Their main goal is an insight to what their users are listening to, however they cannot find an easy way to query those data. Therefore, they brought me in, an data engineer, in order to extract their data from S3, stage them in Redshift, and transform data into a set of dimensional tables for their analytics team.

In this project, I've implemented an ETL pipeline using Python to process the data from the 2 sources: song data and log data, under the format of *[JSON](https://en.wikipedia.org/wiki/JSON)* files.  

## How to run the project
- Prequisites:
    - Have *[newest Python](https://www.python.org/)* (or at least 3.7) installed in your system (you can download it *[here](https://www.python.org/downloads/)*)
    - Install psycopg2 and boto3
    - An AWS account
- Steps:
    - Open terminal in the code folder
    - Run the following:
        - `cd src`
        - `python iac.py init`
        - `python create_tables.py`
        - `python etl.py`
        - Optional: 
            - `cd ../test`
            - `python test.py` for testing analytic queries
            - `cd ../src`  
        - `python iac.py cleanup` (this is a must, otherwise it would cost a lot)


> In case you want to drop all the tables and run again, you first run `python create_tables.py` in prior to `python etl.py` in `src` folder.

## Sources

`iac.py`: codes for setting up REDSHIFT services or cleaning them up
`sql_queries.py`: contain all the query commands used in `create_tables.py` and `etl.py` 
`create_tables.py`: contain all the scripts used to drop and create tables or schemas
`etl.py`: contains all the scripts for staging, transforming and loading data into dimensional tables

> **Note**: Other `.py` file(s) are for **debugging** purposes and completely **optional**

## Database schema design

Regarding the choice of schema, I chose the *[Star schema](https://en.wikipedia.org/wiki/Star_schema)* due to the fact that I want my queries to be simplier with fewer JOIN operations. Furthermore, aggregation can also benefit from it which might help Sparkify's analysts a lot.

There lies the `songplay` fact table in the center of the schema surrounded by 4 dimension tables: `users`, `time`, `artists` and `songs`. Each of the 4 has its primary key acting as the foreign keys of `songplays`. Furthermore, there is the foreign key `artist_id` in relation `songs` which references the primary key of relation `artists`, this forms a star-like schema, just like its name. In addition to that, I set distribution style of the table `time`, `songs` and `artists` to be `ALL` since they are fairly small and can speed up the queries, whereas `users` and `songplays` are too big to do so.


![Sparkify Database Schema](/schema/schema.png "Sparkify Database Schema")

This kind of schema allows the analysts to easily query the data directly from the `songplays` table, and for ad-hoc queries (queries that cannot be determined prior to the moment the query is issued), they simply just perform `JOIN` and some optional aggregation functions.

## DATASETS
Here is a sample of 2 kinds of data that this database will perform ETL on, you can take a peek if you are interested as they are located in the subfolder `data` in `sources`

```
{
   data/song_data/../../*.json
   {
      "num_songs": 1,
      "artist_id": "ARJIE2Y1187B994AB7", 
      "artist_latitude": null, 
      "artist_longitude": null, 
      "artist_location": "", 
      "artist_name": "Line Renaud", 
      "song_id": "SOUPIRU12A6D4FA1E1", 
      "title": "Der Kleine Dompfaff", 
      "duration": 152.92036, 
      "year": 0
   }
   ...
   data/log_data/../../*.json
   {
      {
         "artist":"N.E.R.D. FEATURING MALICE",
         "auth":"Logged In",
         "firstName":"Jayden",
         "gender":"M",
         "itemInSession":0,
         "lastName":"Fox",
         "length":288.9922,
         "level":"free",
         "location":"New Orleans-Metairie, LA",
         "method":"PUT",
         "page":"NextSong",
         "registration":1541033612796.0,
         "sessionId":184,
         "song":"Am I High (Feat. Malice)",
         "status":200,
         "ts":1541121934796,
         "userAgent":"\"Mozilla\/5.0 (Windows NT 6.3; WOW64) AppleWebKit\/537.36 (KHTML, like Gecko) Chrome\/36.0.1985.143 Safari\/537.36\"",
         "userId":"101"
      }
   }
}
```
## Example queries

1) Get the list of cities which have most **paid** users (for business strategies)

```
   SELECT location AS city, COUNT(location) AS listener_count 
      FROM dwh.songplays 
      WHERE level = 'paid' 
      GROUP BY city 
      ORDER by listener_count DESC 
```

<img src="/test/test_query_1.png" width="450" height="450"/>

2) From the results of the query above, let's find the average play per city

```
    SELECT (query_2.total_play / query_2.number_of_city) AS avg_play_each_city 
    FROM (
        SELECT SUM(query_1.listener_count) AS total_play, COUNT(city) AS number_of_city FROM (
            SELECT location AS city, COUNT(location) AS listener_count 
            FROM dwh.songplays 
            WHERE level = 'paid' 
            GROUP BY city 
            ORDER by listener_count DESC
            ) query_1
        ) query_2
```
<img src="/test/test_query_2.png" width="200" height="60"/>
