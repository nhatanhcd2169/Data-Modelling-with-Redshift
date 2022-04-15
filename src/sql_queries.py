import configparser


# CONFIG
config = configparser.ConfigParser()
config.read("../dwh.cfg")

LOG_DATA = config.get("S3", "log_data")
SONG_DATA = config.get("S3", "song_data")
LOG_JSONPATH = config.get("S3", "log_jsonpath")

ROLE_ARN = config.get("IAM_ROLE", "role_arn")
# DROP TABLES & SCHEMAS
staging_schema_drop = "DROP SCHEMA IF EXISTS staging CASCADE"
dwh_schema_drop = "DROP SCHEMA IF EXISTS dwh CASCADE"
staging_events_table_drop = "DROP table IF EXISTS staging_events"
staging_songs_table_drop = "DROP table IF EXISTS staging_songs"
songplay_table_drop = "DROP table IF EXISTS songplays"
user_table_drop = "DROP table IF EXISTS users"
song_table_drop = "DROP table IF EXISTS songs"
artist_table_drop = "DROP table IF EXISTS artists"
time_table_drop = "DROP table IF EXISTS time"


# CREATE TABLES & SCHEMAS
staging_schema_create = "CREATE SCHEMA IF NOT EXISTS staging"
dwh_schema_create = "CREATE SCHEMA IF NOT EXISTS dwh"
staging_schema_searchpath = "SET search_path TO staging"
dwh_schema_searchpath = "SET search_path TO dwh"

staging_events_table_create = """
    CREATE TABLE IF NOT EXISTS staging_events (
            artist              varchar,
            auth                varchar,
            firstName           varchar,
            gender              varchar,
            itemInSession       integer,
            lastName            varchar,
            length              double precision,
            level               varchar,
            location            varchar,
            method              varchar,
            page                varchar,
            registration        numeric,
            sessionId           integer,
            song                varchar,
            status              integer,
            ts                  bigint,
            userAgent           varchar,
            userId              integer
        );
"""

staging_songs_table_create = """
    CREATE TABLE IF NOT EXISTS staging_songs (
            num_songs           integer,
            artist_id           varchar,
            artist_latitude     numeric,
            artist_longitude    numeric,
            artist_location     varchar,
            artist_name         varchar,
            song_id             varchar,
            title               varchar,
            duration            double precision,
            year                integer
        ) ;
"""

songplay_table_create = """
    CREATE TABLE IF NOT EXISTS songplays (
            songplay_id         INT IDENTITY(0,1)       PRIMARY KEY, 
            start_time          timestamp,
            user_id             integer                 DISTKEY, 
            level               varchar, 
            song_id             varchar, 
            artist_id           varchar, 
            session_id          varchar, 
            location            varchar, 
            user_agent          varchar
        );
"""

user_table_create = """
    CREATE TABLE IF NOT EXISTS users (
            user_id             integer PRIMARY KEY     DISTKEY, 
            first_name          varchar, 
            last_name           varchar, 
            gender              varchar, 
            level               varchar
        )
"""

song_table_create = """
    CREATE TABLE IF NOT EXISTS songs (
            song_id             varchar PRIMARY KEY, 
            title               varchar, 
            artist_id           varchar, 
            year                integer, 
            duration            numeric
        ) diststyle all;
"""

artist_table_create = """
    CREATE TABLE IF NOT EXISTS artists (
            artist_id           varchar PRIMARY KEY, 
            name                varchar, 
            location            varchar, 
            latitude            numeric, 
            longitude           numeric
        ) diststyle all;
"""


time_table_create = """
    CREATE TABLE IF NOT EXISTS time (
            start_time          timestamp PRIMARY KEY, 
            hour                integer, 
            day                 integer, 
            week                integer, 
            month               integer, 
            year                integer, 
            weekday             integer
        ) diststyle all;
"""

# STAGING TABLES

staging_events_copy = (
    """
    COPY staging_events
    FROM {}
    IAM_ROLE '{}'
    JSON {}
"""
).format(LOG_DATA, ROLE_ARN, LOG_JSONPATH)

staging_songs_copy = (
    """
    COPY staging_songs
    FROM {}
    IAM_ROLE '{}'
    JSON 'auto'
"""
).format(SONG_DATA, ROLE_ARN)

# FINAL TABLES

songplay_table_insert = """
    INSERT INTO songplays (
            start_time, user_id, level, song_id, 
            artist_id, session_id, location, user_agent
        ) 
    SELECT DISTINCT date_add('ms', se.ts, '1970-01-01'), se.userId, se.level, ss.song_id, 
            ss.artist_id, se.sessionId, se.location, se.userAgent
    FROM staging.staging_events AS se 
    JOIN staging.staging_songs AS ss
    ON se.song = ss.title
    AND se.artist = ss.artist_name
    AND se.length = ss.duration
    WHERE se.page = 'NextSong';
"""

user_table_insert = """
    INSERT INTO users (user_id, first_name, last_name, gender, level)
    SELECT DISTINCT se.userId, se.firstName, se.lastName, se.gender, FIRST_VALUE(se.level) OVER (PARTITION BY se.userId ORDER BY se.ts DESC ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) 
    FROM staging.staging_events AS se
    WHERE se.userId IS NOT NULL AND se.page = 'NextSong'
"""

# https://www.sqltutorial.org/sql-window-functions/
# https://www.sqltutorial.org/sql-window-functions/sql-first_value/

song_table_insert = """
    INSERT INTO songs (song_id, title, artist_id, year, duration)
    SELECT DISTINCT ss.song_id, ss.title, ss.artist_id, ss.year, ss.duration
    FROM staging.staging_songs AS ss
    WHERE ss.song_id IS NOT NULL;
"""

artist_table_insert = """
    INSERT INTO artists (artist_id, name, location, latitude, longitude)
    SELECT DISTINCT ss.artist_id, ss.title, ss.artist_location, ss.artist_latitude, ss.artist_longitude
    FROM staging.staging_songs AS ss
    WHERE ss.artist_id IS NOT NULL;
"""


time_table_insert = """
    INSERT INTO time (start_time, hour, day, week, month, year, weekday)
    SELECT DISTINCT    
        s.start_time AS st, 
        EXTRACT (hour FROM st), 
        EXTRACT (day FROM st),  
        EXTRACT (week FROM st),  
        EXTRACT (day FROM st),  
        EXTRACT (year FROM st),  
        EXTRACT (dow FROM st)
    FROM songplays s;
"""

# QUERY LISTS

create_table_queries = [
    staging_schema_searchpath,
    staging_events_table_create,
    staging_songs_table_create,
    dwh_schema_searchpath,
    songplay_table_create,
    user_table_create,
    song_table_create,
    artist_table_create,
    time_table_create,
]
drop_table_queries = [
    staging_events_table_drop,
    staging_songs_table_drop,
    songplay_table_drop,
    user_table_drop,
    song_table_drop,
    artist_table_drop,
    time_table_drop,
]
create_schema_queries = [staging_schema_create, dwh_schema_create]
drop_schema_queries = [staging_schema_drop, dwh_schema_drop]
copy_table_queries = [
    staging_schema_searchpath,
    staging_events_copy,
    staging_songs_copy,
]
insert_table_queries = [
    dwh_schema_searchpath,
    songplay_table_insert,
    user_table_insert,
    song_table_insert,
    artist_table_insert,
    time_table_insert,
]
