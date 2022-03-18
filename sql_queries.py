import configparser


# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')

# DROP TABLES

staging_events_table_drop = "DROP TABLE IF EXISTS staging_events;"
staging_songs_table_drop = "DROP TABLE IF EXISTS staging_songs;"
songplay_table_drop = "DROP TABLE IF EXISTS songplays;"
user_table_drop = "DROP TABLE IF EXISTS users;"
song_table_drop = "DROP TABLE IF EXISTS songs;"
artist_table_drop = "DROP TABLE IF EXISTS artists;"
time_table_drop = "DROP TABLE IF EXISTS time;"

# CREATE TABLES

staging_events_table_create= ("""
CREATE TABLE IF NOT EXISTS staging_events (
    artist VARCHAR,
    auth VARCHAR,
    firstName VARCHAR(50),
    gender CHAR,
    itemInSession INTEGER,
    lastName VARCHAR(50),
    length FLOAT,
    level VARCHAR,
    location VARCHAR,
    method VARCHAR,
    page VARCHAR,
    registration FLOAT,
    sessionId INTEGER,
    song VARCHAR,
    status INTEGER,
    ts BIGINT,
    userAgent VARCHAR,
    userId INTEGER
)
""")

staging_songs_table_create = ("""
CREATE TABLE IF NOT EXISTS staging_songs
(
    num_songs INTEGER,
    artist_id VARCHAR,
    artist_latitude FLOAT,
    artist_longitude FLOAT,
    artist_location VARCHAR,
    artist_name VARCHAR,
    song_id VARCHAR,
    title VARCHAR,
    duration FLOAT,
    year FLOAT
);
""")

songplay_table_create = ("""
CREATE TABLE IF NOT EXISTS songplays
(
    songplay_id INTEGER IDENTITY (1, 1) PRIMARY KEY ,
    start_time TIMESTAMP,
    user_id INTEGER,
    level VARCHAR,
    song_id VARCHAR,
    artist_id VARCHAR,
    session_id INTEGER,
    location VARCHAR,
    user_agent VARCHAR
)
DISTSTYLE KEY
DISTKEY ( start_time )
SORTKEY ( start_time );
""")

user_table_create = ("""
CREATE TABLE IF NOT EXISTS users
(
    userId INTEGER not null PRIMARY KEY sortkey,
    firstName VARCHAR(50),
    lastName VARCHAR(50),
    gender CHAR(1) ENCODE BYTEDICT,
    level VARCHAR ENCODE BYTEDICT
)
diststyle all;
""")

song_table_create = ("""
CREATE TABLE IF NOT EXISTS songs
(
    song_id VARCHAR not null PRIMARY KEY sortkey,
    title VARCHAR,
    artist_id VARCHAR,
    year INTEGER ENCODE BYTEDICT,
    duration FLOAT
)
diststyle all;
""")

artist_table_create = ("""
CREATE TABLE IF NOT EXISTS artists
(
    artist_id VARCHAR not null PRIMARY KEY sortkey,
    name VARCHAR,
    location VARCHAR,
    latitude FLOAT,
    longitude FLOAT
)
diststyle all;
""")

time_table_create = ("""
CREATE TABLE IF NOT EXISTS time
(
    start_time  TIMESTAMP PRIMARY KEY ,
    hour INTEGER,
    day INTEGER,
    week INTEGER,
    month INTEGER,
    year INTEGER ENCODE BYTEDICT ,
    weekday VARCHAR(9) ENCODE BYTEDICT
)
DISTSTYLE KEY
DISTKEY ( start_time )
SORTKEY (start_time);
""")

# STAGING TABLES
#copying data from s3 bucket into the staging tables
staging_events_copy = ("""
COPY staging_events
FROM {}
iam_role {}
FORMAT AS json {};
""").format(config['S3']['LOG_DATA'], config['IAM_ROLE']['ARN'], config['S3']['LOG_JSONPATH'])

staging_songs_copy = ("""
COPY staging_songs
FROM {}
iam_role {}
FORMAT AS json 'auto';
""").format(config['S3']['SONG_DATA'], config['IAM_ROLE']['ARN'])

# FINAL TABLES

songplay_table_insert = ("""
INSERT INTO songplays (
    start_time,
    user_id,
    level,
    song_id,
    artist_id,
    session_id,
    location,
    user_agent
) 
SELECT DISTINCT timestamp 'epoch' + CAST(se.ts AS BIGINT)/1000 * interval '1 second' as start_time,
    se.userId as user_id,
    se.level as level,
    ss.song_id as song_id,
    ss.artist_id as artist_id,
    se.sessionId as session_id,
    se.location as location,
    se.userAgent as user_agent
FROM staging_events se
JOIN staging_songs ss ON (se.song = ss.title AND se.artist = ss.artist_name) 
""")

user_table_insert = ("""
INSERT INTO users (
    userId,
    firstName,
    lastName,
    gender,
    level
)
SELECT DISTINCT se.userId as userId,
    se.firstName as firstName,
    se.lastName as lastName,
    se.gender as gender,
    se.level as level
FROM staging_events se
where userId IS NOT NULL
""")

song_table_insert = ("""
INSERT INTO songs (
    song_id,
    title,
    artist_id,
    year,
    duration
)
SELECT DISTINCT ss.song_id as song_id,
    ss.title as title,
    ss.artist_id as artist_id,
    ss.year as year,
    ss.duration as duration
FROM staging_songs ss
""")

artist_table_insert = ("""
INSERT INTO artists (
    artist_id,
    name,
    location,
    latitude,
    longitude
)
SELECT DISTINCT ss.artist_id as artist_id,
    ss.artist_name as name,
    ss.artist_location as location,
    ss.artist_latitude as latitude,
    ss.artist_longitude as longitude
FROM staging_songs ss
""")

time_table_insert = ("""
INSERT INTO time (
    start_time,
    hour,
    day,
    week,
    month,
    year,
    weekday
)
SELECT DISTINCT timestamp 'epoch' + CAST(se.ts AS BIGINT)/1000 * interval '1 second' as start_time,
    extract(HOUR FROM timestamp 'epoch' + CAST(se.ts AS BIGINT)/1000 * interval '1 second') as hour,
    extract(DAY FROM timestamp 'epoch' + CAST(se.ts AS BIGINT)/1000 * interval '1 second') as day,
    extract(WEEK FROM timestamp 'epoch' + CAST(se.ts AS BIGINT)/1000 * interval '1 second') as week,
    extract(MONTH FROM timestamp 'epoch' + CAST(se.ts AS BIGINT)/1000 * interval '1 second') as month,
    extract(YEAR FROM timestamp 'epoch' + CAST(se.ts AS BIGINT)/1000 * interval '1 second') as year,
    extract(DAY FROM timestamp 'epoch' + CAST(se.ts AS BIGINT)/1000 * interval '1 second') as weekday
FROM staging_events se
""")



# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]
