import configparser


# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')
LOG_DATA = config.get('S3','LOG_DATA')
LOG_JSONPATH = config.get('S3','LOG_JSONPATH')
SONG_DATA = config.get('S3','SONG_DATA')
ARN = config.get('IAM_ROLE','ARN')

# DROP TABLES

staging_events_table_drop = "DROP TABLE IF EXISTS event_data;"
staging_songs_table_drop = "DROP TABLE IF EXISTS song_data;"
songplay_table_drop = "DROP TABLE IF EXISTS songplays;"
user_table_drop = "DROP TABLE IF EXISTS users;"
song_table_drop = "DROP TABLE IF EXISTS songs;"
artist_table_drop = "DROP TABLE IF EXISTS artists;"
time_table_drop = "DROP TABLE IF EXISTS time;"

# CREATE TABLES

staging_events_table_create= ("""
    CREATE TABLE event_data
    (
        artist VARCHAR(MAX),
        auth VARCHAR(15) NOT NULL,
        firstName VARCHAR(30),
        gender VARCHAR(1),
        itemInSession INTEGER NOT NULL,
        lastName VARCHAR(30),
        length FLOAT,
        level VARCHAR(4) NOT NULL,
        location VARCHAR(MAX),
        method VARCHAR(4) NOT NULL,
        page VARCHAR(20) NOT NULL,
        registration BIGINT,
        sessionId INTEGER NOT NULL,
        song VARCHAR(MAX),
        status INTEGER NOT NULL,
        ts BIGINT,
        userAgent VARCHAR(MAX),
        userId INTEGER
    );
""")

staging_songs_table_create = ("""
    CREATE TABLE song_data
    (
        num_songs INTEGER,
        artist_id VARCHAR(30) NOT NULL,
        artist_latitude FLOAT,
        artist_longitude FLOAT,
        artist_location VARCHAR(MAX),
        artist_name VARCHAR(MAX),
        song_id VARCHAR(30),
        title VARCHAR(MAX),
        duration FLOAT,
        year INTEGER
    );
""")

songplay_table_create = ("""
    CREATE TABLE songplays
    (
      songplay_id INTEGER IDENTITY(0,1) PRIMARY KEY, 
      start_time TIMESTAMP NOT NULL, 
      user_id INTEGER,
      level VARCHAR(4) NOT NULL,
      song_id VARCHAR(30) NOT NULL SORTKEY DISTKEY,
      artist_id VARCHAR(30) NOT NULL,
      session_id INTEGER NOT NULL,
      location VARCHAR(MAX),
      user_agent VARCHAR(MAX) 
    );
""")

user_table_create = (""" 
    CREATE TABLE users
    (
        user_id INTEGER PRIMARY KEY,
        first_name VARCHAR(30),
        last_name VARCHAR(30),
        gender VARCHAR(1),
        level VARCHAR(4)
    );
""")

song_table_create = ("""
    CREATE TABLE songs
    (
        song_id VARCHAR(30) PRIMARY KEY SORTKEY DISTKEY,
        title VARCHAR(MAX) NOT NULL,
        artist_id VARCHAR(30) NOT NULL,
        year INTEGER NOT NULL,
        duration FLOAT NOT NULL
    );
""")

artist_table_create = ("""
    CREATE TABLE artists
    (
        artist_id VARCHAR(MAX) PRIMARY KEY,
        name VARCHAR(MAX),
        location VARCHAR(MAX),
        latitude FLOAT,
        longitude FLOAT    
    );
""")

time_table_create = ("""
    CREATE TABLE time
    (
        start_time TIMESTAMP PRIMARY KEY SORTKEY,
        hour INTEGER NOT NULL,
        day INTEGER NOT NULL,
        week INTEGER NOT NULL,
        month VARCHAR(10) NOT NULL,
        year INTEGER NOT NULL,
        weekday VARCHAR(10) NOT NULL
    );
""")

# STAGING TABLES

staging_events_copy = ("""
    COPY event_data FROM '{}'
    CREDENTIALS 'aws_iam_role={}'
    REGION 'us-west-2'
    FORMAT AS JSON {};
""").format(LOG_DATA, ARN, LOG_JSONPATH)

staging_songs_copy = ("""
    COPY song_data FROM '{}'
    CREDENTIALS 'aws_iam_role={}'
    REGION 'us-west-2'
    FORMAT AS JSON 'auto';
""").format(SONG_DATA, ARN)

# FINAL TABLES

songplay_table_insert = ("""INSERT INTO songplays (start_time, user_id, level, song_id, artist_id, session_id, location, user_agent)
                                SELECT TIMESTAMP 'epoch' + e.ts/1000 * INTERVAL '1 second', e.userId, e.level, s.song_id, s.artist_id, e.sessionId, e.location, e.userAgent
                                FROM event_data e
                                JOIN song_data s 
                                    ON (e.artist = s.artist_name AND e.song = s.title)
""")

user_table_insert = ("""INSERT INTO users (user_id, first_name, last_name, gender, level)
                        SELECT DISTINCT(userId), firstName, lastName, gender, level
                        FROM event_data
                        WHERE userId IS NOT NULL
""")

song_table_insert = ("""INSERT INTO songs (song_id, title, artist_id, year, duration)
                        SELECT song_id, title, artist_id, year, duration
                        FROM song_data
                        WHERE song_id IS NOT NULL
""")

artist_table_insert = ("""INSERT INTO artists (artist_id, name, location, latitude, longitude)
                            SELECT DISTINCT(artist_id), artist_name, artist_location, artist_latitude, artist_longitude
                            FROM song_data
                            WHERE artist_id IS NOT NULL
""")

time_table_insert = ("""INSERT INTO time (start_time, hour, day, week, month, year, weekday)
                        SELECT DISTINCT(start_time) AS t,
                            EXTRACT(hour FROM t), EXTRACT(day FROM t), EXTRACT(week FROM t), EXTRACT(month FROM t), EXTRACT(year FROM t), EXTRACT(dayofweek FROM t)
                        FROM songplays
""")

# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]
