# DROP TABLES

songplay_table_drop = "DROP TABLE IF EXISTS songplays;"
user_table_drop = "DROP TABLE IF EXISTS users;"
song_table_drop = "DROP TABLE IF EXISTS songs;"
artist_table_drop = "DROP TABLE IF EXISTS artists;"
time_table_drop = "DROP TABLE IF EXISTS time;"

# CREATE TABLES

songplay_table_create = ("""
CREATE TABLE songplays(
    songplay_id SERIAL PRIMARY KEY, 
    start_time timestamp, 
    user_id INT NOT NULL, 
    level varchar(255), 
    song_id varchar(255),
    artist_id varchar(255), 
    session_id varchar(255),
    location varchar(255), 
    user_agent varchar(255)
)
""")

user_table_create = ("""
CREATE TABLE Users(
    user_id INT PRIMARY KEY NOT NULL, 
    first_name varchar(255), 
    last_name varchar(255), 
    gender varchar(255), 
    level varchar(255)
)
""")

song_table_create = ("""
CREATE TABLE songs(
    song_id varchar(255) PRIMARY KEY, 
    title varchar(255), 
    artist_id varchar(255), 
    year INT, 
    duration double precision 
)
""")

artist_table_create = ("""
CREATE TABLE artists(
    artist_id varchar(255) PRIMARY KEY, 
    name varchar(255), 
    location varchar(255), 
    latitude double precision, 
    longitude double precision 
)
""")

time_table_create = ("""
CREATE TABLE Time(
    start_time TIMESTAMP PRIMARY KEY NOT NULL, 
    hour INT, 
    day INT, 
    week INT, 
    month INT, 
    year INT, 
    weekday INT
)
""")

# INSERT RECORDS

songplay_table_insert = ("""
INSERT INTO songplays (start_time, user_id, level, song_id, artist_id, session_id, location, user_agent) \
                 VALUES (%s, %s,%s, %s, %s,%s, %s, %s)
""")

user_table_insert = ("""
INSERT INTO users (user_id, first_name, last_name, gender, level
) \
                 VALUES (%s, %s, %s, %s, %s) \
                 ON CONFLICT(user_id) DO UPDATE SET level = excluded.level;
""")

song_table_insert = ("""
INSERT INTO songs (song_id, title, artist_id, year, duration) \
                 VALUES (%s, %s, %s, %s, %s) \
                 ON CONFLICT (song_id) DO NOTHING;
""")

artist_table_insert = ("""
INSERT INTO artists (artist_id, name, location, latitude, longitude) \
                 VALUES (%s, %s, %s, %s, %s) \
                 ON CONFLICT (artist_id) DO NOTHING;
""")


time_table_insert = ("""
INSERT INTO time (start_time, hour, day, week, month, year, weekday) \
                 VALUES (%s, %s, %s, %s, %s, %s, %s) \
                 ON CONFLICT (start_time) DO NOTHING;
""")

# FIND SONGS
##Since the log file does not specify an ID for either the song or the artist, you'll need to get the song ID and artist ID by querying the songs and artists tables to find matches based on song title, artist name, and song duration tim 
# song_id, title, artist_id, year, duration
song_select = ("""
SELECT s.song_id, a.artist_id
FROM songs s
JOIN artists a
     ON a.artist_id=s.artist_id 
WHERE s.title = %s
AND a.name = %s
AND s.duration = %s
""")

# QUERY LISTS

create_table_queries = [songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]