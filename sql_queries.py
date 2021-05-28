# DROP TABLES

songplay_table_drop = "DROP TABLE IF EXISTS songplays"
user_table_drop = "DROP TABLE IF EXISTS users"
song_table_drop = "DROP TABLE IF EXISTS songs"
artist_table_drop = "DROP TABLE IF EXISTS artists"
time_table_drop = "DROP TABLE IF EXISTS time"

# CREATE TABLES

songplay_table_create = "CREATE TABLE IF NOT EXISTS songplays \
( \
songplay_id text NOT NULL PRIMARY KEY, \
start_time int NOT NULL, \
length_played numeric, \
user_id int, \
level text, \
song_id text, \
artist_id text, \
session_id int, \
location text, \
user_agent text \
);"

user_table_create = "CREATE TABLE IF NOT EXISTS users \
( \
user_unique_id text NOT NULL PRIMARY KEY, \
user_id int NOT NULL, \
first_name text, \
last_name text, \
gender text, \
level text \
);"

song_table_create = "CREATE TABLE IF NOT EXISTS songs \
( \
song_id text NOT NULL PRIMARY KEY, \
title text, \
artist_id text, \
year int, \
duration numeric \
);"

artist_table_create = "CREATE TABLE IF NOT EXISTS artists \
( \
artist_id text NOT NULL PRIMARY KEY, \
name text, \
location text, \
latitude numeric, \
longitude numeric \
);"

time_table_create = "CREATE TABLE IF NOT EXISTS time \
( \
songplay_id text NOT NULL PRIMARY KEY, \
start_time int NOT NULL, \
hour int, \
day int, \
week int, \
month int, \
year int, \
weekday int \
);"

# INSERT RECORDS performed in "etl.py" by using Pandas DataFrame.to_sql() lines: 115 - 119


# FIND SONGS

song_select = "SELECT songplay_id, user_id, level, songplays.song_id, title, artists.name \
               FROM ((songplays JOIN songs ON songplays.song_id = songs.song_id) \
               JOIN artists ON songs.artist_id = artists.artist_id) \
               WHERE songplays.song_id IS NOT NULL"

# QUERY LISTS

create_table_queries = [songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]