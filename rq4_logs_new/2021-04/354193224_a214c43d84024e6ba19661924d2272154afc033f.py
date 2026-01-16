# DROP TABLES

songplay_table_drop = "DROP TABLE IF EXISTS songplays"
user_table_drop = "DROP TABLE IF EXISTS users"
song_table_drop = "DROP TABLE IF EXISTS songs"
artist_table_drop = "DROP TABLE IF EXISTS artists"
time_table_drop = "DROP TABLE IF EXISTS time"

# CREATE TABLES

songplay_table_create = ("""
CREATE TABLE IF NOT EXISTS songplays 
(songplay_id serial, 
level text, 
session_id int, 
location varchar, 
user_agent varchar, 
song_id varchar, 
artist_id varchar, 
start_time timestamp not null, 
user_id varchar not null,
primary key(songplay_id));
""")

user_table_create = ("""
CREATE TABLE IF NOT EXISTS users
(user_id varchar,
first_name varchar,
last_name varchar,
gender text, 
level text,
primary key (user_id));
""")

song_table_create = ("""
CREATE TABLE IF NOT EXISTS songs 
(song_id varchar,
title varchar,
artist_id varchar,
year int,
duration decimal,
primary key(song_id));
""")

artist_table_create = ("""
CREATE TABLE IF NOT EXISTS artists 
(artist_id varchar,
name varchar,
location varchar,
latitude decimal,
longitude decimal,
primary key(artist_id));
""")

time_table_create = ("""
CREATE TABLE IF NOT EXISTS time
(start_time timestamp,
hour int,
day int,
week int,
month int,
year int,
weekday int,
primary key(start_time));
""")

# INSERT RECORDS

#we don't handle conflicts here, as the primary key autoincrements
songplay_table_insert = ("""
INSERT INTO songplays
(start_time, user_id, level, song_id, artist_id, session_id, location, user_agent)
VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
""")

#users changing their streaming plans (i.e. contained by the 'level' column) can cause upsert conflicts. On conflict, update user 'level'
user_table_insert = ("""
INSERT INTO users 
(user_id, first_name, last_name, gender, level)
VALUES (%s, %s, %s, %s, %s)
ON CONFLICT (user_id) DO UPDATE SET level = excluded.level;
""")

#song_id upsert conflicts are likely erroneous, because these fields don't get updated over time. On conflict, do nothing
song_table_insert = ("""
INSERT INTO songs
(song_id, title, artist_id, year, duration)
VALUES (%s, %s, %s, %s, %s)
ON CONFLICT (song_id) DO NOTHING;
""")

#artist_id upsert conflicts could occur when artists change their name, location and lat/long. But not in this project, so on conflict, do nothing
artist_table_insert = ("""
INSERT INTO artists
(artist_id, name, location, latitude, longitude)
VALUES (%s, %s, %s, %s, %s)
ON CONFLICT (artist_id) DO NOTHING;
""")

#start_time upsert conflicts are likely erroneous. On conflict, do nothing
time_table_insert = ("""
INSERT INTO time
(start_time, hour, day, week, month, year, weekday)
VALUES (%s, %s, %s, %s, %s, %s, %s)
ON CONFLICT (start_time) DO NOTHING;
""")

# FIND SONGS

song_select = ("""
SELECT songs.song_id, artists.artist_id
FROM songs JOIN artists ON songs.artist_id = artists.artist_id
WHERE songs.title = %s AND
artists.name = %s AND
songs.duration = %s;
""")

# QUERY LISTS
create_table_queries = [songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]