import os
import glob
import uuid
import psycopg2
import pandas as pd
import re
from datetime import datetime
from sql_queries import *
from sqlalchemy import create_engine
from create_tables import create_database, drop_tables
pd.options.mode.chained_assignment = None


def gather_all_data():
    """
    Description: This function gathers logs data from "data/log_data/2018/11/" 
    directory and songs metadata from multiple directories with the following
    pattern "data/song_data/**/**/**/*.json". Gathered data saved into the 
    respective DataFrames. Logs entries associated with "NextSong" are 
    selected for further transformations. 
    
    Arguments:
        None

    Returns:
        logs_songs: is the DataFrame obtained by left join on logs and songs.
        df_songs_lib: is the DataFrame that containes songs metadata.
    """
    
    json_pattern = os.path.join("data/log_data/2018/11/",'*.json')
    file_list = sorted(glob.glob(json_pattern))

    dfs = []
    for file in file_list:
        data = pd.read_json(file, lines=True)
        #match = re.search(r'\d{4}-\d{2}-\d{2}', file)
        #date = datetime.strptime(match.group(), '%Y-%m-%d').date()
        #data["date"] = date
        dfs.append(data)
    df_logs = pd.concat(dfs, ignore_index=True) # (8056, 18)
    songplays_raw = df_logs.loc[df_logs["page"] == "NextSong"].reset_index(drop=True) # (6820, 18)

    song_path = glob.glob("data/song_data/**/**/**/*.json")
    dfs_ = []

    for songs in song_path:
        sdata = pd.read_json(songs, lines=True)
        dfs_.append(sdata)
    df_songs_lib = pd.concat(dfs_, ignore_index=True) # (71, 10)

    logs_songs = pd.merge(songplays_raw, df_songs_lib, left_on=["artist","song"], right_on=["artist_name","title"], how='left') # (6820, 28)
    return logs_songs, df_songs_lib

def prepare_tables(logs_songs, df_songs_lib):
    # prepare the data for DB friendly readings
    """
    Description: This function further transforms logs and songs library DataFrames 
    to forme 5 DataFrames which are the prototypes of the future fact and dimension 
    tables for the database with a star schema.

    Arguments:
        logs_songs: is the DataFrame obtained by left join on logs and songs.
        df_songs_lib: is the DataFrame that containes songs metadata.

    Returns:
        songplays_df_: the DataFrame which is the prototype of a fact table
        users_df_: the DataFrame which is the prototype of a users dimension table
        songs_df_: the DataFrame which is the prototype of a songs dimension table
        artists_df_: the DataFrame which is the prototype of a artists dimension table
        time_df_: the DataFrame which is the prototype of a time dimension table
    """
    
    logs_songs["songplay_id"] = 1
    logs_songs["songplay_id"] = logs_songs.songplay_id.apply(lambda x: uuid.uuid4().hex)
    
    songplays_df = logs_songs[["songplay_id", "ts", "length", "userId", "level", "song_id", "artist_id", "sessionId", "location", "userAgent"]]
    songplays_df.columns = ["songplay_id", "start_time", "length_played", "user_id", "level", "song_id", "artist_id", "session_id", "location", "user_agent"]
    songplays_df_ = songplays_df.where(pd.notnull(songplays_df), None)

    users_df = logs_songs[["userId", "firstName", "lastName", "gender", "level"]].drop_duplicates().reset_index(drop=True) # (129, 5)
    users_df.columns = ["user_id", "first_name", "last_name", "gender", "level"]
    users_df["user_unique_id"] = 1
    users_df["user_unique_id"] = users_df.user_unique_id.apply(lambda x: uuid.uuid4().hex)
    users_df_ = users_df.where(pd.notnull(users_df), None)
    cols = users_df_.columns.tolist()
    cols = cols[-1:] + cols[:-1]
    users_df_ = users_df_[cols]

    songs_df = df_songs_lib[["song_id", "title", "artist_id", "year", "duration"]].drop_duplicates().reset_index(drop=True) # (71, 5)
    songs_df_ = songs_df.where(pd.notnull(songs_df), None)
    
    artists_df = df_songs_lib[["artist_id", "artist_name", "artist_location", "artist_latitude", "artist_longitude"]].drop_duplicates().reset_index(drop=True) # (69, 5)
    artists_df.columns = ["artist_id", "name", "location", "latitude", "longitude"]
    artists_df_ = artists_df.where(pd.notnull(artists_df), None)

    time_df = songplays_df[["songplay_id", "start_time"]]
    time_df["hour"] = time_df.start_time.apply(lambda x: pd.Timestamp(x, unit='ms').hour)
    time_df["day"] = time_df.start_time.apply(lambda x: pd.Timestamp(x, unit='ms').day)
    time_df["week"] = time_df.start_time.apply(lambda x: pd.Timestamp(x, unit='ms').week)
    time_df["month"] = time_df.start_time.apply(lambda x: pd.Timestamp(x, unit='ms').month)
    time_df["year"] = time_df.start_time.apply(lambda x: pd.Timestamp(x, unit='ms').year)
    time_df["weekday"] = time_df.start_time.apply(lambda x: pd.Timestamp(x, unit='ms').weekday()) # Monday == 0 … Sunday == 6
    time_df_ = time_df.where(pd.notnull(time_df), None) # time_df.shape = (6820, 8)
    
    return songplays_df_, users_df_, songs_df_, artists_df_, time_df_

def main():
    cur, conn = create_database()
    engine = create_engine('postgresql://student:student@127.0.0.1:5432/sparkifydb')
    drop_tables(cur, conn)
    
    logs_songs, df_songs_lib = gather_all_data()
    songplays_df, users_df, songs_df, artists_df, time_df = prepare_tables(logs_songs, df_songs_lib)
    
    # INSERT RECORDS
    songplays_df.to_sql('songplays', engine, index=False)
    users_df.to_sql('users', engine, index=False)
    songs_df.to_sql('songs', engine, index=False)
    artists_df.to_sql('artists', engine, index=False)
    time_df.to_sql('time', engine, index=False)
    
    with engine.connect() as con:
        con.execute('ALTER TABLE songplays ADD PRIMARY KEY (songplay_id);')
        con.execute('ALTER TABLE users ADD PRIMARY KEY (user_unique_id);')
        con.execute('ALTER TABLE songs ADD PRIMARY KEY (song_id);')
        con.execute('ALTER TABLE artists ADD PRIMARY KEY (artist_id);')
        con.execute('ALTER TABLE time ADD PRIMARY KEY (songplay_id);')

    conn.close()


if __name__ == "__main__":
    main()