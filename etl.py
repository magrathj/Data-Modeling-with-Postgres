import os
import glob
import psycopg2
import pandas as pd
from sql_queries import *
import uuid

def process_song_file(cur, filepath): 
    '''
    Function used to process song files; extracting the json document from the given filepath and outputting song and artist data into PostGres tables

    @param cur: this is a first param
    @param filepath: this is a second param
    '''
    # open song file
    df = pd.read_json(filepath, lines=True)

    # insert song record
    song_data = list(df[['song_id', 
                        'title', 
                        'artist_id', 
                        'year', 
                        'duration']].values[0])
    cur.execute(song_table_insert, song_data)
    
    # insert artist record
    artist_data = list(df[["artist_id", 
                          "artist_name", 
                          "artist_location", 
                          "artist_latitude", 
                          "artist_longitude"]]
                          .drop_duplicates()
                          .values[0]                          
                       )
    
    cur.execute(artist_table_insert, artist_data)


def process_log_file(cur, filepath):
    '''
    Function used to process log files; extracting the json document from the given filepath and outputting time, user, and songplay data into PostGres tables

    @param cur: this is a first param
    @param filepath: this is a second param
    '''
    # open log file
    df = pd.read_json(filepath, lines=True)

    # filter by NextSong action
    df = df.loc[df['page'] == "NextSong"]

    # convert timestamp column to datetime
    df["ts_converted"] = pd.to_datetime(df.ts, unit='ms').sort_values()
    t = df[["ts", "ts_converted"]]
    t["hour"] = t["ts_converted"].dt.hour
    t["day"] = t["ts_converted"].dt.day
    t["weekofyear"] = df["ts_converted"].dt.weekofyear
    t["month"] = df["ts_converted"].dt.month
    t["year"] = df["ts_converted"].dt.year
    t["weekday"] = df["ts_converted"].dt.weekday
    
    
    # insert time data records
    time_df = t[["ts_converted", 
                 "hour",
                 "day", 
                 "weekofyear",
                 "month", 
                 "year",
                 "weekday"]].drop_duplicates()


    for i, row in time_df.iterrows():
        cur.execute(time_table_insert, list(row))

    # load user table
    user_df = df[["userId", 
                  "firstName", 
                  "lastName", 
                  "gender", 
                  "level"]].drop_duplicates()
    user_df = user_df[(user_df['userId'].astype(str).str.len() != 0) ]

    # insert user records
    for i, row in user_df.iterrows():
        cur.execute(user_table_insert, row)

    # insert songplay records
    for index, row in df.iterrows():

        # get songid and artistid from song and artist tables
        cur.execute(song_select, (row.song, row.artist, row.length))
        results = cur.fetchone()

        if results:
            songid, artistid = results
        else:
            songid, artistid = None, None
        
        # insert songplay record
        songplay_data = (
                         row.ts_converted, 
                         row.userId, 
                         row.level, 
                         songid, 
                         artistid, 
                         row.sessionId, 
                         row.location, 
                         row.userAgent
        )
        
        cur.execute(songplay_table_insert, songplay_data)


def process_data(cur, conn, filepath, func):
    '''
    Function used as main method to call other functions given specified path and function call. 

    @param cur: this is a first param
    @param conn: this is a second param
    @param filepath: this is a second param
    @param conn: this is a second param
    '''
    # get all files matching extension from directory
    all_files = []
    for root, dirs, files in os.walk(filepath):
        files = glob.glob(os.path.join(root,'*.json'))
        for f in files :
            all_files.append(os.path.abspath(f))

    # get total number of files found
    num_files = len(all_files)
    print('{} files found in {}'.format(num_files, filepath))

    # iterate over files and process
    for i, datafile in enumerate(all_files, 1):
        func(cur, datafile)
        conn.commit()
        print('{}/{} files processed.'.format(i, num_files))


def main():
    '''
    This function calls all the processing functions within this application. 
    '''
    
    print('-----------------------')
    print('Starting main ')
    conn = psycopg2.connect("host=127.0.0.1 dbname=sparkifydb user=student password=student")
    cur = conn.cursor()

    process_data(cur, conn, filepath='data/song_data', func=process_song_file)
    process_data(cur, conn, filepath='data/log_data', func=process_log_file)
    conn.commit()

    conn.close()
    print('Ending main ')
    print('-----------------------')

if __name__ == "__main__":
    main()