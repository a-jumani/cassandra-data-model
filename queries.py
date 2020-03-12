# Query 1
# give me the artist, song title and song's length in the music app
# history that was heard during sessionId = 338, and itemInSession = 4

# PRIMARY KEY explanation
# The constraint dictates that both sessionId and itemInSession should be part
# of the primary key. They also uniquely identify all the rows in
# event_datafile_new.csv. Since this type of query will be used to fetch only
# one record at a time, using a clustering column for sorted output won't be
# useful. Hence, both columns become part of partition key.
table1 = """CREATE TABLE IF NOT EXISTS song_plays (
    session_id bigint,
    item_in_session int,
    artist text,
    song_title text,
    song_length double,
    PRIMARY KEY((session_id, item_in_session))
)"""

insert1 = "INSERT INTO song_plays (session_id, item_in_session, \
           artist, song_title, song_length) VALUES (%s, %s, %s, %s, %s)"


def data1(line):
    return (int(line[8]), int(line[3]), line[0], line[9], float(line[5]))


# Query 2
# give me only the following: name of artist, song (sorted by itemInSession)
# and user (first and last name) for userid = 10, sessionid = 182

# PRIMARY KEY explanation
# To avoid overwriting records, we must identify each row from
# event_datafile_new.csv, hence, both sessionId and itemInSession are part of
# the primary key. userId is also part of the primary key since it is part of
# the where clause. We need the records sorted by itemInSession so it was made
# a clustering column. Both userId and sessionId, hence, became part of
# partition key.
table2 = """CREATE TABLE IF NOT EXISTS song_sessions (
    user_id int,
    session_id bigint,
    item_in_session int,
    artist text,
    song_title text,
    user_first_name text,
    user_last_name text,
    PRIMARY KEY((user_id, session_id), item_in_session))"
)"""

insert2 = "INSERT INTO song_sessions (user_id, session_id, item_in_session, \
           artist, song_title, user_first_name, user_last_name) VALUES (%s, \
           %s, %s, %s, %s, %s, %s)"


def data2(line):
    return (int(line[10]), int(line[8]), int(line[3]), line[0], line[9],
            line[1], line[4])


# Query 3
# give me every user name (first and last) in my music app history who
# listened to the song 'All Hands Against His Own'

# PRIMARY KEY explanation
# We do not care in what session did a user listen to a particular song, hence,
# we only need songTitle against userId to answer the query. As we will be
# searching by only songTitle, userId cannot be part of the partition key.
table3 = """CREATE TABLE IF NOT EXISTS song_by_users (
    song_title text,
    user_id int,
    user_first_name text,
    user_last_name text,
    PRIMARY KEY((song_title), user_id)
)"""

insert3 = "INSERT INTO song_by_users (song_title, user_id, user_first_name, \
           user_last_name) VALUES (%s, %s, %s, %s)"


def data3(line):
    return (line[9], int(line[10]), line[1], line[4])


tables = [table1, table2, table3]
inserts = [insert1, insert2, insert3]
data_extractions = [data1, data2, data3]
