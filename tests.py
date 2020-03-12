from cassandra.cluster import Cluster


def print_data(lengths, header, data):
    """ Pretty print fetched data.

    Args:
        lengths print length of each field
        header first row of headers
        data iterable of rows
    Precondition:
        len(lengths) == len(header) == len(data[i])
    """
    format_string = ' '.join(map(lambda x: "%%-%ss" % x, lengths))
    print(format_string % header)
    for row in data:
        print(format_string %
              tuple(str(val)[:l] for val, l in zip(row, lengths)))


if __name__ == "__main__":

    # establish a session with local cluster
    cluster = Cluster()
    session = cluster.connect()

    # set session to events keyspace
    session.set_keyspace("events")

    # test query 1
    rows = session.execute(
        "SELECT artist, song_title, song_length FROM song_plays \
         WHERE session_id=%s AND item_in_session=%s",
        (139, 7)
    )
    print_data((20, 20, 11), ("Artist", "Song", "Duration"), rows)

    # test query 2
    rows = session.execute(
        "SELECT artist, song_title, user_first_name, user_last_name \
         FROM song_sessions WHERE user_id=%s AND session_id=%s",
        (8, 139)
    )
    print_data(
        lengths=(15, 30, 12, 12),
        header=("Artist", "Song Title", "First Name", "Last Name"),
        data=rows
    )

    # test query 3
    rows = session.execute(
        "SELECT user_first_name, user_last_name FROM song_by_users \
         WHERE song_title=%s",
        ("Eye Of The Tiger",)
    )
    print_data((12, 12), ("First Name", "Last Name"), rows)

    session.shutdown()
    cluster.shutdown()
