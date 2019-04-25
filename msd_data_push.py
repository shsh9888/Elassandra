import os
import sys
import logging
import argparse
import hdf5_getters
from cassandra.cluster import Cluster
from cassandra.query import SimpleStatement

# ------------------------------
# SCRIPT CONFIG
KEYSPACE = "music"
TABLENAME = 'msd'

FIELDS = [
    'artist_familiarity',
    'artist_hotttnesss',
    'artist_id',
    'artist_mbid',
    'artist_playmeid',
    'artist_7digitalid',
    'artist_latitude',
    'artist_longitude',
    'artist_location',
    'artist_name',
    'release',
    'release_7digitalid',
    'song_id',
    'song_hotttnesss',
    'title',
    'track_7digitalid',
    'similar_artists',
    'analysis_sample_rate',
    'audio_md5',
    'danceability',
    'duration',
    'end_of_fade_in',
    'energy',
    'key',
    'key_confidence',
    'loudness',
    'mode',
    'mode_confidence',
    'start_of_fade_out',
    'tempo',
    'time_signature',
    'time_signature_confidence',
    'track_id',
    'year'
]

query = SimpleStatement("INSERT INTO {}.{} ("
                        "track_id, artist_familiarity, artist_hotttnesss, artist_id, artist_mbid, artist_playmeid, "
                        "artist_7digitalid, artist_latitude, artist_longitude, artist_location, artist_name, release, "
                        "release_7digitalid, song_id, song_hotttnesss, title, track_7digitalid, similar_artists, "
                        "analysis_sample_rate, audio_md5, danceability, duration, end_of_fade_in, energy, key, "
                        "key_confidence, loudness, mode, mode_confidence, start_of_fade_out, tempo, time_signature, "
                        "time_signature_confidence, year) "
                        "VALUES ("
                        "%(track_id)s, %(artist_familiarity)s, %(artist_hotttnesss)s, %(artist_id)s, %(artist_mbid)s, "
                        "%(artist_playmeid)s, %(artist_7digitalid)s, %(artist_latitude)s, %(artist_longitude)s, "
                        "%(artist_location)s, %(artist_name)s, %(release)s, %(release_7digitalid)s, %(song_id)s, "
                        "%(song_hotttnesss)s, %(title)s, %(track_7digitalid)s, %(similar_artists)s, "
                        "%(analysis_sample_rate)s, %(audio_md5)s, %(danceability)s, %(duration)s, %(end_of_fade_in)s, "
                        "%(energy)s, %(key)s, %(key_confidence)s, %(loudness)s, %(mode)s, %(mode_confidence)s, "
                        "%(start_of_fade_out)s, %(tempo)s, %(time_signature)s, %(time_signature_confidence)s, %(year)s)"
                        .format(KEYSPACE, TABLENAME))


# ------------------------------
# METHODS

def validate_db(session):
    # keyspace exists?
    if len(session.execute("SELECT * FROM system_schema.keyspaces WHERE keyspace_name = '{}'".format(KEYSPACE))
                   .current_rows) > 0:
        logging.info("Verified keyspace existence for '{}'.".format(KEYSPACE))
    else:
        logging.error("Keyspace '{}' does not exist. Run the schema setup first.".format(KEYSPACE))
        raise RuntimeError()

    # table exists?
    if len(session.execute("SELECT table_name FROM system_schema.tables WHERE keyspace_name='{}' AND table_name='{}'"
                                   .format(KEYSPACE, TABLENAME)).current_rows) > 0:
        logging.info("Verified table existence for '{}'.".format(TABLENAME))
    else:
        logging.error("Table '{}' does not exist. Run the schema setup first.".format(TABLENAME))
        raise RuntimeError()

    return


def parse_args(argv=sys.argv[1:]):
    parser = argparse.ArgumentParser()
    req = parser.add_argument_group('required arguments')
    req.add_argument("-d", "--directory", help="data directory location", nargs=1, type=str)
    req.add_argument("-c", "--cluster", help="one or more cluster ip addresses", nargs="+", type=str)

    args = parser.parse_args(argv)

    if args.directory and args.cluster:
        return args.directory, args.cluster
    else:
        parser.print_help()
        exit(2)


def get_data_from_file(filepath):
    h5 = hdf5_getters.open_h5_file_read(filepath)
    try:
        data = dict()
        for field in FIELDS:
            result = getattr(hdf5_getters, 'get_{}'.format(field))(h5)
            data[field] = result

        return data
    finally:
        h5.close()


def _connect_cassandra(cluster_ips):
    try:
        cluster = Cluster(cluster_ips)
        session = cluster.connect()
    except Exception as e:
        logging.exception(e)
        raise
    else:
        logging.info("Connected to Cassandra cluster {}".format(cluster_ips))
        return session


def push_msd_data():
    try:
        data_dir, cluster_ips = parse_args()
        session = _connect_cassandra(cluster_ips)

        validate_db(session)

        for dir, subdirs, files in os.walk(data_dir[0]):
            for file in files:
                filepath = os.path.join(dir, file)
                data = get_data_from_file(filepath)

                # Insert data into table
                session.execute(query, data)

                data.pop('similar_artists')
                logging.debug(data)

            logging.info("Pushed data from '{}'".format(os.path.abspath(dir)))

    except Exception as e:
        logging.exception(e)
        logging.error("Script terminated unsucessfully.")
    else:
        logging.info("Script terminated successfully.")


# ------------------------------
# DRIVER
if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s", handlers=(
        logging.FileHandler("msd_push_log.txt", mode="w"), logging.StreamHandler(sys.stdout)))

    push_msd_data()
