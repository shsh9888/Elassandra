import sys
import logging
from cassandra.cluster import Cluster

# ------------------------------
# SCRIPT CONFIG
KEYSPACE = 'music'
REPLICATION_FACTOR = 3
LYRICS_TABLE = 'lyrics'
MSD_TABLE = 'msd'


# ------------------------------
# METHODS
def _create_keyspace(session):
    try:
        session.execute("CREATE KEYSPACE IF NOT EXISTS {} "
                        "WITH replication = {{ 'class': 'NetworkTopologyStrategy', 'DC1': {} }}"
                        .format(KEYSPACE, REPLICATION_FACTOR))
    except Exception as e:
        logging.exception(e)
        raise
    else:
        logging.info("Created KEYSPACE '{}'".format(KEYSPACE))


def _create_tables(session):
    try:
        session.execute("USE {}".format(KEYSPACE))

        session.execute("CREATE TABLE IF NOT EXISTS {} ( "
                        "track_id text PRIMARY KEY, "
                        "artist_familiarity double, "
                        "artist_hotttnesss double, "
                        "artist_id text, "
                        "artist_mbid text, "
                        "artist_playmeid int, "
                        "artist_7digitalid int, "
                        "artist_latitude double, "
                        "artist_longitude double, "
                        "artist_location text, "
                        "artist_name text, "
                        "release text, "
                        "release_7digitalid int, "
                        "song_id text, "
                        "song_hotttnesss double, "
                        "title text, "
                        "track_7digitalid int, "
                        "similar_artists list<text>, "
                        "analysis_sample_rate int, "
                        "audio_md5 text, "
                        "danceability double, "
                        "duration double, "
                        "end_of_fade_in double, "
                        "energy double, "
                        "key int, "
                        "key_confidence double, "
                        "loudness double, "
                        "mode int, "
                        "mode_confidence double, "
                        "start_of_fade_out double, "
                        "tempo double, "
                        "time_signature int, "
                        "time_signature_confidence double, "
                        "year int"
                        ")".format(MSD_TABLE))

        logging.info("Created TABLE: {}.{}".format(KEYSPACE, MSD_TABLE))

        session.execute("CREATE TABLE IF NOT EXISTS {} ( "
                        "track_id text PRIMARY KEY, "
                        "mxm_track_id text, "
                        "counts map<text, int> "
                        ")".format(LYRICS_TABLE))

        logging.info("Created TABLE: {}.{}".format(KEYSPACE, LYRICS_TABLE))

    except Exception as e:
        logging.exception(e)
        raise


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


def setup_schema(cluster_ips):
    try:
        session = _connect_cassandra(cluster_ips)

        _create_keyspace(session)
        _create_tables(session)

    except Exception:
        logging.error("Script terminated unsucessfully.")
    else:
        logging.info("Script terminated successfully.")


# ------------------------------
# DRIVER
if __name__ == '__main__':

    if len(sys.argv) == 1:
        print("ERROR: No IP address specified. Provide space separated IP addresses (at least one required).")

    logging.basicConfig(level=logging.DEBUG, format="%(asctime)s %(levelname)s %(message)s", handlers=(
                            logging.FileHandler("schema_setup_log.txt", mode="w"), logging.StreamHandler(sys.stdout)))

    setup_schema(sys.argv[1:])
