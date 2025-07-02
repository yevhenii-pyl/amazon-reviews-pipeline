import os
from cassandra.cluster import Cluster

def get_cassandra_session():
    cassandra_host = os.getenv("CASSANDRA_HOST")
    cassandra_keyspace = os.getenv("CASSANDRA_KEYSPACE")
    cluster = Cluster([cassandra_host])
    session = cluster.connect(cassandra_keyspace)
    return session