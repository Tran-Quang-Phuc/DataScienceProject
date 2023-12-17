import psycopg2
from config import DBConfig


def connect_to_db(connect_url):
    return psycopg2.connect(connect_url)

def sync_to_db(
    warehouse_table: str,
    db_table: str,
    connect_url: DBConfig.connection_url
):
    pass
    