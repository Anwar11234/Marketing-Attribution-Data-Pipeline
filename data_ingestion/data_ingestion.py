import duckdb 
import os
from dotenv import load_dotenv
import logging

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s'
)
logger = logging.getLogger(__name__)

load_dotenv()

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
RAW_DATA_DIR = os.path.join(BASE_DIR, '..', 'raw_data')

GOOGLE_ADS_DIR = os.path.join(RAW_DATA_DIR, 'google_ads')
SEGMENT_DIR    = os.path.join(RAW_DATA_DIR, 'segment')

pg_conn_str = (
    f"host={os.environ['PG_HOST']} "
    f"port={os.environ.get('PG_PORT', '5432')} "
    f"dbname={os.environ['PG_DB']} "
    f"user={os.environ['PG_USER']} "
    f"password={os.environ['PG_PASSWORD']}"
)

with duckdb.connect(os.path.join(BASE_DIR, '..', 'marketing_data.duckdb')) as conn:
    conn.execute(""" CREATE SCHEMA IF NOT EXISTS raw""")

    conn.execute(f"""
        DROP TABLE IF EXISTS raw.google_ads;
        CREATE TABLE IF NOT EXISTS raw.google_ads AS
        SELECT * FROM read_csv('{GOOGLE_ADS_DIR}/*.csv')
    """)

    logger.info("raw.google_ads schema:\n%s", conn.execute("DESCRIBE raw.google_ads").df().to_string())
    logger.info("raw.google_ads row count:\n%s", conn.execute("SELECT COUNT(*) FROM raw.google_ads").df().to_string())

    conn.execute(f"""
        DROP TABLE IF EXISTS raw.segment_tracks;         
        CREATE TABLE IF NOT EXISTS raw.segment_tracks AS
        SELECT 
            message_id,
            type,
            event,
            timestamp,
            user_id,
            anonymous_id,
            json(properties) as properties,
            json(context) as context,
            session_id
                
        FROM read_json('{SEGMENT_DIR}/segment_tracks.jsonl', sample_size=-1)

    """)

    logger.info("raw.segment_tracks schema:\n%s", conn.execute("DESCRIBE raw.segment_tracks").df().to_string())
    logger.info("raw.segment_tracks row count:\n%s", conn.execute("SELECT COUNT(*) FROM raw.segment_tracks").df().to_string())

    conn.execute(f"""
        DROP TABLE IF EXISTS raw.app_conversions;
        CREATE TABLE IF NOT EXISTS raw.app_conversions AS 
        SELECT * 
        FROM postgres_scan(
            '{pg_conn_str}',
            'public',
            'app_conversions'
        )
    """)

    logger.info("raw.app_conversions schema:\n%s", conn.execute("DESCRIBE raw.app_conversions").df().to_string())
    logger.info("raw.app_conversions row count:\n%s", conn.execute("SELECT COUNT(*) FROM raw.app_conversions").df().to_string())