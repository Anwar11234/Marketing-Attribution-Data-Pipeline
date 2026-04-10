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

with duckdb.connect(os.path.join(BASE_DIR, '..', 'marketing_data.duckdb')) as conn:
    print(conn.execute("show all tables").df())