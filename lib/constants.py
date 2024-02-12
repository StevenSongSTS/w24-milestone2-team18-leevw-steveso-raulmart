import os

from dotenv import load_dotenv

load_dotenv()
DB_URL = os.getenv("DB_CONNECTION_STRING")
BENZINGA_KEY = os.getenv("BENZINGA_KEY")
SHARED_RANDOM_STATE = 1337
MIN_TEXT_SUMMARY_LEN = 100
MAX_TEXT_SUMMARY_LEN = 510
