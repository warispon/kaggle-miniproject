# config.py
from sqlalchemy import create_engine

# ----------------- Database config -----------------
DB_USER = "postgres"
DB_PASSWORD = "postgres"
DB_HOST = "localhost"   # ใช้จากเครื่อง host ต่อเข้า container
DB_PORT = "5432"
DB_NAME = "kaggle_db"

SCHEMA_RAW = "raw_data"
SCHEMA_PROD = "production"
RAW_TABLE_NAME = "data_raw"
PRODUCTION_TABLE_NAME = "data_prod"

DATABASE_URL = (
    f"postgresql+psycopg2://{DB_USER}:{DB_PASSWORD}"
    f"@{DB_HOST}:{DB_PORT}/{DB_NAME}"
)

def get_engine():
    """สร้าง SQLAlchemy engine ตัวเดียวใช้ร่วมกัน"""
    return create_engine(DATABASE_URL)

# ----------------- Google Sheets config -----------------
GOOGLE_SHEETS_SPREADSHEET_NAME = "Kaggle Pipeline Output"
GOOGLE_SHEETS_WORKSHEET_NAME = "ProductionData"

# ----------------- Source file config -----------------
KAGGLE_CSV_PATH = "data.csv"
SERVICE_ACCOUNT_FILE = "credentials.json"
