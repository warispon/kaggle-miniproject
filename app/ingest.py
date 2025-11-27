import pandas as pd
from sqlalchemy import text
from config import (
    get_engine,
    SCHEMA_RAW,
    RAW_TABLE_NAME,
    KAGGLE_CSV_PATH,
)

def run():
    """อ่านไฟล์ Kaggle (data.csv) แล้วโหลดเข้าตารางใน schema raw_data"""
    print("🚀 [INGEST] เริ่มดึงข้อมูลจากไฟล์ Kaggle เข้าฐานข้อมูล...")

    # 1) อ่านไฟล์ CSV จาก Kaggle
    df = pd.read_csv(KAGGLE_CSV_PATH, encoding="cp1252")
    print(f"[INGEST] อ่านไฟล์ {KAGGLE_CSV_PATH} ได้ {len(df)} แถว")

    engine = get_engine()

    # 2) สร้าง schema raw_data ถ้ายังไม่มี
    with engine.begin() as conn:
        conn.execute(text(f"CREATE SCHEMA IF NOT EXISTS {SCHEMA_RAW};"))
        print(f"[INGEST] ตรวจสอบ/สร้าง schema '{SCHEMA_RAW}' เรียบร้อย")

    # 3) เขียน DataFrame ลง PostgreSQL
    df.to_sql(
        RAW_TABLE_NAME,
        con=engine,
        schema=SCHEMA_RAW,
        if_exists="replace",
        index=False,
    )
    print(
        f"[INGEST] เขียนข้อมูลเข้า PostgreSQL -> {SCHEMA_RAW}.{RAW_TABLE_NAME} "
        f"จำนวน {len(df)} แถวเรียบร้อย"
    )

if __name__ == "__main__":
    run()