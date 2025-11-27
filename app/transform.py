# transform.py
import pandas as pd
from sqlalchemy import text
from datetime import datetime

from config import (
    get_engine,
    SCHEMA_RAW,
    RAW_TABLE_NAME,
    SCHEMA_PROD,
    PRODUCTION_TABLE_NAME,
)

def run():
    """อ่านข้อมูลจาก raw_data -> แปลง -> ส่งไป schema production"""
    print("🛠️ [TRANSFORM] เริ่มอ่านข้อมูลจาก schema raw_data...")

    engine = get_engine()

    # 1) อ่านข้อมูลจากตาราง raw_data
    query_raw = f'SELECT * FROM "{SCHEMA_RAW}"."{RAW_TABLE_NAME}";'
    df_raw = pd.read_sql(query_raw, engine)
    print(f"[TRANSFORM] ดึงข้อมูลจาก {SCHEMA_RAW}.{RAW_TABLE_NAME} ได้ {len(df_raw)} แถว")

    # 2) ตัวอย่างการ clean แบบ generic
    df = df_raw.copy()

    numeric_cols = df.select_dtypes(include=["number"]).columns
    object_cols = df.select_dtypes(include=["object"]).columns

    df[numeric_cols] = df[numeric_cols].fillna(0)
    df[object_cols] = df[object_cols].fillna("Unknown")

    df["_pipeline_transformed_at"] = datetime.utcnow()

    # 3) สร้าง schema production ถ้ายังไม่มี
    with engine.begin() as conn:
        conn.execute(text(f"CREATE SCHEMA IF NOT EXISTS {SCHEMA_PROD};"))
        print(f"[TRANSFORM] ตรวจสอบ/สร้าง schema '{SCHEMA_PROD}' เรียบร้อย")

    # 4) เขียน DataFrame ที่ transform แล้วไปเก็บ schema production
    df.to_sql(
        PRODUCTION_TABLE_NAME,
        con=engine,
        schema=SCHEMA_PROD,
        if_exists="replace",
        index=False,
    )
    print(
        f"[TRANSFORM] เขียนข้อมูลไปที่ {SCHEMA_PROD}.{PRODUCTION_TABLE_NAME} "
        f"จำนวน {len(df)} แถวเรียบร้อย"
    )

if __name__ == "__main__":
    run()
