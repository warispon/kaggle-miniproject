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
    """
    ดึงข้อมูลจาก raw_data.data_raw (Online Retail CSV)
    ทำความสะอาด + เพิ่มคอลัมน์ที่ใช้วิเคราะห์
    แล้วเก็บลง production.data_prod
    """
    print("🛠️ [TRANSFORM] เริ่มอ่านข้อมูลจาก schema raw_data...")

    engine = get_engine()

    # 1) อ่านข้อมูลจากตาราง raw_data
    query_raw = f'SELECT * FROM "{SCHEMA_RAW}"."{RAW_TABLE_NAME}";'
    df_raw = pd.read_sql(query_raw, engine)
    print(f"[TRANSFORM] ดึงข้อมูลจาก {SCHEMA_RAW}.{RAW_TABLE_NAME} ได้ {len(df_raw)} แถว")

    df = df_raw.copy()

    # ---------- 2) ทำความสะอาดข้อมูล (Clean) ----------

    # 2.1 ลบแถวซ้ำ (duplicate rows)
    before = len(df)
    df = df.drop_duplicates()
    after = len(df)
    print(f"[TRANSFORM] ลบข้อมูลซ้ำ {before - after} แถว (เหลือ {after} แถว)")

    # 2.2 กรองแถวที่มี Quantity และ UnitPrice > 0 เท่านั้น
    #     (ตัดคืนสินค้า / ใบแจ้งหนี้ที่เป็น credit note ออก)
    before = len(df)
    df = df[(df["Quantity"] > 0) & (df["UnitPrice"] > 0)]
    after = len(df)
    print(
        f"[TRANSFORM] กรองให้เหลือเฉพาะรายการขายจริง (Quantity>0, UnitPrice>0): "
        f"ตัดออก {before - after} แถว (เหลือ {after} แถว)"
    )

    # 2.3 ตัดแถวที่ไม่มี CustomerID (ถ้าต้องการวิเคราะห์ตามลูกค้า)
    before = len(df)
    df = df.dropna(subset=["CustomerID"])
    after = len(df)
    print(
        f"[TRANSFORM] ลบแถวที่ไม่มี CustomerID ออก {before - after} แถว "
        f"(เหลือ {after} แถว)"
    )

    # 2.4 แปลง InvoiceDate จาก string เป็น datetime
    #     dataset นี้อยู่ในรูป "12/1/2010 8:26" → ใช้ to_datetime ให้ช่วยเดา format
    df["InvoiceDate"] = pd.to_datetime(df["InvoiceDate"], errors="coerce")
    before = len(df)
    df = df.dropna(subset=["InvoiceDate"])
    after = len(df)
    print(
        f"[TRANSFORM] แปลง InvoiceDate เป็น datetime และลบค่าที่แปลงไม่ได้ "
        f"{before - after} แถว (เหลือ {after} แถว)"
    )

    # 2.5 จัดชนิดข้อมูลตัวเลขให้ถูกต้อง (กันกรณีอ่านจาก DB แล้วเป็น object)
    df["Quantity"] = pd.to_numeric(df["Quantity"], errors="coerce").fillna(0).astype(int)
    df["UnitPrice"] = pd.to_numeric(df["UnitPrice"], errors="coerce").fillna(0.0)

    # 2.6 เติมข้อความ "Unknown" ให้ฟิลด์ตัวหนังสือที่ว่าง/เป็น NaN
    text_cols = ["Description", "Country"]
    for col in text_cols:
        if col in df.columns:
            df[col] = df[col].fillna("Unknown").astype("string")

    # ---------- 3) สร้างคอลัมน์ใหม่สำหรับการวิเคราะห์ (Feature Engineering) ----------

    # 3.1 ยอดขายต่อรายการ (TotalPrice = Quantity * UnitPrice)
    df["TotalPrice"] = df["Quantity"] * df["UnitPrice"]

    # 3.2 แยกวันที่/เวลาออกมาเป็น Year / Month / Day / Hour / DateOnly
    df["InvoiceYear"] = df["InvoiceDate"].dt.year
    df["InvoiceMonth"] = df["InvoiceDate"].dt.month
    df["InvoiceDay"] = df["InvoiceDate"].dt.day
    df["InvoiceHour"] = df["InvoiceDate"].dt.hour
    df["InvoiceDateOnly"] = df["InvoiceDate"].dt.date
    df["InvoiceWeekday"] = df["InvoiceDate"].dt.day_name()

    # 3.3 timestamp ของ pipeline (บอกว่าข้อมูลชุดนี้ถูก transform เมื่อไหร่)
    df["_pipeline_transformed_at"] = datetime.utcnow()

    # ---------- 4) เขียนข้อมูลลง schema production ----------

    # สร้าง schema production ถ้ายังไม่มี
    with engine.begin() as conn:
        conn.execute(text(f"CREATE SCHEMA IF NOT EXISTS {SCHEMA_PROD};"))
        print(f"[TRANSFORM] ตรวจสอบ/สร้าง schema '{SCHEMA_PROD}' เรียบร้อย")

    # เขียนทับตารางเดิมทุกครั้ง (if_exists="replace")
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
