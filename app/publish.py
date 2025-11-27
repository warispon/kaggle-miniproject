# publish.py
import math
import pandas as pd
import gspread
from gspread_dataframe import set_with_dataframe

from config import (
    get_engine,
    SCHEMA_PROD,
    PRODUCTION_TABLE_NAME,
    GOOGLE_SHEETS_SPREADSHEET_NAME,
    GOOGLE_SHEETS_WORKSHEET_NAME,
    SERVICE_ACCOUNT_FILE,
)

def run():
    """อ่านข้อมูลจาก production แล้วเขียนขึ้น Google Sheets
    แบบเริ่มจากแถวที่ 1 จนถึงแถวสุดท้าย และเคารพ limit 10M cells
    """
    print("☁️ [PUBLISH] เริ่มอ่านข้อมูลจาก schema production...")

    engine = get_engine()

    # ดึงข้อมูลจากตาราง production
    query_prod = f'SELECT * FROM "{SCHEMA_PROD}"."{PRODUCTION_TABLE_NAME}";'
    df = pd.read_sql(query_prod, engine)
    total_rows = len(df)
    n_cols = len(df.columns)

    print(f"[PUBLISH] ดึงข้อมูลจาก {SCHEMA_PROD}.{PRODUCTION_TABLE_NAME} ได้ {total_rows} แถว, {n_cols} คอลัมน์")

    # ---- คำนวณ limit ของ Google Sheets ----
    MAX_CELLS_PER_SHEET = 9_000_000   # กันชนจาก 10M นิดหน่อย
    effective_cols = n_cols           # จำนวนคอลัมน์จริงที่เราจะเขียน

    max_rows_per_sheet = max(1, MAX_CELLS_PER_SHEET // effective_cols)
    print(f"[PUBLISH] 1 ชีตสามารถรองรับได้ประมาณ {max_rows_per_sheet} แถว")

    # ต้องแบ่งเป็นกี่ชีต (เผื่ออนาคตข้อมูลเยอะกว่า 1 ชีต)
    num_sheets = math.ceil(total_rows / max_rows_per_sheet)
    print(f"[PUBLISH] ต้องแบ่งข้อมูลเป็น {num_sheets} ชีต")

    # เชื่อมต่อ Google Sheets
    print("[PUBLISH] กำลังเชื่อมต่อ Google Sheets ผ่าน Service Account...")
    gc = gspread.service_account(filename=SERVICE_ACCOUNT_FILE)
    sh = gc.open(GOOGLE_SHEETS_SPREADSHEET_NAME)

    for i in range(num_sheets):
        start_row = i * max_rows_per_sheet
        end_row = min((i + 1) * max_rows_per_sheet, total_rows)

        df_chunk = df.iloc[start_row:end_row].copy()
        chunk_rows = len(df_chunk)

        sheet_title = f"{GOOGLE_SHEETS_WORKSHEET_NAME}_{i+1}"

        print(
            f"[PUBLISH] เขียนช่วงแถว {start_row}–{end_row-1} "
            f"({chunk_rows} แถว) ไปที่ชีต '{sheet_title}'"
        )

        # -------- จัดการชีตให้สะอาด: ลบชีตเก่าแล้วสร้างใหม่ --------
        try:
            ws_old = sh.worksheet(sheet_title)
            print(f"[PUBLISH] พบ worksheet เดิม: {sheet_title} -> ลบชีตเก่า")
            try:
                sh.del_worksheet(ws_old)
            except Exception as e:
                # ถ้าลบไม่ได้ (เช่น เป็นชีตสุดท้าย) ให้ fallback เป็น clear
                print(f"[PUBLISH] ลบชีตเก่าไม่สำเร็จ ({e}) -> ใช้การ clear แทน")
                ws_old.clear()
                ws = ws_old
            else:
                # ถ้าลบได้สำเร็จ -> สร้างชีตใหม่
                ws = sh.add_worksheet(
                    title=sheet_title,
                    rows=str(chunk_rows + 1),   # +1 เผื่อ header
                    cols=str(effective_cols),
                )
        except gspread.WorksheetNotFound:
            print(f"[PUBLISH] ไม่พบ worksheet: {sheet_title} -> สร้างใหม่")
            ws = sh.add_worksheet(
                title=sheet_title,
                rows=str(chunk_rows + 1),       # แถวพอดีกับข้อมูล + header
                cols=str(effective_cols),       # เท่ากับจำนวนคอลัมน์จริง
            )

        # -------- เขียน DataFrame ลงชีต (เริ่มที่ row 1 เสมอ) --------
        set_with_dataframe(ws, df_chunk, include_index=False, include_column_header=True)

    print("[PUBLISH] เขียนข้อมูลทุกชีตลง Google Sheets เรียบร้อยแล้ว 🎉")

if __name__ == "__main__":
    run()
