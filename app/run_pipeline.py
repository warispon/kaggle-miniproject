# run_pipeline.py
from ingest import run as run_ingest
from transform import run as run_transform
from publish import run as run_publish

def main():
    print("=====================================")
    print("🚀 เริ่มรัน Automated Data Pipeline")
    print("=====================================")

    run_ingest()
    print("-------------------------------------")
    run_transform()
    print("-------------------------------------")
    run_publish()

    print("=====================================")
    print("✅ Pipeline เสร็จสมบูรณ์แล้ว")
    print("=====================================")

if __name__ == "__main__":
    main()
