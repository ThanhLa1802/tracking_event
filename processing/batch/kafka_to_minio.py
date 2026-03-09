import json
import time
import uuid
import pandas as pd
from kafka import KafkaConsumer
import boto3
from botocore.client import Config
import os

# 1. Khởi tạo cấu hình MinIO (S3 Compatible)
# Cổng API chuẩn của MinIO là 9000
s3_client = boto3.client(
    's3',
    endpoint_url='http://minio:9000',     
    aws_access_key_id='admin',            
    aws_secret_access_key='password123',  
    config=Config(signature_version='s3v4'),
    region_name='us-east-1'
)

BUCKET_NAME = "recsys-data-lake"

try:
    s3_client.head_bucket(Bucket=BUCKET_NAME)
except:
    s3_client.create_bucket(Bucket=BUCKET_NAME)
    print(f"✅ Đã tạo/kiểm tra bucket: {BUCKET_NAME}")

# 2. Khởi tạo Kafka Consumer (Dùng Group ID riêng biệt!)
# Sửa 'minio:9092' thành 'kafka:9092'
KAFKA_BROKER = os.getenv('KAFKA_BROKER', 'kafka:9092')

consumer = KafkaConsumer(
    'raw_user_events',
    bootstrap_servers=[KAFKA_BROKER],
    auto_offset_reset='earliest',
    enable_auto_commit=True,
    group_id='datalake_group_v1', # Tên group riêng biệt, độc lập với Streaming
    value_deserializer=lambda x: json.loads(x.decode('utf-8')),
    consumer_timeout_ms=10000 
)

BATCH_SIZE = 1

def upload_to_minio():
    print("Đang gom dữ liệu từ Kafka để đẩy lên MinIO...")
    events_data = []
    
    for message in consumer:
        events_data.append(message.value)
        
        if len(events_data) >= BATCH_SIZE:
            save_and_upload(events_data)
            events_data.clear() 
            
    if len(events_data) > 0:
        save_and_upload(events_data)
        
    print("Đã đợi 10 giây không có message mới. Script tự động kết thúc!")

def save_and_upload(data):
    df = pd.DataFrame(data)
    timestamp_str = time.strftime("%Y%m%d_%H%M%S")
    file_name = f"events_{timestamp_str}_{uuid.uuid4().hex[:6]}.parquet"
    local_path = f"/tmp/{file_name}"
    
    # Ép kiểu dữ liệu để Parquet không bị lỗi
    df = df.astype(str)
    
    df.to_parquet(local_path, engine='pyarrow', index=False)
    
    date_partition = time.strftime("year=%Y/month=%m/day=%d")
    s3_key = f"raw_events/{date_partition}/{file_name}"
    
    s3_client.upload_file(local_path, BUCKET_NAME, s3_key)
    print(f"Đã upload {len(data)} events lên MinIO tại: {s3_key}")

if __name__ == "__main__":
    upload_to_minio()