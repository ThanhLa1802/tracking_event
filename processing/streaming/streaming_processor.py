import json
import time
import os
from datetime import datetime
from confluent_kafka import Consumer, KafkaError
import redis
import clickhouse_connect

# Khởi tạo các biến môi trường
KAFKA_BROKER = os.getenv('KAFKA_BROKER', 'kafka:9092')
REDIS_HOST = os.getenv('REDIS_HOST', 'redis')
CLICKHOUSE_HOST = os.getenv('CLICKHOUSE_HOST', 'clickhouse')

conf = {
    'bootstrap.servers': KAFKA_BROKER,
    'group.id': 'enrichment_group_final_2',
    'auto.offset.reset': 'earliest',
    'enable.auto.commit': True,
    'error_cb': lambda err: print(f"[CẢNH BÁO KAFKA]: {err}")
}

print(f"Đang kết nối tới Kafka tại {KAFKA_BROKER} bằng Confluent-Kafka...")
consumer = Consumer(conf)
consumer.subscribe(['raw_user_events'])
print("✅ Đã subscribe topic: raw_user_events!")

# Kết nối Redis & ClickHouse
redis_client = redis.Redis(host=REDIS_HOST, port=6379, db=0, decode_responses=True)
ch_client = None
while ch_client is None:
    try:
        ch_client = clickhouse_connect.get_client(host=CLICKHOUSE_HOST, port=8123, username='admin', password='password')
        print("✅ Đã kết nối thành công tới ClickHouse")
    except Exception as e:
        print(f"ClickHouse chưa sẵn sàng, thử lại sau 5 giây...")
        time.sleep(5)

# --- THAY ĐỔI QUAN TRỌNG: TĂNG BATCH SIZE ---
BATCH_SIZE = 5000 

def process_stream():
    print("🚀 Bắt đầu hút dữ liệu bằng xe tải (Micro-batching)...")
    
    while True:
        # 1. KAFKA ÉP XUNG: Hút 1 lần 5000 messages (hoặc chờ tối đa 1 giây)
        messages = consumer.consume(num_messages=BATCH_SIZE, timeout=1.0)
        
        if not messages:
            continue
            
        valid_events = []
        
        # 2. REDIS ÉP XUNG: Dùng Pipeline để gom hàng ngàn lệnh hgetall lại
        pipeline = redis_client.pipeline()
        
        for msg in messages:
            if msg.error():
                continue
                
            try:
                event = json.loads(msg.value().decode('utf-8'))
                valid_events.append(event)
                # Thay vì gọi Redis ngay, ta nhét lệnh vào Pipeline
                pipeline.hgetall(f"item:{event['item_id']}")
            except Exception as e:
                print(f"Lỗi parse JSON: {e}")
                
        if not valid_events:
            continue
            
        # Thực thi 5000 lệnh Redis trong MỘT LẦN GỌI MẠNG duy nhất!
        redis_results = pipeline.execute()
        
        # Lắp ráp dữ liệu
        batch_data = []
        for i, event in enumerate(valid_events):
            item_metadata = redis_results[i]
            
            # Xử lý an toàn nếu Redis không có data
            category = item_metadata.get('category', 'Unknown') if item_metadata else 'Unknown'
            price = float(item_metadata.get('price', 0.0)) if item_metadata else 0.0
            
            event_time = datetime.fromtimestamp(event['timestamp'] / 1000.0)
            
            row = [event['user_id'], event['session_id'], event['event_type'], event['item_id'], category, price, event_time]
            batch_data.append(row)
            
        # 3. CLICKHOUSE ÉP XUNG: Bulk Insert nguyên lô 5000 dòng
        if batch_data:
            try:
                ch_client.insert('enriched_events', batch_data, 
                               column_names=['user_id', 'session_id', 'event_type', 'item_id', 'category', 'price', 'event_time'])
                print(f"🎉 Đã dọn dẹp và chèn siêu tốc lô {len(batch_data)} sự kiện vào ClickHouse!")
            except Exception as e:
                print(f"❌ Lỗi ghi ClickHouse: {e}")

if __name__ == "__main__":
    redis_client.hset("item:item_abc", mapping={"category": "Laptop", "price": "1500.00"})
    process_stream()