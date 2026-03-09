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
    'group.id': 'enrichment_group_final',
    'auto.offset.reset': 'earliest',
    'enable.auto.commit': True,
    'error_cb': lambda err: print(f"[CẢNH BÁO KAFKA]: {err}"),
    'debug': 'broker' 
}

print(f"Đang kết nối tới Kafka tại {KAFKA_BROKER} bằng Confluent-Kafka...")
consumer = Consumer(conf)
consumer.subscribe(['raw_user_events'])
print("✅ Đã subscribe topic: raw_user_events!")

# 2. Kết nối Redis & ClickHouse
redis_client = redis.Redis(host=REDIS_HOST, port=6379, db=0, decode_responses=True)
ch_client = None
while ch_client is None:
    try:
        print(f"Đang thử kết nối tới ClickHouse tại {CLICKHOUSE_HOST}:8123...")
        ch_client = clickhouse_connect.get_client(host=CLICKHOUSE_HOST, port=8123, username='admin', password='password')
        print("Đã kết nối thành công tới ClickHouse")
    except Exception as e:
        print(f"ClickHouse chưa sẵn sàng, thử lại sau 5 giây... (Lỗi: {e})")
        time.sleep(5)

BATCH_SIZE = 1

def process_stream():
    print("Bắt đầu lắng nghe dữ liệu từ Kafka...")
    batch_data = []

    # Sử dụng vòng lặp while True và hàm poll() chuẩn mực
    tick = 0
    while True:
        # Chờ tối đa 1 giây để lấy message
        msg = consumer.poll(timeout=1.0)
        tick += 1
        
        if tick % 5 == 0:
            print("⏳ Vẫn đang chờ dữ liệu từ Kafka...")
        if msg is None:
            continue
        
        # Xử lý nếu có lỗi từ Kafka
        if msg.error():
            if msg.error().code() == KafkaError._PARTITION_EOF:
                # Báo hiệu đã đọc hết message hiện tại trong partition
                continue
            else:
                print(f"Lỗi Kafka: {msg.error()}")
                continue

        # 3. Xử lý message thành công
        try:
            event = json.loads(msg.value().decode('utf-8'))
            item_id = event['item_id']

            # Làm giàu dữ liệu
            item_metadata = redis_client.hgetall(f"item:{item_id}")
            category = item_metadata.get('category', 'Unknown')
            price = float(item_metadata.get('price', 0.0))

            event_time = datetime.fromtimestamp(event['timestamp'] / 1000.0)

            row = [event['user_id'], event['session_id'], event['event_type'], item_id, category, price, event_time]
            batch_data.append(row)

            # Ghi vào ClickHouse
            if len(batch_data) >= BATCH_SIZE:
                ch_client.insert('enriched_events', batch_data, 
                               column_names=['user_id', 'session_id', 'event_type', 'item_id', 'category', 'price', 'event_time'])
                print(f"🎉 Đã insert thành công 1 bản ghi (Event: {event['event_type']} - Item: {item_id})")
                batch_data.clear()
        
        except Exception as e:
            print(f"Lỗi xử lý dữ liệu: {e}")

if __name__ == "__main__":
    redis_client.hset("item:item_8847", mapping={"category": "Smartphone", "price": "799.00"})
    redis_client.hset("item:item_1234", mapping={"category": "Laptop", "price": "1200.00"})
    process_stream()