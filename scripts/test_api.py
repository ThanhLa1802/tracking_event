import requests
import random
import json
from concurrent.futures import ThreadPoolExecutor
import time

API_URL = "http://localhost:8002/track" # Đổi lại port Golang API của bạn nếu cần

# Dữ liệu giả lập
USER_IDS = [f"user_{i}" for i in range(1, 100)]
ITEM_IDS = [f"item_{i}" for i in range(1000, 1050)]
EVENTS = ["view", "add_to_cart", "purchase"]

def fire_event(request_id):
    payload = {
        "user_id": random.choice(USER_IDS),
        "item_id": random.choice(ITEM_IDS),
        "event_type": random.choice(EVENTS),
        "session_id": f"session{random.choice(USER_IDS)}"
    }
    
    try:
        response = requests.post(API_URL, json=payload, headers={"Content-Type": "application/json"})
        if request_id % 100 == 0:
            print(f"Đã bắn request thứ {request_id} - Status: {response.status_code}")
    except Exception as e:
        print(f"Lỗi ở request {request_id}: {e}")

# Kịch bản ép xung: Bắn 5000 requests, mở 50 luồng (threads) cùng lúc
TOTAL_REQUESTS = 5000
CONCURRENT_THREADS = 50

print(f"🚀 Bắt đầu nã {TOTAL_REQUESTS} events vào API...")
start_time = time.time()

with ThreadPoolExecutor(max_workers=CONCURRENT_THREADS) as executor:
    executor.map(fire_event, range(TOTAL_REQUESTS))

end_time = time.time()
print(f"🔥 Đã xong! Thời gian chạy: {end_time - start_time:.2f} giây")