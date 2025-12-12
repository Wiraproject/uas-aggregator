import os
import time
import uuid
import random
import requests
import datetime
import sys

TARGET_URL = os.getenv("TARGET_URL", "http://aggregator:8080/publish")
DELAY = float(os.getenv("DELAY", "0.01"))
DUPLICATION_RATE = float(os.getenv("DUPLICATION_RATE", "0.3")) 
MAX_EVENTS = int(os.getenv("MAX_EVENTS", "20000"))

sent_events_history = []
MAX_HISTORY = 2000

def generate_event():
    return {
        "topic": random.choice(["order.created", "payment.success", "user.login", "sensor.read"]),
        "event_id": str(uuid.uuid4()),
        "timestamp": datetime.datetime.now().isoformat(),
        "source": "publisher-service-01",
        "payload": {
            "amount": random.randint(10, 1000),
            "user_id": random.randint(1, 500)
        }
    }

def run_publisher():
    print(f"🚀 Publisher mulai! Target: {TARGET_URL}")
    print(f"🎯 Target: {MAX_EVENTS} events (Stop otomatis setelah tercapai)")
    print(f"⚙️ Config: Delay={DELAY}s, Duplication Rate={DUPLICATION_RATE*100}%")
    
    time.sleep(5)
    
    count = 0
    start_time = time.time()

    while count < MAX_EVENTS:
        try:
            is_duplicate = False
            
            if len(sent_events_history) > 0 and random.random() < DUPLICATION_RATE:
                event_data = random.choice(sent_events_history)
                is_duplicate = True
                log_prefix = "[OLD/DUP]"
            else:
                event_data = generate_event()
                sent_events_history.append(event_data)
                if len(sent_events_history) > MAX_HISTORY:
                    sent_events_history.pop(0)
                log_prefix = "[NEW]"

            response = requests.post(TARGET_URL, json=event_data, timeout=5)
            
            if count % 500 == 0:
                print(f"Progress: {count}/{MAX_EVENTS} events sent... (Last: {response.status_code})")
            
            count += 1
            
            if DELAY > 0:
                time.sleep(DELAY)

        except Exception as e:
            print(f"❌ Error: {e}")
            time.sleep(1)

    end_time = time.time()
    duration = end_time - start_time
    print(f"\n✅ SELESAI! Terkirim: {count} events.")
    print(f"⏱️ Waktu Total: {duration:.2f} detik")
    print(f"⚡ Rata-rata: {count/duration:.2f} req/detik")
    
    print("Publisher idle (menunggu dimatikan manual)...")
    while True:
        time.sleep(10)

if __name__ == "__main__":
    run_publisher()