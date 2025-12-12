import asyncio
import json
import os
import logging
import time
from datetime import datetime
from fastapi import FastAPI, Depends, HTTPException
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.exc import OperationalError
from sqlalchemy import select, func
from sqlalchemy.dialects.postgresql import insert

import redis.asyncio as redis
from contextlib import asynccontextmanager
from database import engine, Base, get_db, AsyncSessionLocal
import models
import schemas

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("aggregator")

# --- KONFIGURASI ---
BROKER_URL = os.getenv("BROKER_URL", "redis://broker:6379/0")
QUEUE_NAME = "events_queue"
redis_client = None

# --- STATISTIK & METRIK ---
START_TIME = time.time() # Waktu mulai aplikasi

stats = {
    "received": 0,          # Total request masuk ke API
    "unique_processed": 0,  # Berhasil masuk DB
    "duplicate_dropped": 0, # Ditolak DB
    "total_latency": 0.0    # Akumulasi waktu (detik) dari Publisher -> DB
}

# --- HELPER DB ---
async def init_db(retries=5, delay=3):
    """Retry koneksi DB saat startup"""
    for i in range(retries):
        try:
            logger.info(f"Startup: Mencoba koneksi database ({i+1}/{retries})...")
            async with engine.begin() as conn:
                await conn.run_sync(Base.metadata.create_all)
            logger.info("Startup: Koneksi sukses & Tabel siap.")
            return
        except (OSError, OperationalError) as e:
            logger.warning(f"Database belum siap. Retrying in {delay}s... Error: {e}")
            await asyncio.sleep(delay)
    raise RuntimeError("Gagal konek ke database.")

# --- WORKER (BACKGROUND TASK) ---

async def process_event_in_db(event_data):
    """Worker: Simpan ke DB & Hitung Latency"""
    async with AsyncSessionLocal() as db:
        try:
            # 1. Hitung Latency (Waktu Proses - Waktu Event Dibuat)
            try:
                event_ts = datetime.fromisoformat(event_data['timestamp'])
                # Jika event_ts naive (tidak ada timezone), anggap lokal
                if event_ts.tzinfo is None:
                    event_ts = event_ts.replace(tzinfo=None)
                
                arrival_ts = datetime.now()
                latency = (arrival_ts - event_ts).total_seconds()
                
                # Hindari latency negatif jika jam tidak sinkron sedikit
                if latency > 0:
                    stats["total_latency"] += latency
            except Exception:
                pass # Abaikan jika format tanggal salah

            # 2. Simpan ke DB (Atomic Upsert)
            stmt = insert(models.ProcessedEvent).values(
                topic=event_data['topic'],
                event_id=event_data['event_id'],
                timestamp=event_data['timestamp'],
                source=event_data.get('source'),
                payload=event_data.get('payload')
            )
            stmt = stmt.on_conflict_do_nothing(index_elements=['topic', 'event_id'])
            
            result = await db.execute(stmt)
            await db.commit()
            
            # 3. Update Counter
            if result.rowcount > 0:
                stats["unique_processed"] += 1
            else:
                stats["duplicate_dropped"] += 1
                
            # Logging Periodik
            total_ops = stats["unique_processed"] + stats["duplicate_dropped"]
            if total_ops % 500 == 0:
                logger.info(
                    f"WORKER >> Processed: {stats['unique_processed']} | "
                    f"Dropped: {stats['duplicate_dropped']}"
                )

        except Exception as e:
            logger.error(f"DB Error di Worker: {e}")

async def consume_events():
    """Looping Consumer Redis"""
    logger.info("👷 Background Worker Started!")
    await asyncio.sleep(2)

    try:
        consumer_redis = redis.from_url(BROKER_URL, decode_responses=True)
    except Exception as e:
        logger.error(f"Gagal konek Redis Consumer: {e}")
        return

    while True:
        try:
            result = await consumer_redis.brpop(QUEUE_NAME, timeout=1)
            if result:
                _, data_str = result
                event_data = json.loads(data_str)
                await process_event_in_db(event_data)
            await asyncio.sleep(0.001)
        except Exception as e:
            logger.error(f"Worker Loop Error: {e}")
            await asyncio.sleep(1)

# --- LIFECYCLE ---
@asynccontextmanager
async def lifespan(app: FastAPI):
    global redis_client
    await init_db()
    redis_client = redis.from_url(BROKER_URL, decode_responses=True)
    asyncio.create_task(consume_events())
    yield
    if redis_client:
        await redis_client.close()

app = FastAPI(lifespan=lifespan)

# --- ENDPOINTS ---

@app.get("/")
async def root():
    return {"status": "alive", "service": "aggregator"}

@app.post("/publish", status_code=202)
async def publish_event(event: schemas.EventCreate):
    stats["received"] += 1
    try:
        event_json = event.model_dump_json()
        await redis_client.lpush(QUEUE_NAME, event_json)
        return {"status": "queued", "id": event.event_id}
    except Exception as e:
        raise HTTPException(status_code=500, detail="Internal Broker Error")

@app.get("/events", response_model=list[schemas.EventResponse])
async def get_events(topic: str = None, limit: int = 20, db: AsyncSession = Depends(get_db)):
    query = select(models.ProcessedEvent).order_by(models.ProcessedEvent.id.desc()).limit(limit)
    if topic:
        query = query.where(models.ProcessedEvent.topic == topic)
    result = await db.execute(query)
    return result.scalars().all()

@app.get("/stats")
async def get_stats(db: AsyncSession = Depends(get_db)):
    """
    Menampilkan statistik lengkap:
    - Counter (Received, Processed, Dropped)
    - Throughput (Events per Second)
    - Latency (Average ms)
    - Duplicate Rate (%)
    """
    # 1. Ambil data real dari DB
    result = await db.execute(select(func.count(models.ProcessedEvent.id)))
    db_count = result.scalar()
    
    # 2. Cek Antrian
    queue_depth = 0
    if redis_client:
        queue_depth = await redis_client.llen(QUEUE_NAME)

    # 3. Kalkulasi Metrik Performa
    uptime_seconds = time.time() - START_TIME
    total_processed_attempts = stats["unique_processed"] + stats["duplicate_dropped"]
    
    # Throughput (Events/detik yang berhasil diproses worker)
    throughput_eps = 0
    if uptime_seconds > 0:
        throughput_eps = round(total_processed_attempts / uptime_seconds, 2)
        
    # Average Latency (ms)
    avg_latency_ms = 0
    if total_processed_attempts > 0:
        avg_latency_ms = round((stats["total_latency"] / total_processed_attempts) * 1000, 2)
        
    # Duplicate Rate (%)
    duplicate_rate_percent = 0
    if total_processed_attempts > 0:
        duplicate_rate_percent = round((stats["duplicate_dropped"] / total_processed_attempts) * 100, 2)

    return {
        "uptime_stats": {
            "received_api": stats["received"],
            "processed_worker": stats["unique_processed"],
            "dropped_worker": stats["duplicate_dropped"]
        },
        "performance_metrics": {
            "throughput_eps": throughput_eps,           # Events Per Second
            "avg_latency_ms": avg_latency_ms,           # Rata-rata waktu proses
            "duplicate_rate_percent": duplicate_rate_percent, # Persentase duplikat
            "uptime_seconds": round(uptime_seconds, 2)
        },
        "system_state": {
            "database_rows": db_count,
            "queue_depth": queue_depth
        }
    }