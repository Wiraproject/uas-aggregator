import asyncio
import logging
from fastapi import FastAPI, Depends, HTTPException
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.exc import OperationalError
from sqlalchemy import select, func
from sqlalchemy.dialects.postgresql import insert # Khusus Postgres

from contextlib import asynccontextmanager
from database import engine, Base, get_db
import models
import schemas

# Setup Logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("aggregator")

# Statistik In-Memory (Sederhana)
stats = {
    "received": 0,
    "unique_processed": 0,
    "duplicate_dropped": 0
}

async def init_db(retries=5, delay=3):
    """Fungsi retry koneksi DB saat startup"""
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

@asynccontextmanager
async def lifespan(app: FastAPI):
    await init_db()
    yield

app = FastAPI(lifespan=lifespan)

# --- ENDPOINTS ---

@app.post("/publish", status_code=201)
async def publish_event(event: schemas.EventCreate, db: AsyncSession = Depends(get_db)):
    """
    Menerima event tunggal.
    Menggunakan INSERT ... ON CONFLICT DO NOTHING untuk idempotency atomik.
    """
    stats["received"] += 1
    
    # 1. Siapkan statement INSERT
    stmt = insert(models.ProcessedEvent).values(
        topic=event.topic,
        event_id=event.event_id,
        timestamp=event.timestamp,
        source=event.source,
        payload=event.payload
    )
    
    # 2. Tambahkan klausa ON CONFLICT DO NOTHING
    stmt = stmt.on_conflict_do_nothing(
        index_elements=['topic', 'event_id']
    )
    
    # 3. Eksekusi
    result = await db.execute(stmt)
    await db.commit()
    
    status_msg = ""
    # 4. Cek hasil
    if result.rowcount > 0:
        stats["unique_processed"] += 1
        status_msg = "processed"
        # HAPUS logger per item
    else:
        stats["duplicate_dropped"] += 1
        status_msg = "dropped_duplicate"
        # HAPUS logger per item

    # LOGGING YANG LEBIH RAPI (Batch Logging)
    # Hanya print log setiap kelipatan 500 request
    if stats["received"] % 500 == 0:
        logger.info(
            f"STATS UPDATE >> Total: {stats['received']} | "
            f"Processed: {stats['unique_processed']} | "
            f"Dropped: {stats['duplicate_dropped']}"
        )

    return {"status": status_msg, "id": event.event_id}

@app.get("/events", response_model=list[schemas.EventResponse])
async def get_events(topic: str = None, limit: int = 20, db: AsyncSession = Depends(get_db)):
    """Mengambil daftar event yang tersimpan"""
    query = select(models.ProcessedEvent).order_by(models.ProcessedEvent.id.desc()).limit(limit)
    
    if topic:
        query = query.where(models.ProcessedEvent.topic == topic)
        
    result = await db.execute(query)
    return result.scalars().all()

@app.get("/stats")
async def get_stats(db: AsyncSession = Depends(get_db)):
    """
    Statistik performa.
    Hitung total rows di DB untuk akurasi 'unique_processed' jangka panjang.
    """
    # Hitung total di DB (source of truth)
    result = await db.execute(select(func.count(models.ProcessedEvent.id)))
    db_count = result.scalar()
    
    return {
        "uptime_stats": stats,
        "database_total_rows": db_count
    }