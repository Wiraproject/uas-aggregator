import os
from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession
from sqlalchemy.orm import sessionmaker, declarative_base

# Ambil URL dari environment variable (yang kita set di docker-compose)
# Perhatikan: Kita ubah drivernya jadi postgresql+asyncpg
DATABASE_URL = os.getenv("DATABASE_URL", "postgresql+asyncpg://user:password@storage:5432/events_db")

# Jika URL di docker-compose belum pakai asyncpg, kita patch manual di sini
if DATABASE_URL.startswith("postgresql://"):
    DATABASE_URL = DATABASE_URL.replace("postgresql://", "postgresql+asyncpg://", 1)

# Membuat Engine
engine = create_async_engine(DATABASE_URL, echo=False)

# Membuat Session Factory
AsyncSessionLocal = sessionmaker(
    bind=engine,
    class_=AsyncSession,
    expire_on_commit=False
)

# Base class untuk Model
Base = declarative_base()

# Dependency Injection untuk FastAPI (dipakai nanti di API)
async def get_db():
    async with AsyncSessionLocal() as session:
        yield session