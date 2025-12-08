import pytest
import pytest_asyncio # Wajib import ini untuk fix error
import uuid
import datetime
from httpx import AsyncClient, ASGITransport

# URL Aggregator dalam jaringan Docker Compose
BASE_URL = "http://aggregator:8080"

# --- PERBAIKAN DI SINI ---
# Gunakan @pytest_asyncio.fixture, bukan @pytest.fixture biasa
@pytest_asyncio.fixture
async def client():
    # Timeout agak panjang jaga-jaga kalau server lambat
    async with AsyncClient(base_url=BASE_URL, timeout=10.0) as c:
        yield c

def get_valid_payload(event_id=None):
    if not event_id:
        event_id = str(uuid.uuid4())
    return {
        "topic": "test.integration",
        "event_id": event_id,
        "timestamp": datetime.datetime.now().isoformat(),
        "source": "pytest-integration",
        "payload": {"value": 999}
    }

# --- TEST CASES ---

@pytest.mark.asyncio
async def test_health_check(client):
    """Pastikan service aggregator hidup"""
    response = await client.get("/stats")
    assert response.status_code == 200

@pytest.mark.asyncio
async def test_publish_valid_event(client):
    """Kirim event valid ke API asli"""
    data = get_valid_payload()
    response = await client.post("/publish", json=data)
    assert response.status_code == 201
    assert response.json()["status"] == "processed"

@pytest.mark.asyncio
async def test_deduplication_real_db(client):
    """Test Deduplikasi di Database asli"""
    unique_id = str(uuid.uuid4())
    data = get_valid_payload(event_id=unique_id)

    # Kirim 1
    res1 = await client.post("/publish", json=data)
    assert res1.status_code == 201
    
    # Kirim 2 (Duplikat)
    res2 = await client.post("/publish", json=data)
    assert res2.status_code in [200, 201]
    assert res2.json()["status"] == "dropped_duplicate"

@pytest.mark.asyncio
async def test_validation_error(client):
    """Kirim data tanpa topic"""
    data = get_valid_payload()
    del data["topic"]
    response = await client.post("/publish", json=data)
    assert response.status_code == 422

@pytest.mark.asyncio
async def test_stats_consistency(client):
    """Cek endpoint stats"""
    response = await client.get("/stats")
    assert response.status_code == 200
    stats = response.json()
    assert "uptime_stats" in stats
    assert "database_total_rows" in stats