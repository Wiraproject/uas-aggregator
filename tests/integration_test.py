import pytest
import pytest_asyncio
import uuid
import datetime
import asyncio
from httpx import AsyncClient

BASE_URL = "http://aggregator:8080"

@pytest_asyncio.fixture
async def client():
    async with AsyncClient(base_url=BASE_URL, timeout=10.0) as c:
        yield c

def get_valid_payload(event_id=None, topic="test.integration"):
    if not event_id:
        event_id = str(uuid.uuid4())
    return {
        "topic": topic,
        "event_id": event_id,
        "timestamp": datetime.datetime.now().isoformat(),
        "source": "pytest-integration",
        "payload": {"value": 999}
    }

# ==========================================
# KELOMPOK 1: Validasi Skema (Negative Tests)
# (Validation tetap terjadi di awal, jadi status code tetap 422)
# ==========================================

@pytest.mark.asyncio
async def test_01_validation_missing_topic(client):
    data = get_valid_payload()
    del data["topic"]
    response = await client.post("/publish", json=data)
    assert response.status_code == 422

@pytest.mark.asyncio
async def test_02_validation_missing_event_id(client):
    data = get_valid_payload()
    del data["event_id"]
    response = await client.post("/publish", json=data)
    assert response.status_code == 422

@pytest.mark.asyncio
async def test_03_validation_missing_timestamp(client):
    data = get_valid_payload()
    del data["timestamp"]
    response = await client.post("/publish", json=data)
    assert response.status_code == 422

@pytest.mark.asyncio
async def test_04_validation_invalid_json(client):
    response = await client.post("/publish", content="bukan json", headers={"Content-Type": "application/json"})
    assert response.status_code == 422

@pytest.mark.asyncio
async def test_05_validation_empty_body(client):
    response = await client.post("/publish", json={})
    assert response.status_code == 422

# ==========================================
# KELOMPOK 2: Flow Normal (Positive Tests)
# (Sekarang return 202 Accepted & "queued")
# ==========================================

@pytest.mark.asyncio
async def test_06_publish_valid_event(client):
    """Test: Kirim event valid -> Masuk Antrian (202)"""
    data = get_valid_payload()
    response = await client.post("/publish", json=data)
    assert response.status_code == 202
    assert response.json()["status"] == "queued"

@pytest.mark.asyncio
async def test_07_publish_complex_payload(client):
    data = get_valid_payload()
    data["payload"] = {"user": {"id": 1}, "meta": [1, 2]}
    response = await client.post("/publish", json=data)
    assert response.status_code == 202

@pytest.mark.asyncio
async def test_08_publish_long_topic(client):
    data = get_valid_payload(topic="a" * 200)
    response = await client.post("/publish", json=data)
    assert response.status_code == 202

@pytest.mark.asyncio
async def test_09_publish_optional_fields(client):
    data = {"topic": "t", "event_id": str(uuid.uuid4()), "timestamp": datetime.datetime.now().isoformat()}
    response = await client.post("/publish", json=data)
    assert response.status_code == 202

# ==========================================
# KELOMPOK 3: Idempotency & Deduplication
# (Karena Async, kita cek via /stats atau /events, bukan response langsung)
# ==========================================

@pytest.mark.asyncio
async def test_10_deduplication_logic(client):
    """Test: Kirim 2x. API selalu bilang 'queued', tapi stats harus mencatat duplicate."""
    data = get_valid_payload()
    
    r_stats = await client.get("/stats")
    initial_dupes = r_stats.json()["uptime_stats"]["duplicate_dropped"]

    await client.post("/publish", json=data)
    await client.post("/publish", json=data)

    await asyncio.sleep(2)

    r_stats_final = await client.get("/stats")
    final_dupes = r_stats_final.json()["uptime_stats"]["duplicate_dropped"]
    
    assert final_dupes > initial_dupes

@pytest.mark.asyncio
async def test_11_deduplication_same_id_diff_topic(client):
    shared_id = str(uuid.uuid4())
    await client.post("/publish", json=get_valid_payload(event_id=shared_id, topic="A"))
    await client.post("/publish", json=get_valid_payload(event_id=shared_id, topic="B"))
    
    await asyncio.sleep(2)
    
    r = await client.get("/events?limit=50")
    events = r.json()

    found = [e for e in events if e['event_id'] == shared_id]
    assert len(found) >= 2

# ==========================================
# KELOMPOK 4: Observability
# ==========================================

@pytest.mark.asyncio
async def test_12_stats_structure(client):
    response = await client.get("/stats")
    assert response.status_code == 200
    js = response.json()
    
    assert "uptime_stats" in js
    assert "performance_metrics" in js
    assert "system_state" in js
    
    assert "queue_depth" in js["system_state"]
    assert "database_rows" in js["system_state"]

@pytest.mark.asyncio
async def test_13_events_list(client):
    response = await client.get("/events")
    assert response.status_code == 200
    assert isinstance(response.json(), list)

@pytest.mark.asyncio
async def test_14_events_filter(client):
    unique_topic = f"filter.{uuid.uuid4()}"
    await client.post("/publish", json=get_valid_payload(topic=unique_topic))
    
    await asyncio.sleep(1)

    response = await client.get(f"/events?topic={unique_topic}")
    assert len(response.json()) >= 1

# ==========================================
# KELOMPOK 5: HTTP Errors
# ==========================================

@pytest.mark.asyncio
async def test_15_method_not_allowed(client):
    response = await client.get("/publish")
    assert response.status_code == 405

@pytest.mark.asyncio
async def test_16_not_found(client):
    response = await client.post("/ngawur", json={})
    assert response.status_code == 404

@pytest.mark.asyncio
async def test_17_health_check(client):
    response = await client.get("/")
    assert response.status_code == 200
    assert response.json()["status"] == "alive"