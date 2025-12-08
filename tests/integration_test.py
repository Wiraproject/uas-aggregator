import pytest
import pytest_asyncio
import uuid
import datetime
from httpx import AsyncClient

# URL Aggregator dalam jaringan Docker Compose
BASE_URL = "http://aggregator:8080"

# Fixture Client (Async)
@pytest_asyncio.fixture
async def client():
    async with AsyncClient(base_url=BASE_URL, timeout=10.0) as c:
        yield c

# Helper: Membuat data dummy yang valid
def get_valid_payload(event_id=None, topic="test.integration"):
    if not event_id:
        event_id = str(uuid.uuid4())
    return {
        "topic": topic,
        "event_id": event_id,
        "timestamp": datetime.datetime.now().isoformat(),
        "source": "pytest-integration",
        "payload": {"value": 999, "status": "active"}
    }

# ==========================================
# KELOMPOK 1: Validasi Skema & Input (Negative Tests)
# ==========================================

@pytest.mark.asyncio
async def test_01_validation_missing_topic(client):
    """Test: Payload tanpa 'topic' harus ditolak (422)"""
    data = get_valid_payload()
    del data["topic"]
    response = await client.post("/publish", json=data)
    assert response.status_code == 422

@pytest.mark.asyncio
async def test_02_validation_missing_event_id(client):
    """Test: Payload tanpa 'event_id' harus ditolak (422)"""
    data = get_valid_payload()
    del data["event_id"]
    response = await client.post("/publish", json=data)
    assert response.status_code == 422

@pytest.mark.asyncio
async def test_03_validation_missing_timestamp(client):
    """Test: Payload tanpa 'timestamp' harus ditolak (422)"""
    data = get_valid_payload()
    del data["timestamp"]
    response = await client.post("/publish", json=data)
    assert response.status_code == 422

@pytest.mark.asyncio
async def test_04_validation_invalid_json(client):
    """Test: Mengirim string mentah bukan JSON (422)"""
    response = await client.post("/publish", content="bukan json", headers={"Content-Type": "application/json"})
    assert response.status_code == 422

@pytest.mark.asyncio
async def test_05_validation_empty_body(client):
    """Test: Mengirim body kosong {} (422)"""
    response = await client.post("/publish", json={})
    assert response.status_code == 422

# ==========================================
# KELOMPOK 2: Flow Normal & Variasi Data (Positive Tests)
# ==========================================

@pytest.mark.asyncio
async def test_06_publish_valid_event(client):
    """Test: Kirim event valid standar (201)"""
    data = get_valid_payload()
    response = await client.post("/publish", json=data)
    assert response.status_code == 201
    assert response.json()["status"] == "processed"

@pytest.mark.asyncio
async def test_07_publish_complex_payload(client):
    """Test: Kirim event dengan payload JSON bersarang (201)"""
    data = get_valid_payload()
    data["payload"] = {
        "user": {"id": 1, "name": "Test"},
        "meta": [1, 2, 3],
        "flag": True
    }
    response = await client.post("/publish", json=data)
    assert response.status_code == 201

@pytest.mark.asyncio
async def test_08_publish_long_topic(client):
    """Test: Kirim topic string sangat panjang (201)"""
    data = get_valid_payload(topic="a" * 200) # Topic 200 karakter
    response = await client.post("/publish", json=data)
    assert response.status_code == 201

@pytest.mark.asyncio
async def test_09_publish_optional_fields(client):
    """Test: Kirim tanpa field opsional 'source' atau 'payload' (201)"""
    data = {
        "topic": "test.minimal",
        "event_id": str(uuid.uuid4()),
        "timestamp": datetime.datetime.now().isoformat()
    }
    response = await client.post("/publish", json=data)
    assert response.status_code == 201

# ==========================================
# KELOMPOK 3: Idempotency & Deduplication (Core Logic)
# ==========================================

@pytest.mark.asyncio
async def test_10_deduplication_exact_match(client):
    """Test: Kirim event SAMA PERSIS 2x. Kedua harus 200 (Dropped)"""
    data = get_valid_payload()
    
    # Kirim 1
    await client.post("/publish", json=data)
    
    # Kirim 2
    res2 = await client.post("/publish", json=data)
    assert res2.status_code in [200, 201]
    assert res2.json()["status"] == "dropped_duplicate"

@pytest.mark.asyncio
async def test_11_deduplication_same_id_diff_topic(client):
    """Test: ID sama tapi Topic beda => HARUS DITERIMA (Unik)"""
    shared_id = str(uuid.uuid4())
    
    # Topic A
    data1 = get_valid_payload(event_id=shared_id, topic="topic.A")
    res1 = await client.post("/publish", json=data1)
    assert res1.json()["status"] == "processed"

    # Topic B
    data2 = get_valid_payload(event_id=shared_id, topic="topic.B")
    res2 = await client.post("/publish", json=data2)
    assert res2.json()["status"] == "processed"

# ==========================================
# KELOMPOK 4: Observability & Endpoints Lain
# ==========================================

@pytest.mark.asyncio
async def test_12_stats_endpoint_structure(client):
    """Test: Endpoint /stats mengembalikan struktur JSON yang benar"""
    response = await client.get("/stats")
    assert response.status_code == 200
    json_resp = response.json()
    assert "uptime_stats" in json_resp
    assert "received" in json_resp["uptime_stats"]
    assert "database_total_rows" in json_resp

@pytest.mark.asyncio
async def test_13_events_endpoint_list(client):
    """Test: Endpoint /events mengembalikan list"""
    response = await client.get("/events")
    assert response.status_code == 200
    assert isinstance(response.json(), list)

@pytest.mark.asyncio
async def test_14_events_filter_query(client):
    """Test: Endpoint /events dengan filter topic"""
    unique_topic = f"filter.test.{uuid.uuid4()}"
    data = get_valid_payload(topic=unique_topic)
    await client.post("/publish", json=data)

    # Filter
    response = await client.get(f"/events?topic={unique_topic}")
    events = response.json()
    assert len(events) >= 1
    assert events[0]["topic"] == unique_topic

# ==========================================
# KELOMPOK 5: HTTP Protocol & Errors
# ==========================================

@pytest.mark.asyncio
async def test_15_method_not_allowed(client):
    """Test: Akses /publish dengan GET harusnya 405 Method Not Allowed"""
    response = await client.get("/publish")
    assert response.status_code == 405

@pytest.mark.asyncio
async def test_16_not_found_route(client):
    """Test: Akses route ngawur harusnya 404"""
    response = await client.post("/ngawur-url", json={})
    assert response.status_code == 404

@pytest.mark.asyncio
async def test_17_health_root(client):
    """Test: Akses root / untuk health check"""
    response = await client.get("/")
    assert response.status_code == 200