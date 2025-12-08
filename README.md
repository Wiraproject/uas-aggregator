**UAS Sistem Terdistribusi**

Sistem ini adalah implementasi **Distributed Log Aggregator** yang dirancang untuk menangani ribuan event dengan jaminan **Idempotency** (anti-duplikasi), **Data Persistence**, dan **Concurrency Control**. Dibangun menggunakan Python (FastAPI), Docker Compose, PostgreSQL, dan Redis.

## 📋 Fitur Utama
1.  **Idempotent Consumer:** Menggunakan database constraints (`UNIQUE(topic, event_id)`) untuk menjamin event yang sama persis tidak diproses dua kali.
2.  **Concurrency Control:** Menangani race condition menggunakan transaksi atomik database (`INSERT ... ON CONFLICT DO NOTHING`).
3.  **Data Persistence:** Data tersimpan aman di PostgreSQL menggunakan Docker Named Volumes, tahan terhadap restart container.
4.  **High Performance:** Mampu memproses 20.000+ event dengan throughput tinggi menggunakan `asyncio` dan `asyncpg`.
5.  **Fault Tolerance:** Implementasi *Retry Logic* dengan *Backoff* saat koneksi database belum siap.
6.  **Observability:** Endpoint `/stats` untuk memantau metrik *received*, *processed*, dan *dropped*.

---

## 🏗️ Arsitektur Sistem

Sistem terdiri dari 4 layanan yang diorkestrasi oleh Docker Compose:

```mermaid
graph LR
    P[Publisher Service] -- HTTP POST (Batch/Single) --> A[Aggregator Service]
    A -- Atomic Insert --> D[(Postgres Storage)]
    A -- Pub/Sub (Opsional) --> R[(Redis Broker)]
    T[Tester Service] -- Integration Test --> A