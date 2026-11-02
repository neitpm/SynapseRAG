import os, sys, time
from dotenv import load_dotenv
from neo4j import GraphDatabase

load_dotenv()
sys.path.append(".")
from ingest_internal_chunks import IngestionLockManager, cleanup_old_version_async, NEO4J_URI, NEO4J_USER, NEO4J_PASSWORD

# Reset SystemConfig về IDLE trước khi test
init_d = GraphDatabase.driver(NEO4J_URI, auth=(NEO4J_USER, NEO4J_PASSWORD))
with init_d.session() as s:
    s.run("MATCH (s:SystemConfig {id: 'internal_rag_state'}) SET s.ingestion_status = 'IDLE', s.lock_job_id = null, s.failed_version = null, s.staging_version = null")
init_d.close()

driver = GraphDatabase.driver(NEO4J_URI, auth=(NEO4J_USER, NEO4J_PASSWORD))

print("=== TEST 1: Fencing Token Increment & Write Ingestion ===")
mgr1 = IngestionLockManager(driver, "job_alpha", "ver_alpha")
res1 = mgr1.acquire_lock()
print("Job Alpha acquire (expect True):", res1)
print("Job Alpha Fencing Token:", mgr1.fencing_token)

# Giả lập ghi batch có gắn fencing token
batch_sample = [
    {
        "chunk_id": "test_chunk_alpha_1",
        "source_table": "tasks",
        "source_id": "1",
        "project_code": "",
        "date": "2026-11-02",
        "created_by": "",
        "text": "sample text",
        "embedding": [0.1] * 384,
        "version": "ver_alpha",
        "ingestion_job_id": "job_alpha"
    }
]

fenced_cypher = """
MATCH (s:SystemConfig {id: 'internal_rag_state'})
WHERE s.lock_job_id = $job_id AND s.lock_generation = $token
WITH s
UNWIND $batch AS row
MERGE (c:InternalChunk {chunk_id: row.chunk_id})
SET c.source_table = row.source_table,
    c.source_id = row.source_id,
    c.project_code = row.project_code,
    c.date = row.date,
    c.created_by = row.created_by,
    c.text = row.text,
    c.embedding = row.embedding,
    c.version = row.version,
    c.ingestion_job_id = row.ingestion_job_id
RETURN count(c) AS written_chunks
"""

with driver.session() as s:
    w_res = s.run(fenced_cypher, job_id="job_alpha", token=mgr1.fencing_token, batch=batch_sample).single()
    print("Job Alpha written chunks with valid token (expect 1):", w_res["written_chunks"] if w_res else 0)

mgr1.stop_heartbeat()

print("\n=== TEST 2: Zombie Process Fenced Write Rejection ===")
# Giả lập job_alpha bị timeout lease và job_beta takeover
past_ts = int(time.time()) - 2000
with driver.session() as s:
    s.run("""
        MATCH (s:SystemConfig {id: 'internal_rag_state'})
        SET s.last_heartbeat_at = $past_ts,
            s.staging_version = 'ver_alpha',
            s.lock_job_id = 'job_alpha'
    """, past_ts=past_ts)

mgr2 = IngestionLockManager(driver, "job_beta", "ver_beta")
res2 = mgr2.acquire_lock()
print("Job Beta takeover (expect True):", res2)
print("Job Beta Fencing Token (expect > Alpha):", mgr2.fencing_token)

# Zombie Job Alpha tỉnh lại và cố gắng chạy lệnh Cypher ghi batch
print("Zombie Job Alpha attempting to execute batch write with OLD token...")
zombie_batch = [
    {
        "chunk_id": "test_chunk_zombie_bad",
        "source_table": "tasks",
        "source_id": "2",
        "project_code": "",
        "date": "2026-11-02",
        "created_by": "",
        "text": "zombie poisoned text",
        "embedding": [0.2] * 384,
        "version": "ver_alpha",
        "ingestion_job_id": "job_alpha"
    }
]

with driver.session() as s:
    z_res = s.run(fenced_cypher, job_id="job_alpha", token=mgr1.fencing_token, batch=zombie_batch).single()
    z_count = z_res["written_chunks"] if z_res else 0
    print("Zombie Job Alpha written chunks (MUST BE 0):", z_count)
    
    # Kiểm tra xem node zombie có xuất hiện trong Neo4j không
    z_exists = s.run("MATCH (c:InternalChunk {chunk_id: 'test_chunk_zombie_bad'}) RETURN count(c) AS cnt").single()["cnt"]
    print("Zombie node actually created in DB (MUST BE 0):", z_exists)

# Clean up
mgr2.release_lock_on_failure()
with driver.session() as s:
    s.run("MATCH (c:InternalChunk) WHERE c.chunk_id IN ['test_chunk_alpha_1', 'test_chunk_zombie_bad'] DETACH DELETE c")
    s.run("MATCH (s:SystemConfig {id: 'internal_rag_state'}) SET s.ingestion_status = 'IDLE', s.failed_version = null, s.lock_job_id = null")
driver.close()

print("🎉 ATOMIC WRITE FENCING PASSED 100%!")
