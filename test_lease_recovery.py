import os, sys, time
from dotenv import load_dotenv
from neo4j import GraphDatabase

load_dotenv()
sys.path.append(".")
from ingest_internal_chunks import IngestionLockManager, LEASE_DURATION_SECONDS

driver = GraphDatabase.driver("bolt://127.0.0.1:7687", auth=("neo4j", "graphrag_password123"))

print("=== TEST 1: Normal Acquisition & Rejection ===")
mgr_live = IngestionLockManager(driver, "job_live", "ver_live")
mgr_other = IngestionLockManager(driver, "job_other", "ver_other")

res1 = mgr_live.acquire_lock()
print("Live acquire (expect True):", res1)
res2 = mgr_other.acquire_lock()
print("Other acquire while live held (expect False):", res2)

mgr_live.stop_heartbeat()

print("\n=== TEST 2: Hard Crash Recovery on Lease Expired ===")
now_ts = int(time.time())
past_ts = now_ts - 2000 # Hết hạn lease (> 1800s)
with driver.session() as s:
    s.run("""
        MATCH (s:SystemConfig {id: 'internal_rag_state'})
        SET s.last_heartbeat_at = $past_ts,
            s.staging_version = 'ver_orphaned'
    """, past_ts=past_ts)
    s.run("MERGE (c:InternalChunk {chunk_id: 'chunk_orphan_1'}) SET c.version = 'ver_orphaned', c.ingestion_job_id = 'job_live'")

# Job recovery xuất hiện và tiếp quản
mgr_recovery = IngestionLockManager(driver, "job_recovery", "ver_new")
res3 = mgr_recovery.acquire_lock()
print("Recovery acquire on expired lease (expect True):", res3)

# Xác nhận chunk mồ côi đã bị dọn dẹp
with driver.session() as s:
    orphans = s.run("MATCH (c:InternalChunk {chunk_id: 'chunk_orphan_1'}) RETURN count(c) AS cnt").single()["cnt"]
    print("Orphaned chunks remaining (expect 0):", orphans)

# Clean up
mgr_recovery.release_lock_on_failure()
with driver.session() as s:
    s.run("MATCH (s:SystemConfig {id: 'internal_rag_state'}) SET s.ingestion_status = 'IDLE', s.failed_version = null")

driver.close()
print("🎉 All Tests Completed Successfully!")
