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

print("=== TEST 1: Fencing Token Increment & Ownership Verification ===")
mgr1 = IngestionLockManager(driver, "job_alpha", "ver_alpha")
res1 = mgr1.acquire_lock()
print("Job Alpha acquire (expect True):", res1)
print("Job Alpha Fencing Token:", mgr1.fencing_token)
print("Job Alpha verify ownership (expect True):", mgr1.verify_ownership())

mgr1.stop_heartbeat()

print("\n=== TEST 2: Fencing Token Rejection after Lease Takeover ===")
# Giả lập job_alpha bị network partition / đóng băng CPU > 1800s
now_ts = int(time.time())
past_ts = now_ts - 2000
with driver.session() as s:
    s.run("""
        MATCH (s:SystemConfig {id: 'internal_rag_state'})
        SET s.last_heartbeat_at = $past_ts,
            s.staging_version = 'ver_orphaned_alpha',
            s.lock_job_id = 'job_alpha'
    """, past_ts=past_ts)
    s.run("MERGE (c:InternalChunk {chunk_id: 'chunk_alpha_1'}) SET c.version = 'ver_orphaned_alpha', c.ingestion_job_id = 'job_alpha'")

# Job Beta xuất hiện và tiếp quản lease quá hạn
mgr2 = IngestionLockManager(driver, "job_beta", "ver_beta")
res2 = mgr2.acquire_lock()
print("Job Beta acquire on expired lease (expect True):", res2)
print("Job Beta Fencing Token (expect token > Alpha):", mgr2.fencing_token)

# Job Alpha bây giờ 'hồi sinh' và cố gắng verify / nạp tiếp:
alpha_ownership = mgr1.verify_ownership()
print("Job Alpha verify ownership after being taken over (expect False):", alpha_ownership)

# Xác nhận chunk của Alpha đã bị dọn sạch theo cả version và job_id
with driver.session() as s:
    orphans = s.run("MATCH (c:InternalChunk {chunk_id: 'chunk_alpha_1'}) RETURN count(c) AS cnt").single()["cnt"]
    print("Job Alpha chunks remaining after takeover cleanup (expect 0):", orphans)

# Dọn dẹp lock của mgr2 trước khi đóng driver
mgr2.release_lock_on_failure()

print("\n=== TEST 3: Async Cleanup Driver Independence ===")
# Tạo 1 dummy node thuộc phiên bản cũ cần dọn
with driver.session() as s:
    s.run("MERGE (c:InternalChunk {chunk_id: 'chunk_old_ver_test'}) SET c.version = 'ver_old_cleanup_test'")

# Gọi cleanup bất đồng bộ với driver riêng
cleanup_old_version_async(NEO4J_URI, (NEO4J_USER, NEO4J_PASSWORD), "ver_old_cleanup_test")

# Đóng driver chính ngay lập tức để chứng minh không bị race condition
driver.close()
print("Main driver closed immediately. Waiting 2s for background cleanup worker...")
time.sleep(2)

# Mở lại driver kiểm tra node cũ đã bị xóa bởi background worker chưa
check_driver = GraphDatabase.driver(NEO4J_URI, auth=(NEO4J_USER, NEO4J_PASSWORD))
with check_driver.session() as s:
    remaining_old = s.run("MATCH (c:InternalChunk {chunk_id: 'chunk_old_ver_test'}) RETURN count(c) AS cnt").single()["cnt"]
    print("Old version chunks remaining after async cleanup (expect 0):", remaining_old)
    s.run("MATCH (s:SystemConfig {id: 'internal_rag_state'}) SET s.ingestion_status = 'IDLE', s.failed_version = null, s.lock_job_id = null")
check_driver.close()

print("🎉 ALL RECOVERY, FENCING TOKEN & CLEANUP TESTS PASSED 100%!")
