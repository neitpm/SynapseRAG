import sys
from neo4j import GraphDatabase
from ingest_internal_chunks import perform_pre_swap_smoke_tests, NEO4J_URI, NEO4J_USER, NEO4J_PASSWORD

driver = GraphDatabase.driver(NEO4J_URI, auth=(NEO4J_USER, NEO4J_PASSWORD))
with driver.session() as s:
    cfg = s.run("MATCH (s:SystemConfig {id: 'internal_rag_state'}) RETURN s.active_version AS ver").single()
    active_ver = cfg["ver"]
    
    # Lấy 1 chunk mẫu thực tế từ active version
    sample = s.run("""
        MATCH (c:InternalChunk {version: $ver})
        WHERE c.text CONTAINS 'máy biến áp' OR c.text CONTAINS 'thực hiện' OR c.text CONTAINS 'thi công'
        RETURN c.chunk_id AS chunk_id, c.text AS text, c.embedding AS embedding
        LIMIT 1
    """, ver=active_ver).single()
    
    chunk_dict = {
        "chunk_id": sample["chunk_id"],
        "text": sample["text"],
        "embedding": sample["embedding"]
    }
    
    print("Testing smoke test with real chunk:", chunk_dict["chunk_id"])
    print("Text snippet:", chunk_dict["text"][:100])
    res = perform_pre_swap_smoke_tests(s, active_ver, chunk_dict)
    print("Smoke test result (expect True):", res)
driver.close()
