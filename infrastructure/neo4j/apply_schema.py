import sys
from pathlib import Path

# Thêm root vào sys.path để import shared
ROOT_DIR = Path(__file__).resolve().parent.parent.parent
sys.path.append(str(ROOT_DIR))

from shared.config import settings
from neo4j import GraphDatabase

def init_neo4j():
    print(f"Connecting to Neo4j at {settings.NEO4J_URI}...")
    driver = GraphDatabase.driver(
        settings.NEO4J_URI,
        auth=(settings.NEO4J_USER, settings.NEO4J_PASSWORD)
    )
    
    with driver.session() as session:
        # Kiểm tra kết nối
        result = session.run("RETURN 1 AS connected")
        record = result.single()
        if record and record["connected"] == 1:
            print("Successfully connected to Neo4j!")
            
        # Áp dụng file init-schema.cypher
        cypher_file = ROOT_DIR / "infrastructure" / "neo4j" / "init-schema.cypher"
        with open(cypher_file, "r", encoding="utf-8") as f:
            statements = [stmt.strip() for stmt in f.read().split(";") if stmt.strip() and not stmt.strip().startswith("#")]

        for stmt in statements:
            # Loại bỏ các dòng comment cypher // hoặc #
            clean_stmt = "\n".join([line for line in stmt.splitlines() if not line.strip().startswith(("//", "#"))])
            if clean_stmt.strip():
                print(f"Executing Cypher:\n{clean_stmt}\n---")
                session.run(clean_stmt)

    driver.close()
    print("Neo4j Schema & Vector Index initialized successfully!")

if __name__ == "__main__":
    init_neo4j()
