import sys
import logging
from pathlib import Path
from typing import List, Optional
import strawberry
from fastapi import FastAPI
from strawberry.fastapi import GraphQLRouter
from neo4j import GraphDatabase
from sentence_transformers import SentenceTransformer

# Hỗ trợ UTF-8 cho Windows Terminal
sys.stdout.reconfigure(encoding="utf-8")

ROOT_DIR = Path(__file__).resolve().parent.parent.parent
sys.path.append(str(ROOT_DIR))

from shared.config import settings

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger("rag-graphql-gateway")

# Khởi tạo SentenceTransformer model (dùng chung all-MiniLM-L6-v2 384-dims)
logger.info(f"Đang tải Embedding model '{settings.EMBEDDING_MODEL}'...")
embedder = SentenceTransformer(settings.EMBEDDING_MODEL)

# Khởi tạo Neo4j driver
neo4j_driver = GraphDatabase.driver(
    settings.NEO4J_URI,
    auth=(settings.NEO4J_USER, settings.NEO4J_PASSWORD)
)

# ============================================================================
# GraphQL Schema Types
# ============================================================================

@strawberry.type
class SourceChunk:
    chunk_id: str
    article_title: str
    article_url: Optional[str]
    score: float
    text: str
    related_categories: List[str]
    mentioned_articles: List[str]

@strawberry.type
class RAGAnswer:
    question: str
    answer: str
    sources: List[SourceChunk]

@strawberry.type
class Query:
    @strawberry.field
    def ask(self, question: str, top_k: int = 3) -> RAGAnswer:
        """
        Truy vấn GraphRAG:
        1. Embed câu hỏi thành vector 384-dims.
        2. Vector Search trên Neo4j để tìm top_k Chunk gần nhất.
        3. Graph Traversal: Mở rộng context qua quan hệ đồ thị (HAS_CHUNK, IN_CATEGORY, MENTIONS).
        4. Tổng hợp câu trả lời kèm nguồn trích dẫn bài viết.
        """
        logger.info(f"🔍 Nhận câu hỏi: '{question}'")

        # 1. Tính toán embedding vector cho câu hỏi
        q_vec = embedder.encode(question).tolist()

        # 2. Vector search + Graph traversal mở rộng trong 1 câu Cypher duy nhất
        cypher = """
            CALL db.index.vector.queryNodes('chunk_vector_index', $top_k, $q_vec)
            YIELD node, score
            MATCH (a:Article)-[:HAS_CHUNK]->(node)
            
            // Mở rộng Graph Traversal: tìm category của bài viết
            OPTIONAL MATCH (a)-[:IN_CATEGORY]->(cat:Category)
            
            // Mở rộng Graph Traversal: tìm các bài viết được Chunk nhắc đến
            OPTIONAL MATCH (node)-[:MENTIONS]->(m:Article)
            
            RETURN 
                node.chunk_id AS chunk_id,
                node.text AS text,
                score,
                a.title AS article_title,
                a.url AS article_url,
                collect(DISTINCT cat.name) AS categories,
                collect(DISTINCT m.title) AS mentions
            ORDER BY score DESC
        """

        sources = []
        context_snippets = []

        with neo4j_driver.session() as session:
            results = session.run(cypher, {"q_vec": q_vec, "top_k": top_k}).data()
            for r in results:
                sources.append(
                    SourceChunk(
                        chunk_id=r["chunk_id"],
                        article_title=r["article_title"],
                        article_url=r["article_url"],
                        score=round(r["score"], 4),
                        text=r["text"],
                        related_categories=r["categories"][:3] if r["categories"] else [],
                        mentioned_articles=r["mentions"] if r["mentions"] else []
                    )
                )
                context_snippets.append(f"[{r['article_title']}] {r['text'].strip()}")

        # 3. Tạo câu trả lời tổng hợp dựa trên ngữ cảnh lấy từ Graph & Vector
        if context_snippets:
            # Tóm tắt câu trả lời từ context đồ thị
            primary_chunk = sources[0]
            answer = (
                f"Dựa trên cơ sở tri thức đồ thị SynapseRAG từ bài viết '{primary_chunk.article_title}':\n"
                f"{primary_chunk.text.strip()}\n\n"
                f"(Khám phá đồ thị: Chủ đề này liên quan đến danh mục [{', '.join(primary_chunk.related_categories)}])"
            )
        else:
            answer = "Rất tiếc, hệ thống chưa tìm thấy thông tin phù hợp trong đồ thị tri thức cho câu hỏi này."

        return RAGAnswer(
            question=question,
            answer=answer,
            sources=sources
        )

# ============================================================================
# FastAPI App
# ============================================================================

schema = strawberry.Schema(query=Query)
graphql_app = GraphQLRouter(schema)

app = FastAPI(title="SynapseRAG GraphQL Gateway", version="1.0.0")
app.include_router(graphql_app, prefix="/graphql")

@app.get("/")
def health_check():
    return {
        "service": "SynapseRAG GraphQL Gateway",
        "status": "HEALTHY",
        "graphql_playground": "/graphql"
    }

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="127.0.0.1", port=8000)

