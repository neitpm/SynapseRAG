import sys
import logging
import time
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
class StepMetric:
    key: str
    value: str

@strawberry.type
class DebugStep:
    name: str
    status: str
    duration_ms: float
    details: str
    metrics: List[StepMetric]

@strawberry.type
class DebugRAGAnswer:
    question: str
    answer: str
    sources: List[SourceChunk]
    total_duration_ms: float
    steps: List[DebugStep]

# ============================================================================
# Core Query Execution Helper
# ============================================================================

def _execute_rag_pipeline(question: str, top_k: int, collect_debug: bool = False):
    """
    Thực thi pipeline tìm kiếm GraphRAG.
    Trả về (answer_str, sources_list, debug_steps, total_duration_ms).
    Đảm bảo:
    - ask() gọi hàm này với collect_debug=False, giữ nguyên 100% format và logic cũ.
    - debugAsk() gọi hàm này với collect_debug=True để thu thập trace chi tiết.
    - An toàn: Không lộ API keys, passwords hay full vector raw data.
    """
    t_start = time.perf_counter()
    steps: List[DebugStep] = []
    sources: List[SourceChunk] = []
    context_snippets: List[str] = []

    # ----------------------------------------------------
    # BƯỚC 1: Embedding câu hỏi
    # ----------------------------------------------------
    step1_start = time.perf_counter()
    q_vec = None
    try:
        q_vec = embedder.encode(question).tolist()
        step1_dur = round((time.perf_counter() - step1_start) * 1000, 2)
        if collect_debug:
            steps.append(
                DebugStep(
                    name="1. Question Embedding",
                    status="SUCCESS",
                    duration_ms=step1_dur,
                    details=f"Sinh vector nhúng với mô hình cục bộ '{settings.EMBEDDING_MODEL}'.",
                    metrics=[
                        StepMetric(key="embedding_model", value=settings.EMBEDDING_MODEL),
                        StepMetric(key="vector_dimensions", value=str(len(q_vec))),
                        StepMetric(key="vector_preview", value=f"[{q_vec[0]:.4f}, {q_vec[1]:.4f}, ...]")
                    ]
                )
            )
    except Exception as e:
        step1_dur = round((time.perf_counter() - step1_start) * 1000, 2)
        logger.error(f"Lỗi embedding câu hỏi: {e}")
        if collect_debug:
            steps.append(
                DebugStep(
                    name="1. Question Embedding",
                    status="FAILED",
                    duration_ms=step1_dur,
                    details="Lỗi khi tạo embedding cho câu hỏi.",
                    metrics=[StepMetric(key="error", value="Embedding computation error")]
                )
            )
        total_ms = round((time.perf_counter() - t_start) * 1000, 2)
        return "Lỗi trong quá trình sinh vector nhúng cho câu hỏi.", [], steps, total_ms

    # ----------------------------------------------------
    # BƯỚC 2: Vector Search & Graph Traversal (Neo4j)
    # ----------------------------------------------------
    step2_start = time.perf_counter()
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

    all_categories = set()
    all_mentions = set()
    all_articles = set()

    try:
        with neo4j_driver.session() as session:
            results = session.run(cypher, {"q_vec": q_vec, "top_k": top_k}).data()
            for r in results:
                cats = [c for c in r["categories"] if c]
                mentions = [m for m in r["mentions"] if m]
                all_categories.update(cats)
                all_mentions.update(mentions)
                all_articles.add(r["article_title"])

                sources.append(
                    SourceChunk(
                        chunk_id=r["chunk_id"],
                        article_title=r["article_title"],
                        article_url=r["article_url"],
                        score=round(r["score"], 4),
                        text=r["text"],
                        related_categories=cats[:3],
                        mentioned_articles=mentions
                    )
                )
                context_snippets.append(f"[{r['article_title']}] {r['text'].strip()}")

        step2_dur = round((time.perf_counter() - step2_start) * 1000, 2)
        if collect_debug:
            steps.append(
                DebugStep(
                    name="2. Vector Search & Graph Traversal",
                    status="SUCCESS",
                    duration_ms=step2_dur,
                    details="Tìm kiếm vector top_k trên Neo4j index và mở rộng đồ thị qua quan hệ HAS_CHUNK, IN_CATEGORY, MENTIONS.",
                    metrics=[
                        StepMetric(key="top_k_requested", value=str(top_k)),
                        StepMetric(key="chunks_found", value=str(len(sources))),
                        StepMetric(key="articles_matched", value=", ".join(all_articles) if all_articles else "None"),
                        StepMetric(key="graph_categories_expanded", value=str(len(all_categories))),
                        StepMetric(key="graph_mentions_expanded", value=str(len(all_mentions))),
                        StepMetric(key="top_match_score", value=str(sources[0].score) if sources else "0.0")
                    ]
                )
            )
    except Exception as e:
        step2_dur = round((time.perf_counter() - step2_start) * 1000, 2)
        logger.error(f"Lỗi truy vấn Neo4j GraphRAG: {e}")
        if collect_debug:
            steps.append(
                DebugStep(
                    name="2. Vector Search & Graph Traversal",
                    status="FAILED",
                    duration_ms=step2_dur,
                    details="Lỗi khi truy vấn đồ thị và index vector trên Neo4j.",
                    metrics=[StepMetric(key="error", value="Database query error")]
                )
            )
        total_ms = round((time.perf_counter() - t_start) * 1000, 2)
        return "Lỗi khi kết nối hoặc truy vấn Neo4j Graph Database.", [], steps, total_ms

    # ----------------------------------------------------
    # BƯỚC 3: Xây dựng câu trả lời (Answer Synthesis Strategy)
    # ----------------------------------------------------
    step3_start = time.perf_counter()
    if context_snippets:
        primary_chunk = sources[0]
        answer = (
            f"Dựa trên cơ sở tri thức đồ thị SynapseRAG từ bài viết '{primary_chunk.article_title}':\n"
            f"{primary_chunk.text.strip()}\n\n"
            f"(Khám phá đồ thị: Chủ đề này liên quan đến danh mục [{', '.join(primary_chunk.related_categories)}])"
        )
        strategy_desc = "Extractive Synthesis (Trích xuất đoạn văn bản có điểm vector cao nhất kèm context quan hệ đồ thị; chưa dùng LLM generative)."
    else:
        answer = "Rất tiếc, hệ thống chưa tìm thấy thông tin phù hợp trong đồ thị tri thức cho câu hỏi này."
        strategy_desc = "No match found (Không tìm thấy chunk đạt ngưỡng trong cơ sở tri thức)."

    step3_dur = round((time.perf_counter() - step3_start) * 1000, 2)
    if collect_debug:
        steps.append(
            DebugStep(
                name="3. Answer Synthesis",
                status="SUCCESS",
                duration_ms=step3_dur,
                details=strategy_desc,
                metrics=[
                    StepMetric(key="synthesis_strategy", value="Extractive from Top Chunk + Graph Metadata"),
                    StepMetric(key="generative_llm_used", value="false"),
                    StepMetric(key="context_snippets_count", value=str(len(context_snippets))),
                    StepMetric(key="primary_source_article", value=sources[0].article_title if sources else "None")
                ]
            )
        )

    total_ms = round((time.perf_counter() - t_start) * 1000, 2)
    return answer, sources, steps, total_ms


# ============================================================================
# GraphQL Query Root
# ============================================================================

@strawberry.type
class Query:
    @strawberry.field
    def ask(self, question: str, top_k: int = 3) -> RAGAnswer:
        """
        Truy vấn GraphRAG chuẩn dành cho người dùng:
        1. Embed câu hỏi thành vector 384-dims.
        2. Vector Search trên Neo4j để tìm top_k Chunk gần nhất.
        3. Graph Traversal: Mở rộng context qua quan hệ đồ thị (HAS_CHUNK, IN_CATEGORY, MENTIONS).
        4. Tổng hợp câu trả lời kèm nguồn trích dẫn bài viết.
        """
        logger.info(f"🔍 [ask] Nhận câu hỏi: '{question}'")
        answer, sources, _, _ = _execute_rag_pipeline(question, top_k, collect_debug=False)
        return RAGAnswer(
            question=question,
            answer=answer,
            sources=sources
        )

    @strawberry.field
    def debug_ask(self, question: str, top_k: int = 3) -> DebugRAGAnswer:
        """
        Truy vấn GraphRAG kèm Trace từng bước dành cho Developer:
        Có thể bật/tắt qua biến môi trường RAG_DEBUG_TRACE_ENABLED (mặc định: True).
        """
        if not settings.RAG_DEBUG_TRACE_ENABLED:
            return DebugRAGAnswer(
                question=question,
                answer="Chế độ debug trace đang bị tắt ở môi trường này (RAG_DEBUG_TRACE_ENABLED=false). Vui lòng sử dụng field 'ask'.",
                sources=[],
                total_duration_ms=0.0,
                steps=[
                    DebugStep(
                        name="Access Control",
                        status="DISABLED",
                        duration_ms=0.0,
                        details="RAG_DEBUG_TRACE_ENABLED is false. Debug trace is disabled.",
                        metrics=[]
                    )
                ]
            )

        logger.info(f"🛠️ [debugAsk] Nhận câu hỏi debug: '{question}'")
        answer, sources, steps, total_ms = _execute_rag_pipeline(question, top_k, collect_debug=True)
        return DebugRAGAnswer(
            question=question,
            answer=answer,
            sources=sources,
            total_duration_ms=total_ms,
            steps=steps
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
        "debug_trace_enabled": settings.RAG_DEBUG_TRACE_ENABLED,
        "graphql_playground": "/graphql"
    }

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="127.0.0.1", port=8000)
