import sys
import logging
import time
import json
import asyncio
import urllib.request
from pathlib import Path
from typing import List, Optional, AsyncGenerator, Dict, Any
import strawberry
from fastapi import FastAPI, Query as FastAPIQuery
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import StreamingResponse
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
# LLM Generation via Gemini API
# ============================================================================

def generate_llm_answer(question: str, context: str, graph_categories: list[str]) -> tuple[str, str]:
    """
    Tạo câu trả lời tự nhiên thông qua Gemini LLM API dựa trên Context đồ thị.
    Nếu API gặp sự cố hoặc chưa cấu hình key, fallback an toàn sang Extractive synthesis.
    """
    if not settings.GEMINI_API_KEY:
        fallback = f"Dựa trên cơ sở tri thức đồ thị:\n{context[:400]}..."
        return fallback, "Extractive Fallback (No Gemini API key)"

    prompt = f"""Bạn là trợ lý AI thông minh của nền tảng đồ thị tri thức SynapseRAG.
Hãy trả lời câu hỏi của người dùng một cách mạch lạc, tự nhiên, chính xác dựa trên Ngữ cảnh được trích xuất từ Đồ thị tri thức (GraphRAG) dưới đây.

[CÁC CHỦ ĐỀ LIÊN QUAN TRONG ĐỒ THỊ]:
{', '.join(graph_categories) if graph_categories else 'Không có'}

[NGỮ CẢNH TRI THỨC TRÍCH XUẤT]:
{context}

[CÂU HỎI]:
{question}

[YÊU CẦU]:
- Trả lời bằng tiếng Việt, rõ ràng, gãy gọn, có cấu trúc.
- Sử dụng trực tiếp thông tin trong ngữ cảnh.
- Không bịa đặt thông tin nằm ngoài ngữ cảnh được cung cấp.
"""

    url = f"https://generativelanguage.googleapis.com/v1beta/models/{settings.GEMINI_MODEL}:generateContent?key={settings.GEMINI_API_KEY}"
    headers = {"Content-Type": "application/json"}
    payload = {
        "contents": [{"parts": [{"text": prompt}]}],
        "generationConfig": {
            "temperature": 0.2,
            "maxOutputTokens": 600
        }
    }

    try:
        req = urllib.request.Request(url, data=json.dumps(payload).encode("utf-8"), headers=headers)
        with urllib.request.urlopen(req, timeout=12) as resp:
            data = json.loads(resp.read().decode("utf-8"))
            answer_text = data["candidates"][0]["content"]["parts"][0]["text"]
            return answer_text.strip(), f"Generative LLM ({settings.GEMINI_MODEL})"
    except Exception as e:
        logger.error(f"Lỗi khi gọi Gemini LLM API: {e}")
        fallback = f"[Fallback trích xuất đồ thị]:\n{context[:400]}..."
        return fallback, f"Extractive Fallback (Gemini call failed: {type(e).__name__})"

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
# Unified Core Query Execution Helper (DÙNG CHUNG CHO CẢ GRAPHQL & SSE STREAM)
# ============================================================================

def execute_rag_pipeline(
    question: str, 
    top_k: int = 3, 
    collect_debug: bool = False,
    progress_callback: Optional[Any] = None
) -> tuple[str, list[dict], list[DebugStep], float]:
    """
    Pipeline cốt lõi duy nhất cho RAG:
    - Bước 1: Embedding câu hỏi
    - Bước 2: Neo4j Vector Search + Graph Traversal
    - Bước 3: Gemini LLM Generation
    Nếu có progress_callback, gọi callback đồng bộ theo từng event để SSE stream phát ra ngay lập tức.
    """
    t_start = time.perf_counter()
    steps: List[DebugStep] = []
    sources: list[dict] = []
    context_snippets: list[str] = []

    def notify(event_type: str, data: dict):
        if progress_callback:
            progress_callback(event_type, data)

    notify("pipeline_start", {
        "question": question,
        "top_k": top_k,
        "timestamp": time.time(),
        "active_node": "gateway"
    })

    # ----------------------------------------------------
    # BƯỚC 1: Embedding câu hỏi (Bên trong Gateway)
    # ----------------------------------------------------
    notify("step_start", {
        "step_id": 1,
        "name": "1. Question Embedding",
        "active_node": "gateway",
        "active_edge": "client_to_gateway",
        "details": f"Gateway đang nhúng câu hỏi thành vector bằng mô hình '{settings.EMBEDDING_MODEL}'..."
    })

    step1_start = time.perf_counter()
    q_vec = None
    try:
        q_vec = embedder.encode(question).tolist()
        step1_dur = round((time.perf_counter() - step1_start) * 1000, 2)
        step_metrics = [
            StepMetric(key="embedding_model", value=settings.EMBEDDING_MODEL),
            StepMetric(key="vector_dimensions", value=str(len(q_vec))),
            StepMetric(key="vector_preview", value=f"[{q_vec[0]:.4f}, {q_vec[1]:.4f}, ...]")
        ]
        if collect_debug:
            steps.append(
                DebugStep(
                    name="1. Question Embedding",
                    status="SUCCESS",
                    duration_ms=step1_dur,
                    details=f"Gateway hoàn tất sinh vector 384 chiều ({step1_dur}ms).",
                    metrics=step_metrics
                )
            )
        notify("step_complete", {
            "step_id": 1,
            "name": "1. Question Embedding",
            "status": "SUCCESS",
            "duration_ms": step1_dur,
            "active_node": "gateway",
            "details": f"Vector 384 chiều đã sẵn sàng ({step1_dur}ms).",
            "metrics": [{"key": m.key, "value": m.value} for m in step_metrics]
        })
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
        notify("step_failed", {
            "step_id": 1,
            "name": "1. Question Embedding",
            "status": "FAILED",
            "duration_ms": step1_dur,
            "error": "Lỗi khi sinh vector nhúng"
        })
        total_ms = round((time.perf_counter() - t_start) * 1000, 2)
        return "Lỗi trong quá trình sinh vector nhúng cho câu hỏi.", [], steps, total_ms

    # ----------------------------------------------------
    # BƯỚC 2: Neo4j Vector Search & Graph Traversal
    # ----------------------------------------------------
    notify("step_start", {
        "step_id": 2,
        "name": "2. Vector Search & Graph Traversal",
        "active_node": "neo4j",
        "active_edge": "gateway_to_neo4j",
        "details": "Truy vấn Neo4j Index Vector và mở rộng đồ thị qua quan hệ HAS_CHUNK, IN_CATEGORY, MENTIONS..."
    })

    step2_start = time.perf_counter()
    cypher = """
        CALL db.index.vector.queryNodes('chunk_vector_index', $top_k, $q_vec)
        YIELD node, score
        MATCH (a:Article)-[:HAS_CHUNK]->(node)
        OPTIONAL MATCH (a)-[:IN_CATEGORY]->(cat:Category)
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

                sources.append({
                    "chunk_id": r["chunk_id"],
                    "article_title": r["article_title"],
                    "article_url": r["article_url"],
                    "score": round(r["score"], 4),
                    "text": r["text"],
                    "related_categories": cats[:3],
                    "mentioned_articles": mentions
                })
                context_snippets.append(f"• [Nguồn: {r['article_title']} | Độ tương đồng: {r['score']:.2f}]\n{r['text'].strip()}")

        step2_dur = round((time.perf_counter() - step2_start) * 1000, 2)
        step2_metrics = [
            StepMetric(key="top_k_requested", value=str(top_k)),
            StepMetric(key="chunks_found", value=str(len(sources))),
            StepMetric(key="articles_matched", value=", ".join(all_articles) if all_articles else "None"),
            StepMetric(key="graph_categories_expanded", value=str(len(all_categories))),
            StepMetric(key="graph_mentions_expanded", value=str(len(all_mentions))),
            StepMetric(key="top_match_score", value=str(sources[0]["score"]) if sources else "0.0")
        ]
        if collect_debug:
            steps.append(
                DebugStep(
                    name="2. Vector Search & Graph Traversal",
                    status="SUCCESS",
                    duration_ms=step2_dur,
                    details=f"Tìm thấy {len(sources)} chunks, mở rộng {len(all_categories)} categories và {len(all_mentions)} mentions ({step2_dur}ms).",
                    metrics=step2_metrics
                )
            )
        notify("step_complete", {
            "step_id": 2,
            "name": "2. Vector Search & Graph Traversal",
            "status": "SUCCESS",
            "duration_ms": step2_dur,
            "active_node": "neo4j",
            "details": f"Tìm thấy {len(sources)} chunks và mở rộng đồ thị ({step2_dur}ms).",
            "metrics": [{"key": m.key, "value": m.value} for m in step2_metrics]
        })
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
        notify("step_failed", {
            "step_id": 2,
            "name": "2. Vector Search & Graph Traversal",
            "status": "FAILED",
            "duration_ms": step2_dur,
            "error": "Lỗi truy vấn cơ sở dữ liệu đồ thị Neo4j."
        })
        total_ms = round((time.perf_counter() - t_start) * 1000, 2)
        return "Lỗi khi kết nối hoặc truy vấn Neo4j Graph Database.", [], steps, total_ms

    # ----------------------------------------------------
    # BƯỚC 3: Tạo câu trả lời tự nhiên qua LLM (Gemini API)
    # ----------------------------------------------------
    notify("step_start", {
        "step_id": 3,
        "name": "3. Answer Generation (LLM)",
        "active_node": "gemini",
        "active_edge": "neo4j_to_gemini",
        "details": f"Gửi context đồ thị đến Google Gemini ({settings.GEMINI_MODEL}) tổng hợp câu trả lời tự nhiên..."
    })

    step3_start = time.perf_counter()
    if context_snippets:
        full_context = "\n\n".join(context_snippets)
        answer, strategy_desc = generate_llm_answer(question, full_context, list(all_categories))
        llm_used_flag = "true" if "Generative LLM" in strategy_desc else "false"
    else:
        answer = "Rất tiếc, hệ thống chưa tìm thấy thông tin phù hợp trong cơ sở tri thức đồ thị cho câu hỏi này."
        strategy_desc = "No match found"
        llm_used_flag = "false"

    step3_dur = round((time.perf_counter() - step3_start) * 1000, 2)
    step3_metrics = [
        StepMetric(key="synthesis_strategy", value=strategy_desc),
        StepMetric(key="generative_llm_used", value=llm_used_flag),
        StepMetric(key="llm_model", value=settings.GEMINI_MODEL),
        StepMetric(key="context_chunks_fed", value=str(len(context_snippets)))
    ]
    if collect_debug:
        steps.append(
            DebugStep(
                name="3. Answer Generation (LLM)",
                status="SUCCESS",
                duration_ms=step3_dur,
                details=f"Chiến lược: {strategy_desc}",
                metrics=step3_metrics
            )
        )
    notify("step_complete", {
        "step_id": 3,
        "name": "3. Answer Generation (LLM)",
        "status": "SUCCESS",
        "duration_ms": step3_dur,
        "active_node": "gemini",
        "details": f"Hoàn tất sinh câu trả lời bằng {strategy_desc} ({step3_dur}ms).",
        "metrics": [{"key": m.key, "value": m.value} for m in step3_metrics]
    })

    total_ms = round((time.perf_counter() - t_start) * 1000, 2)
    notify("pipeline_complete", {
        "question": question,
        "answer": answer,
        "sources": sources,
        "total_duration_ms": total_ms,
        "active_node": "result",
        "active_edge": "gemini_to_result"
    })

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
        Vector search -> Graph Traversal -> LLM sinh câu trả lời tự nhiên kèm nguồn.
        """
        logger.info(f"🔍 [ask] Nhận câu hỏi: '{question}'")
        answer, raw_sources, _, _ = execute_rag_pipeline(question, top_k, collect_debug=False)
        sources = [
            SourceChunk(
                chunk_id=s["chunk_id"],
                article_title=s["article_title"],
                article_url=s["article_url"],
                score=s["score"],
                text=s["text"],
                related_categories=s["related_categories"],
                mentioned_articles=s["mentioned_articles"]
            ) for s in raw_sources
        ]
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
        answer, raw_sources, steps, total_ms = execute_rag_pipeline(question, top_k, collect_debug=True)
        sources = [
            SourceChunk(
                chunk_id=s["chunk_id"],
                article_title=s["article_title"],
                article_url=s["article_url"],
                score=s["score"],
                text=s["text"],
                related_categories=s["related_categories"],
                mentioned_articles=s["mentioned_articles"]
            ) for s in raw_sources
        ]
        return DebugRAGAnswer(
            question=question,
            answer=answer,
            sources=sources,
            total_duration_ms=total_ms,
            steps=steps
        )

# ============================================================================
# FastAPI App & SSE Realtime Stream Endpoint
# ============================================================================

schema = strawberry.Schema(query=Query)
graphql_app = GraphQLRouter(schema)

app = FastAPI(title="SynapseRAG GraphQL Gateway", version="1.0.0")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

app.include_router(graphql_app, prefix="/graphql")

@app.get("/")
def health_check():
    return {
        "service": "SynapseRAG GraphQL Gateway",
        "status": "HEALTHY",
        "llm_model": settings.GEMINI_MODEL,
        "debug_trace_enabled": settings.RAG_DEBUG_TRACE_ENABLED,
        "graphql_playground": "/graphql"
    }

@app.get("/api/debug/ask/stream")
async def stream_rag_debug(question: str = FastAPIQuery(..., description="Câu hỏi cần RAG"), top_k: int = 3):
    """
    SSE stream endpoint cho Developer UI, chạy cùng một hàm `execute_rag_pipeline` duy nhất.
    """
    event_queue: asyncio.Queue = asyncio.Queue()

    def progress_callback(event_type: str, data: dict):
        event_queue.put_nowait((event_type, data))

    async def run_pipeline_task():
        loop = asyncio.get_event_loop()
        await loop.run_in_executor(
            None,
            lambda: execute_rag_pipeline(
                question=question, 
                top_k=top_k, 
                collect_debug=True, 
                progress_callback=progress_callback
            )
        )
        await event_queue.put(("DONE_STREAM", {}))

    asyncio.create_task(run_pipeline_task())

    async def event_generator() -> AsyncGenerator[str, None]:
        while True:
            event_type, data = await event_queue.get()
            if event_type == "DONE_STREAM":
                break
            yield f"event: {event_type}\ndata: {json.dumps(data, ensure_ascii=False)}\n\n"

    return StreamingResponse(event_generator(), media_type="text/event-stream")

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="127.0.0.1", port=8000)
