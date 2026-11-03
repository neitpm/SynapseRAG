import sys
import logging
import time
import json
import asyncio
import urllib.request
from pathlib import Path
from typing import List, Optional, AsyncGenerator, Dict, Any, Tuple
import strawberry
from fastapi import FastAPI, Query as FastAPIQuery, Request, Header, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import StreamingResponse
from strawberry.fastapi import GraphQLRouter
from neo4j import GraphDatabase
from sentence_transformers import SentenceTransformer
from rank_bm25 import BM25Okapi
import re
from prometheus_client import Counter, Histogram, Gauge, generate_latest, CONTENT_TYPE_LATEST
from fastapi.responses import Response

# Reconfigure UTF-8 for Windows Terminal
sys.stdout.reconfigure(encoding="utf-8")

ROOT_DIR = Path(__file__).resolve().parent.parent.parent
sys.path.append(str(ROOT_DIR))

from shared.config import settings
from wiki_dynamic import extract_keyword_candidate, fetch_and_store_wiki_on_demand
from project_router import execute_structured_query, match_project_in_question, is_internal_query_context
from auth_rbac import verify_access_token, AuthUser, create_access_token

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger("rag-graphql-gateway")

# ============================================================================
# PROMETHEUS OBSERVABILITY METRICS
# ============================================================================
# 1. Đo lường xác thực (Auth results)
AUTH_REQUESTS_TOTAL = Counter(
    "rag_auth_requests_total",
    "Tổng số request xác thực tài khoản",
    ["status"]  # "success" | "missing_token" | "invalid_token" | "forbidden"
)

# 2. Đo lường request theo chế độ thực thi (Mode)
RAG_REQUESTS_TOTAL = Counter(
    "rag_requests_total",
    "Tổng số truy vấn RAG theo chế độ",
    ["mode"]  # "extractive_internal" | "structured_cypher" | "graph_external"
)

# 3. Đo lường thời gian thực thi Extractive RAG (Latency Histogram)
RAG_EXTRACTIVE_DURATION_SECONDS = Histogram(
    "rag_extractive_duration_seconds",
    "Thời gian thực thi pipeline Extractive RAG (giây)",
    buckets=[0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0]
)

# 4. Đo lường số lần retrieval rỗng (Recall Drop / Candidate-Pool Poisoning Alert)
RAG_RETRIEVAL_EMPTY_TOTAL = Counter(
    "rag_retrieval_empty_total",
    "Số lần truy vấn Extractive RAG trả về 0 chunk kết quả",
    ["target_project"]
)

# 5. Ingestion & System State Metrics (Scraped from SystemConfig)
RAG_ACTIVE_VERSION_INFO = Gauge(
    "rag_active_version_info",
    "Phiên bản active_version hiện hành đang phục vụ truy vấn",
    ["version"]
)

RAG_INGESTION_METRICS_SOURCE_UP = Gauge(
    "rag_ingestion_metrics_source_up",
    "Trạng thái kết nối và đọc telemetry ingestion từ CSDL Neo4j (1=thành công, 0=thất bại)"
)

INGESTION_LOCK_TAKEOVERS_COUNT = Gauge(
    "rag_ingestion_lock_takeovers_count",
    "Tổng số lần Ingestion Lock bị takeover do timeout/lease recovery (đọc từ SystemConfig)"
)

INGESTION_ROLLBACKS_COUNT = Gauge(
    "rag_ingestion_rollbacks_count",
    "Tổng số lần job nạp thất bại và thực hiện rollback staging (đọc từ SystemConfig)"
)

INGESTION_LAST_DURATION_SECONDS = Gauge(
    "rag_ingestion_last_duration_seconds",
    "Thời gian chạy hoàn tất của job ingestion gần nhất (giây)"
)

INGESTION_LAST_EMBED_DURATION_SECONDS = Gauge(
    "rag_ingestion_last_embed_duration_seconds",
    "Thời gian tính toán embedding của job ingestion gần nhất (giây)"
)

INGESTION_LAST_SMOKE_DURATION_SECONDS = Gauge(
    "rag_ingestion_last_smoke_duration_seconds",
    "Thời gian chạy smoke test pre-swap của job ingestion gần nhất (giây)"
)

INGESTION_LAST_TOTAL_CHUNKS = Gauge(
    "rag_ingestion_last_total_chunks",
    "Tổng số chunks đã nạp thành công của phiên bản active_version hiện hành"
)

# 9. Đo lường chất lượng dữ liệu đồ thị (Data Quality Observability)
DATA_QUALITY_UNCLASSIFIED_PROJECTS = Gauge(
    "rag_data_quality_unclassified_projects_total",
    "Số lượng node Project trong CSDL đồ thị chưa được gán nhãn thuộc tính project_type"
)

# Khởi tạo SentenceTransformer model (dùng chung all-MiniLM-L6-v2 384-dims)
logger.info(f"Đang tải Embedding model '{settings.EMBEDDING_MODEL}'...")
embedder = SentenceTransformer(settings.EMBEDDING_MODEL)

# Khởi tạo Neo4j driver
neo4j_driver = GraphDatabase.driver(
    settings.NEO4J_URI,
    auth=(settings.NEO4J_USER, settings.NEO4J_PASSWORD)
)

# Đảm bảo Fulltext Lucene / BM25 index và Vector Index tồn tại trên Neo4j
try:
    with neo4j_driver.session() as init_s:
        # Fulltext Lucene index
        init_s.run("CREATE FULLTEXT INDEX chunk_fulltext_index IF NOT EXISTS FOR (c:Chunk) ON EACH [c.text]")
        # 384-dimensions Vector Index for all-MiniLM-L6-v2
        init_s.run("""
            CREATE VECTOR INDEX chunk_vector_index IF NOT EXISTS
            FOR (c:Chunk) ON (c.embedding)
            OPTIONS {indexConfig: {
              `vector.dimensions`: 384,
              `vector.similarity_function`: 'cosine'
            }}
        """)
        logger.info("✅ Neo4j Vector index 'chunk_vector_index' & Fulltext index 'chunk_fulltext_index' đã sẵn sàng.")
except Exception as e:
    logger.warning(f"Không thể khởi tạo index trên Neo4j: {e}")

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
- Trả lời bằng tiếng Việt, rõ ràng, gãy gọn, khách quan, có cấu trúc.
- Ưu tiên sử dụng trực tiếp các dữ kiện có trong ngữ cảnh.
- Nếu ngữ cảnh có chứa mốc thời gian chuyển giao quyền lực hoặc người đảm nhiệm chức vụ gần nhất (ví dụ: "Từ năm 2013, Tập Cận Bình kế nhiệm..."), hãy giải thích rõ ràng và khẳng định chức vụ đó.
- Trường hợp ngữ cảnh chưa đề cập đầy đủ, bạn có thể bổ sung kiến thức bách khoa chính xác về thực thể đó để trả lời câu hỏi của người dùng một cách hữu ích nhất, đồng thời lưu ý nguồn gốc thông tin.
"""

    url = f"https://generativelanguage.googleapis.com/v1beta/models/{settings.GEMINI_MODEL}:generateContent?key={settings.GEMINI_API_KEY}"
    headers = {"Content-Type": "application/json"}
    payload = {
        "contents": [{"parts": [{"text": prompt}]}],
        "generationConfig": {
            "temperature": 0.2,
            "maxOutputTokens": 1500
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
    # Trường Metadata trích dẫn chuyên biệt cho Dữ liệu Nội bộ (Extractive Lineage)
    source_table: Optional[str] = None
    source_id: Optional[str] = None
    project_code: Optional[str] = None
    date: Optional[str] = None

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
    progress_callback: Optional[Any] = None,
    auth_user: Optional[AuthUser] = None
) -> tuple[str, list[dict], list[DebugStep], float]:
    """
    Pipeline cốt lõi duy nhất cho RAG:
    - Kiểm tra cấu trúc & RBAC
    - Phân loại Extractive RAG Nội bộ hoặc Tổng hợp tri thức
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

    # =========================================================================
    # QUERY ROUTER: Kiểm tra câu hỏi nghiệp vụ / thống kê có cấu trúc trước
    # Sử dụng Parameterized Cypher định nghĩa sẵn, có kiểm tra quyền auth_user
    # =========================================================================
    structured_res = execute_structured_query(question, neo4j_driver, auth_user=auth_user)
    if structured_res:
        intent = structured_res.get("intent")
        router_dur = round((time.perf_counter() - t_start) * 1000, 2)
        logger.info(f"🎯 Query Router: Khớp câu hỏi cấu trúc nội bộ [{intent}] ({router_dur}ms)")
        
        notify("step_start", {
            "step_id": 1,
            "name": "1. Project Query Router",
            "active_node": "gateway",
            "active_edge": "client_to_gateway",
            "details": f"Phân loại câu hỏi nghiệp vụ dự án [{intent}]. Điều phối xử lý..."
        })
        notify("step_complete", {
            "step_id": 1,
            "name": "1. Project Query Router",
            "status": "SUCCESS" if intent != "PERMISSION_DENIED" else "FAILED",
            "duration_ms": router_dur,
            "active_node": "gateway",
            "details": f"Định tuyến: {intent}.",
            "metrics": [{"key": "intent", "value": intent}]
        })

        if intent == "PERMISSION_DENIED":
            notify("step_failed", {
                "step_id": 2,
                "name": "2. Security & RBAC Enforcement",
                "status": "FORBIDDEN",
                "duration_ms": 0.0,
                "error": "Tài khoản không có quyền truy cập dự án này."
            })
        elif intent == "AMBIGUOUS_EXPENSE_QUERY":
            notify("step_info", {
                "step_id": 2,
                "name": "2. Scope Clarification Required",
                "active_node": "gateway",
                "details": "Câu hỏi chi phí chưa xác định rõ phạm vi (dự án cụ thể hay toàn công ty). Đang yêu cầu người dùng làm rõ..."
            })
        else:
            cypher_real_dur = structured_res.get("cypher_duration_ms", 0.0)
            notify("step_start", {
                "step_id": 2,
                "name": "2. Parameterized Cypher Analytics",
                "active_node": "neo4j",
                "active_edge": "gateway_to_neo4j",
                "details": "Thực thi truy vấn Cypher định nghĩa sẵn có tham số để tổng hợp số liệu chính xác 100%..."
            })
            notify("step_complete", {
                "step_id": 2,
                "name": "2. Parameterized Cypher Analytics",
                "status": "SUCCESS",
                "duration_ms": cypher_real_dur,
                "active_node": "neo4j",
                "details": f"Truy vấn đồ thị quan hệ thành công ({cypher_real_dur}ms). Trích xuất số liệu hạch toán.",
                "metrics": [
                    {"key": "query_type", "value": "Deterministic Cypher Aggregation"},
                    {"key": "measured_latency_ms", "value": str(cypher_real_dur)}
                ]
            })

        formatted_answer = structured_res.get("formatted_answer", "")
        total_ms = round((time.perf_counter() - t_start) * 1000, 2)
        RAG_REQUESTS_TOTAL.labels(mode="structured_cypher").inc()
        notify("pipeline_complete", {
            "question": question,
            "answer": formatted_answer,
            "sources": [],
            "total_duration_ms": total_ms,
            "active_node": "result",
            "active_edge": "neo4j_to_result" if intent not in ["AMBIGUOUS_EXPENSE_QUERY", "PERMISSION_DENIED"] else "gateway_to_result"
        })
        return formatted_answer, [], steps, total_ms

    # =========================================================================
    # NHÁNH 2: EXTRACTIVE RAG CHO DỮ LIỆU NỘI BỘ (Nhật trình, sự cố, ghi chú)
    # Tuyệt đối không gửi sang Gemini API, lọc quyền hạn ngay trong truy vấn DB
    # =========================================================================
    matched_proj = match_project_in_question(question, neo4j_driver)
    if is_internal_query_context(question, neo4j_driver):
        logger.info(f"🔒 Chuyển sang Extractive RAG Nội bộ cho câu hỏi: '{question[:60]}' (Project: {matched_proj})")
        return run_internal_extractive_pipeline(
            question=question,
            top_k=top_k,
            collect_debug=collect_debug,
            progress_callback=progress_callback,
            target_project=matched_proj,
            auth_user=auth_user
        )

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
    # BƯỚC 2: Neo4j Hybrid Search (Vector Cosine + BM25 Lexical + Graph Traversal)
    # ----------------------------------------------------
    notify("step_start", {
        "step_id": 2,
        "name": "2. Hybrid Search (Vector + BM25) & Graph",
        "active_node": "neo4j",
        "active_edge": "gateway_to_neo4j",
        "details": "Tìm kiếm kết hợp Hybrid Search: Vector Cosine + BM25 Okapi & mở rộng quan hệ đồ thị..."
    })

    step2_start = time.perf_counter()
    
    # 1. Truy vấn Vector Index (Lấy top_k * 2 ứng viên)
    vector_cypher = """
        CALL db.index.vector.queryNodes('chunk_vector_index', $fetch_k, $q_vec)
        YIELD node, score
        MATCH (a:Article)-[:HAS_CHUNK]->(node)
        OPTIONAL MATCH (a)-[:IN_CATEGORY]->(cat:Category)
        OPTIONAL MATCH (node)-[:MENTIONS]->(m:Article)
        RETURN 
            node.chunk_id AS chunk_id,
            node.text AS text,
            score AS vector_score,
            a.title AS article_title,
            a.url AS article_url,
            collect(DISTINCT cat.name) AS categories,
            collect(DISTINCT m.title) AS mentions
        ORDER BY vector_score DESC
    """

    # 2. Truy vấn BM25 Fulltext Index
    fulltext_cypher = """
        CALL db.index.fulltext.queryNodes('chunk_fulltext_index', $ft_query)
        YIELD node, score
        MATCH (a:Article)-[:HAS_CHUNK]->(node)
        OPTIONAL MATCH (a)-[:IN_CATEGORY]->(cat:Category)
        OPTIONAL MATCH (node)-[:MENTIONS]->(m:Article)
        RETURN 
            node.chunk_id AS chunk_id,
            node.text AS text,
            score AS bm25_score,
            a.title AS article_title,
            a.url AS article_url,
            collect(DISTINCT cat.name) AS categories,
            collect(DISTINCT m.title) AS mentions
        LIMIT $fetch_k
    """

    def tokenize_text(t: str) -> list[str]:
        return re.findall(r'\w+', t.lower())

    def run_hybrid_retrieval(session, query_text: str, q_embedding: list[float], target_k: int):
        fetch_k = max(target_k * 3, 15)
        # Vector results
        v_res = session.run(vector_cypher, {"q_vec": q_embedding, "fetch_k": fetch_k}).data()
        
        # BM25 / Fulltext query construction (Lấy các từ khóa quan trọng)
        query_words = tokenize_text(query_text)
        ft_query_str = " OR ".join([w for w in query_words if len(w) > 1]) if query_words else query_text
        ft_res = []
        try:
            ft_res = session.run(fulltext_cypher, {"ft_query": ft_query_str, "fetch_k": fetch_k}).data()
        except Exception as fte:
            logger.warning(f"Lỗi truy vấn fulltext index (sẽ dùng BM25Okapi python): {fte}")

        # Hợp nhất các ứng viên từ cả Vector Search và Fulltext
        candidate_pool: dict[str, dict] = {}
        for r in v_res:
            cid = r["chunk_id"]
            candidate_pool[cid] = {
                "chunk_id": cid,
                "text": r["text"],
                "article_title": r["article_title"],
                "article_url": r["article_url"],
                "categories": r["categories"],
                "mentions": r["mentions"],
                "vector_score": float(r["vector_score"]),
                "bm25_score": 0.0
            }

        for r in ft_res:
            cid = r["chunk_id"]
            if cid in candidate_pool:
                candidate_pool[cid]["bm25_score"] = float(r["bm25_score"])
            else:
                candidate_pool[cid] = {
                    "chunk_id": cid,
                    "text": r["text"],
                    "article_title": r["article_title"],
                    "article_url": r["article_url"],
                    "categories": r["categories"],
                    "mentions": r["mentions"],
                    "vector_score": 0.5, # Điểm mặc định cho từ khóa khớp nhưng vector chưa chạm
                    "bm25_score": float(r["bm25_score"])
                }

        # Nếu tập ứng viên có sẵn, tính thêm BM25Okapi chính xác trên token tiếng Việt
        cand_list = list(candidate_pool.values())
        if cand_list:
            tokenized_corpus = [tokenize_text(c["text"]) for c in cand_list]
            bm25_calc = BM25Okapi(tokenized_corpus)
            python_bm25_scores = bm25_calc.get_scores(tokenize_text(query_text))
            
            # Chuẩn hóa min-max điểm BM25 về [0, 1]
            max_bm25 = max(python_bm25_scores) if len(python_bm25_scores) > 0 else 1.0
            min_bm25 = min(python_bm25_scores) if len(python_bm25_scores) > 0 else 0.0
            range_bm25 = (max_bm25 - min_bm25) if (max_bm25 - min_bm25) > 1e-6 else 1.0

            for i, c in enumerate(cand_list):
                norm_bm25 = (python_bm25_scores[i] - min_bm25) / range_bm25
                v_score = c["vector_score"]
                # Điểm kết hợp Hybrid: 60% Vector Semantic + 40% BM25 Keyword
                c["hybrid_score"] = round(0.6 * v_score + 0.4 * norm_bm25, 4)
                c["bm25_raw"] = round(float(python_bm25_scores[i]), 2)

            cand_list.sort(key=lambda x: x["hybrid_score"], reverse=True)

        return cand_list[:target_k]

    all_categories = set()
    all_mentions = set()
    all_articles = set()

    try:
        with neo4j_driver.session() as session:
            final_candidates = run_hybrid_retrieval(session, question, q_vec, top_k)
            for r in final_candidates:
                cats = [c for c in r["categories"] if c]
                mentions = [m for m in r["mentions"] if m]
                all_categories.update(cats)
                all_mentions.update(mentions)
                all_articles.add(r["article_title"])

                sources.append({
                    "chunk_id": r["chunk_id"],
                    "article_title": r["article_title"],
                    "article_url": r["article_url"],
                    "score": r["hybrid_score"],
                    "text": r["text"],
                    "related_categories": cats[:3],
                    "mentioned_articles": mentions
                })
                context_snippets.append(f"• [Nguồn: {r['article_title']} | Điểm Hybrid (Vector+BM25): {r['hybrid_score']:.2f}]\n{r['text'].strip()}")

        step2_dur = round((time.perf_counter() - step2_start) * 1000, 2)
        top_score = sources[0]["score"] if sources else 0.0

        # BẢO MẬT & CÁCH LY TUYỆT ĐỐI: 
        # Trên nhánh internal-project-copilot, TẮT HOÀN TOÀN tính năng tự động cào Wikipedia ngầm (Auto On-demand Crawl).
        # Tránh gửi bất kỳ thực thể, câu hỏi hay từ khóa chưa phân loại nào ra ngoài internet.
        # Wikipedia chỉ được nạp có chủ đích qua pipeline offline hoặc admin console.
        pass

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
    RAG_REQUESTS_TOTAL.labels(mode="graph_external").inc()
    notify("pipeline_complete", {
        "question": question,
        "answer": answer,
        "sources": sources,
        "total_duration_ms": total_ms,
        "active_node": "result",
        "active_edge": "gemini_to_result"
    })

    return answer, sources, steps, total_ms


def run_internal_extractive_pipeline(
    question: str,
    top_k: int = 5,
    collect_debug: bool = False,
    progress_callback: Optional[Any] = None,
    target_project: Optional[Tuple[str, str]] = None,
    auth_user: Optional[AuthUser] = None
) -> tuple[str, list[dict], list[DebugStep], float]:
    """
    Extractive RAG thuần túy cho Dữ liệu Nội bộ:
    1. Nhúng câu hỏi cục bộ (all-MiniLM-L6-v2)
    2. Overfetch ứng viên từ Neo4j: Vector Cosine + BM25 Fulltext
    3. Áp dụng điều kiện RBAC ngay trong truy vấn Database (Cypher WHERE)
    4. Trả về đoạn trích nguyên văn và trích dẫn lineage (source_table, source_id, project_code, date)
    """
    t_start = time.perf_counter()
    steps: List[DebugStep] = []
    sources: list[dict] = []

    def notify(event_type: str, data: dict):
        if progress_callback:
            progress_callback(event_type, data)

    target_code = target_project[0] if target_project else None
    target_name = target_project[1] if target_project else None

    # KIỂM TRA PHÂN QUYỀN TRƯỚC TRUY VẤN (Pre-retrieval RBAC Check)
    if not auth_user:
        denied_msg = "⛔ **Truy cập bị từ chối (401 Unauthorized):** Yêu cầu Authorization Bearer token hợp lệ để truy cập nhật trình và ghi chú dự án."
        total_ms = round((time.perf_counter() - t_start) * 1000, 2)
        notify("pipeline_complete", {
            "question": question,
            "answer": denied_msg,
            "sources": [],
            "total_duration_ms": total_ms,
            "active_node": "result",
            "active_edge": "gateway_to_result"
        })
        return denied_msg, [], steps, total_ms

    # Nếu câu hỏi nhắm vào 1 dự án cụ thể mà user không có quyền -> Chặn ngay lập tức
    if target_code and not auth_user.can_access_project(target_code):
        notify("step_start", {
            "step_id": 1,
            "name": "1. Security & RBAC Enforcement",
            "active_node": "gateway",
            "active_edge": "client_to_gateway",
            "details": f"Kiểm tra quyền truy cập dự án {target_code}..."
        })
        notify("step_failed", {
            "step_id": 1,
            "name": "1. Security & RBAC Enforcement",
            "status": "FORBIDDEN",
            "duration_ms": 0.5,
            "error": f"Tài khoản {auth_user.user_id} không có quyền truy cập dự án {target_name} ({target_code})."
        })
        AUTH_REQUESTS_TOTAL.labels(status="forbidden").inc()
        denied_msg = f"⛔ **Truy cập bị từ chối (403 Forbidden):** Tài khoản của bạn (`{auth_user.user_id}`) không được phân quyền xem nhật trình và ghi chú kỹ thuật của dự án **{target_name} ({target_code})**."
        total_ms = round((time.perf_counter() - t_start) * 1000, 2)
        notify("pipeline_complete", {
            "question": question,
            "answer": denied_msg,
            "sources": [],
            "total_duration_ms": total_ms,
            "active_node": "result",
            "active_edge": "gateway_to_result"
        })
        return denied_msg, [], steps, total_ms

    # Step 1: Query Router Notification
    notify("step_start", {
        "step_id": 1,
        "name": "1. Project Internal Extractive Router",
        "active_node": "gateway",
        "active_edge": "client_to_gateway",
        "details": f"Định tuyến sang Extractive RAG Nội bộ (Dự án: {target_name or 'Toàn hệ thống nội bộ'}). Đảm bảo cách ly 100% dữ liệu."
    })
    step1_dur = round((time.perf_counter() - t_start) * 1000, 2)
    notify("step_complete", {
        "step_id": 1,
        "name": "1. Project Internal Extractive Router",
        "status": "SUCCESS",
        "duration_ms": step1_dur,
        "active_node": "gateway",
        "details": "Đã chuyển nhánh Extractive RAG nội bộ. Không sử dụng external LLM.",
        "metrics": [
            {"key": "mode", "value": "Extractive RAG (Zero-leakage, No external LLM)"},
            {"key": "target_project", "value": f"{target_name} ({target_code})" if target_code else "All allowed internal"},
            {"key": "auth_user", "value": auth_user.user_id if auth_user else "anonymous"}
        ]
    })

    # Step 2: Embedding câu hỏi cục bộ
    notify("step_start", {
        "step_id": 2,
        "name": "2. Local Question Embedding",
        "active_node": "gateway",
        "active_edge": "client_to_gateway",
        "details": f"Nhúng câu hỏi nội bộ cục bộ bằng '{settings.EMBEDDING_MODEL}' (384-dims)..."
    })
    emb_start = time.perf_counter()
    q_vec = embedder.encode(question).tolist()
    emb_dur = round((time.perf_counter() - emb_start) * 1000, 2)
    notify("step_complete", {
        "step_id": 2,
        "name": "2. Local Question Embedding",
        "status": "SUCCESS",
        "duration_ms": emb_dur,
        "active_node": "gateway",
        "details": f"Đã sinh vector 384 chiều cục bộ ({emb_dur}ms).",
        "metrics": [{"key": "vector_dimensions", "value": "384"}]
    })

    # Step 3: Hybrid Retrieval trên Neo4j (Vector Cosine + BM25 Fulltext)
    # LỌC QUYỀN HẠN TRỰC TIẾP TRONG CÂU LỆNH CYPHER (PRE-FILTER TẠI DB)
    notify("step_start", {
        "step_id": 3,
        "name": "3. Internal Vector & Fulltext Hybrid Retrieval",
        "active_node": "neo4j",
        "active_edge": "gateway_to_neo4j",
        "details": "Truy xuất ứng viên, ép điều kiện phân quyền RBAC trực tiếp trong Cypher..."
    })
    retrieval_start = time.perf_counter()

    query_words = re.findall(r"\w+", question.lower())
    # Làm sạch từ khóa để tạo chuỗi Lucene an toàn (chỉ chứa ký tự chữ và số)
    sanitized_words = [re.sub(r"[^a-zA-Z0-9_\u00C0-\u1EF9]", "", w) for w in query_words if len(w) > 1]
    ft_query_str = " OR ".join(sanitized_words) if sanitized_words else "du_an"
    # Overfetch lớn trên Neo4j 5.20 để bù đắp việc xếp hạng toàn cục trước khi Cypher lọc
    fetch_k = max(top_k * 15, 150)

    # Chuẩn bị tham số quyền hạn cho Cypher (KHÔNG BAO GIỜ MẶC ĐỊNH LÀ ADMIN)
    is_admin = auth_user.is_admin
    allowed_list = auth_user.allowed_projects if not is_admin else []

    candidate_pool: dict[str, dict] = {}
    try:
        with neo4j_driver.session() as session:
            # Lấy active_version hiện hành từ SystemConfig
            cfg_res = session.run("MATCH (s:SystemConfig {id: 'internal_rag_state'}) RETURN s.active_version AS active_version").single()
            active_version = cfg_res["active_version"] if cfg_res and cfg_res.get("active_version") else None
            if active_version:
                RAG_ACTIVE_VERSION_INFO.labels(version=active_version).set(1)

            # Cypher Vector Query có điều kiện RBAC & Active Version ngay tại DB
            vector_rbac_cypher = """
            CALL db.index.vector.queryNodes('internal_chunk_vector_index', $fetch_k, $q_vec)
            YIELD node, score
            WHERE ($is_admin = true OR node.project_code IN $allowed_list)
              AND ($target_code IS NULL OR node.project_code = $target_code)
              AND ($active_version IS NULL OR node.version = $active_version)
            RETURN node.chunk_id AS chunk_id,
                   node.source_table AS source_table,
                   node.source_id AS source_id,
                   node.project_code AS project_code,
                   node.date AS date,
                   node.text AS text,
                   score AS vector_score
            """
            v_res = session.run(vector_rbac_cypher, {
                "fetch_k": fetch_k,
                "q_vec": q_vec,
                "is_admin": is_admin,
                "allowed_list": allowed_list,
                "target_code": target_code,
                "active_version": active_version
            }).data()

            # Cypher Fulltext Query: Sử dụng chuỗi Lucene đã sanitize trực tiếp để tương thích với Neo4j 5.20 procedure
            fulltext_rbac_cypher = f"""
            CALL db.index.fulltext.queryNodes('internal_chunk_fulltext_index', '{ft_query_str}')
            YIELD node, score
            WHERE ($is_admin = true OR node.project_code IN $allowed_list)
              AND ($target_code IS NULL OR node.project_code = $target_code)
              AND ($active_version IS NULL OR node.version = $active_version)
            RETURN node.chunk_id AS chunk_id,
                   node.source_table AS source_table,
                   node.source_id AS source_id,
                   node.project_code AS project_code,
                   node.date AS date,
                   node.text AS text,
                   score AS bm25_score
            LIMIT $fetch_k
            """
            ft_res = []
            try:
                ft_res = session.run(fulltext_rbac_cypher, {
                    "fetch_k": fetch_k,
                    "is_admin": is_admin,
                    "allowed_list": allowed_list,
                    "target_code": target_code,
                    "active_version": active_version
                }).data()
            except Exception as fte:
                logger.warning(f"Lỗi truy vấn fulltext index: {fte}")

            for r in v_res:
                cid = r["chunk_id"]
                candidate_pool[cid] = {
                    "chunk_id": cid,
                    "source_table": r["source_table"],
                    "source_id": r["source_id"],
                    "project_code": r["project_code"],
                    "date": r["date"],
                    "text": r["text"],
                    "vector_score": float(r["vector_score"]),
                    "bm25_score": 0.0
                }

            for r in ft_res:
                cid = r["chunk_id"]
                if cid in candidate_pool:
                    candidate_pool[cid]["bm25_score"] = float(r["bm25_score"])
                else:
                    candidate_pool[cid] = {
                        "chunk_id": cid,
                        "source_table": r["source_table"],
                        "source_id": r["source_id"],
                        "project_code": r["project_code"],
                        "date": r["date"],
                        "text": r["text"],
                        "vector_score": 0.5,
                        "bm25_score": float(r["bm25_score"])
                    }

        cand_list = list(candidate_pool.values())

        if cand_list:
            tokenized_corpus = [re.findall(r"\w+", c["text"].lower()) for c in cand_list]
            bm25_calc = BM25Okapi(tokenized_corpus)
            python_bm25_scores = bm25_calc.get_scores(query_words)
            max_bm = max(python_bm25_scores) if len(python_bm25_scores) > 0 else 1.0
            min_bm = min(python_bm25_scores) if len(python_bm25_scores) > 0 else 0.0
            rng_bm = (max_bm - min_bm) if (max_bm - min_bm) > 1e-6 else 1.0

            # Nhận diện các ý định nghiệp vụ đặc thù (Business Concept Intent) từ câu hỏi
            q_clean = question.lower()
            is_signing_status_query = any(p in q_clean for p in ["chưa ký", "chưa được ký", "đã ký", "được ký", "đang ký", "ký duyệt", "phê duyệt", "chưa duyệt", "kí duyệt"])

            # Từ khóa dừng (stop words) không mang bằng chứng nghiệp vụ cốt lõi
            stop_terms = {"dự", "án", "nào", "đang", "có", "các", "những", "cho", "của", "với", "được", "trong", "là", "bị", "ở", "tại"}
            # Các từ chung chung, xuất hiện trong hầu hết các công việc/thiết bị công trường
            generic_domain_words = {"tiến", "độ", "phát", "triển", "tính", "năng", "công", "việc", "hạng", "mục", "vật", "tư", "thiết", "bị"}
            key_query_terms = [w for w in query_words if len(w) > 1 and w not in stop_terms]
            # Từ khóa đặc thù (distinctive): không nằm trong stop words và không nằm trong các từ chung chung
            distinctive_query_terms = [w for w in key_query_terms if w not in generic_domain_words]

            filtered_candidates = []
            seen_texts = set()

            for i, c in enumerate(cand_list):
                norm_bm = (python_bm25_scores[i] - min_bm) / rng_bm
                c["norm_bm"] = round(norm_bm, 4)
                v_score = c["vector_score"]
                c["hybrid_score"] = round(0.6 * v_score + 0.4 * norm_bm, 4)
                c["bm25_raw"] = float(python_bm25_scores[i])

                # 1. Khử trùng lặp nội dung (Deduplication) theo chuỗi văn bản chuẩn hóa
                norm_text = re.sub(r"\s+", " ", c["text"].strip().lower())
                if norm_text in seen_texts:
                    continue
                seen_texts.add(norm_text)

                text_lower = c["text"].lower()

                # 2. Bộ lọc bảo thủ MVP: Kiểm tra bằng chứng cụm từ nghiệp vụ (Concept Evidence)
                # Tuyệt đối không dùng token đơn lẻ "ký" (tránh khớp "đăng ký")
                if is_signing_status_query:
                    # Với ý định tra cứu ký duyệt: Bắt buộc trong văn bản phải chứa cụm từ ký/phê duyệt thực sự
                    has_signing_evidence = any(
                        re.search(rf"\b{re.escape(phrase)}\b", text_lower)
                        for phrase in ["đã ký", "chưa ký", "ký duyệt", "phê duyệt", "chưa duyệt", "kí duyệt", "đã kí", "chưa kí"]
                    )
                    if not has_signing_evidence:
                        # Từ chối dứt khoát nếu không có bằng chứng ký duyệt nghiệp vụ
                        continue

                # 3. Với các câu hỏi thông thường:
                # Nếu câu hỏi có từ khóa đặc thù (distinctive), BẮT BUỘC chunk phải chứa ít nhất 1 từ đặc thù đó
                # Tránh trường hợp chỉ khớp các từ chung chung như 'độ', 'phát', 'công' rồi bốc các chunk sai lệch
                if distinctive_query_terms:
                    has_key_term = any(re.search(rf"\b{re.escape(dt)}\b", text_lower) for dt in distinctive_query_terms)
                else:
                    has_key_term = any(re.search(rf"\b{re.escape(kt)}\b", text_lower) for kt in key_query_terms) if key_query_terms else True

                # Ngưỡng vector tuyệt đối tạm thời cho MVP (Cần đánh giá lại khi có tập golden test)
                # Lưu ý: Nếu chunk được khớp bởi BM25 Fulltext (có mặt từ khóa khớp chính xác), vector_score fallback = 0.5
                # nên ta cho phép chunk có vector_score >= 0.65 HOẶC được tìm thấy từ BM25 (v_score == 0.5 và norm_bm >= 0.4)
                is_strong_match = (v_score >= 0.65) or (v_score == 0.5 and norm_bm >= 0.4)
                if is_strong_match and has_key_term:
                    filtered_candidates.append(c)

            filtered_candidates.sort(key=lambda x: x["hybrid_score"], reverse=True)
            for fc in filtered_candidates[:10]:
                logger.info(f"🔎 [cand] {fc['chunk_id'][:30]} | hybrid: {fc['hybrid_score']} | v: {fc['vector_score']} | norm_bm: {fc.get('norm_bm')}")
            final_chunks = filtered_candidates[:top_k]
        else:
            final_chunks = []

        retrieval_dur = round((time.perf_counter() - retrieval_start) * 1000, 2)
        notify("step_complete", {
            "step_id": 3,
            "name": "3. Internal Vector & Fulltext Hybrid Retrieval",
            "status": "SUCCESS",
            "duration_ms": retrieval_dur,
            "active_node": "neo4j",
            "details": f"Tìm thấy {len(final_chunks)} đoạn trích phù hợp trong phạm vi cho phép ({retrieval_dur}ms).",
            "metrics": [
                {"key": "rbac_filter_applied", "value": "true (Enforced in DB query)"},
                {"key": "candidates_evaluated", "value": str(len(cand_list))},
                {"key": "top_matches_selected", "value": str(len(final_chunks))}
            ]
        })
    except Exception as e:
        logger.error(f"Lỗi khi truy xuất Extractive RAG: {e}")
        retrieval_dur = round((time.perf_counter() - retrieval_start) * 1000, 2)
        notify("step_failed", {
            "step_id": 3,
            "name": "3. Internal Vector & Fulltext Hybrid Retrieval",
            "status": "FAILED",
            "duration_ms": retrieval_dur,
            "error": "Lỗi truy vấn dữ liệu nội bộ."
        })
        final_chunks = []

    # Step 4: Trình bày trích dẫn nguyên văn (Extractive Synthesis)
    notify("step_start", {
        "step_id": 4,
        "name": "4. Extractive Lineage & Citation Assembly",
        "active_node": "gateway",
        "active_edge": "neo4j_to_gateway",
        "details": "Đóng gói các đoạn trích nguyên văn và metadata nguồn (Loại trừ created_by bảo vệ quyền riêng tư)..."
    })
    synthesis_start = time.perf_counter()

    if final_chunks:
        lines = []
        if target_name:
            lines.append(f"🔍 **Kết quả trích xuất nhật trình & ghi chú dự án: {target_name} ({target_code})**\n")
        else:
            lines.append("🔍 **Kết quả trích xuất nhật trình & ghi chú kỹ thuật nội bộ:**\n")

        for idx, c in enumerate(final_chunks, 1):
            src_table = c["source_table"]
            src_id = c["source_id"]
            pcode = c["project_code"]
            date_str = f" | Ngày: {c['date']}" if c.get("date") else ""
            table_vn = {
                "field_logs": "Nhật trình hiện trường",
                "tasks": "Nhiệm vụ công việc",
                "material_plans": "Kế hoạch vật tư / Thiết bị"
            }.get(src_table, src_table)

            # Metadata trích dẫn đầy đủ, không lộ created_by
            lines.append(f"### {idx}. Nguồn: `{table_vn}` ({src_table}){date_str}")
            lines.append(f"- **Mã dự án:** `{pcode}` | **Bản ghi gốc (ID):** `{src_id}`")
            lines.append(f"- **Độ khớp (Hybrid Score):** `{c['hybrid_score']:.4f}`")
            lines.append("```text")
            lines.append(c["text"].strip())
            lines.append("```\n")

            sources.append({
                "chunk_id": c["chunk_id"],
                "article_title": f"{table_vn} ({pcode})",
                "article_url": None,
                "score": c["hybrid_score"],
                "text": c["text"],
                "related_categories": [src_table, pcode],
                "mentioned_articles": [],
                "source_table": src_table,
                "source_id": src_id,
                "project_code": pcode,
                "date": c.get("date")
            })

        lines.append("*(Ghi chú an toàn: Toàn bộ nội dung trên được trích xuất nguyên văn trực tiếp từ CSDL nội bộ. Không gửi dữ liệu sang LLM đám mây)*")
        answer_text = "\n".join(lines)
    else:
        scope_str = f"của dự án **{target_name} ({target_code})**" if target_name else "các dự án"
        answer_text = (
            f"⚠️ **Không tìm thấy thông tin phù hợp trong các nguồn hiện được lập chỉ mục:**\n\n"
            f"Trong kho dữ liệu nhật trình, nhiệm vụ công việc và kế hoạch vật tư hiện tại của {scope_str}, "
            f"không tìm thấy tài liệu hoặc ghi chú nào đề cập đến nội dung bạn yêu cầu.\n\n"
            f"*(Hệ thống tuân thủ nguyên tắc Extractive RAG: Chỉ trích xuất từ dữ liệu có thật đã nạp, không tự suy diễn và không tự động tìm kiếm ngoài internet)*"
        )

    synthesis_dur = round((time.perf_counter() - synthesis_start) * 1000, 2)
    notify("step_complete", {
        "step_id": 4,
        "name": "4. Extractive Lineage & Citation Assembly",
        "status": "SUCCESS",
        "duration_ms": synthesis_dur,
        "active_node": "gateway",
        "details": f"Đã đóng gói {len(sources)} trích dẫn nguyên văn ({synthesis_dur}ms).",
        "metrics": [{"key": "created_by_redacted", "value": "true (RBAC Boundary Protected)"}]
    })

    total_ms = round((time.perf_counter() - t_start) * 1000, 2)
    RAG_REQUESTS_TOTAL.labels(mode="extractive_internal").inc()
    RAG_EXTRACTIVE_DURATION_SECONDS.observe((time.perf_counter() - t_start))
    if len(final_chunks) == 0:
        RAG_RETRIEVAL_EMPTY_TOTAL.labels(target_project=target_code or "all").inc()
    notify("pipeline_complete", {
        "question": question,
        "answer": answer_text,
        "sources": sources,
        "total_duration_ms": total_ms,
        "active_node": "result",
        "active_edge": "gateway_to_result"
    })

    return answer_text, sources, steps, total_ms


# ============================================================================
# GraphQL Query Root
# ============================================================================

@strawberry.type
class Query:
    @strawberry.field
    def ask(self, info: strawberry.Info, question: str, top_k: int = 3) -> RAGAnswer:
        """
        Truy vấn GraphRAG chuẩn dành cho người dùng:
        Vector search -> Graph Traversal -> LLM sinh câu trả lời tự nhiên kèm nguồn.
        Bảo mật: Ép phân quyền RBAC dựa trên info.context['auth_user'].
        """
        auth_user = info.context.get("auth_user")
        logger.info(f"🔍 [ask] Nhận câu hỏi: '{question}' (User: {auth_user.user_id if auth_user else 'anonymous'})")
        answer, raw_sources, _, _ = execute_rag_pipeline(question, top_k, collect_debug=False, auth_user=auth_user)
        sources = [
            SourceChunk(
                chunk_id=s["chunk_id"],
                article_title=s["article_title"],
                article_url=s["article_url"],
                score=s["score"],
                text=s["text"],
                related_categories=s["related_categories"],
                mentioned_articles=s["mentioned_articles"],
                source_table=s.get("source_table"),
                source_id=s.get("source_id"),
                project_code=s.get("project_code"),
                date=s.get("date")
            ) for s in raw_sources
        ]
        return RAGAnswer(
            question=question,
            answer=answer,
            sources=sources
        )

    @strawberry.field
    def debug_ask(self, info: strawberry.Info, question: str, top_k: int = 3) -> DebugRAGAnswer:
        """
        Truy vấn GraphRAG kèm Trace từng bước dành cho Developer:
        Có thể bật/tắt qua biến môi trường RAG_DEBUG_TRACE_ENABLED (mặc định: True).
        """
        auth_user = info.context.get("auth_user")
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

        logger.info(f"🛠️ [debugAsk] Nhận câu hỏi debug: '{question}' (User: {auth_user.user_id if auth_user else 'anonymous'})")
        answer, raw_sources, steps, total_ms = execute_rag_pipeline(question, top_k, collect_debug=True, auth_user=auth_user)
        sources = [
            SourceChunk(
                chunk_id=s["chunk_id"],
                article_title=s["article_title"],
                article_url=s["article_url"],
                score=s["score"],
                text=s["text"],
                related_categories=s["related_categories"],
                mentioned_articles=s["mentioned_articles"],
                source_table=s.get("source_table"),
                source_id=s.get("source_id"),
                project_code=s.get("project_code"),
                date=s.get("date")
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

async def get_context(request: Request) -> Dict[str, Any]:
    # Nếu là HTTP GET và accept html (trình duyệt tải GraphQL IDE UI) -> Cho phép tải UI
    accept_header = request.headers.get("accept", "")
    if request.method == "GET" and "text/html" in accept_header:
        return {"request": request, "auth_user": None}

    auth_header = request.headers.get("authorization") or request.headers.get("Authorization")
    logger.info(f"🔑 [get_context] Auth header received: {bool(auth_header)} (len: {len(auth_header) if auth_header else 0})")

    if not auth_header:
        AUTH_REQUESTS_TOTAL.labels(status="missing_token").inc()
        raise HTTPException(
            status_code=401,
            detail="401 Unauthorized: Yêu cầu Authorization Bearer token hợp lệ để truy vấn GraphQL."
        )
    try:
        auth_user = verify_access_token(auth_header)
        AUTH_REQUESTS_TOTAL.labels(status="success").inc()
    except PermissionError as pe:
        logger.warning(f"⛔ [get_context] verify_access_token PermissionError: {pe}")
        AUTH_REQUESTS_TOTAL.labels(status="forbidden").inc()
        raise HTTPException(status_code=401, detail=str(pe))
    except Exception as e:
        logger.warning(f"⚠️ [get_context] verify_access_token Error: {e}")
        AUTH_REQUESTS_TOTAL.labels(status="invalid_token").inc()
        raise HTTPException(status_code=401, detail=f"401 Unauthorized: Token không hợp lệ ({str(e)})")

    return {"request": request, "auth_user": auth_user}

schema = strawberry.Schema(query=Query)
graphql_app = GraphQLRouter(schema, context_getter=get_context)

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
async def stream_rag_debug(
    question: str = FastAPIQuery(..., description="Câu hỏi cần RAG"),
    top_k: int = 3,
    authorization: Optional[str] = Header(None, description="Bearer Token xác thực RBAC bắt buộc")
):
    """
    SSE stream endpoint cho Developer UI, chạy cùng một hàm `execute_rag_pipeline` duy nhất.
    Bảo mật: Bắt buộc Authorization header hợp lệ. Trả 401 ngay lập tức nếu thiếu hoặc sai token.
    """
    if not authorization:
        AUTH_REQUESTS_TOTAL.labels(status="missing_token").inc()
        raise HTTPException(
            status_code=401,
            detail="401 Unauthorized: Yêu cầu Authorization Bearer token hợp lệ để kết nối SSE stream."
        )

    try:
        auth_user = verify_access_token(authorization)
        AUTH_REQUESTS_TOTAL.labels(status="success").inc()
    except PermissionError as pe:
        AUTH_REQUESTS_TOTAL.labels(status="forbidden").inc()
        raise HTTPException(status_code=401, detail=str(pe))
    except Exception as e:
        AUTH_REQUESTS_TOTAL.labels(status="invalid_token").inc()
        raise HTTPException(status_code=401, detail=f"401 Unauthorized: Token không hợp lệ ({str(e)})")

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
                progress_callback=progress_callback,
                auth_user=auth_user
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

@app.get("/metrics")
def metrics_endpoint(
    x_metrics_token: Optional[str] = Header(None, alias="X-Metrics-Token")
):
    """
    Prometheus Scrape Endpoint:
    Được bảo vệ bằng Secret Header X-Metrics-Token (riêng biệt, không tái sử dụng JWT).
    Trả HTTP 403 Forbidden nếu thiếu hoặc sai token.
    Đồng bộ thông số ingestion từ SystemConfig Neo4j trước khi xuất metrics.
    Cập nhật rag_ingestion_metrics_source_up (1=OK, 0=Fail) và reset label version cũ.
    """
    configured_token = settings.METRICS_SCRAPE_TOKEN
    if configured_token:
        if not x_metrics_token or x_metrics_token.strip() != configured_token:
            raise HTTPException(
                status_code=403,
                detail="403 Forbidden: Yêu cầu X-Metrics-Token hợp lệ để scrape metrics nội bộ."
            )

    try:
        with neo4j_driver.session() as session:
            cfg = session.run("""
                MATCH (s:SystemConfig {id: 'internal_rag_state'})
                RETURN s.active_version AS active_version,
                       coalesce(s.total_lock_takeovers, 0) AS total_lock_takeovers,
                       coalesce(s.total_rollbacks, 0) AS total_rollbacks,
                       coalesce(s.last_ingestion_duration_sec, 0.0) AS last_ingestion_duration_sec,
                       coalesce(s.last_embedding_duration_sec, 0.0) AS last_embedding_duration_sec,
                       coalesce(s.last_smoke_duration_sec, 0.0) AS last_smoke_duration_sec,
                       coalesce(s.total_chunks, 0) AS total_chunks
            """).single()
            if cfg:
                active_ver = cfg.get("active_version")
                if active_ver:
                    RAG_ACTIVE_VERSION_INFO.clear()
                    RAG_ACTIVE_VERSION_INFO.labels(version=active_ver).set(1)
                INGESTION_LOCK_TAKEOVERS_COUNT.set(cfg["total_lock_takeovers"])
                INGESTION_ROLLBACKS_COUNT.set(cfg["total_rollbacks"])
                INGESTION_LAST_DURATION_SECONDS.set(cfg["last_ingestion_duration_sec"])
                INGESTION_LAST_EMBED_DURATION_SECONDS.set(cfg["last_embedding_duration_sec"])
                INGESTION_LAST_SMOKE_DURATION_SECONDS.set(cfg["last_smoke_duration_sec"])
                INGESTION_LAST_TOTAL_CHUNKS.set(cfg["total_chunks"])
                RAG_INGESTION_METRICS_SOURCE_UP.set(1)
            else:
                RAG_INGESTION_METRICS_SOURCE_UP.set(0)

            # Đồng bộ metric chất lượng dữ liệu: số node Project thiếu project_type
            dq_res = session.run("""
                MATCH (p:Project)
                WHERE p.project_type IS NULL OR trim(p.project_type) = ''
                RETURN count(p) AS unclassified_cnt
            """).single()
            if dq_res:
                DATA_QUALITY_UNCLASSIFIED_PROJECTS.set(dq_res["unclassified_cnt"])
    except Exception as e:
        logger.warning(f"Không thể sync ingestion metrics từ Neo4j: {e}")
        RAG_INGESTION_METRICS_SOURCE_UP.set(0)

    return Response(content=generate_latest(), media_type=CONTENT_TYPE_LATEST)

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
