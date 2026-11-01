import logging
import re
import urllib.request
import urllib.parse
import json
import wikipediaapi
from langchain_text_splitters import RecursiveCharacterTextSplitter
from neo4j import Driver
from shared.config import settings

logger = logging.getLogger("wiki-on-demand")

wiki = wikipediaapi.Wikipedia(
    user_agent="SynapseRAG-Bot/1.0 (contact@synapserag.local)",
    language="vi",
    extract_format=wikipediaapi.ExtractFormat.WIKI
)

text_splitter = RecursiveCharacterTextSplitter(
    chunk_size=500,
    chunk_overlap=50,
    separators=["\n\n", "\n", ". ", " ", ""]
)

def extract_keyword_candidate(question: str) -> str:
    """
    Trích xuất từ khóa chủ đề tiềm năng từ câu hỏi tiếng Việt.
    Nếu hỏi về chức vụ ('ai là [chức vụ] [quốc gia]'): bổ sung cả dạng 'Danh sách [chức vụ] [quốc gia]'.
    """
    cleaned = re.sub(
        r'(?i)\b(cho tôi biết|bạn có biết|hãy giải thích|giải thích|tìm hiểu về|nói về|định nghĩa|ai đang là|ai là|ai|là ai|là gì|ở đâu|như thế nào|tại sao|ứng dụng ra sao|về|của|có những gì|ra sao|thế nào)\b', 
        ' ', 
        question
    )
    cleaned = re.sub(r'[?.,!\"\'\(\)]', ' ', cleaned)
    topic = " ".join(cleaned.split()).strip()

    is_who = bool(re.search(r'(?i)\b(ai là|ai đang là|là ai|ai)\b', question))
    if is_who and topic and not topic.lower().startswith("danh sách"):
        # Với câu hỏi hỏi người đảm nhiệm chức vụ, ưu tiên tìm bài Danh sách
        search_target = f"Danh sách {topic}"
        best_title = search_wikipedia_title(search_target)
        if best_title:
            return best_title

    return topic

def search_wikipedia_title(query: str) -> str | None:
    """
    Tìm kiếm tiêu đề bài viết chính xác nhất trên Wikipedia tiếng Việt:
    1. Thử dùng Wikipedia Search API (list=search) - hỗ trợ full-text và chuyển hướng ngữ nghĩa.
    2. Fallback sang OpenSearch API.
    """
    if not query or len(query) < 2:
        return None

    # Cách 1: Full-text Search API
    try:
        url = f"https://vi.wikipedia.org/w/api.php?action=query&list=search&srsearch={urllib.parse.quote(query)}&srlimit=5&format=json"
        req = urllib.request.Request(url, headers={"User-Agent": "SynapseRAG-Bot/1.0 (contact@synapserag.local)"})
        with urllib.request.urlopen(req, timeout=6) as resp:
            data = json.loads(resp.read().decode("utf-8"))
            search_items = data.get("query", {}).get("search", [])
            if search_items:
                best_title = search_items[0]["title"]
                logger.info(f"🔎 Wikipedia Search API tìm thấy: '{best_title}' cho query: '{query}'")
                return best_title
    except Exception as e:
        logger.warning(f"Lỗi khi gọi Wikipedia Search API: {e}")

    # Cách 2: OpenSearch API
    try:
        url = f"https://vi.wikipedia.org/w/api.php?action=opensearch&search={urllib.parse.quote(query)}&limit=3&namespace=0&format=json"
        req = urllib.request.Request(url, headers={"User-Agent": "SynapseRAG-Bot/1.0 (contact@synapserag.local)"})
        with urllib.request.urlopen(req, timeout=5) as resp:
            data = json.loads(resp.read().decode("utf-8"))
            if data and len(data) > 1 and len(data[1]) > 0:
                return data[1][0]
    except Exception as e:
        logger.warning(f"Lỗi khi tìm kiếm Wikipedia OpenSearch API: {e}")

    return None

def fetch_and_store_wiki_on_demand(topic: str, neo4j_driver: Driver, embedder) -> tuple[bool, str, int]:
    """
    Tự động cào bài viết mới từ Wikipedia và nhúng thẳng vào Neo4j (On-demand Ingestion)
    Trả về: (thành công hay không, tiêu đề bài viết, số lượng chunk đã nạp)
    """
    if not topic or len(topic) < 2:
        return False, "", 0

    target_title = search_wikipedia_title(topic) or topic
    logger.info(f"🌐 [ON-DEMAND WIKI] Bắt đầu lấy bài viết '{target_title}' trên Wikipedia...")

    page = wiki.page(target_title)
    if not page.exists():
        page = wiki.page(target_title.capitalize())
        if not page.exists():
            logger.warning(f"❌ Bài viết '{target_title}' không tồn tại trên Wikipedia!")
            return False, "", 0

    content = page.text
    if not content or len(content) < 50:
        return False, "", 0

    page_id = str(page.pageid)
    title = page.title
    url = page.fullurl
    categories = [
        cat.replace("Category:", "").replace("Thể loại:", "").strip() 
        for cat in list(page.categories.keys())[:8]
        if not any(k in cat.lower() for k in ["bài viết", "bản mẫu", "cs1:", "liên kết wayback", "hatnote"])
    ]

    # Phân mảnh văn bản (Chunking)
    chunks = text_splitter.split_text(content)
    if not chunks:
        return False, "", 0

    selected_chunks = chunks[:50] # Lấy tối đa 50 chunks bao quát đầy đủ bài viết
    embeddings = embedder.encode(selected_chunks, batch_size=16, show_progress_bar=False)

    chunk_records = []
    for i, (text, emb) in enumerate(zip(selected_chunks, embeddings)):
        chunk_records.append({
            "chunk_id": f"{page_id}_{i}",
            "index": i,
            "text": text,
            "embedding": emb.tolist() if hasattr(emb, "tolist") else list(emb)
        })

    # Lưu đồng thời Article, Categories và Chunks vào Neo4j
    with neo4j_driver.session() as session:
        session.run("""
            MERGE (a:Article {page_id: $page_id})
            SET a.title = $title,
                a.url = $url,
                a.chunk_count = $chunk_count,
                a.updated_at = datetime()
        """, {
            "page_id": page_id,
            "title": title,
            "url": url,
            "chunk_count": len(selected_chunks)
        })

        if categories:
            session.run("""
                MATCH (a:Article {page_id: $page_id})
                UNWIND $categories AS cat_name
                MERGE (cat:Category {name: cat_name})
                MERGE (a)-[:IN_CATEGORY]->(cat)
            """, {
                "page_id": page_id,
                "categories": categories
            })

        session.run("""
            UNWIND $chunks AS item
            MATCH (a:Article {page_id: $page_id})
            MERGE (c:Chunk {chunk_id: item.chunk_id})
            SET c.text = item.text,
                c.index = item.index,
                c.embedding = item.embedding
            MERGE (a)-[:HAS_CHUNK]->(c)
        """, {
            "page_id": page_id,
            "chunks": chunk_records
        })

    logger.info(f"✅ [ON-DEMAND WIKI] Đã nạp thành công bài '{title}' ({len(selected_chunks)} chunks) vào Neo4j!")
    return True, title, len(selected_chunks)
