# SynapseRAG: Event-Driven GraphRAG Microservices Platform

[![Architecture: Microservices](https://img.shields.io/badge/Architecture-Microservices-blue.svg)](#-kiến-trúc-luồng-dữ-liệu-data-pipeline-architecture)
[![Broker: Apache Kafka / Redpanda](https://img.shields.io/badge/Broker-Kafka%20%2F%20Redpanda-red.svg)](https://redpanda.com/)
[![Storage: Apache Cassandra](https://img.shields.io/badge/Raw%20Store-Apache%20Cassandra-1f8cb2.svg)](https://cassandra.apache.org/)
[![Graph & Vector: Neo4j](https://img.shields.io/badge/Graph%20%26%20Vector-Neo4j-008cc1.svg)](https://neo4j.com/)
[![API: GraphQL](https://img.shields.io/badge/API-GraphQL-e535ab.svg)](https://graphql.org/)

<p align="center">
  <img src="assets/synapse-rag-architecture-3d.jpg" alt="SynapseRAG 3D Architecture Diagram" width="100%" style="border-radius: 12px; box-shadow: 0 8px 24px rgba(0,0,0,0.5);" />
</p>

**SynapseRAG** là hệ thống kiến trúc Microservices phân tán kết hợp **Event-Driven Streaming (Kafka / Redpanda)**, **Kho lưu trữ NoSQL thô (Apache Cassandra)**, **Đồ thị tri thức & Vector (Neo4j GraphRAG)** và **Cổng truy vấn linh hoạt (GraphQL API)**.

---

## 🏗️ Kiến trúc luồng dữ liệu (Data Pipeline Architecture)

Hệ thống áp dụng mô hình **Fan-out Architecture** qua Kafka topic `wiki-raw-articles`. Hai Worker chạy độc lập bằng 2 Consumer Groups riêng biệt, giải quyết triệt để bài toán Dual-write / Distributed Transactions:

```
                  ┌───────────────────────────────┐
                  │ 1. wiki-producer              │  Cào / Fetch bài viết từ Wikipedia
                  └───────────────┬───────────────┘
                                  │  Topic: `wiki-raw-articles` (Key = page_id)
                                  ▼
                   ┌─────────────────────────────┐
                   │    Apache Kafka / Redpanda  │
                   └──────────────┬──────────────┘
                                  │
         ┌────────────────────────┴────────────────────────┐
         │ (Consumer Group: `cassandra-sink-group`)        │ (Consumer Group: `ai-ingestion-group`)
         ▼                                                 ▼
┌───────────────────────────────┐                 ┌───────────────────────────────┐
│ 2. cassandra-sink-worker      │                 │ 3. ai-ingestion-worker        │
│ Lưu trữ bất biến bài viết thô │                 │ Clean text, Chunking,         │
│ phục vụ Data Lake & Replay    │                 │ Sinh Vector Embedding         │
└───────────────┬───────────────┘                 └───────────────┬───────────────┘
                ▼                                                 ▼
┌───────────────────────────────┐                 ┌───────────────────────────────┐
│ Apache Cassandra              │                 │ Neo4j (Graph + Vector Index)  │
│ Keyspace: `wiki_knowledge`    │                 │ Lưu Node (:Article), (:Chunk) │
│ Table: `raw_articles`         │                 │ Mối quan hệ [:MENTIONS],...   │
└───────────────────────────────┘                 └───────────────┬───────────────┘
                                                                  │
                                                                  ▼
                                                  ┌───────────────────────────────┐
                                                  │ 4. rag-graphql-gateway        │
                                                  │ Cung cấp GraphQL Endpoint     │
                                                  │ Hybrid Search & RAG Answer    │
                                                  └───────────────┬───────────────┘
                                                                  │
                                                                  ▼
                                                      [ Client / UI Dashboard ]
```

---

## 🚀 Hướng dẫn vận hành nhanh

### 1. Khởi động hạ tầng Broker & Databases
Khởi động cụm Redpanda, Neo4j 5.20 và Cassandra qua Docker:
```powershell
wsl -d Ubuntu bash -c "cd /mnt/c/Projects/Microsevice && docker compose up -d"
```
* **Redpanda Console:** [http://localhost:8080](http://localhost:8080)
* **Neo4j Browser:** [http://localhost:7474](http://localhost:7474) (User: `neo4j` / Pass: `graphrag_password123`)
* **Cassandra CQL:** `localhost:9042`

### 2. Chạy Service 1 (`wiki-producer`)
Chạy producer để nạp thêm bài viết Wikipedia vào Kafka:
```powershell
python services/wiki-producer/main.py "Trí tuệ nhân tạo"
```

### 3. Chạy Workers lưu trữ
Mở terminal riêng cho từng worker:
```powershell
# Worker 1: Đổ vào Cassandra Raw Data Lake
python services/cassandra-sink-worker/main.py

# Worker 2: Chunking, Embedding và nạp GraphRAG vào Neo4j
python services/ai-ingestion-worker/main.py
```

### 4. Khởi chạy Cổng truy vấn `rag-graphql-gateway`
```powershell
python services/rag-graphql-gateway/main.py
```
* **GraphQL Playground:** [http://localhost:8000/graphql](http://localhost:8000/graphql)
* **Real-time SSE Stream Endpoint:** `GET /api/debug/ask/stream?question=...&top_k=3`

### 5. Khởi chạy Giao diện Developer 3D Explorer (`frontend`)
```powershell
cd frontend
npm run dev
```
Truy cập giao diện tại: **[http://localhost:5173](http://localhost:5173)**. Giao diện cung cấp sơ đồ mạng 3D thời gian thực, timeline trace từng bước (Embedding -> Neo4j Vector/Graph -> Gemini LLM) và hiển thị trích dẫn nguồn Wikipedia.

---

## 🔍 Hướng dẫn truy vấn GraphQL

### 1. Query chuẩn dành cho người dùng (`ask`)
Dùng để hỏi đáp thông thường, trả về câu trả lời trích xuất cùng danh sách nguồn liên quan:

```graphql
query {
  ask(question: "Python là gì và dùng để làm gì?", topK: 2) {
    question
    answer
    sources {
      articleTitle
      articleUrl
      score
      relatedCategories
      mentionedArticles
      text
    }
  }
}
```

### 2. Query dành cho Developer (`debugAsk` trace từng bước)
Cung cấp trace chi tiết từng giai đoạn xử lý (thời gian chạy `ms`, số liệu embedding, vector search, mở rộng đồ thị category/mentions, chiến lược tổng hợp câu trả lời):

#### Bật/Tắt chế độ Debug:
Cấu hình trong file `.env`:
```env
# Bật trace (môi trường Development - Mặc định)
RAG_DEBUG_TRACE_ENABLED=true

# Tắt trace (môi trường Production)
RAG_DEBUG_TRACE_ENABLED=false
```

#### Cú pháp truy vấn Developer:
```graphql
query {
  debugAsk(question: "Trí tuệ nhân tạo là gì?", topK: 2) {
    question
    answer
    totalDurationMs
    steps {
      name
      status
      durationMs
      details
      metrics {
        key
        value
      }
    }
    sources {
      articleTitle
      score
      relatedCategories
      mentionedArticles
    }
  }
}
```

#### Ví dụ phản hồi của `debugAsk`:
```json
{
  "data": {
    "debugAsk": {
      "question": "Trí tuệ nhân tạo là gì?",
      "totalDurationMs": 57.06,
      "steps": [
        {
          "name": "1. Question Embedding",
          "status": "SUCCESS",
          "durationMs": 25.51,
          "details": "Sinh vector nhúng với mô hình cục bộ 'all-MiniLM-L6-v2'.",
          "metrics": [
            { "key": "embedding_model", "value": "all-MiniLM-L6-v2" },
            { "key": "vector_dimensions", "value": "384" },
            { "key": "vector_preview", "value": "[-0.0567, -0.0403, ...]" }
          ]
        },
        {
          "name": "2. Vector Search & Graph Traversal",
          "status": "SUCCESS",
          "durationMs": 31.45,
          "details": "Tìm kiếm vector top_k trên Neo4j index và mở rộng đồ thị qua quan hệ HAS_CHUNK, IN_CATEGORY, MENTIONS.",
          "metrics": [
            { "key": "top_k_requested", "value": "2" },
            { "key": "chunks_found", "value": "2" },
            { "key": "articles_matched", "value": "Trí tuệ nhân tạo" },
            { "key": "graph_categories_expanded", "value": "3" },
            { "key": "graph_mentions_expanded", "value": "1" },
            { "key": "top_match_score", "value": "0.8623" }
          ]
        },
        {
          "name": "3. Answer Synthesis",
          "status": "SUCCESS",
          "durationMs": 0.01,
          "details": "Extractive Synthesis (Trích xuất đoạn văn bản có điểm vector cao nhất kèm context quan hệ đồ thị; chưa dùng LLM generative).",
          "metrics": [
            { "key": "synthesis_strategy", "value": "Extractive from Top Chunk + Graph Metadata" },
            { "key": "generative_llm_used", "value": "false" },
            { "key": "context_snippets_count", "value": "2" },
            { "key": "primary_source_article", "value": "Trí tuệ nhân tạo" }
          ]
        }
      ],
      "sources": [
        {
          "articleTitle": "Trí tuệ nhân tạo",
          "score": 0.8623,
          "relatedCategories": ["Trí tuệ nhân tạo", "Học máy"],
          "mentionedArticles": []
        }
      ]
    }
  }
}
```


---

## 🛡️ Quy trình Tiền xử lý Dữ liệu Nội bộ (Enterprise Data Preprocessing & Sanitization)

Trên nhánh phát triển **`feat/internal-project-copilot`**, hệ thống hỗ trợ nạp và phân tích dữ liệu quản trị dự án xây lắp (trạm biến áp, đường dây, nhân sự, vật tư, chi phí). Để bảo vệ dữ liệu và đảm bảo độ chính xác của Graph Analytics, quy trình tiền xử lý được chuẩn hóa qua các bước nghiêm ngặt:

```
[ Raw Supabase Datasets ] (datasets/)
           │
           ▼
┌────────────────────────────────────────────────────────┐
│  Bước 1: Streaming RFC 4180 Multiline Parser           │  profile_and_sanitize.mjs
│  - Hỗ trợ ô chứa ký tự xuống dòng (\n) trong ngoặc kép │
│  - Cách ly hàng lỗi lệch cột (_malformed_records/)     │
└──────────┬─────────────────────────────────────────────┘
           │
           ▼
┌────────────────────────────────────────────────────────┐
│  Bước 2: Data Sanitization (Khử dữ liệu nhạy cảm)      │  datasets_sanitized/
│  - Xóa bỏ mật khẩu: `engineers.password`               │
│  - Xóa bỏ liên hệ cá nhân bên ngoài: phone, address    │
│  - Cách ly 100% bảng tin nhắn riêng: `direct_messages` │
│  - Lọc bỏ 4 bảng rỗng (0 rows): issues, payrolls,...   │
└──────────┬─────────────────────────────────────────────┘
           │
           ▼
┌────────────────────────────────────────────────────────┐
│  Bước 3: Type Casting & Orphan FK Preservation         │  ingest_knowledge_graph.py
│  - Ép kiểu số: `total_amount`, `progress` sang số thực │
│  - Giữ lại ID khóa ngoại mồ côi (không đoán mò)        │
│  - Lọc bỏ `is_section_header = true` khỏi tiến độ      │
└──────────┬─────────────────────────────────────────────┘
           │
           ▼
┌────────────────────────────────────────────────────────┐
│  Bước 4: Nạp Enterprise Knowledge Graph vào Neo4j      │
│  - Tạo 7 nhãn Node: Project, Task, MaterialPlan,       │
│    PurchasingPlan, Expense, Material, FieldLog, Eng.   │
│  - Thiết lập 11 loại liên kết quan hệ nghiệp vụ        │
└──────────┬─────────────────────────────────────────────┘
           │
           ▼
┌────────────────────────────────────────────────────────┐
│  Bước 5: Parameterized Cypher Query Router             │  project_router.py
│  - Định tuyến câu hỏi thống kê tiến độ, chi phí        │
│  - Tuyệt đối không để LLM tự sinh Cypher tùy ý         │
│  - Tắt Wikipedia crawl đối với thực thể nội bộ         │
└────────────────────────────────────────────────────────┘
```

### 1. Chuẩn hóa phân tích cú pháp (RFC 4180 CSV Parsing)
- **Vấn đề thực tế:** Các trường ghi chú (`notes`), mô tả công việc (`job_content`), nhật trình thi công (`field_logs`) chứa ký tự xuống dòng (`\n`) nằm bên trong ô. Việc đọc từng dòng vật lý thông thường (`readline`) sẽ chẻ 1 bản ghi thành nhiều dòng rác làm sai lệch thống kê.
- **Giải pháp:** Sử dụng thư viện `csv-parse` theo chuẩn RFC 4180 với cơ chế streaming (`profile_and_sanitize.mjs`):
  - Giữ nguyên vẹn 1.000 Tasks, 2.788 Kế hoạch vật tư, 1.146 Đơn mua sắm, 928 Phiếu chi.
  - Tỷ lệ khớp khóa ngoại `tasks.project_code` -> `projects.code` đạt **100.0%**.

### 2. Khử dữ liệu nhạy cảm & Bảo vệ riêng tư (Sanitization)
Toàn bộ dữ liệu sạch được lưu độc lập tại thư mục `datasets_sanitized/` (đã đưa vào `.gitignore`):
- **Bảo vệ danh tính:** Loại bỏ hoàn toàn `password`, `phone`, `email` trong `engineers.csv`.
- **Cách ly tin nhắn:** Loại bỏ hoàn toàn bảng `direct_messages` khỏi pipeline nạp đồ thị công khai.
- **Báo cáo an toàn:** File tổng kết `_data_profiling_and_cleaning_report.json` không ghi log bất kỳ chuỗi văn bản mẫu nào chứa thông tin riêng tư.

### 3. Xử lý khóa ngoại mồ côi (Orphan Foreign Keys)
- Dữ liệu thực tế ghi nhận 8 task trỏ tới 2 ID kỹ sư cũ và 4 đơn mua sắm trỏ tới 4 ID kế hoạch vật tư cũ.
- **Nguyên tắc nạp:** Giữ nguyên vẹn giá trị ID gốc trên thuộc tính của Node để phục vụ kiểm toán (audit trail), **không tự suy đoán hay thay thế bằng kỹ sư khác**, chỉ tạo quan hệ `[:ASSIGNED_TO]` khi Node mục tiêu thực sự tồn tại.

### 4. Quy tắc tính toán & Bộ định tuyến truy vấn (Query Router)
- **Tiến độ dự án:** Vì cột `projects.progress_percent` gốc là `0`, hệ thống tính tiến độ trung bình thực tế từ các task con:
  $$\text{Tiến độ trung bình} = \frac{1}{N} \sum_{i=1}^{N} (\text{task.progress}_i \times 100) \quad \text{với } \text{is\_section\_header} = \text{false}$$
- **Chi phí hạch toán:** Sử dụng trường `total_amount` (thay vì `amount`) để tổng hợp số tiền thực chi trên 928 phiếu chi (`sum(e.total_amount)` = 399.209.368 VNĐ).
- **Bảo mật mô hình:** Tắt toàn bộ luồng cào Wikipedia tự động đối với các thực thể dự án nội bộ để ngăn chặn rò rỉ tên công trình ra ngoài internet.
