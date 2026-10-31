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

## 📁 Cấu trúc thư mục Monorepo

```text
C:\Projects\Microsevice\
├── .env.example                     # Biến môi trường mẫu chung
├── .env                             # Biến môi trường hiện tại
├── .gitignore
├── docker-compose.yml               # Quản lý hạ tầng container (Redpanda Kafka, UI, DBs)
├── README.md                        # Tài liệu hướng dẫn kiến trúc & vận hành
│
├── shared/                          # Module dùng chung (Data Contracts & Config tập trung)
│   ├── __init__.py
│   ├── config.py                    # Nạp biến môi trường tập trung
│   └── schemas/
│       ├── __init__.py
│       └── wiki_message.py          # Pydantic schema contract (có schema_version, page_id,...)
│
├── services/                        # Các Microservices độc lập
│   │
│   ├── wiki-producer/               # Service 1: Thu thập Wikipedia -> Push vào Kafka [✅ ĐÃ HOÀN THÀNH]
│   │   ├── config.py
│   │   ├── requirements.txt
│   │   └── main.py                  # Entrypoint nạp bài viết và đẩy Kafka
│   │
│   ├── cassandra-sink-worker/       # Service 2: Consume Kafka -> Ghi vào Cassandra Raw Data Lake
│   │   ├── config.py
│   │   ├── requirements.txt
│   │   └── main.py
│   │
│   ├── ai-ingestion-worker/         # Service 3: Consume Kafka -> Chunking -> Embedding -> Neo4j
│   │   ├── config.py
│   │   ├── requirements.txt
│   │   └── main.py
│   │
│   └── rag-graphql-gateway/         # Service 4: GraphQL API (FastAPI + Strawberry) + LLM RAG Pipeline
│       ├── config.py
│       ├── requirements.txt
│       └── main.py
│
└── infrastructure/                  # Scripts khởi tạo database và hạ tầng
    ├── neo4j/
    │   ├── init-schema.cypher       # Script tạo Vector Index & Unique Constraints
    │   └── apply_schema.py          # Python runner khởi tạo schema Neo4j
    └── cassandra/
        └── init-keyspace.cql        # CQL script tạo Keyspace & Table với Partition Key chuẩn
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
Truy cập GraphQL Playground tại: **[http://localhost:8000/graphql](http://localhost:8000/graphql)**

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
