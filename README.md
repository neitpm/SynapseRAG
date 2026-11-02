# SynapseRAG: Enterprise Project Copilot & Knowledge Graph Platform

[![Architecture: Microservices](https://img.shields.io/badge/Architecture-Microservices-blue.svg)](#-kiến-trúc-hệ-thống-enterprise-project-copilot)
[![Graph & Vector: Neo4j](https://img.shields.io/badge/Graph%20%26%20Vector-Neo4j-008cc1.svg)](https://neo4j.com/)
[![API: FastAPI & SSE](https://img.shields.io/badge/API-FastAPI%20%26%20SSE-009688.svg)](https://fastapi.tiangolo.com/)
[![Database: Supabase](https://img.shields.io/badge/Database-Supabase%20PostgreSQL-3ecf8e.svg)](https://supabase.com/)

> **Nhánh phát triển:** `feat/internal-project-copilot`  
> Nhánh này chuyên biệt hóa toàn bộ hệ thống phục vụ bài toán **Quản trị điều hành dự án cơ điện / trạm biến áp 110kV–220kV** dựa trên dữ liệu nội bộ doanh nghiệp.

---

## 🏗️ Kiến trúc Hệ thống (Enterprise Project Copilot)

Hệ thống xử lý song song hai luồng: **Truy vấn cấu trúc tất định (Deterministic Structured Cypher)** và **Đồ thị tri thức (Knowledge Graph)**:

```
┌────────────────────────────────────────────────────────┐
│              Client / Developer 3D UI                  │
└──────────────────────────┬─────────────────────────────┘
                           │ (SSE Stream / GraphQL)
                           ▼
┌────────────────────────────────────────────────────────┐
│           Enterprise Gateway & Query Router            │
│  - Phân loại ý định (Intent Classification)            │
│  - Parameterized Cypher Templates (Chống Injection)   │
│  - Cách ly 100% dữ liệu nội bộ với bên ngoài          │
└──────────┬───────────────────────────────────┬─────────┘
           │ (Cypher Aggregation)              │ (Node Traversal)
           ▼                                   ▼
┌────────────────────────────────────────────────────────┐
│               Neo4j 5.20 Knowledge Graph               │
│  - Project (24)       - Task (1,000)                   │
│  - MaterialPlan (2,788) - PurchasingPlan (1,146)       │
│  - Expense (928)      - Material (94)                  │
│  - Engineer (19)      - FieldLog (17)                  │
│  - Relationships: [:HAS_TASK], [:INCURRED_EXPENSE],... │
└────────────────────────────────────────────────────────┘
```

---

## 🛡️ Quy trình Tiền xử lý & Nạp Dữ liệu (Data Preprocessing & Ingestion)

Dữ liệu gốc xuất từ Supabase được xử lý qua pipeline nghiêm ngặt trước khi nạp vào Neo4j:

```
[ Raw Datasets ] (datasets/ - Đã ignore)
        │
        ▼
┌────────────────────────────────────────────────────────┐
│  Bước 1: Streaming RFC 4180 Multiline Parser           │  profile_and_sanitize.mjs
│  - Hỗ trợ ô chứa ký tự xuống dòng (\n) trong ngoặc kép │
│  - Bảo toàn 1.000 tasks, 2.788 vật tư, 928 phiếu chi   │
│  - Cách ly bản ghi lệch cột (_malformed_records/)      │
└───────┬────────────────────────────────────────────────┘
        │
        ▼
┌────────────────────────────────────────────────────────┐
│  Bước 2: Data Sanitization (Khử dữ liệu nhạy cảm)      │  datasets_sanitized/
│  - Xóa bỏ mật khẩu: `engineers.password`               │
│  - Xóa bỏ liên hệ cá nhân bên ngoài: phone, address    │
│  - Cách ly 100% bảng tin nhắn riêng: `direct_messages` │
│  - Lọc bỏ 4 bảng rỗng (0 rows): issues, payrolls,...   │
└───────┬────────────────────────────────────────────────┘
        │
        ▼
┌────────────────────────────────────────────────────────┐
│  Bước 3: Ingestion & Orphan FK Preservation            │  ingest_knowledge_graph.py
│  - Nạp 7 nhãn Node và 11 loại Relationship             │
│  - Bảo toàn ID khóa ngoại mồ côi (không đoán mò)       │
│  - Ép kiểu số: `total_amount`, `progress`              │
└───────┬────────────────────────────────────────────────┘
        │
        ▼
┌────────────────────────────────────────────────────────┐
│  Bước 4: Parameterized Cypher Query Router             │  project_router.py
│  - Tiến độ thực tế: Loại trừ `is_section_header = true`│
│  - Chi phí hạch toán: Sử dụng `sum(total_amount)`      │
│  - Đo độ trễ thực tế qua `time.perf_counter()`         │
│  - Yêu cầu làm rõ phạm vi khi câu hỏi mơ hồ            │
└────────────────────────────────────────────────────────┘
```

---

## 🚀 Hướng dẫn vận hành nhanh

### 1. Khởi động hệ thống (Docker Compose)
Chạy toàn bộ dịch vụ backend (Neo4j, Gateway, Frontend 3D):
```bash
wsl -d Ubuntu bash -c "cd /mnt/c/Projects/Microsevice && docker compose up -d"
```
* **Frontend Dashboard & 3D Visualizer:** [http://localhost:5173](http://localhost:5173)
* **Neo4j Browser:** [http://localhost:7474](http://localhost:7474) (`neo4j` / `graphrag_password123`)
* **Gateway API & Swagger:** [http://localhost:8000/docs](http://localhost:8000/docs)

### 2. Tiền xử lý dữ liệu & Nạp đồ thị
Khi có dữ liệu mới từ Supabase:
```bash
# 1. Khảo sát và làm sạch dữ liệu nhạy cảm (RFC 4180 Parser)
node profile_and_sanitize.mjs

# 2. Nạp cấu trúc Knowledge Graph vào Neo4j
python ingest_knowledge_graph.py
```

---

## 💡 Mẫu câu hỏi hệ thống hỗ trợ (Query Templates)

1. **Tiến độ dự án (Đo bằng trung bình task thực tế, loại bỏ header):**
   - *"Tiến độ dự án Phước Đông 6 hiện tại là bao nhiêu?"*
   - *"Tiến độ trạm biến áp Lục Yên đạt bao nhiêu %?"*
2. **Chi phí & Tạm ứng (Hạch toán theo `total_amount`):**
   - *"Chi phí của dự án Phước Đông 6 là bao nhiêu?"*
   - *"Tổng chi phí toàn công ty là bao nhiêu?"*
   - *(Nếu hỏi "Chi phí là bao nhiêu" -> Hệ thống sẽ yêu cầu người dùng chỉ rõ tên dự án)*
3. **Kế hoạch vật tư & Đơn mua sắm:**
   - *"Vật tư nào chưa đặt hàng thuộc dự án Lục Yên?"*
   - *"Tổng số vật tư của dự án Phước Đông 6?"*
4. **Nhân sự & Người quản lý:**
   - *"Ai đang quản lý dự án Lục Yên?"*
   - *"Ai phụ trách trạm biến áp Phước Đông 6?"*
