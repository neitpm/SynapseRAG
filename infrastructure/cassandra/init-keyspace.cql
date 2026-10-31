-- ==============================================================================
-- Cassandra Keyspace & Table Definition for Wikipedia Raw Data Lake
-- ==============================================================================

-- 1. Tạo Keyspace (Database)
CREATE KEYSPACE IF NOT EXISTS wiki_knowledge
WITH replication = {
    'class': 'SimpleStrategy',
    'replication_factor': 1
};

-- 2. Tạo Table lưu trữ bài viết thô (Query-Driven Modeling)
-- Partition Key: (page_id) giúp phân tán dữ liệu đều giữa các nodes
-- Clustering Key: (crawled_at DESC) để lưu lại các phiên bản cập nhật của bài viết theo thời gian
CREATE TABLE IF NOT EXISTS wiki_knowledge.raw_articles (
    page_id int,
    crawled_at timestamp,
    schema_version text,
    title text,
    url text,
    content text,
    summary text,
    categories set<text>,
    metadata map<text, text>,
    PRIMARY KEY ((page_id), crawled_at)
) WITH CLUSTERING ORDER BY (crawled_at DESC);
