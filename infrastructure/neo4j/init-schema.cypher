# Đảm bảo tính duy nhất của bài viết theo URL hoặc pageid
CREATE CONSTRAINT article_url_unique IF NOT EXISTS
FOR (a:Article) REQUIRE a.url IS UNIQUE;

# Đảm bảo tính duy nhất của Chunk ID
CREATE CONSTRAINT chunk_id_unique IF NOT EXISTS
FOR (c:Chunk) REQUIRE c.id IS UNIQUE;

# Vector Index cho các Chunk để tìm kiếm tương đồng ngữ nghĩa (Cosine Similarity)
# Mặc định dimension=384 (ví dụ mô hình all-MiniLM-L6-v2)
CREATE VECTOR INDEX chunk_vector_index IF NOT EXISTS
FOR (c:Chunk) ON (c.embedding)
OPTIONS {
  indexConfig: {
    `vector.dimensions`: 384,
    `vector.similarity_function`: 'cosine'
  }
};
