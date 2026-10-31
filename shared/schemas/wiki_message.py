from typing import Optional, List, Dict, Any
from pydantic import BaseModel, Field
from datetime import datetime, timezone

class WikiArticleMessage(BaseModel):
    """
    Contract schema cho message đẩy vào Kafka topic `wiki-raw-articles`.
    Bao gồm phiên bản schema để dễ dàng nâng cấp (versioning) trong tương lai.
    """
    schema_version: str = Field(default="1.0", description="Phiên bản contract của message")
    page_id: int = Field(..., description="ID bài viết trên Wikipedia")
    title: str = Field(..., description="Tiêu đề bài viết")
    url: str = Field(..., description="Đường dẫn bài viết")
    content: str = Field(..., description="Toàn bộ nội dung thô (raw text) của bài viết")
    summary: Optional[str] = Field(None, description="Tóm tắt ngắn (nếu có)")
    categories: List[str] = Field(default_factory=list, description="Danh mục/Chủ đề")
    timestamp: str = Field(default_factory=lambda: datetime.now(timezone.utc).isoformat())
    metadata: Dict[str, Any] = Field(default_factory=dict, description="Thông tin mở rộng")
