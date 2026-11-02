import os
import time
import logging
from typing import Optional, List, Dict, Any
import jwt

logger = logging.getLogger("auth-rbac")

# BẮT BUỘC CẤU HÌNH JWT_SECRET KHI KHỞI ĐỘNG (Fail-fast, cấm dùng default hardcoded)
JWT_SECRET = os.getenv("JWT_SECRET", "").strip()
if not JWT_SECRET or len(JWT_SECRET) < 32:
    raise RuntimeError(
        "LỖI KHỞI ĐỘNG BẢO MẬT: Biến môi trường JWT_SECRET chưa được cấu hình hoặc quá ngắn (tối thiểu 32 ký tự). "
        "Hệ thống từ chối khởi động với secret mặc định."
    )

JWT_ALGORITHM = "HS256"
JWT_ISSUER = "synapse-rag-gateway"
JWT_AUDIENCE = "synapse-rag-clients"

class AuthUser:
    def __init__(self, user_id: str, role: str, allowed_projects: List[str]):
        self.user_id = user_id
        self.role = role.lower() # "admin" | "engineer" | "viewer"
        self.allowed_projects = allowed_projects

    @property
    def is_admin(self) -> bool:
        return self.role == "admin" or "*" in self.allowed_projects

    def can_access_project(self, project_code: str) -> bool:
        if self.is_admin:
            return True
        if not project_code:
            return False
        return project_code in self.allowed_projects

def create_access_token(
    user_id: str,
    role: str,
    allowed_projects: List[str],
    expires_in_seconds: int = 3600
) -> str:
    """
    Sinh JWT token chuẩn bằng PyJWT với đầy đủ claim exp, iat, iss, aud
    """
    now = int(time.time())
    payload = {
        "sub": user_id,
        "role": role,
        "allowed_projects": allowed_projects,
        "iat": now,
        "exp": now + expires_in_seconds,
        "iss": JWT_ISSUER,
        "aud": JWT_AUDIENCE
    }
    return jwt.encode(payload, JWT_SECRET, algorithm=JWT_ALGORITHM)

def verify_access_token(token_header: Optional[str]) -> AuthUser:
    """
    Xác thực token nghiêm ngặt (Zero Trust):
    - Trả lỗi ngay nếu thiếu token (None hoặc rỗng)
    - Trả lỗi ngay nếu token không bắt đầu bằng 'Bearer '
    - Bắt buộc thuật toán HS256 (từ chối none/RS256/khác)
    - Kiểm tra chữ ký, thời hạn exp, iss, aud
    - Kiểm tra đầy đủ các claim bắt buộc
    """
    if not token_header or not token_header.strip():
        raise PermissionError("401 Unauthorized: Yêu cầu Authorization Bearer token hợp lệ để truy cập hệ thống.")

    raw = token_header.strip()
    if not raw.lower().startswith("bearer "):
        raise PermissionError("401 Unauthorized: Định dạng token không hợp lệ (Phải có tiền tố 'Bearer ').")

    token = raw[7:].strip()
    if not token:
        raise PermissionError("401 Unauthorized: Chuỗi Bearer token bị rỗng.")

    try:
        payload = jwt.decode(
            token,
            JWT_SECRET,
            algorithms=[JWT_ALGORITHM],
            issuer=JWT_ISSUER,
            audience=JWT_AUDIENCE,
            options={
                "require": ["exp", "iat", "iss", "aud", "sub", "role", "allowed_projects"]
            }
        )
    except jwt.ExpiredSignatureError:
        raise PermissionError("401 Unauthorized: Token đã hết hạn truy cập (Token expired).")
    except jwt.InvalidAlgorithmError:
        raise PermissionError("401 Unauthorized: Thuật toán mã hóa token không được chấp nhận.")
    except jwt.PyJWTError as e:
        raise PermissionError(f"401 Unauthorized: Chữ ký token hoặc cấu trúc không hợp lệ ({str(e)}).")

    user_id = str(payload.get("sub", "")).strip()
    if not user_id:
        raise PermissionError("401 Unauthorized: Token thiếu định danh người dùng (sub).")

    role = str(payload.get("role", "viewer")).strip().lower()
    allowed_projects = payload.get("allowed_projects", [])
    if not isinstance(allowed_projects, list):
        allowed_projects = []

    return AuthUser(user_id=user_id, role=role, allowed_projects=allowed_projects)
