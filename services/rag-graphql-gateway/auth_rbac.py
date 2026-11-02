import os
import time
import logging
from typing import Optional, List, Dict, Any
import jwt

logger = logging.getLogger("auth-rbac")

JWT_SECRET = os.getenv("JWT_SECRET", "synapse_rag_super_secret_jwt_key_2026")
JWT_ALGORITHM = "HS256"
JWT_ISSUER = "synapse-rag-gateway"
JWT_AUDIENCE = "synapse-rag-clients"

class AuthUser:
    def __init__(self, user_id: str, role: str, allowed_projects: List[str]):
        self.user_id = user_id
        self.role = role # "admin" | "engineer" | "viewer"
        self.allowed_projects = allowed_projects

    @property
    def is_admin(self) -> bool:
        return self.role.lower() == "admin" or "*" in self.allowed_projects

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

def verify_access_token(token: str) -> AuthUser:
    """
    Xác thực token bằng PyJWT:
    - Bắt buộc thuật toán HS256 (chống algorithm confusion/none attack)
    - Kiểm tra chữ ký, thời hạn exp, iss, aud
    - Từ chối mặc định nếu token thiếu hoặc không hợp lệ
    """
    if not token:
        raise ValueError("Thiếu Authorization token.")

    if token.lower().startswith("bearer "):
        token = token[7:].strip()

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
        raise ValueError("Token đã hết hạn truy cập (Expired).")
    except jwt.InvalidAlgorithmError:
        raise ValueError("Thuật toán mã hóa token không được chấp nhận.")
    except jwt.PyJWTError as e:
        raise ValueError(f"Xác thực token thất bại: {str(e)}")

    user_id = payload.get("sub", "")
    role = payload.get("role", "viewer")
    allowed_projects = payload.get("allowed_projects", [])
    if not isinstance(allowed_projects, list):
        allowed_projects = []

    return AuthUser(user_id=user_id, role=role, allowed_projects=allowed_projects)
