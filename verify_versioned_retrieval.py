import os
import requests
from dotenv import load_dotenv

load_dotenv()
import sys
import os
import requests
from pathlib import Path
from dotenv import load_dotenv

if sys.platform == "win32":
    sys.stdout.reconfigure(encoding="utf-8")

load_dotenv()
ROOT_DIR = Path(__file__).resolve().parent
sys.path.append(str(ROOT_DIR / "services" / "rag-graphql-gateway"))

from auth_rbac import create_access_token

PROJ_PHUOC_TAN = "TRAM_BIEN_AP_110KV_PHUOC_TAN"
PROJ_BAC_NINH = "TRAM_BIEN_AP_500KV_BAC_NINH"

token_alice = create_access_token("alice", "engineer", [PROJ_PHUOC_TAN])
token_bob = create_access_token("bob", "engineer", [PROJ_BAC_NINH])
token_admin = create_access_token("admin_user", "admin", [])

query = """
query AskProject($q: String!) {
  ask(question: $q, topK: 3) {
    answer
    sources {
      chunkId
      sourceTable
      sourceId
      projectCode
      date
    }
  }
}
"""

url = "http://localhost:8000/graphql"

print(f"--- TEST 1: Alice (Allowed: {PROJ_PHUOC_TAN}) ---")
res_a = requests.post(
    url,
    json={"query": query, "variables": {"q": f"Nhật trình hiện trường và vật tư thiết bị dự án {PROJ_PHUOC_TAN}"}},
    headers={"Authorization": f"Bearer {token_alice}"}
)
print("Alice status:", res_a.status_code)
sources_a = res_a.json().get("data", {}).get("ask", {}).get("sources", [])
print(f"Alice retrieved {len(sources_a)} sources:")
for s in sources_a:
    print(f"  - Chunk: {s['chunkId']} | Table: {s['sourceTable']} | Proj: {s['projectCode']} | Date: {s['date']}")

print(f"\n--- TEST 2: Bob (Forbidden project: asks about {PROJ_PHUOC_TAN} while allowed {PROJ_BAC_NINH}) ---")
res_b = requests.post(
    url,
    json={"query": query, "variables": {"q": f"Nhật trình hiện trường và vật tư thiết bị dự án {PROJ_PHUOC_TAN}"}},
    headers={"Authorization": f"Bearer {token_bob}"}
)
print("Bob status:", res_b.status_code)
ans_b = res_b.json().get("data", {}).get("ask", {}).get("answer", "")
print("Bob answer preview:\n", ans_b[:180])

print("\n--- TEST 3: Admin (All projects) ---")
res_adm = requests.post(
    url,
    json={"query": query, "variables": {"q": f"Nhật trình thi công và sự cố hiện trường dự án {PROJ_PHUOC_TAN}"}},
    headers={"Authorization": f"Bearer {token_admin}"}
)
print("Admin status:", res_adm.status_code)
sources_adm = res_adm.json().get("data", {}).get("ask", {}).get("sources", [])
print(f"Admin retrieved {len(sources_adm)} sources:")
for s in sources_adm:
    print(f"  - Chunk: {s['chunkId']} | Table: {s['sourceTable']} | Proj: {s['projectCode']}")

