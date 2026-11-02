import logging
import re
from typing import Optional, Dict, Any, Tuple
from neo4j import Driver

logger = logging.getLogger("project-query-router")

# Danh mục các mẫu câu hỏi định nghĩa sẵn (Whitelisted Templates)
# Đảm bảo an toàn 100%, không cho phép LLM tự sinh Cypher tùy ý

def match_project_in_question(question: str, driver: Driver) -> Optional[Tuple[str, str]]:
    """
    Tìm kiếm project_code hoặc tên dự án xuất hiện trong câu hỏi dựa trên CSDL Neo4j
    """
    with driver.session() as session:
        result = session.run("MATCH (p:Project) RETURN p.code AS code, p.name AS name")
        projects = list(result)
        
    q_lower = question.lower()
    for row in projects:
        code = row["code"]
        name = row["name"]
        if code and code.lower() in q_lower:
            return code, name
        if name:
            name_lower = name.lower()
            if name_lower in q_lower:
                return code, name
            # Lọc bỏ các từ tiền tố loại công trình như 'trạm biến áp', 'tba', 'đường dây', '110kv', '220kv', '500kv'
            cleaned_core = re.sub(r"(trạm biến áp|tba|đường dây|\d+kv)\s*", "", name_lower).strip()
            if len(cleaned_core) >= 4 and cleaned_core in q_lower:
                return code, name
    return None

def match_engineer_in_question(question: str, driver: Driver) -> Optional[Tuple[str, str]]:
    """
    Tìm tên kỹ sư xuất hiện trong câu hỏi
    """
    with driver.session() as session:
        result = session.run("MATCH (e:Engineer) RETURN e.id AS id, e.name AS name")
        engineers = list(result)
        
    q_lower = question.lower()
    for row in engineers:
        eng_id = row["id"]
        eng_name = row["name"]
        if eng_name and eng_name.lower() in q_lower:
            return eng_id, eng_name
    return None

def execute_structured_query(question: str, driver: Driver) -> Optional[Dict[str, Any]]:
    """
    Xác định ý định và thực thi Cypher định nghĩa sẵn có tham số (Parameterized Cypher).
    Trả về Dict kết quả nếu khớp câu hỏi cấu trúc, ngược lại trả về None.
    """
    q_lower = question.lower()

    # -------------------------------------------------------------------------
    # INTENT 1: Tiến độ trung bình các task (loại trừ section header, quy đổi 0-1 sang %)
    # -------------------------------------------------------------------------
    if any(k in q_lower for k in ["tiến độ", "hoàn thành bao nhiêu", "%", "tiến độ trung bình"]):
        proj_match = match_project_in_question(question, driver)
        if proj_match:
            p_code, p_name = proj_match
            cypher = """
            MATCH (p:Project {code: $code})-[:HAS_TASK]->(t:Task)
            WHERE coalesce(t.is_section_header, false) = false
            RETURN p.name AS project_name,
                   p.code AS project_code,
                   count(t) AS total_work_tasks,
                   sum(CASE WHEN t.is_done = true OR t.status = 'Đã hoàn thành' THEN 1 ELSE 0 END) AS completed_tasks,
                   sum(CASE WHEN t.status = 'Đang làm' THEN 1 ELSE 0 END) AS in_progress_tasks,
                   sum(CASE WHEN t.status = 'Chưa làm' THEN 1 ELSE 0 END) AS pending_tasks,
                   avg(coalesce(t.progress, 0.0)) * 100 AS avg_progress_percent
            """
            with driver.session() as session:
                rec = session.run(cypher, code=p_code).single()
                if rec and rec["total_work_tasks"] > 0:
                    avg_p = round(rec["avg_progress_percent"] or 0.0, 1)
                    return {
                        "intent": "PROJECT_PROGRESS",
                        "project_code": p_code,
                        "project_name": rec["project_name"],
                        "raw_data": dict(rec),
                        "formatted_answer": (
                            f"📊 **Báo cáo tiến độ dự án: {rec['project_name']} ({p_code})**\n\n"
                            f"- **Tiến độ trung bình các task (loại trừ tiêu đề mục):** `{avg_p}%`\n"
                            f"- **Tổng số hạng mục công việc thực tế:** {rec['total_work_tasks']}\n"
                            f"- **Đã hoàn thành:** {rec['completed_tasks']} hạng mục\n"
                            f"- **Đang thi công:** {rec['in_progress_tasks']} hạng mục\n"
                            f"- **Chưa thực hiện:** {rec['pending_tasks']} hạng mục\n\n"
                            f"*(Lưu ý: Chỉ số tiến độ được tính bằng trung bình cộng tỷ lệ hoàn thành của các task thành phần không phải tiêu đề mục)*"
                        )
                    }

    # -------------------------------------------------------------------------
    # INTENT 2: Thống kê chi phí / tạm ứng dự án (sử dụng total_amount)
    # -------------------------------------------------------------------------
    if any(k in q_lower for k in ["chi phí", "tổng chi", "tạm ứng", "tiền ăn", "kinh phí", "tiêu bao nhiêu"]):
        proj_match = match_project_in_question(question, driver)
        p_code = proj_match[0] if proj_match else None
        p_name = proj_match[1] if proj_match else "Toàn công ty"

        if p_code:
            cypher = """
            MATCH (p:Project {code: $code})-[:INCURRED_EXPENSE]->(e:Expense)
            RETURN p.name AS project_name,
                   count(e) AS expense_count,
                   sum(coalesce(e.total_amount, 0.0)) AS total_expenses_vnd,
                   collect({content: e.content, amount: e.total_amount, spender: e.spender_name, date: e.date})[0..5] AS sample_expenses
            """
            with driver.session() as session:
                rec = session.run(cypher, code=p_code).single()
                if rec and rec["expense_count"] > 0:
                    total_vnd = f"{int(rec['total_expenses_vnd']):,}".replace(",", ".")
                    return {
                        "intent": "PROJECT_EXPENSES",
                        "project_code": p_code,
                        "project_name": p_name,
                        "raw_data": dict(rec),
                        "formatted_answer": (
                            f"💰 **Thống kê chi phí dự án: {p_name} ({p_code})**\n\n"
                            f"- **Tổng chi phí đã ghi nhận:** `{total_vnd} VNĐ`\n"
                            f"- **Tổng số phiếu/khoản chi:** {rec['expense_count']} giao dịch\n"
                            f"- Nguồn dữ liệu: Bảng chi phí thực tế (`total_amount`) đã được hạch toán."
                        )
                    }
        else:
            # Tổng chi phí toàn công ty
            cypher = """
            MATCH (e:Expense)
            RETURN count(e) AS expense_count,
                   sum(coalesce(e.total_amount, 0.0)) AS total_expenses_vnd
            """
            with driver.session() as session:
                rec = session.run(cypher).single()
                if rec:
                    total_vnd = f"{int(rec['total_expenses_vnd']):,}".replace(",", ".")
                    return {
                        "intent": "ALL_EXPENSES",
                        "formatted_answer": (
                            f"💰 **Tổng chi phí toàn công ty:** `{total_vnd} VNĐ` "
                            f"(trên tổng số {rec['expense_count']} khoản chi phí đã ghi nhận)."
                        )
                    }

    # -------------------------------------------------------------------------
    # INTENT 3: Người quản lý / Kỹ sư phụ trách dự án
    # -------------------------------------------------------------------------
    if any(k in q_lower for k in ["ai quản lý", "ai phụ trách", "chỉ huy trưởng", "quản lý dự án", "kỹ sư phụ trách"]):
        proj_match = match_project_in_question(question, driver)
        if proj_match:
            p_code, p_name = proj_match
            cypher = """
            MATCH (p:Project {code: $code})
            OPTIONAL MATCH (mgr:Engineer)-[:MANAGES]->(p)
            OPTIONAL MATCH (eng:Engineer)-[:ASSIGNED_TO]->(t:Task)<-[:HAS_TASK]-(p)
            RETURN p.name AS project_name,
                   p.manager_name AS manager_name,
                   mgr.title AS manager_title,
                   collect(DISTINCT eng.name) AS assigned_engineers
            """
            with driver.session() as session:
                rec = session.run(cypher, code=p_code).single()
                if rec:
                    mgr = rec["manager_name"] or "(Chưa cập nhật)"
                    engs = [e for e in rec["assigned_engineers"] if e]
                    eng_str = ", ".join(engs) if engs else "Chưa có kỹ sư gán cụ thể"
                    return {
                        "intent": "PROJECT_MANAGERS",
                        "formatted_answer": (
                            f"👷 **Nhân sự phụ trách dự án: {rec['project_name']} ({p_code})**\n\n"
                            f"- **Người quản lý dự án (Project Manager):** **{mgr}**\n"
                            f"- **Các kỹ sư tham gia thực hiện nhiệm vụ:** {eng_str}"
                        )
                    }

    # -------------------------------------------------------------------------
    # INTENT 4: Danh mục vật tư / tồn kho dự án
    # -------------------------------------------------------------------------
    if any(k in q_lower for k in ["vật tư", "tồn kho", "thiết bị", "kế hoạch vật tư"]):
        proj_match = match_project_in_question(question, driver)
        if proj_match:
            p_code, p_name = proj_match
            cypher = """
            MATCH (p:Project {code: $code})
            OPTIONAL MATCH (p)-[:REQUIRES_MATERIAL]->(mp:MaterialPlan)
            OPTIONAL MATCH (p)-[:STOCKS_MATERIAL]->(m:Material)
            RETURN p.name AS project_name,
                   count(DISTINCT mp) AS plan_count,
                   count(DISTINCT m) AS stock_material_count,
                   collect(DISTINCT m.name)[0..5] AS sample_materials
            """
            with driver.session() as session:
                rec = session.run(cypher, code=p_code).single()
                if rec and (rec["plan_count"] > 0 or rec["stock_material_count"] > 0):
                    sample_str = ", ".join(rec["sample_materials"]) if rec["sample_materials"] else ""
                    stock_info = f"\n- **Số mã vật tư trong kho dự án:** {rec['stock_material_count']} (Ví dụ: {sample_str})" if rec["stock_material_count"] > 0 else ""
                    return {
                        "intent": "PROJECT_MATERIALS",
                        "formatted_answer": (
                            f"📦 **Tình hình vật tư dự án: {rec['project_name']} ({p_code})**\n\n"
                            f"- **Tổng số danh mục trong Kế hoạch vật tư:** {rec['plan_count']} đầu mục{stock_info}"
                        )
                    }

    return None
