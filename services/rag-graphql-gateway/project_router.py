import logging
import re
import time
from typing import Optional, Dict, Any, Tuple
from neo4j import Driver

logger = logging.getLogger("project-query-router")

def match_project_in_question(question: str, driver: Driver) -> Optional[Tuple[str, str]]:
    """
    Tìm kiếm project_code hoặc tên dự án xuất hiện trong câu hỏi dựa trên CSDL Neo4j
    """
    try:
        with driver.session() as session:
            result = session.run("MATCH (p:Project) RETURN p.code AS code, p.name AS name")
            projects = list(result)
    except Exception as e:
        logger.warning(f"Lỗi truy vấn danh sách dự án từ Neo4j: {e}")
        return None
        
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
            # Lọc bỏ tiền tố loại công trình như 'trạm biến áp', 'tba', 'đường dây', '110kv', '220kv', '500kv'
            cleaned_core = re.sub(r"(trạm biến áp|tba|đường dây|\d+kv)\s*", "", name_lower).strip()
            if len(cleaned_core) >= 4 and cleaned_core in q_lower:
                return code, name
    return None

def execute_structured_query(question: str, driver: Driver) -> Optional[Dict[str, Any]]:
    """
    Xác định ý định và thực thi Parameterized Cypher định nghĩa sẵn.
    Đo thời gian thực tế của câu truy vấn Cypher và trả về `cypher_dur_ms`.
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
            c_start = time.perf_counter()
            with driver.session() as session:
                rec = session.run(cypher, code=p_code).single()
            c_dur = round((time.perf_counter() - c_start) * 1000, 2)

            if rec and rec["total_work_tasks"] > 0:
                avg_p = round(rec["avg_progress_percent"] or 0.0, 1)
                return {
                    "intent": "PROJECT_PROGRESS",
                    "project_code": p_code,
                    "project_name": rec["project_name"],
                    "cypher_duration_ms": c_dur,
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
    # INTENT 2: Thống kê chi phí / tạm ứng dự án
    # Chỉ trả kết quả khi có phạm vi rõ ràng:
    # - Có tên dự án cụ thể -> Trả chi phí dự án đó
    # - Có từ khóa rõ ràng chỉ toàn công ty ("toàn công ty", "toàn bộ", "tất cả dự án", "công ty") -> Trả chi phí công ty
    # - Nếu hỏi mơ hồ ("chi phí là bao nhiêu", "hết bao nhiêu tiền") -> Yêu cầu chỉ rõ phạm vi
    # -------------------------------------------------------------------------
    if any(k in q_lower for k in ["chi phí", "tổng chi", "tạm ứng", "kinh phí", "tiêu bao nhiêu"]):
        proj_match = match_project_in_question(question, driver)

        if proj_match:
            p_code, p_name = proj_match
            cypher = """
            MATCH (p:Project {code: $code})-[:INCURRED_EXPENSE]->(e:Expense)
            RETURN p.name AS project_name,
                   count(e) AS expense_count,
                   sum(coalesce(e.total_amount, 0.0)) AS total_expenses_vnd
            """
            c_start = time.perf_counter()
            with driver.session() as session:
                rec = session.run(cypher, code=p_code).single()
            c_dur = round((time.perf_counter() - c_start) * 1000, 2)

            if rec and rec["expense_count"] > 0:
                total_vnd = f"{int(rec['total_expenses_vnd']):,}".replace(",", ".")
                return {
                    "intent": "PROJECT_EXPENSES",
                    "project_code": p_code,
                    "project_name": p_name,
                    "cypher_duration_ms": c_dur,
                    "raw_data": dict(rec),
                    "formatted_answer": (
                        f"💰 **Thống kê chi phí dự án: {p_name} ({p_code})**\n\n"
                        f"- **Tổng chi phí đã ghi nhận:** `{total_vnd} VNĐ`\n"
                        f"- **Tổng số phiếu/khoản chi:** {rec['expense_count']} giao dịch\n"
                        f"- Nguồn dữ liệu: Bảng chi phí thực tế (`total_amount`) đã được hạch toán."
                    )
                }
            else:
                return {
                    "intent": "PROJECT_EXPENSES",
                    "project_code": p_code,
                    "project_name": p_name,
                    "cypher_duration_ms": c_dur,
                    "formatted_answer": f"💰 Dự án **{p_name} ({p_code})** hiện chưa ghi nhận khoản chi phí hoặc phiếu chi nào trong hệ thống."
                }

        # Nếu câu hỏi nói rõ phạm vi toàn công ty
        is_explicit_company = any(k in q_lower for k in ["toàn công ty", "toàn bộ", "tất cả dự án", "cả công ty", "kho tổng"])
        if is_explicit_company:
            cypher = """
            MATCH (e:Expense)
            RETURN count(e) AS expense_count,
                   sum(coalesce(e.total_amount, 0.0)) AS total_expenses_vnd
            """
            c_start = time.perf_counter()
            with driver.session() as session:
                rec = session.run(cypher).single()
            c_dur = round((time.perf_counter() - c_start) * 1000, 2)

            if rec:
                total_vnd = f"{int(rec['total_expenses_vnd']):,}".replace(",", ".")
                return {
                    "intent": "ALL_EXPENSES",
                    "cypher_duration_ms": c_dur,
                    "formatted_answer": (
                        f"💰 **Tổng chi phí toàn công ty:** `{total_vnd} VNĐ` "
                        f"(trên tổng số {rec['expense_count']} khoản chi phí đã ghi nhận)."
                    )
                }

        # Câu hỏi hỏi chi phí nhưng mơ hồ không rõ phạm vi
        return {
            "intent": "AMBIGUOUS_EXPENSE_QUERY",
            "cypher_duration_ms": 0.0,
            "formatted_answer": (
                "❓ **Yêu cầu làm rõ phạm vi chi phí:**\n\n"
                "Bạn đang muốn tra cứu chi phí của **dự án cụ thể nào** (ví dụ: *'Chi phí dự án Phước Đông 6'*, *'Chi phí dự án Lục Yên'*) "
                "hay muốn xem **tổng chi phí toàn công ty** (*'Tổng chi phí toàn công ty là bao nhiêu'*)? Vui lòng cung cấp tên dự án để hệ thống hạch toán chính xác."
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
            c_start = time.perf_counter()
            with driver.session() as session:
                rec = session.run(cypher, code=p_code).single()
            c_dur = round((time.perf_counter() - c_start) * 1000, 2)

            if rec:
                mgr = rec["manager_name"] or "(Chưa cập nhật)"
                engs = [e for e in rec["assigned_engineers"] if e]
                eng_str = ", ".join(engs) if engs else "Chưa có kỹ sư gán cụ thể"
                return {
                    "intent": "PROJECT_MANAGERS",
                    "cypher_duration_ms": c_dur,
                    "formatted_answer": (
                        f"👷 **Nhân sự phụ trách dự án: {rec['project_name']} ({p_code})**\n\n"
                        f"- **Người quản lý dự án (Project Manager):** **{mgr}**\n"
                        f"- **Các kỹ sư tham gia thực hiện nhiệm vụ:** {eng_str}"
                    )
                }

    # -------------------------------------------------------------------------
    # INTENT 4: Danh mục vật tư / Đơn hàng mua sắm
    # Phân biệt rành mạch giữa 'Chưa đặt hàng' và thiếu dữ liệu (NULL / trống)
    # -------------------------------------------------------------------------
    if any(k in q_lower for k in ["vật tư chưa đặt", "chưa đặt hàng", "vật tư chưa có"]):
        proj_match = match_project_in_question(question, driver)
        if proj_match:
            p_code, p_name = proj_match
            cypher = """
            MATCH (p:Project {code: $code})-[:HAS_PURCHASE_PLAN]->(pp:PurchasingPlan)
            RETURN p.name AS project_name,
                   sum(CASE WHEN trim(coalesce(pp.order_status, '')) = 'Chưa đặt hàng' THEN 1 ELSE 0 END) AS pending_orders_count,
                   sum(CASE WHEN trim(coalesce(pp.order_status, '')) = '' THEN 1 ELSE 0 END) AS unstated_status_count,
                   collect(CASE WHEN trim(coalesce(pp.order_status, '')) = 'Chưa đặt hàng' THEN pp.content ELSE null END)[0..5] AS sample_pending_items
            """
            c_start = time.perf_counter()
            with driver.session() as session:
                rec = session.run(cypher, code=p_code).single()
            c_dur = round((time.perf_counter() - c_start) * 1000, 2)

            if rec:
                pending_cnt = rec["pending_orders_count"] or 0
                unstated_cnt = rec["unstated_status_count"] or 0
                sample_pending = [it for it in (rec["sample_pending_items"] or []) if it]
                
                lines = [f"📦 **Vật tư thuộc dự án: {rec['project_name']} ({p_code})**\n"]
                lines.append(f"- **Số hạng mục xác nhận 'Chưa đặt hàng':** {pending_cnt} mục")
                if sample_pending:
                    lines.append("- **Một số hạng mục chưa đặt tiêu biểu:**")
                    for it in sample_pending:
                        lines.append(f"  - {it}")
                if unstated_cnt > 0:
                    lines.append(f"\n*(Lưu ý: Có thêm {unstated_cnt} hạng mục chưa cập nhật trạng thái đặt hàng trong hệ thống)*")

                return {
                    "intent": "PENDING_PURCHASE_ORDERS",
                    "cypher_duration_ms": c_dur,
                    "formatted_answer": "\n".join(lines)
                }

    if any(k in q_lower for k in ["tổng số vật tư", "kế hoạch vật tư", "danh mục vật tư"]):
        proj_match = match_project_in_question(question, driver)
        if proj_match:
            p_code, p_name = proj_match
            cypher = """
            MATCH (p:Project {code: $code})-[:REQUIRES_MATERIAL]->(mp:MaterialPlan)
            RETURN p.name AS project_name,
                   count(mp) AS plan_count
            """
            c_start = time.perf_counter()
            with driver.session() as session:
                rec = session.run(cypher, code=p_code).single()
            c_dur = round((time.perf_counter() - c_start) * 1000, 2)

            if rec and rec["plan_count"] > 0:
                return {
                    "intent": "PROJECT_MATERIALS_SUMMARY",
                    "cypher_duration_ms": c_dur,
                    "formatted_answer": (
                        f"📦 **Kế hoạch vật tư dự án: {rec['project_name']} ({p_code})**\n\n"
                        f"- **Tổng số danh mục trong Kế hoạch vật tư:** {rec['plan_count']} đầu mục.\n"
                        f"*(Để tra cứu chi tiết tiến độ đặt hàng, vui lòng hỏi: 'Vật tư nào chưa đặt hàng thuộc dự án {p_name}')*"
                    )
                }

    return None
