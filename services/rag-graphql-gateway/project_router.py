import logging
import re
import time
from typing import Optional, Dict, Any, Tuple, List
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
                avg_pct = rec["avg_progress_percent"] or 0.0
                return {
                    "intent": "PROJECT_TASK_PROGRESS",
                    "cypher_duration_ms": c_dur,
                    "formatted_answer": (
                        f"📊 **Báo cáo tiến độ dự án: {rec['project_name']} ({p_code})**\n\n"
                        f"- **Tiến độ trung bình các task:** {avg_pct:.1f}%\n"
                        f"- **Tổng số nhiệm vụ chuyên môn (loại trừ tiêu đề mục):** {rec['total_work_tasks']} tasks\n"
                        f"  + Hoàn thành: {rec['completed_tasks']}\n"
                        f"  + Đang thực hiện: {rec['in_progress_tasks']}\n"
                        f"  + Chưa thực hiện: {rec['pending_tasks']}\n"
                        f"*(Số liệu được tính toán thời gian thực bằng Parameterized Cypher từ Neo4j)*"
                    )
                }

    # -------------------------------------------------------------------------
    # INTENT 2: Thống kê chi phí thực tế hạch toán (Actual Expenses)
    # -------------------------------------------------------------------------
    if any(k in q_lower for k in ["chi phí", "giải ngân", "tiêu bao nhiêu", "chi bao nhiêu", "hạch toán"]):
        proj_match = match_project_in_question(question, driver)
        if proj_match:
            p_code, p_name = proj_match
            cypher = """
            MATCH (p:Project {code: $code})-[:HAS_EXPENSE]->(e:Expense)
            RETURN p.name AS project_name,
                   p.code AS project_code,
                   count(e) AS expense_records,
                   sum(coalesce(e.total_amount, 0.0)) AS total_actual_cost
            """
            c_start = time.perf_counter()
            with driver.session() as session:
                rec = session.run(cypher, code=p_code).single()
            c_dur = round((time.perf_counter() - c_start) * 1000, 2)

            if rec and rec["expense_records"] > 0:
                total_cost = rec["total_actual_cost"] or 0.0
                formatted_cost = f"{total_cost:,.0f} VNĐ"
                return {
                    "intent": "PROJECT_ACTUAL_EXPENSES",
                    "cypher_duration_ms": c_dur,
                    "formatted_answer": (
                        f"💰 **Chi phí thực tế đã hạch toán cho dự án: {rec['project_name']} ({p_code})**\n\n"
                        f"- **Tổng chi phí giải ngân thực tế:** {formatted_cost}\n"
                        f"- **Số chứng từ chi phí hạch toán:** {rec['expense_records']} phiếu\n"
                        f"*(Số liệu trích xuất trực tiếp từ bảng hạch toán phiếu chi thực tế `expenses.csv`)*"
                    )
                }
            else:
                return {
                    "intent": "PROJECT_ACTUAL_EXPENSES",
                    "cypher_duration_ms": c_dur,
                    "formatted_answer": f"Dự án **{p_name} ({p_code})** hiện chưa phát sinh chứng từ chi phí thực tế trong hệ thống hạch toán."
                }
        else:
            # Câu hỏi mơ hồ về chi phí: Không tự suy diễn hay trả tổng toàn công ty khi chưa chỉ rõ phạm vi
            if any(k in q_lower for k in ["toàn công ty", "tất cả dự án", "toàn bộ dự án", "tổng công ty"]):
                cypher = """
                MATCH (e:Expense)
                RETURN count(e) AS total_records,
                       sum(coalesce(e.total_amount, 0.0)) AS grand_total
                """
                c_start = time.perf_counter()
                with driver.session() as session:
                    rec = session.run(cypher).single()
                c_dur = round((time.perf_counter() - c_start) * 1000, 2)
                
                total_val = rec["grand_total"] or 0.0
                return {
                    "intent": "COMPANY_WIDE_EXPENSES",
                    "cypher_duration_ms": c_dur,
                    "formatted_answer": (
                        f"🏢 **Tổng chi phí giải ngân toàn công ty (Tất cả dự án):**\n\n"
                        f"- **Tổng kinh phí thực tế:** {total_val:,.0f} VNĐ\n"
                        f"- **Tổng số chứng từ hạch toán:** {rec['total_records']} phiếu\n"
                        f"*(Lưu ý: Thống kê dựa trên toàn bộ 928 phiếu chi hạch toán)*"
                    )
                }
            else:
                # Câu hỏi không rõ phạm vi: Yêu cầu làm rõ, không chạy Cypher và không trả số liệu đoán mò
                return {
                    "intent": "AMBIGUOUS_EXPENSE_QUERY",
                    "cypher_duration_ms": 0.0,
                    "formatted_answer": (
                        "⚠️ **Yêu cầu làm rõ phạm vi chi phí:**\n\n"
                        "Bạn đang muốn tra cứu chi phí của **dự án cụ thể nào** hay **tổng chi phí toàn công ty**?\n"
                        "Vui lòng nêu rõ tên hoặc mã dự án (ví dụ: *'Chi phí dự án Phước Đông 6'* hoặc *'Tổng chi phí toàn công ty'*)."
                    )
                }

    # -------------------------------------------------------------------------
    # INTENT 3: Tra cứu vật tư chưa đặt hàng (Tách biệt rõ Chưa đặt hàng vs NULL/Trống)
    # -------------------------------------------------------------------------
    if any(k in q_lower for k in ["chưa đặt hàng", "chưa đặt", "đơn mua sắm chưa đặt"]):
        proj_match = match_project_in_question(question, driver)
        if proj_match:
            p_code, p_name = proj_match
            cypher = """
            MATCH (p:Project {code: $code})-[:HAS_PURCHASE_PLAN]->(pp:PurchasingPlan)
            RETURN p.name AS project_name,
                   sum(CASE WHEN trim(coalesce(pp.order_status, '')) = 'Chưa đặt hàng' THEN 1 ELSE 0 END) AS pending_orders_count,
                   sum(CASE WHEN trim(coalesce(pp.order_status, '')) = '' THEN 1 ELSE 0 END) AS unstated_status_count,
                   collect(CASE WHEN trim(coalesce(pp.order_status, '')) = 'Chưa đặt hàng' THEN pp.item_name ELSE null END)[0..5] AS sample_pending_items
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

    # -------------------------------------------------------------------------
    # INTENT 4: Tổng số danh mục trong kế hoạch vật tư
    # -------------------------------------------------------------------------
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

def is_internal_query_context(question: str, driver: Driver) -> bool:
    """
    Kiểm tra xem câu hỏi có thuộc ngữ cảnh dữ liệu nội bộ dự án không.
    Trả về True nếu:
    1. Trùng khớp với bất kỳ project_code hoặc tên dự án nào trong DB Neo4j.
    2. Chứa các từ khóa nghiệp vụ dự án nội bộ (nhật trình, hiện trường, sự cố, ghi chú kỹ thuật, quy cách...).
    """
    if match_project_in_question(question, driver):
        return True
    
    q_lower = question.lower()
    internal_keywords = [
        "nhật trình", "hiện trường", "công trường", "sự cố", "phát sinh",
        "ghi chú", "quy cách", "tiêu chuẩn kỹ thuật", "vật tư", "kế hoạch",
        "hạng mục", "joint box", "odf", "tiến độ", "chi phí", "hạch toán"
    ]
    return any(k in q_lower for k in internal_keywords)
