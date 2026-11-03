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

def execute_structured_query(question: str, driver: Driver, auth_user: Any) -> Optional[Dict[str, Any]]:
    """
    Xác định ý định và thực thi Parameterized Cypher định nghĩa sẵn.
    Đo thời gian thực tế của câu truy vấn Cypher và trả về `cypher_dur_ms`.
    BẢO MẬT: Bắt buộc auth_user hợp lệ. Mặc định từ chối nếu không có danh tính.
    """
    if not auth_user:
        return {
            "intent": "PERMISSION_DENIED",
            "cypher_duration_ms": 0.0,
            "formatted_answer": "⛔ **Truy cập bị từ chối (401 Unauthorized):** Yêu cầu xác thực tài khoản để truy vấn số liệu dự án."
        }

    q_lower = question.lower()

    # -------------------------------------------------------------------------
    # INTENT 0: Thống kê số lượng / Danh sách dự án trong hệ thống
    # CHỈ kích hoạt khi câu hỏi THỰC SỰ là hỏi số lượng hoặc danh sách dự án
    # -------------------------------------------------------------------------
    is_project_listing = any(k in q_lower for k in [
        "bao nhiêu dự án", "số lượng dự án", "danh sách dự án", "có những dự án nào",
        "liệt kê dự án", "tổng số dự án"
    ])
    if is_project_listing and not any(k in q_lower for k in ["chi phí", "tiến độ", "vật tư", "nhật trình", "hồ sơ", "chưa ký", "sự cố"]):
        # Trích xuất bộ lọc động (Dynamic regex / entity extraction)
        # 1. Trích xuất cấp điện áp bất kỳ (ví dụ: 10kv, 10 kv, 10-kv, 110kv, 110 kV, 110-kV, 220kv, 500kv,...)
        voltage_match = re.search(r"\b(\d+)[\s\-]*kv\b", q_lower)
        filter_keyword = None
        filter_label = None

        if voltage_match:
            val_num = voltage_match.group(1)
            raw_v = f"{val_num}kv"
            filter_keyword = raw_v
            filter_label = f"{val_num}kV"
        else:
            # 2. Trích xuất loại hình hoặc từ khóa cụm danh từ sau "dự án"
            # Ví dụ: "dự án trạm biến áp", "dự án đường dây", "dự án thủy điện", v.v.
            after_match = re.search(r"dự án\s+([\w\s]+)", q_lower)
            if after_match:
                extracted = after_match.group(1).strip()
                # Loại trừ các từ để hỏi hoặc số lượng thông thường
                cleaned = re.sub(r"^(nào|gì|đang có|này|kia|ở|tại)\s*", "", extracted).strip()
                if len(cleaned) >= 3 and cleaned not in ["nào", "gì", "này", "kia"]:
                    filter_keyword = cleaned
                    filter_label = cleaned.title()

        if not auth_user.is_admin:
            # Nếu là user thường: Chỉ hiển thị các dự án user được phân quyền (có áp dụng filter nếu có)
            allowed = auth_user.allowed_projects
            if not allowed:
                return {
                    "intent": "PROJECT_LIST_RBAC",
                    "cypher_duration_ms": 0.0,
                    "formatted_answer": f"Tài khoản của bạn (`{auth_user.user_id}`) hiện chưa được phân quyền truy cập dự án nào trong hệ thống."
                }
            if filter_keyword:
                if filter_keyword.endswith("kv"):
                    cypher = """
                    MATCH (p:Project)
                    WHERE p.code IN $allowed AND (p.name =~ ('(?i).*\\\\b' + $filter + '\\\\b.*') OR p.code =~ ('(?i).*\\\\b' + $filter + '\\\\b.*'))
                    RETURN count(p) AS total_projects, collect(p.name + ' (' + p.code + ')') AS project_list
                    """
                else:
                    cypher = """
                    MATCH (p:Project)
                    WHERE p.code IN $allowed AND (toLower(p.name) CONTAINS $filter OR toLower(p.code) CONTAINS $filter)
                    RETURN count(p) AS total_projects, collect(p.name + ' (' + p.code + ')') AS project_list
                    """
                params = {"allowed": allowed, "filter": filter_keyword}
            else:
                cypher = """
                MATCH (p:Project)
                WHERE p.code IN $allowed
                RETURN count(p) AS total_projects, collect(p.name + ' (' + p.code + ')') AS project_list
                """
                params = {"allowed": allowed}

            c_start = time.perf_counter()
            with driver.session() as session:
                rec = session.run(cypher, **params).single()
            c_dur = round((time.perf_counter() - c_start) * 1000, 2)
            total = rec["total_projects"]
            if total > 0:
                proj_items = "\n".join([f"- {name}" for name in rec["project_list"]])
                qualifier = f" thuộc phân loại **{filter_label}**" if filter_label else ""
                formatted_answer = (
                    f"📁 **Danh sách dự án{qualifier} bạn được phân quyền truy cập ({total} dự án):**\n\n"
                    f"{proj_items}\n\n"
                    f"*(Tài khoản: `{auth_user.user_id}` - Phân quyền theo dự án được gán)*"
                )
            else:
                formatted_answer = f"Trong các dự án bạn được phân quyền, hiện không có dự án nào thuộc phân loại **{filter_label}**."

            return {
                "intent": "PROJECT_LIST_RBAC",
                "cypher_duration_ms": c_dur,
                "formatted_answer": formatted_answer
            }
        else:
            # Nếu là Admin: Hiển thị toàn bộ hoặc lọc theo điều kiện
            if filter_keyword:
                # Nếu là cấp điện áp (kết thúc bằng 'kv'): so khớp theo từ ranh giới regex trong Cypher để 10kv không nuốt nhầm 110kv
                if filter_keyword.endswith("kv"):
                    cypher = """
                    MATCH (p:Project)
                    WHERE p.name =~ ('(?i).*\\\\b' + $filter + '\\\\b.*') OR p.code =~ ('(?i).*\\\\b' + $filter + '\\\\b.*')
                    RETURN count(p) AS total_projects, collect(p.name + ' (' + p.code + ')') AS project_list
                    """
                else:
                    cypher = """
                    MATCH (p:Project)
                    WHERE toLower(p.name) CONTAINS $filter OR toLower(p.code) CONTAINS $filter
                    RETURN count(p) AS total_projects, collect(p.name + ' (' + p.code + ')') AS project_list
                    """
                params = {"filter": filter_keyword}
            else:
                cypher = """
                MATCH (p:Project)
                RETURN count(p) AS total_projects, collect(p.name + ' (' + p.code + ')') AS project_list
                """
                params = {}

            c_start = time.perf_counter()
            with driver.session() as session:
                rec = session.run(cypher, **params).single()
            c_dur = round((time.perf_counter() - c_start) * 1000, 2)
            total = rec["total_projects"]
            if total > 0:
                proj_items = "\n".join([f"- {name}" for name in rec["project_list"]])
                qualifier = f" **{filter_label}**" if filter_label else ""
                formatted_answer = (
                    f"⚡ **Tổng quan hệ thống: Hiện có {total} dự án{qualifier} đang quản lý:**\n\n"
                    f"{proj_items}\n\n"
                    f"*(Quyền Quản trị viên: Toàn quyền truy cập tất cả các dự án)*"
                )
            else:
                formatted_answer = f"Không tìm thấy dự án nào thuộc phân loại **{filter_label}** trong hệ thống."

            return {
                "intent": "PROJECT_LIST_ALL",
                "cypher_duration_ms": c_dur,
                "formatted_answer": formatted_answer
            }

    # -------------------------------------------------------------------------
    # INTENT 1: Tiến độ trung bình các task (loại trừ section header, quy đổi 0-1 sang %)
    # -------------------------------------------------------------------------
    if any(k in q_lower for k in ["tiến độ", "hoàn thành bao nhiêu", "%", "tiến độ trung bình"]):
        proj_match = match_project_in_question(question, driver)
        if proj_match:
            p_code, p_name = proj_match
            # RBAC Check: Nếu user không có quyền truy cập dự án này -> Từ chối ngay
            if not auth_user.can_access_project(p_code):
                return {
                    "intent": "PERMISSION_DENIED",
                    "cypher_duration_ms": 0.0,
                    "formatted_answer": f"⛔ **Truy cập bị từ chối (403 Forbidden):** Tài khoản của bạn (`{auth_user.user_id}`) không được phân quyền truy cập thông tin của dự án **{p_name} ({p_code})**."
                }

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
            # RBAC Check
            if not auth_user.can_access_project(p_code):
                return {
                    "intent": "PERMISSION_DENIED",
                    "cypher_duration_ms": 0.0,
                    "formatted_answer": f"⛔ **Truy cập bị từ chối (403 Forbidden):** Tài khoản của bạn (`{auth_user.user_id}`) không được phân quyền xem chi phí hạch toán của dự án **{p_name} ({p_code})**."
                }

            cypher = """
            MATCH (p:Project {code: $code})-[:INCURRED_EXPENSE]->(e:Expense)
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
            # Kiểm tra intent Xếp hạng Top dự án chi phí cao nhất (Top Expense Ranking)
            is_top_ranking_query = any(k in q_lower for k in [
                "nhiều nhất", "lớn nhất", "cao nhất", "tốn nhất", "top", "xếp hạng", 
                "chiếm chi phí nhiều nhất", "chiếm nhiều nhất"
            ])
            if is_top_ranking_query:
                # Bắt buộc chỉ Admin mới có quyền xem xếp hạng đa dự án
                if not auth_user.is_admin:
                    return {
                        "intent": "PERMISSION_DENIED",
                        "cypher_duration_ms": 0.0,
                        "formatted_answer": (
                            "⛔ **Truy cập bị từ chối (403 Forbidden):**\n"
                            "Yêu cầu xếp hạng và so sánh chi phí đa dự án đòi hỏi quyền Quản trị viên (Admin).\n"
                            f"Tài khoản hiện tại (`{auth_user.user_id}`) chỉ có quyền truy cập trong phạm vi dự án được phân công."
                        )
                    }

                # 1. Truy vấn Bảng xếp hạng Top 5 Dự án công trình xây lắp (chỉ lấy đúng project_type = 'CONSTRUCTION')
                cypher_construction = """
                MATCH (p:Project)-[:INCURRED_EXPENSE]->(e:Expense)
                WHERE p.project_type = 'CONSTRUCTION'
                WITH p, count(e) AS expense_count, sum(coalesce(e.total_amount, 0.0)) AS total_cost
                ORDER BY total_cost DESC
                LIMIT 5
                RETURN p.name AS project_name,
                       p.code AS project_code,
                       expense_count,
                       total_cost
                """
                # 2. Truy vấn riêng các hạng mục Chi phí vận hành / Quản lý nội bộ (OPERATIONAL)
                cypher_operational = """
                MATCH (p:Project)-[:INCURRED_EXPENSE]->(e:Expense)
                WHERE p.project_type = 'OPERATIONAL'
                WITH p, count(e) AS expense_count, sum(coalesce(e.total_amount, 0.0)) AS total_cost
                ORDER BY total_cost DESC
                RETURN p.name AS project_name,
                       p.code AS project_code,
                       expense_count,
                       total_cost
                """
                # 3. Kiểm tra chất lượng dữ liệu TOÀN BỘ danh mục dự án (không phụ thuộc vào việc đã có phiếu chi hay chưa)
                cypher_unclassified = """
                MATCH (p:Project)
                WHERE p.project_type IS NULL OR trim(p.project_type) = ''
                RETURN count(p) AS unclassified_count, collect(p.code) AS unclassified_codes
                """
                c_start = time.perf_counter()
                with driver.session() as session:
                    construction_records = list(session.run(cypher_construction))
                    operational_records = list(session.run(cypher_operational))
                    unclass_res = session.run(cypher_unclassified).single()
                c_dur = round((time.perf_counter() - c_start) * 1000, 2)

                if construction_records or operational_records:
                    lines = ["🏆 **BẢNG XẾP HẠNG TOP DỰ ÁN CÔNG TRÌNH CÓ CHI PHÍ CAO NHẤT:**\n"]
                    lines.append("*(Bảng chỉ xếp hạng các dự án xây lắp công trình `CONSTRUCTION`, không bao gồm hợp đồng thương mại `COMMERCIAL`)*\n")
                    for idx, r in enumerate(construction_records, start=1):
                        cost_str = f"{r['total_cost']:,.0f} VNĐ"
                        lines.append(f"{idx}. **{r['project_name']}** (`{r['project_code']}`): **{cost_str}** ({r['expense_count']} phiếu chi)")
                    
                    if operational_records:
                        lines.append("\n🏢 **CHI PHÍ VẬN HÀNH & QUẢN LÝ NỘI BỘ (Hạng mục riêng):**")
                        for r in operational_records:
                            cost_str = f"{r['total_cost']:,.0f} VNĐ"
                            lines.append(f"- **{r['project_name']}** (`{r['project_code']}`): **{cost_str}** ({r['expense_count']} phiếu chi)")

                    if unclass_res and unclass_res["unclassified_count"] > 0:
                        lines.append(f"\n⚠️ **Cảnh báo chất lượng dữ liệu danh mục:** Phát hiện {unclass_res['unclassified_count']} dự án trong hệ thống chưa được phân loại `project_type` ({unclass_res['unclassified_codes']}).")

                    lines.append("\n*(Dữ liệu phân loại tường minh theo thuộc tính nghiệp vụ `project_type: CONSTRUCTION vs OPERATIONAL` từ CSDL đồ thị)*")
                    return {
                        "intent": "TOP_EXPENSE_PROJECTS_RANKING",
                        "cypher_duration_ms": c_dur,
                        "formatted_answer": "\n".join(lines)
                    }

            # Câu hỏi toàn công ty: Chỉ Admin mới được quyền xem tổng kinh phí toàn công ty
            if any(k in q_lower for k in ["toàn công ty", "tất cả dự án", "toàn bộ dự án", "tổng công ty"]):
                if not auth_user.is_admin:
                    return {
                        "intent": "PERMISSION_DENIED",
                        "cypher_duration_ms": 0.0,
                        "formatted_answer": "⛔ **Truy cập bị từ chối (403 Forbidden):** Chỉ tài khoản Quản trị viên (Admin) mới có quyền truy cập tổng chi phí toàn công ty."
                    }

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
            # RBAC Check
            if not auth_user.can_access_project(p_code):
                return {
                    "intent": "PERMISSION_DENIED",
                    "cypher_duration_ms": 0.0,
                    "formatted_answer": f"⛔ **Truy cập bị từ chối (403 Forbidden):** Tài khoản của bạn (`{auth_user.user_id}`) không được phân quyền xem kế hoạch mua sắm của dự án **{p_name} ({p_code})**."
                }

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
            # RBAC Check
            if not auth_user.can_access_project(p_code):
                return {
                    "intent": "PERMISSION_DENIED",
                    "cypher_duration_ms": 0.0,
                    "formatted_answer": f"⛔ **Truy cập bị từ chối (403 Forbidden):** Tài khoản của bạn (`{auth_user.user_id}`) không được phân quyền xem danh mục vật tư của dự án **{p_name} ({p_code})**."
                }

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

    # -------------------------------------------------------------------------
    # INTENT 5: Tình trạng ký kết hồ sơ / hợp đồng mua sắm
    # -------------------------------------------------------------------------
    contract_status_keywords = [
        "chưa ký", "chưa được ký", "đang được ký", "đang ký", "đã ký",
        "hồ sơ chưa ký", "hồ sơ đang ký", "hợp đồng chưa ký", "tình trạng ký",
        "ký hợp đồng", "trình duyệt", "đang trình duyệt", "thương thảo", "phụ lục"
    ]
    if any(k in q_lower for k in contract_status_keywords):
        proj_match = match_project_in_question(question, driver)

        # Trường hợp 1: Hỏi cho 1 dự án cụ thể
        if proj_match:
            p_code, p_name = proj_match
            if not auth_user.can_access_project(p_code):
                return {
                    "intent": "PERMISSION_DENIED",
                    "cypher_duration_ms": 0.0,
                    "formatted_answer": f"⛔ **Truy cập bị từ chối (403 Forbidden):** Tài khoản của bạn (`{auth_user.user_id}`) không được phân quyền xem tình trạng hợp đồng/hồ sơ của dự án **{p_name} ({p_code})**."
                }

            cypher = """
            MATCH (p:Project {code: $code})-[:HAS_PURCHASE_PLAN]->(pp:PurchasingPlan)
            WHERE pp.contract_status IS NOT NULL AND trim(pp.contract_status) <> ''
            RETURN pp.contract_status AS status, count(pp) AS count, collect(pp.content)[0..3] AS samples
            ORDER BY count DESC
            """
            c_start = time.perf_counter()
            with driver.session() as session:
                records = list(session.run(cypher, code=p_code))
            c_dur = round((time.perf_counter() - c_start) * 1000, 2)

            if records:
                lines = [f"📑 **Tình trạng ký kết hồ sơ / hợp đồng dự án: {p_name} ({p_code})**\n"]
                for r in records:
                    lines.append(f"- **{r['status']}:** {r['count']} hạng mục / gói")
                    if r["samples"]:
                        sample_names = ", ".join([f"`{s}`" for s in r["samples"] if s])
                        if sample_names:
                            lines.append(f"  *(Ví dụ: {sample_names})*")
                return {
                    "intent": "PROJECT_CONTRACT_STATUS",
                    "cypher_duration_ms": c_dur,
                    "formatted_answer": "\n".join(lines)
                }

        # Trường hợp 2: Tra cứu tổng hợp toàn hệ thống (hoặc các dự án người dùng được phép xem)
        target_status = None
        if any(k in q_lower for k in ["chưa ký", "chưa được ký", "chưa kí"]):
            target_status = "Chưa ký"
        elif any(k in q_lower for k in ["đang trình duyệt", "trình duyệt"]):
            target_status = "Đang trình duyệt"
        elif any(k in q_lower for k in ["đang thương thảo", "thương thảo"]):
            target_status = "Đang thương thảo"
        elif any(k in q_lower for k in ["đã ký", "đã kí"]):
            target_status = "Đã ký"

        c_start = time.perf_counter()
        with driver.session() as session:
            if target_status:
                if auth_user.role == "admin":
                    cypher = """
                    MATCH (p:Project)-[:HAS_PURCHASE_PLAN]->(pp:PurchasingPlan)
                    WHERE pp.contract_status = $target_status
                    RETURN p.code AS project_code, p.name AS project_name, count(pp) AS count, collect(pp.content)[0..3] AS samples
                    ORDER BY count DESC
                    """
                    records = list(session.run(cypher, target_status=target_status))
                else:
                    cypher = """
                    MATCH (p:Project)-[:HAS_PURCHASE_PLAN]->(pp:PurchasingPlan)
                    WHERE p.code IN $allowed_projects AND pp.contract_status = $target_status
                    RETURN p.code AS project_code, p.name AS project_name, count(pp) AS count, collect(pp.content)[0..3] AS samples
                    ORDER BY count DESC
                    """
                    records = list(session.run(cypher, allowed_projects=auth_user.allowed_projects, target_status=target_status))
            else:
                if auth_user.role == "admin":
                    cypher = """
                    MATCH (p:Project)-[:HAS_PURCHASE_PLAN]->(pp:PurchasingPlan)
                    WHERE pp.contract_status IS NOT NULL AND trim(pp.contract_status) <> ''
                    RETURN p.code AS project_code, p.name AS project_name, pp.contract_status AS status, count(pp) AS count
                    ORDER BY p.code, count DESC
                    """
                    records = list(session.run(cypher))
                else:
                    cypher = """
                    MATCH (p:Project)-[:HAS_PURCHASE_PLAN]->(pp:PurchasingPlan)
                    WHERE p.code IN $allowed_projects AND pp.contract_status IS NOT NULL AND trim(pp.contract_status) <> ''
                    RETURN p.code AS project_code, p.name AS project_name, pp.contract_status AS status, count(pp) AS count
                    ORDER BY p.code, count DESC
                    """
                    records = list(session.run(cypher, allowed_projects=auth_user.allowed_projects))

        c_dur = round((time.perf_counter() - c_start) * 1000, 2)

        if records:
            if target_status:
                total_items = sum(r["count"] for r in records)
                lines = [f"📑 **DANH SÁCH DỰ ÁN CÓ HỒ SƠ / HỢP ĐỒNG Ở TRẠNG THÁI '{target_status.upper()}':**\n"]
                lines.append(f"*(Tìm thấy tổng cộng **{total_items}** hạng mục trên **{len(records)}** dự án)*\n")
                for i, r in enumerate(records, 1):
                    lines.append(f"{i}. **{r['project_name']}** (`{r['project_code']}`): **{r['count']}** hạng mục {target_status.lower()}")
                    if r.get("samples"):
                        samples_str = ", ".join([f"`{s}`" for s in r["samples"] if s])
                        if samples_str:
                            lines.append(f"   - Hạng mục tiêu biểu: {samples_str}")
                return {
                    "intent": "PROJECT_CONTRACT_STATUS_SYSTEM",
                    "cypher_duration_ms": c_dur,
                    "formatted_answer": "\n".join(lines)
                }
            else:
                lines = ["📑 **TỔNG HỢP TÌNH TRẠNG KÝ KẾT HỒ SƠ / HỢP ĐỒNG CÁC DỰ ÁN:**\n"]
                curr_proj = None
                for r in records:
                    if curr_proj != r["project_code"]:
                        curr_proj = r["project_code"]
                        lines.append(f"\n- **{r['project_name']}** (`{r['project_code']}`):")
                    lines.append(f"  + {r['status']}: {r['count']} hạng mục")
                return {
                    "intent": "PROJECT_CONTRACT_STATUS_SYSTEM",
                    "cypher_duration_ms": c_dur,
                    "formatted_answer": "\n".join(lines)
                }

    # -------------------------------------------------------------------------
    # INTENT 6: Dữ liệu Chấm công / Điểm danh nhân sự (Timesheet / Attendance)
    # Tách biệt rõ ràng:
    # 1. Truy vấn tiến độ task chấm công -> Cypher trả progress, status, metadata của Task
    # 2. Bảng chấm công nhân viên thực tế -> Báo chưa tích hợp timesheet
    # -------------------------------------------------------------------------
    attendance_keywords = [
        "chấm công", "cham cong", "điểm danh", "diem danh",
        "bảng công", "bang cong", "ngày công", "ngay cong",
        "nhân viên nào chấm công", "ai chấm công", "ai cham cong"
    ]
    if any(k in q_lower for k in attendance_keywords):
        # Ưu tiên kiểm tra: Nếu câu hỏi hỏi về "ghi chú", "chi tiết", "trao đổi kỹ thuật", "nội dung"
        # -> Bỏ qua Intent có cấu trúc để nhường cho Extractive RAG trích dẫn văn bản
        is_tech_notes_query = any(k in q_lower for k in ["ghi chú", "ghi chu", "kỹ thuật", "lỗi", "bug", "chi tiết", "trao đổi", "nội dung"])
        
        # Kiểm tra xem câu hỏi có phải đang hỏi TIẾN ĐỘ / TÌNH HÌNH TASK tính năng chấm công
        is_task_progress_query = any(k in q_lower for k in ["tiến độ", "tien do", "tình hình", "trạng thái", "hoàn thành chưa", "phát triển", "mới nhất"])
        
        if is_task_progress_query and not is_tech_notes_query:
            # Tra cứu Task tính năng chấm công có cấu trúc bằng Cypher
            cypher = """
            MATCH (p:Project)-[:HAS_TASK]->(t:Task)
            WHERE (toLower(t.name) CONTAINS 'chấm công' OR toLower(t.name) CONTAINS 'cham cong')
            RETURN p.code AS project_code,
                   p.name AS project_name,
                   t.code AS task_code,
                   t.name AS task_name,
                   t.status AS status,
                   t.progress AS progress,
                   t.assigned_engineer_name AS assigned_engineer,
                   t.created_at AS created_at,
                   t.notes AS notes
            ORDER BY t.created_at DESC
            LIMIT 5
            """
            c_start = time.perf_counter()
            with driver.session() as session:
                records = list(session.run(cypher))
            c_dur = round((time.perf_counter() - c_start) * 1000, 2)

            if records:
                lines = ["📊 **BÁO CÁO TIẾN ĐỘ TÍNH NĂNG / NHIỆM VỤ CHẤM CÔNG:**\n"]
                for i, r in enumerate(records, 1):
                    # Kiểm tra RBAC từng dự án
                    if not auth_user.can_access_project(r["project_code"]):
                        continue
                    pct = round(float(r["progress"] or 0.0) * 100, 1) if r["progress"] is not None else 0.0
                    engineer = r["assigned_engineer"].split("|")[0] if r.get("assigned_engineer") else "Chưa phân công"
                    clean_notes = re.sub(r"\[[A-Za-z0-9_\-]+:[^\]]*\]", "", r.get("notes") or "").strip()
                    note_str = f"\n   - *Ghi chú:* {clean_notes}" if clean_notes else ""

                    lines.append(
                        f"{i}. **{r['task_name'].strip()}**\n"
                        f"   - **Dự án:** {r['project_name']} (`{r['project_code']}`)\n"
                        f"   - **Mã nhiệm vụ (ID):** `{r['task_code']}`\n"
                        f"   - **Trạng thái:** `{r['status']}`\n"
                        f"   - **Tiến độ hoàn thành:** **{pct}%**\n"
                        f"   - **Nhân sự phụ trách:** {engineer}{note_str}"
                    )
                if len(lines) > 1:
                    lines.append("\n*(Số liệu tiến độ và trạng thái được truy xuất chính xác 100% từ thuộc tính Task trong CSDL Neo4j)*")
                    return {
                        "intent": "TASK_ATTENDANCE_PROGRESS",
                        "cypher_duration_ms": c_dur,
                        "formatted_answer": "\n".join(lines)
                    }

        # Nếu không phải hỏi tiến độ task, kiểm tra nếu là câu hỏi ghi chú kỹ thuật thì nhường cho Extractive RAG
        is_tech_notes_query = any(k in q_lower for k in ["ghi chú", "ghi chu", "kỹ thuật", "lỗi", "bug", "chi tiết", "trao đổi"])
        if not is_tech_notes_query:
            # Câu hỏi tra cứu dữ liệu chấm công thực tế của nhân viên (ai chấm công, bảng công hôm nay...)
            return {
                "intent": "HR_ATTENDANCE_NOT_INDEXED",
                "cypher_duration_ms": 0.0,
                "formatted_answer": (
                    "ℹ️ **Thông báo về nguồn dữ liệu:**\n\n"
                    "Hiện tại hệ thống **chưa tích hợp nguồn dữ liệu chấm công / điểm danh nhân sự (Timesheet/Attendance)**.\n\n"
                    "- Cơ sở dữ liệu hiện tại chỉ quản lý thông tin dự án, kế hoạch mua sắm vật tư, chi phí và tiến độ nhiệm vụ công việc.\n"
                    "- Nếu bạn đang tìm kiếm các **nhiệm vụ kỹ thuật / phát triển tính năng chấm công** (thuộc dự án Phần mềm công ty), vui lòng nêu rõ: *'Tiến độ phát triển tính năng chấm công'*."
                )
            }

    return None

def is_internal_query_context(question: str, driver: Driver) -> bool:
    """
    Kiểm tra xem câu hỏi có thuộc ngữ cảnh dữ liệu nội bộ dự án không.
    """
    if match_project_in_question(question, driver):
        return True
    
    q_lower = question.lower()
    internal_keywords = [
        "nhật trình", "hiện trường", "công trường", "sự cố", "phát sinh",
        "ghi chú", "quy cách", "tiêu chuẩn kỹ thuật", "vật tư", "kế hoạch",
        "hạng mục", "joint box", "odf", "tiến độ", "chi phí", "hạch toán",
        "dự án", "công trình", "hợp đồng", "gói thầu", "chấm công", "điểm danh",
        "bảng công", "ngày công", "nhân sự", "phân công"
    ]
    return any(k in q_lower for k in internal_keywords)
