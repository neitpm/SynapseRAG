import sys
import os
import csv
import logging
from pathlib import Path
from neo4j import GraphDatabase

if sys.platform == "win32":
    sys.stdout.reconfigure(encoding="utf-8")

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger("graphrag-ingestion")

DATASETS_DIR = Path("datasets_sanitized")
NEO4J_URI = os.getenv("NEO4J_URI", "bolt://127.0.0.1:7687")
NEO4J_USER = os.getenv("NEO4J_USER", "neo4j")
NEO4J_PASSWORD = os.getenv("NEO4J_PASSWORD", "graphrag_password123")

def read_sanitized_csv(file_name: str):
    path = DATASETS_DIR / file_name
    if not path.exists():
        logger.warning(f"File {file_name} không tồn tại.")
        return []
    with open(path, mode="r", encoding="utf-8-sig") as f:
        reader = csv.DictReader(f)
        return list(reader)

def init_neo4j_constraints(session):
    logger.info("🔧 Đang tạo Schema Constraints & Indexes trên Neo4j...")
    constraints = [
        "CREATE CONSTRAINT project_code_unique IF NOT EXISTS FOR (p:Project) REQUIRE p.code IS UNIQUE",
        "CREATE CONSTRAINT engineer_id_unique IF NOT EXISTS FOR (e:Engineer) REQUIRE e.id IS UNIQUE",
        "CREATE CONSTRAINT task_id_unique IF NOT EXISTS FOR (t:Task) REQUIRE t.id IS UNIQUE",
        "CREATE CONSTRAINT material_plan_id_unique IF NOT EXISTS FOR (m:MaterialPlan) REQUIRE m.id IS UNIQUE",
        "CREATE CONSTRAINT purchasing_plan_id_unique IF NOT EXISTS FOR (pp:PurchasingPlan) REQUIRE pp.id IS UNIQUE",
        "CREATE CONSTRAINT expense_id_unique IF NOT EXISTS FOR (ex:Expense) REQUIRE ex.id IS UNIQUE",
        "CREATE CONSTRAINT field_log_id_unique IF NOT EXISTS FOR (fl:FieldLog) REQUIRE fl.id IS UNIQUE",
        "CREATE CONSTRAINT material_id_unique IF NOT EXISTS FOR (mat:Material) REQUIRE mat.id IS UNIQUE"
    ]
    for c in constraints:
        try:
            session.run(c)
        except Exception as e:
            logger.warning(f"Constraint: {e}")

def chunk_list(lst, size=500):
    for i in range(0, len(lst), size):
        yield lst[i:i + size]

def ingest_projects(session):
    rows = read_sanitized_csv("projects.csv")
    if not rows: return
    logger.info(f"🚀 Nạp {len(rows)} Projects...")
    query = """
    UNWIND $batch AS row
    MERGE (p:Project {code: row.code})
    SET p.id = row.id,
        p.name = row.name,
        p.client = row.client,
        p.location = row.location,
        p.contract_value = toFloat(coalesce(nullif(row.contract_value, ''), '0')),
        p.progress_percent = toFloat(coalesce(nullif(row.progress_percent, ''), '0')),
        p.status = row.status,
        p.active_teams = toInteger(coalesce(nullif(row.active_teams, ''), '0')),
        p.total_tasks = toInteger(coalesce(nullif(row.total_tasks, ''), '0')),
        p.completed_tasks = toInteger(coalesce(nullif(row.completed_tasks, ''), '0')),
        p.manager_id = row.manager_id,
        p.manager_name = row.manager_name,
        p.start_date = row.start_date,
        p.end_date = row.end_date,
        p.notes = row.notes,
        p.created_at = row.created_at
    """
    for batch in chunk_list(rows, 500):
        session.run(query, batch=batch)

def ingest_engineers(session):
    rows = read_sanitized_csv("engineers.csv")
    if not rows: return
    logger.info(f"🚀 Nạp {len(rows)} Engineers...")
    query = """
    UNWIND $batch AS row
    MERGE (e:Engineer {id: row.id})
    SET e.code = row.code,
        e.name = row.name,
        e.title = row.title,
        e.role = row.role,
        e.username = row.username,
        e.is_locked = (row.is_locked = 'true'),
        e.project_codes = row.project_codes,
        e.created_at = row.created_at
    """
    for batch in chunk_list(rows, 500):
        session.run(query, batch=batch)

    session.run("""
    MATCH (p:Project)
    WHERE p.manager_id IS NOT NULL AND p.manager_id <> ''
    MATCH (e:Engineer {id: p.manager_id})
    MERGE (e)-[:MANAGES]->(p)
    """)

def ingest_tasks(session):
    rows = read_sanitized_csv("tasks.csv")
    if not rows: return
    logger.info(f"🚀 Nạp {len(rows)} Tasks...")
    query = """
    UNWIND $batch AS row
    MERGE (t:Task {id: row.id})
    SET t.code = row.code,
        t.name = row.name,
        t.project_code = row.project_code,
        t.project_name = row.project_name,
        t.unit = row.unit,
        t.volume = toFloat(coalesce(nullif(row.volume, ''), '0')),
        t.progress = toFloat(coalesce(nullif(row.progress, ''), '0')),
        t.status = row.status,
        t.purchase_status = row.purchase_status,
        t.constr_status = row.constr_status,
        t.priority = row.priority,
        t.section_name = row.section_name,
        t.is_done = (row.is_done = 'true'),
        t.assigned_engineer_id = row.assigned_engineer_id,
        t.assigned_engineer_name = row.assigned_engineer_name,
        t.parent_id = row.parent_id,
        t.notes = row.notes,
        t.due_date = row.due_date,
        t.created_at = row.created_at
    WITH t, row
    WHERE row.project_code IS NOT NULL AND row.project_code <> ''
    MATCH (p:Project {code: row.project_code})
    MERGE (p)-[:HAS_TASK]->(t)
    """
    for batch in chunk_list(rows, 500):
        session.run(query, batch=batch)

    # Link Task -> Engineer (Chỉ khi tồn tại, FK mồ côi vẫn lưu thuộc tính trên Node)
    session.run("""
    MATCH (t:Task)
    WHERE t.assigned_engineer_id IS NOT NULL AND t.assigned_engineer_id <> ''
    MATCH (e:Engineer {id: t.assigned_engineer_id})
    MERGE (e)-[:ASSIGNED_TO]->(t)
    """)

    # Link Sub-task -> Parent Task
    session.run("""
    MATCH (sub:Task)
    WHERE sub.parent_id IS NOT NULL AND sub.parent_id <> ''
    MATCH (parent:Task {id: sub.parent_id})
    MERGE (parent)-[:HAS_SUBTASK]->(sub)
    """)

def ingest_material_plans(session):
    rows = read_sanitized_csv("material_plans.csv")
    if not rows: return
    logger.info(f"🚀 Nạp {len(rows)} Material Plans...")
    query = """
    UNWIND $batch AS row
    MERGE (m:MaterialPlan {id: row.id})
    SET m.stt = row.stt,
        m.job_content = row.job_content,
        m.unit = row.unit,
        m.contract_volume = toFloat(coalesce(nullif(row.contract_volume, ''), '0')),
        m.ordered_volume = toFloat(coalesce(nullif(row.ordered_volume, ''), '0')),
        m.progress = toFloat(coalesce(nullif(row.progress, ''), '0')),
        m.status = row.status,
        m.project_code = row.project_code,
        m.tech_spec_origin = row.tech_spec_origin,
        m.notes = row.notes,
        m.parent_id = row.parent_id,
        m.created_at = row.created_at
    WITH m, row
    WHERE row.project_code IS NOT NULL AND row.project_code <> ''
    MATCH (p:Project {code: row.project_code})
    MERGE (p)-[:REQUIRES_MATERIAL]->(m)
    """
    for batch in chunk_list(rows, 500):
        session.run(query, batch=batch)

    session.run("""
    MATCH (sub:MaterialPlan)
    WHERE sub.parent_id IS NOT NULL AND sub.parent_id <> ''
    MATCH (parent:MaterialPlan {id: sub.parent_id})
    MERGE (parent)-[:HAS_SUBPLAN]->(sub)
    """)

def ingest_purchasing_plans(session):
    rows = read_sanitized_csv("purchasing_plans.csv")
    if not rows: return
    logger.info(f"🚀 Nạp {len(rows)} Purchasing Plans...")
    query = """
    UNWIND $batch AS row
    MERGE (pp:PurchasingPlan {id: row.id})
    SET pp.stt = row.stt,
        pp.content = row.content,
        pp.unit = row.unit,
        pp.contract_volume = toFloat(coalesce(nullif(row.contract_volume, ''), '0')),
        pp.volume_order = toFloat(coalesce(nullif(row.volume_order, ''), '0')),
        pp.unit_price = toFloat(coalesce(nullif(row.unit_price, ''), '0')),
        pp.total_amount = toFloat(coalesce(nullif(row.total_amount, ''), '0')),
        pp.prepay_amount = toFloat(coalesce(nullif(row.prepay_amount, ''), '0')),
        pp.remaining_amount = toFloat(coalesce(nullif(row.remaining_amount, ''), '0')),
        pp.order_status = row.order_status,
        pp.contract_status = row.contract_status,
        pp.selected_supplier = row.selected_supplier,
        pp.project_code = row.project_code,
        pp.material_plan_id = row.material_plan_id,
        pp.notes = row.notes,
        pp.created_at = row.created_at
    WITH pp, row
    WHERE row.project_code IS NOT NULL AND row.project_code <> ''
    MATCH (p:Project {code: row.project_code})
    MERGE (p)-[:HAS_PURCHASE_PLAN]->(pp)
    """
    for batch in chunk_list(rows, 500):
        session.run(query, batch=batch)

    session.run("""
    MATCH (pp:PurchasingPlan)
    WHERE pp.material_plan_id IS NOT NULL AND pp.material_plan_id <> ''
    MATCH (mp:MaterialPlan {id: pp.material_plan_id})
    MERGE (pp)-[:FULFILLS]->(mp)
    """)

def ingest_expenses(session):
    rows = read_sanitized_csv("expenses.csv")
    if not rows: return
    logger.info(f"🚀 Nạp {len(rows)} Expenses...")
    query = """
    UNWIND $batch AS row
    MERGE (ex:Expense {id: row.id})
    SET ex.date = row.date,
        ex.content = row.content,
        ex.amount = toFloat(coalesce(nullif(row.amount, ''), '0')),
        ex.total_amount = toFloat(coalesce(nullif(row.total_amount, ''), '0')),
        ex.category = row.category,
        ex.payment_method = row.payment_method,
        ex.requester = row.requester,
        ex.spender_name = row.spender_name,
        ex.status = row.status,
        ex.project_code = row.project_code,
        ex.notes = row.notes,
        ex.created_at = row.created_at
    WITH ex, row
    WHERE row.project_code IS NOT NULL AND row.project_code <> ''
    MATCH (p:Project {code: row.project_code})
    MERGE (p)-[:INCURRED_EXPENSE]->(ex)
    """
    for batch in chunk_list(rows, 500):
        session.run(query, batch=batch)

def ingest_materials(session):
    rows = read_sanitized_csv("materials.csv")
    if not rows: return
    logger.info(f"🚀 Nạp {len(rows)} Materials...")
    query = """
    UNWIND $batch AS row
    MERGE (mat:Material {id: row.id})
    SET mat.code = row.code,
        mat.name = row.name,
        mat.english_name = row.english_name,
        mat.project_code = row.project_code,
        mat.project_name = row.project_name,
        mat.volume = toFloat(coalesce(nullif(row.volume, ''), '0')),
        mat.unit = row.unit,
        mat.unit_price = toFloat(coalesce(nullif(row.unit_price, ''), '0')),
        mat.status = row.status,
        mat.constr_status = row.constr_status,
        mat.supplier = row.supplier,
        mat.current_stock = toFloat(coalesce(nullif(row.current_stock, ''), '0')),
        mat.total_import = toFloat(coalesce(nullif(row.total_import, ''), '0')),
        mat.total_export = toFloat(coalesce(nullif(row.total_export, ''), '0')),
        mat.category = row.category,
        mat.specs = row.specs,
        mat.notes = row.notes,
        mat.created_at = row.created_at
    WITH mat, row
    WHERE row.project_code IS NOT NULL AND row.project_code <> ''
    MATCH (p:Project {code: row.project_code})
    MERGE (p)-[:STOCKS_MATERIAL]->(mat)
    """
    for batch in chunk_list(rows, 500):
        session.run(query, batch=batch)

def ingest_field_logs(session):
    rows = read_sanitized_csv("field_logs.csv")
    if not rows: return
    logger.info(f"🚀 Nạp {len(rows)} Field Logs...")
    query = """
    UNWIND $batch AS row
    MERGE (fl:FieldLog {id: row.id})
    SET fl.date = row.date,
        fl.weather = row.weather,
        fl.temperature = row.temperature,
        fl.work_completed = row.work_completed,
        fl.equipment_used = row.equipment_used,
        fl.workers_present = toInteger(coalesce(nullif(row.workers_present, ''), '0')),
        fl.issues = row.issues,
        fl.notes = row.notes,
        fl.created_by = row.created_by,
        fl.project_code = row.project_code,
        fl.task_id = row.task_id,
        fl.created_at = row.created_at
    WITH fl, row
    WHERE row.project_code IS NOT NULL AND row.project_code <> ''
    MATCH (p:Project {code: row.project_code})
    MERGE (p)-[:HAS_FIELD_LOG]->(fl)
    """
    for batch in chunk_list(rows, 500):
        session.run(query, batch=batch)

    session.run("""
    MATCH (fl:FieldLog)
    WHERE fl.task_id IS NOT NULL AND fl.task_id <> ''
    MATCH (t:Task {id: fl.task_id})
    MERGE (fl)-[:LOGGED_FOR]->(t)
    """)

    session.run("""
    MATCH (fl:FieldLog)
    WHERE fl.created_by IS NOT NULL AND fl.created_by <> ''
    MATCH (e:Engineer {name: fl.created_by})
    MERGE (e)-[:SUBMITTED_LOG]->(fl)
    """)

def main():
    logger.info("===============================================================")
    logger.info("📦 BẮT ĐẦU NẠP ENTERPRISE KNOWLEDGE GRAPH VÀO NEO4J")
    logger.info(f"🔗 Neo4j URI: {NEO4J_URI}")
    logger.info("===============================================================")

    driver = GraphDatabase.driver(NEO4J_URI, auth=(NEO4J_USER, NEO4J_PASSWORD))
    with driver.session() as session:
        init_neo4j_constraints(session)
        ingest_projects(session)
        ingest_engineers(session)
        ingest_tasks(session)
        ingest_material_plans(session)
        ingest_purchasing_plans(session)
        ingest_expenses(session)
        ingest_materials(session)
        ingest_field_logs(session)

        # Verification summary query
        result = session.run("""
        MATCH (n)
        RETURN labels(n)[0] AS Label, count(n) AS Count
        ORDER BY Count DESC
        """)
        logger.info("\n📊 TỔNG KẾT NODE ĐÃ NẠP VÀO NEO4J:")
        for r in result:
            logger.info(f"   - {r['Label'].ljust(20)}: {r['Count']} nodes")

        rel_result = session.run("""
        MATCH ()-[r]->()
        RETURN type(r) AS Relationship, count(r) AS Count
        ORDER BY Count DESC
        """)
        logger.info("\n🔗 TỔNG KẾT RELATIONSHIP TRÊN ĐỒ THỊ:")
        for r in rel_result:
            logger.info(f"   - {r['Relationship'].ljust(20)}: {r['Count']} links")

    driver.close()
    logger.info("===============================================================")
    logger.info("🎉 HOÀN TẤT NẠP KNOWLEDGE GRAPH THÀNH CÔNG!")
    logger.info("===============================================================")

if __name__ == "__main__":
    main()
