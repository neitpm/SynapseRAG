import fs from 'fs';
import path from 'path';

const SUPABASE_URL = process.env.NEXT_PUBLIC_SUPABASE_URL || 'https://nvdonaaxbtqjfmxtlgzb.supabase.co';
const SUPABASE_KEY = process.env.NEXT_PUBLIC_SUPABASE_PUBLISHABLE_KEY || 'sb_publishable_gzUeVF_f2jadDuuii66pCw_W_0xmqjg';

const DATASETS_DIR = path.resolve('datasets');

const TABLES = [
  'activity_logs',
  'app_versions',
  'direct_messages',
  'document_tracks',
  'engineers',
  'expenses',
  'field_logs',
  'inventory_transactions',
  'issue_timeline_logs',
  'issues',
  'labor_payrolls',
  'material_plans',
  'materials',
  'notifications',
  'project_members',
  'projects',
  'purchasing_plans',
  'tasks'
];

const PAGE_SIZE = 1000;

async function fetchPage(table, offset, limit, format = 'csv') {
  const url = `${SUPABASE_URL}/rest/v1/${table}?select=*&offset=${offset}&limit=${limit}`;
  const headers = {
    'apikey': SUPABASE_KEY,
    'Authorization': `Bearer ${SUPABASE_KEY}`,
    'Accept': format === 'csv' ? 'text/csv' : 'application/json',
    'Prefer': 'count=exact'
  };

  const response = await fetch(url, { headers });
  if (!response.ok) {
    const errorText = await response.text();
    throw new Error(`Failed to fetch ${table} (status ${response.status}): ${errorText}`);
  }

  const contentRange = response.headers.get('content-range');
  let totalCount = null;
  if (contentRange && contentRange.includes('/')) {
    const totalStr = contentRange.split('/')[1];
    if (totalStr !== '*') {
      totalCount = parseInt(totalStr, 10);
    }
  }

  const data = format === 'csv' ? await response.text() : await response.json();
  return { data, totalCount };
}

function inferType(val) {
  if (val === null || val === undefined) return 'text/nullable';
  if (typeof val === 'boolean') return 'boolean';
  if (typeof val === 'number') {
    return Number.isInteger(val) ? 'integer' : 'numeric';
  }
  if (typeof val === 'object') return 'jsonb';
  if (typeof val === 'string') {
    if (/^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/i.test(val)) return 'uuid';
    if (/^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}/.test(val)) return 'timestamp with time zone';
    return 'text';
  }
  return 'text';
}

async function exportTable(table) {
  console.log(`\n======================================================`);
  console.log(`🚀 Bắt đầu export bảng: ${table}`);

  const csvFilePath = path.join(DATASETS_DIR, `${table}.csv`);
  const schemaFilePath = path.join(DATASETS_DIR, `${table}_schema.json`);

  // First fetch row sample as JSON for schema analysis
  let schema = {
    table,
    total_rows: 0,
    columns: {},
    sample_row: null
  };

  try {
    const sampleRes = await fetchPage(table, 0, 1, 'json');
    schema.total_rows = sampleRes.totalCount ?? 0;
    if (Array.isArray(sampleRes.data) && sampleRes.data.length > 0) {
      const sample = sampleRes.data[0];
      schema.sample_row = sample;
      for (const [col, val] of Object.entries(sample)) {
        schema.columns[col] = {
          sample_value: val,
          inferred_type: inferType(val)
        };
      }
    }
  } catch (err) {
    console.warn(`⚠️ Không thể lấy schema JSON cho ${table}:`, err.message);
  }

  // Fetch full CSV with pagination
  const writeStream = fs.createWriteStream(csvFilePath, { encoding: 'utf8' });
  writeStream.write('\ufeff'); // UTF-8 BOM so Microsoft Excel renders Vietnamese correctly
  let offset = 0;
  let fetchedRows = 0;
  let totalRows = schema.total_rows;
  let isFirstBatch = true;

  while (true) {
    const { data: pageText, totalCount } = await fetchPage(table, offset, PAGE_SIZE, 'csv');
    if (totalCount !== null) {
      totalRows = totalCount;
      schema.total_rows = totalRows;
    }

    if (!pageText || pageText.trim().length === 0) {
      break;
    }

    const lines = pageText.split('\n');
    // First line is header
    if (isFirstBatch) {
      writeStream.write(pageText);
      isFirstBatch = false;
      fetchedRows += (lines.length - 1);
    } else {
      // Omit CSV header for subsequent pages
      const rowsWithoutHeader = lines.slice(1).join('\n');
      if (rowsWithoutHeader.length > 0) {
        writeStream.write('\n' + rowsWithoutHeader);
        fetchedRows += (lines.length - 1);
      }
    }

    process.stdout.write(`   Đã tải: ${Math.min(fetchedRows, totalRows || fetchedRows)} / ${totalRows ?? '?'} dòng...\r`);

    if (totalRows !== null && fetchedRows >= totalRows) {
      break;
    }

    offset += PAGE_SIZE;
    if (lines.length <= 1) {
      break;
    }
  }

  writeStream.end();
  fs.writeFileSync(schemaFilePath, JSON.stringify(schema, null, 2), 'utf8');

  console.log(`\n✅ Hoàn tất [${table}]: ${fetchedRows} dòng -> ${table}.csv`);
  return {
    table,
    rows: fetchedRows,
    columns: Object.keys(schema.columns).length,
    columns_list: Object.keys(schema.columns)
  };
}

async function main() {
  console.log(`📦 Bắt đầu xuất toàn bộ cơ sở dữ liệu Supabase vào: ${DATASETS_DIR}`);
  console.log(`🔗 Target URL: ${SUPABASE_URL}`);
  
  if (!fs.existsSync(DATASETS_DIR)) {
    fs.mkdirSync(DATASETS_DIR, { recursive: true });
  }

  const results = [];
  for (const table of TABLES) {
    try {
      const res = await exportTable(table);
      results.push(res);
    } catch (err) {
      console.error(`❌ Lỗi khi xuất bảng ${table}:`, err.message);
      results.push({ table, rows: 0, columns: 0, error: err.message });
    }
  }

  // Write comprehensive master schema summary
  const summaryPath = path.join(DATASETS_DIR, `_database_schema_summary.json`);
  fs.writeFileSync(summaryPath, JSON.stringify({
    exported_at: new Date().toISOString(),
    supabase_url: SUPABASE_URL,
    total_tables: TABLES.length,
    tables: results
  }, null, 2), 'utf8');

  console.log(`\n======================================================`);
  console.log(`🎉 TẤT CẢ DỮ LIỆU VÀ CẤU TRÚC ĐÃ ĐƯỢC XUẤT THÀNH CÔNG VÀO /datasets!`);
  console.log(`📄 Tổng kết cấu trúc lưu tại: datasets/_database_schema_summary.json`);
}

main().catch(err => {
  console.error("FATAL ERROR:", err);
  process.exit(1);
});
