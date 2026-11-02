import fs from 'fs';
import path from 'path';
import { parse } from 'csv-parse';

const RAW_DIR = path.resolve('datasets');
const SANITIZED_DIR = path.resolve('datasets_sanitized');
const MALFORMED_DIR = path.join(SANITIZED_DIR, '_malformed_records');

// Danh sách trường nhạy cảm cần loại bỏ theo từng bảng
const SENSITIVE_FIELDS_MAP = {
  engineers: ['password', 'phone', 'email'],
  direct_messages: ['content', 'file_url'],
  document_tracks: ['phone', 'address']
};

// Bảng cách ly hoàn toàn (không đưa vào sanitized)
const ISOLATED_TABLES = ['direct_messages'];

function escapeCsvCell(cell) {
  if (cell === null || cell === undefined) return '';
  const str = String(cell);
  if (str.includes(',') || str.includes('"') || str.includes('\n') || str.includes('\r')) {
    return `"${str.replace(/"/g, '""')}"`;
  }
  return str;
}

async function processTable(tableName) {
  const csvFile = path.join(RAW_DIR, `${tableName}.csv`);
  if (!fs.existsSync(csvFile)) return null;

  const sanitizedCsvFile = path.join(SANITIZED_DIR, `${tableName}.csv`);
  const malformedFile = path.join(MALFORMED_DIR, `${tableName}_malformed.json`);

  let headers = null;
  let totalRecords = 0;
  let malformedCount = 0;
  const malformedErrors = [];

  const colStats = {};
  const valuesSet = {};
  let keepColIndices = [];
  let sanitizedWriter = null;

  const sensitiveForTable = SENSITIVE_FIELDS_MAP[tableName] || [];

  // Khởi tạo streaming RFC 4180 CSV parser
  const parser = fs.createReadStream(csvFile, { encoding: 'utf8' }).pipe(
    parse({
      bom: true,             // Tự động xử lý UTF-8 BOM
      relax_quotes: true,    // Chấp nhận quote linh hoạt
      relax_column_count: true, // Không crash khi gặp dòng lệch cột mà bắt lỗi
      skip_empty_lines: true
    })
  );

  for await (const record of parser) {
    if (!headers) {
      headers = record.map(h => h.trim());
      headers.forEach((h, idx) => {
        colStats[h] = { nonNullCount: 0, distinctCount: 0, isUnique: false };
        valuesSet[h] = new Set();
        if (!sensitiveForTable.includes(h)) {
          keepColIndices.push(idx);
        }
      });

      if (!ISOLATED_TABLES.includes(tableName)) {
        sanitizedWriter = fs.createWriteStream(sanitizedCsvFile, { encoding: 'utf8' });
        sanitizedWriter.write('\ufeff'); // UTF-8 BOM cho Excel
        const sanitizedHeaders = keepColIndices.map(i => headers[i]);
        sanitizedWriter.write(sanitizedHeaders.map(escapeCsvCell).join(',') + '\n');
      }
      continue;
    }

    // Kiểm tra cấu trúc record
    if (record.length !== headers.length) {
      malformedCount++;
      if (malformedErrors.length < 5) {
        malformedErrors.push({
          recordIndex: totalRecords + 1,
          expectedColumns: headers.length,
          actualColumns: record.length,
          reason: record.length < headers.length ? 'Cột bị thiếu (underflow)' : 'Cột bị thừa (overflow)'
        });
      }
      continue; // Cách ly bản ghi lỗi, không để lọt vào dữ liệu sạch
    }

    totalRecords++;

    // Thống kê giá trị
    headers.forEach((h, idx) => {
      const val = record[idx];
      if (val !== undefined && val !== null && val.trim() !== '') {
        colStats[h].nonNullCount++;
        if (valuesSet[h].size < 35000) {
          valuesSet[h].add(val.trim());
        }
      }
    });

    // Ghi vào file sanitized
    if (sanitizedWriter) {
      const sanitizedCells = keepColIndices.map(i => record[i] || '');
      sanitizedWriter.write(sanitizedCells.map(escapeCsvCell).join(',') + '\n');
    }
  }

  if (sanitizedWriter) {
    sanitizedWriter.end();
  }

  // Ghi nhật ký cách ly nếu có lỗi
  if (malformedCount > 0) {
    fs.mkdirSync(MALFORMED_DIR, { recursive: true });
    fs.writeFileSync(malformedFile, JSON.stringify({
      table: tableName,
      total_malformed: malformedCount,
      sample_issues: malformedErrors
    }, null, 2), 'utf8');
  }

  // Xác định khóa ứng viên (Key Candidates)
  const keyCandidates = [];
  if (headers && totalRecords > 0) {
    headers.forEach(h => {
      colStats[h].distinctCount = valuesSet[h].size;
      colStats[h].isUnique = (colStats[h].nonNullCount === totalRecords) && (colStats[h].distinctCount === totalRecords);
      if (colStats[h].isUnique) {
        keyCandidates.push(h);
      }
    });
  }

  return {
    tableName,
    totalRecords,
    malformedCount,
    columnCount: headers ? headers.length : 0,
    headers: headers || [],
    colStats,
    keyCandidates,
    removedSensitiveColumns: sensitiveForTable,
    isIsolated: ISOLATED_TABLES.includes(tableName),
    valuesSet
  };
}

async function run() {
  console.log("===============================================================");
  console.log("🛠️ KHỞI CHẠY PROFILER MỚI: RFC 4180 MULTILINE CSV PARSER");
  console.log(`📁 Thư mục nguồn (nguyên vẹn): ${RAW_DIR}`);
  console.log(`📁 Thư mục đã làm sạch: ${SANITIZED_DIR}`);
  console.log("===============================================================");

  if (!fs.existsSync(SANITIZED_DIR)) {
    fs.mkdirSync(SANITIZED_DIR, { recursive: true });
  }

  const rawFiles = fs.readdirSync(RAW_DIR).filter(f => f.endsWith('.csv') && !f.includes('_utf8_bom'));
  const tableNames = rawFiles.map(f => f.replace('.csv', ''));

  const profiles = {};
  const emptyTables = [];
  const populatedTables = [];

  for (const t of tableNames) {
    try {
      const p = await processTable(t);
      if (!p) continue;
      profiles[t] = p;
      if (p.totalRecords === 0) {
        emptyTables.push(t);
      } else {
        populatedTables.push(t);
      }
      console.log(`✅ [${t.padEnd(25)}] -> ${String(p.totalRecords).padStart(6)} bản ghi | Lỗi cách ly: ${p.malformedCount}`);
    } catch (err) {
      console.error(`❌ Lỗi đọc bảng ${t}:`, err.message);
    }
  }

  console.log(`\n📊 TỔNG KẾT BẢNG:`);
  console.log(`- Bảng có dữ liệu (${populatedTables.length}): ${populatedTables.join(', ')}`);
  console.log(`- Bảng rỗng (${emptyTables.length}): ${emptyTables.join(', ')}`);

  console.log(`\n🔗 KIỂM TRA TÍNH TOÀN VẸN KHÓA NGOẠI (FOREIGN KEY INTEGRITY):`);

  function checkReference(fromTable, fromCol, toTable, toCol) {
    const sourceProfile = profiles[fromTable];
    const targetProfile = profiles[toTable];
    if (!sourceProfile || !targetProfile) return null;

    const sourceVals = sourceProfile.valuesSet[fromCol];
    const targetVals = targetProfile.valuesSet[toCol];
    if (!sourceVals || !targetVals) return null;

    let matched = 0;
    let unmatched = 0;

    for (const val of sourceVals) {
      if (targetVals.has(val)) {
        matched++;
      } else {
        unmatched++;
      }
    }

    const totalDistinct = sourceVals.size;
    const matchRate = totalDistinct > 0 ? ((matched / totalDistinct) * 100).toFixed(1) : "0.0";
    return {
      from: `${fromTable}.${fromCol}`,
      to: `${toTable}.${toCol}`,
      sourceDistinctCount: totalDistinct,
      matched,
      unmatched,
      matchRate: `${matchRate}%`,
      isValid: unmatched === 0 && totalDistinct > 0
    };
  }

  const fkChecks = [
    // Project Code links
    checkReference('tasks', 'project_code', 'projects', 'code'),
    checkReference('material_plans', 'project_code', 'projects', 'code'),
    checkReference('purchasing_plans', 'project_code', 'projects', 'code'),
    checkReference('expenses', 'project_code', 'projects', 'code'),
    checkReference('field_logs', 'project_code', 'projects', 'code'),
    checkReference('materials', 'project_code', 'projects', 'code'),
    checkReference('document_tracks', 'project_code', 'projects', 'code'),

    // Engineer links
    checkReference('tasks', 'assigned_engineer_id', 'engineers', 'id'),
    checkReference('projects', 'manager_id', 'engineers', 'id'),
    checkReference('field_logs', 'created_by', 'engineers', 'name'),

    // Material Plan links
    checkReference('purchasing_plans', 'material_plan_id', 'material_plans', 'id'),
    checkReference('material_plans', 'parent_id', 'material_plans', 'id'),
    checkReference('tasks', 'parent_id', 'tasks', 'id'),

    // Field log to Task
    checkReference('field_logs', 'task_id', 'tasks', 'id')
  ].filter(Boolean);

  fkChecks.forEach(chk => {
    const icon = chk.isValid ? '✅' : (parseFloat(chk.matchRate) >= 90 ? '⚠️' : '❌');
    console.log(`${icon} [${chk.from} -> ${chk.to}]: Khớp ${chk.matched}/${chk.sourceDistinctCount} giá trị phân biệt (${chk.matchRate}) - Lệch: ${chk.unmatched}`);
  });

  // Lưu báo cáo (tuyệt đối không ghi sample raw text nhạy cảm)
  const serializableProfiles = {};
  for (const [t, p] of Object.entries(profiles)) {
    const { valuesSet, ...cleanProfile } = p;
    serializableProfiles[t] = cleanProfile;
  }

  const reportPath = path.join(SANITIZED_DIR, '_data_profiling_and_cleaning_report.json');
  fs.writeFileSync(reportPath, JSON.stringify({
    generated_at: new Date().toISOString(),
    parser_standard: "RFC 4180 Multiline CSV Stream Parser",
    summary: {
      total_tables: tableNames.length,
      populated_tables: populatedTables,
      empty_tables: emptyTables
    },
    tables: serializableProfiles,
    fk_validation_matrix: fkChecks
  }, null, 2), 'utf8');

  console.log(`\n===============================================================`);
  console.log(`✨ BÁO CÁO MỚI ĐÃ LƯU: datasets_sanitized/_data_profiling_and_cleaning_report.json`);
  console.log(`🛡️ DỮ LIỆU ĐÃ LÀM SẠCH VÀ AN TOÀN ĐÃ LƯU TẠI: /datasets_sanitized`);
  console.log("===============================================================");
}

run().catch(console.error);
