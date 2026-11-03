import React from 'react';
import type { CitationSource } from '../types';
import { Database, Calendar, Hash, FolderGit2, ExternalLink } from 'lucide-react';

interface CitationsListProps {
  sources: CitationSource[];
}

export const CitationsList: React.FC<CitationsListProps> = ({ sources }) => {
  if (!sources || sources.length === 0) return null;

  return (
    <div className="mt-3 pt-3 border-t border-slate-700/60 flex flex-col gap-2.5">
      <div className="flex items-center gap-1.5 text-xs font-semibold text-cyan-400">
        <Database className="w-3.5 h-3.5" />
        <span>Nguồn trích dẫn & Dấu vết dữ liệu ({sources.length}):</span>
      </div>

      <div className="grid grid-cols-1 md:grid-cols-2 gap-2.5">
        {sources.map((s, idx) => {
          const isInternal = !!s.source_table;
          return (
            <div
              key={idx}
              className="p-3 rounded-lg bg-slate-900/90 border border-slate-700/80 hover:border-slate-600 transition flex flex-col justify-between text-xs"
            >
              <div>
                {/* Header: Title / Source Table */}
                <div className="flex items-center justify-between gap-2 mb-1.5">
                  <span className="font-semibold text-slate-200 truncate flex items-center gap-1.5">
                    {s.article_title}
                  </span>
                  <span className="text-[10px] font-mono px-1.5 py-0.5 rounded bg-cyan-950 text-cyan-300 border border-cyan-800">
                    Score: {typeof s.score === 'number' ? s.score.toFixed(3) : s.score}
                  </span>
                </div>

                {/* Structured Metadata Badges */}
                <div className="flex flex-wrap items-center gap-2 mb-2 text-[11px] text-slate-400">
                  {s.source_table && (
                    <span className="px-1.5 py-0.5 rounded bg-slate-800 text-purple-300 font-mono">
                      Bảng: {s.source_table}
                    </span>
                  )}
                  {s.project_code && (
                    <span className="flex items-center gap-1 px-1.5 py-0.5 rounded bg-slate-800 text-cyan-300 font-mono">
                      <FolderGit2 className="w-3 h-3 text-cyan-400" />
                      {s.project_code}
                    </span>
                  )}
                  {s.date && (
                    <span className="flex items-center gap-1 px-1.5 py-0.5 rounded bg-slate-800 text-slate-300">
                      <Calendar className="w-3 h-3 text-slate-400" />
                      {s.date}
                    </span>
                  )}
                  {s.source_id && (
                    <span className="flex items-center gap-1 font-mono text-[10px] text-slate-400 truncate max-w-[140px]" title={s.source_id}>
                      <Hash className="w-3 h-3 text-slate-500" />
                      {s.source_id.slice(0, 8)}...
                    </span>
                  )}
                </div>

                {/* Text Excerpt */}
                <p className="text-[11px] text-slate-300 line-clamp-3 italic bg-slate-950/60 p-2 rounded border border-slate-800/80">
                  "{s.text.trim()}"
                </p>
              </div>

              {/* Bottom links or categories */}
              <div className="mt-2 pt-1.5 border-t border-slate-800/80 flex items-center justify-between text-[10px] text-slate-400">
                <div className="flex flex-wrap gap-1">
                  {s.related_categories?.slice(0, 2).map((cat, i) => (
                    <span key={i} className="px-1 py-0.5 rounded bg-slate-800/80 text-slate-400">
                      #{cat}
                    </span>
                  ))}
                </div>
                {s.article_url && (
                  <a
                    href={s.article_url}
                    target="_blank"
                    rel="noreferrer"
                    className="text-cyan-400 hover:text-cyan-300 flex items-center gap-0.5"
                  >
                    Xem chi tiết <ExternalLink className="w-3 h-3" />
                  </a>
                )}
                {isInternal && (
                  <span className="text-[10px] text-emerald-400 font-medium">
                    ✓ Nội bộ xác thực
                  </span>
                )}
              </div>
            </div>
          );
        })}
      </div>
    </div>
  );
};
