import React from 'react';
import { CheckCircle2, Clock, AlertCircle, Loader2 } from 'lucide-react';

export interface TimelineStep {
  stepId: number;
  name: string;
  status: 'pending' | 'running' | 'completed' | 'failed';
  durationMs?: number;
  details?: string;
  metrics?: { key: string; value: string }[];
}

interface TimelineProps {
  steps: TimelineStep[];
  totalDurationMs?: number;
}

export const Timeline: React.FC<TimelineProps> = ({ steps, totalDurationMs }) => {
  return (
    <div className="flex flex-col gap-3 p-4 bg-slate-900/90 rounded-xl border border-slate-800 h-full overflow-y-auto">
      <div className="flex items-center justify-between pb-2 border-b border-slate-800">
        <h3 className="text-sm font-semibold text-slate-200 flex items-center gap-2">
          <Clock className="w-4 h-4 text-cyan-400" />
          Tiến trình xử lý thời gian thực (Trace Timeline)
        </h3>
        {totalDurationMs !== undefined && totalDurationMs > 0 && (
          <span className="text-xs bg-cyan-950 text-cyan-300 px-2 py-0.5 rounded border border-cyan-800 font-mono">
            Tổng: {totalDurationMs}ms
          </span>
        )}
      </div>

      <div className="flex flex-col gap-3 mt-1">
        {steps.map((s) => {
          let badgeColor = 'bg-slate-800 text-slate-400 border-slate-700';
          let icon = <Clock className="w-4 h-4 text-slate-500" />;

          if (s.status === 'running') {
            badgeColor = 'bg-amber-950/70 text-amber-300 border-amber-600/50';
            icon = <Loader2 className="w-4 h-4 text-amber-400 animate-spin" />;
          } else if (s.status === 'completed') {
            badgeColor = 'bg-emerald-950/70 text-emerald-300 border-emerald-600/50';
            icon = <CheckCircle2 className="w-4 h-4 text-emerald-400" />;
          } else if (s.status === 'failed') {
            badgeColor = 'bg-rose-950/70 text-rose-300 border-rose-600/50';
            icon = <AlertCircle className="w-4 h-4 text-rose-400" />;
          }

          return (
            <div
              key={s.stepId}
              className={`p-3 rounded-lg border transition-all ${badgeColor}`}
            >
              <div className="flex items-center justify-between">
                <div className="flex items-center gap-2">
                  {icon}
                  <span className="font-semibold text-xs tracking-wide">{s.name}</span>
                </div>
                {s.durationMs !== undefined && s.durationMs > 0 && (
                  <span className="text-[11px] font-mono text-slate-400">
                    {s.durationMs}ms
                  </span>
                )}
              </div>

              {s.details && (
                <p className="text-[12px] text-slate-300 mt-1.5 leading-relaxed">
                  {s.details}
                </p>
              )}

              {s.metrics && s.metrics.length > 0 && (
                <div className="mt-2 pt-2 border-t border-slate-700/40 grid grid-cols-2 gap-1.5 text-[11px] font-mono">
                  {s.metrics.map((m, idx) => (
                    <div key={idx} className="bg-slate-950/60 px-2 py-1 rounded">
                      <span className="text-slate-500">{m.key}: </span>
                      <span className="text-cyan-300">{m.value}</span>
                    </div>
                  ))}
                </div>
              )}
            </div>
          );
        })}
      </div>
    </div>
  );
};
