import React, { useRef, useEffect } from 'react';
import type { ChatMessage } from '../types';
import { CitationsList } from './CitationsList';
import { Bot, User, AlertCircle, RefreshCw, Loader2, Sparkles } from 'lucide-react';

interface ChatMessageListProps {
  messages: ChatMessage[];
  loading: boolean;
  onRetry: (message: ChatMessage) => void;
  onSelectSuggested: (q: string) => void;
}

const SUGGESTED_QUESTIONS = [
  "Đang có bao nhiêu công việc của tất cả dự án?",
  "Dự án Phần mềm công ty có bao nhiêu công việc?",
  "Tính năng chấm công đang tiến độ thế nào?",
  "Task chấm công có ghi chú kỹ thuật gì?",
  "Nhân viên nào đã chấm công ngày hôm nay?",
  "Danh sách dự án có những dự án nào?",
  "Vật tư nào chưa đặt hàng thuộc dự án Trạm biến áp 110kV Phước Đông 6?"
];

export const ChatMessageList: React.FC<ChatMessageListProps> = ({
  messages,
  loading,
  onRetry,
  onSelectSuggested
}) => {
  const bottomRef = useRef<HTMLDivElement>(null);

  useEffect(() => {
    bottomRef.current?.scrollIntoView({ behavior: 'smooth' });
  }, [messages, loading]);

  if (messages.length === 0) {
    return (
      <div className="flex-1 flex flex-col items-center justify-center p-8 text-center">
        <div className="w-14 h-14 rounded-2xl bg-gradient-to-tr from-cyan-500/20 to-indigo-600/20 border border-cyan-500/30 flex items-center justify-center mb-4 text-cyan-400 shadow-xl shadow-cyan-500/10">
          <Sparkles className="w-7 h-7" />
        </div>
        <h3 className="text-lg font-bold text-slate-100 mb-2">
          Trợ lý Tri thức Dự án Synapse Copilot
        </h3>
        <p className="text-sm text-slate-400 max-w-md mb-6 leading-relaxed">
          Nhập câu hỏi tiếng Việt tự nhiên để tra cứu tiến độ, khối lượng công việc, kế hoạch vật tư và ghi chú kỹ thuật với kiểm soát phân quyền RBAC đa dự án.
        </p>

        <div className="w-full max-w-xl">
          <div className="text-xs font-semibold text-slate-500 uppercase tracking-wider mb-2 text-left">
            Gợi ý câu hỏi nhanh:
          </div>
          <div className="flex flex-wrap gap-2 justify-start">
            {SUGGESTED_QUESTIONS.map((q, idx) => (
              <button
                key={idx}
                onClick={() => onSelectSuggested(q)}
                className="text-xs text-left px-3 py-2 rounded-lg bg-slate-900 border border-slate-800 hover:border-cyan-500/50 hover:bg-slate-800/80 text-slate-300 hover:text-cyan-300 transition cursor-pointer"
              >
                {q}
              </button>
            ))}
          </div>
        </div>
      </div>
    );
  }

  return (
    <div className="flex-1 overflow-y-auto p-4 space-y-4">
      {messages.map((msg) => {
        const isUser = msg.sender === 'user';
        const isError = msg.status === 'error';

        return (
          <div
            key={msg.id}
            className={`flex gap-3 ${isUser ? 'justify-end' : 'justify-start'}`}
          >
            {!isUser && (
              <div className="w-8 h-8 rounded-lg bg-gradient-to-tr from-cyan-600 to-indigo-600 flex-shrink-0 flex items-center justify-center text-white shadow-md">
                <Bot className="w-4 h-4" />
              </div>
            )}

            <div
              className={`max-w-[85%] md:max-w-[75%] rounded-2xl p-4 text-sm shadow-md transition-all ${
                isUser
                  ? 'bg-gradient-to-r from-cyan-600 to-indigo-600 text-white rounded-br-none'
                  : isError
                  ? 'bg-rose-950/80 border border-rose-800 text-rose-200 rounded-bl-none'
                  : 'bg-slate-900/90 border border-slate-800 text-slate-200 rounded-bl-none'
              }`}
            >
              {/* Header/Timestamp */}
              <div className="flex items-center justify-between gap-3 mb-1 text-[11px] opacity-75">
                <span className="font-semibold">
                  {isUser ? 'Bạn' : 'Trợ lý Synapse Copilot'}
                </span>
                <span className="font-mono">
                  {new Date(msg.timestamp).toLocaleTimeString([], { hour: '2-digit', minute: '2-digit' })}
                </span>
              </div>

              {/* Message Content */}
              <div className="leading-relaxed whitespace-pre-wrap select-text">
                {msg.text}
              </div>

              {/* Streaming Spinner */}
              {msg.status === 'streaming' && (
                <div className="flex items-center gap-2 mt-2 pt-2 border-t border-slate-800 text-xs text-cyan-400 font-mono">
                  <Loader2 className="w-3.5 h-3.5 animate-spin" />
                  <span>Đang tổng hợp câu trả lời và trích xuất nguồn...</span>
                </div>
              )}

              {/* Error Box & Retry */}
              {isError && (
                <div className="mt-3 pt-2 border-t border-rose-800/80 flex items-center justify-between text-xs text-rose-300">
                  <span className="flex items-center gap-1.5">
                    <AlertCircle className="w-4 h-4 text-rose-400 flex-shrink-0" />
                    {msg.errorMessage || 'Xử lý truy vấn thất bại'}
                  </span>
                  <button
                    onClick={() => onRetry(msg)}
                    className="flex items-center gap-1 px-2.5 py-1 rounded bg-rose-900/80 hover:bg-rose-800 text-white font-medium transition cursor-pointer"
                  >
                    <RefreshCw className="w-3 h-3" />
                    Thử lại
                  </button>
                </div>
              )}

              {/* Citations if available */}
              {msg.sources && msg.sources.length > 0 && (
                <CitationsList sources={msg.sources} />
              )}

              {/* Duration badge */}
              {msg.durationMs !== undefined && msg.durationMs > 0 && !isError && (
                <div className="mt-2 text-right">
                  <span className="text-[10px] font-mono text-slate-400 px-1.5 py-0.5 rounded bg-slate-950/60 border border-slate-800">
                    Xử lý: {msg.durationMs}ms
                  </span>
                </div>
              )}
            </div>

            {isUser && (
              <div className="w-8 h-8 rounded-lg bg-slate-800 border border-slate-700 flex-shrink-0 flex items-center justify-center text-slate-300">
                <User className="w-4 h-4" />
              </div>
            )}
          </div>
        );
      })}
      <div ref={bottomRef} />
    </div>
  );
};
