import React from 'react';
import type { Conversation } from '../types';
import { Plus, MessageSquare, Trash2, FolderKanban } from 'lucide-react';

interface ConversationSidebarProps {
  conversations: Conversation[];
  activeId: string;
  onSelect: (id: string) => void;
  onNew: () => void;
  onDelete: (id: string, e: React.MouseEvent) => void;
}

export const ConversationSidebar: React.FC<ConversationSidebarProps> = ({
  conversations,
  activeId,
  onSelect,
  onNew,
  onDelete
}) => {
  return (
    <aside className="w-64 border-r border-slate-800 bg-slate-950/70 flex flex-col h-full flex-shrink-0">
      <div className="p-3 border-b border-slate-800">
        <button
          onClick={onNew}
          className="w-full flex items-center justify-center gap-2 py-2 px-3 rounded-lg bg-cyan-600 hover:bg-cyan-500 text-white text-xs font-semibold shadow-md transition cursor-pointer"
        >
          <Plus className="w-4 h-4" />
          Hội thoại mới
        </button>
      </div>

      <div className="flex-1 overflow-y-auto p-2 space-y-1">
        <div className="text-[11px] font-semibold text-slate-400 px-2 py-1 uppercase tracking-wider flex items-center gap-1.5">
          <FolderKanban className="w-3.5 h-3.5 text-cyan-400" />
          Lịch sử trò chuyện
        </div>

        {conversations.length === 0 ? (
          <div className="text-xs text-slate-400 text-center py-6">
            Chưa có hội thoại nào
          </div>
        ) : (
          conversations.map((c) => {
            const isActive = c.id === activeId;
            return (
              <div
                key={c.id}
                onClick={() => onSelect(c.id)}
                className={`group flex items-center justify-between p-2 rounded-lg text-xs cursor-pointer transition ${
                  isActive
                    ? 'bg-slate-800/90 text-cyan-300 font-medium border border-slate-700'
                    : 'text-slate-300 hover:bg-slate-900/80 hover:text-white'
                }`}
              >
                <div className="flex items-center gap-2 truncate mr-1">
                  <MessageSquare className={`w-3.5 h-3.5 flex-shrink-0 ${isActive ? 'text-cyan-400' : 'text-slate-400'}`} />
                  <span className="truncate">{c.title || 'Hội thoại'}</span>
                </div>

                <button
                  onClick={(e) => onDelete(c.id, e)}
                  title="Xóa hội thoại"
                  className="opacity-0 group-hover:opacity-100 p-1 hover:text-rose-400 text-slate-400 transition"
                >
                  <Trash2 className="w-3.5 h-3.5" />
                </button>
              </div>
            );
          })
        )}
      </div>

      <div className="p-3 border-t border-slate-800 text-[11px] text-slate-400 flex flex-col gap-1">
        <div className="flex items-center justify-between">
          <span>Engine: GraphRAG + Cypher</span>
          <span className="text-emerald-400 font-semibold">Online</span>
        </div>
      </div>
    </aside>
  );
};
