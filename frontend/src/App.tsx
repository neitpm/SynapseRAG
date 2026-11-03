import React, { useState, useEffect } from 'react';
import { Architecture3D } from './components/Architecture3D';
import { Timeline } from './components/Timeline';
import type { TimelineStep } from './components/Timeline';
import { ChatMessageList } from './components/ChatMessageList';
import { ConversationSidebar } from './components/ConversationSidebar';
import { AuthModal } from './components/AuthModal';
import type { ChatMessage, Conversation, CitationSource } from './types';
import {
  Send,
  Cpu,
  Layers,
  KeyRound,
  PanelLeftClose,
  PanelLeftOpen,
  Eye,
  EyeOff,
  ExternalLink
} from 'lucide-react';

const API_BASE = 'http://localhost:8000';
const STORAGE_KEY_CONVS = 'synapse_chat_conversations_v1';
const STORAGE_KEY_TOKEN = 'synapse_jwt_token_v1';

export const App: React.FC = () => {
  // 1. Quản lý Token & Auth
  const [token, setToken] = useState<string>(() => {
    return localStorage.getItem(STORAGE_KEY_TOKEN) || '';
  });
  const [isAuthModalOpen, setIsAuthModalOpen] = useState(false);

  // 2. Quản lý Danh sách Hội thoại (Conversations)
  const [conversations, setConversations] = useState<Conversation[]>(() => {
    try {
      const saved = localStorage.getItem(STORAGE_KEY_CONVS);
      if (saved) return JSON.parse(saved);
    } catch (e) {
      console.warn('Lỗi đọc conversations từ localStorage:', e);
    }
    return [
      {
        id: 'default-conv-1',
        title: 'Tra cứu tiến độ & công việc',
        createdAt: Date.now(),
        messages: []
      }
    ];
  });

  const [activeConvId, setActiveConvId] = useState<string>(() => {
    return conversations[0]?.id || 'default-conv-1';
  });

  // Lưu conversations vào localStorage khi có thay đổi
  useEffect(() => {
    try {
      localStorage.setItem(STORAGE_KEY_CONVS, JSON.stringify(conversations));
    } catch (e) {
      console.warn('Không thể lưu conversations:', e);
    }
  }, [conversations]);

  // 3. Trạng thái Chat & Input
  const [inputText, setInputText] = useState('');
  const [loading, setLoading] = useState(false);
  const [sidebarOpen, setSidebarOpen] = useState(true);
  const [showArchitecture, setShowArchitecture] = useState(true);

  // 4. Trạng thái 3D Visualizer & Timeline Trace
  const [activeNode, setActiveNode] = useState<string | null>(null);
  const [activeEdge, setActiveEdge] = useState<string | null>(null);
  const [totalDuration, setTotalDuration] = useState<number | undefined>(undefined);
  const [viewMode2D, setViewMode2D] = useState(false);

  const initialSteps: TimelineStep[] = [
    { stepId: 1, name: '1. Question Embedding', status: 'pending', details: 'Sinh vector 384 chiều bên trong GraphQL Gateway.' },
    { stepId: 2, name: '2. Hybrid Search (Vector + BM25) & Graph', status: 'pending', details: 'Truy vấn kết hợp Vector Cosine + BM25 Okapi & mở rộng đồ thị Neo4j.' },
    { stepId: 3, name: '3. Answer Generation (LLM)', status: 'pending', details: 'Trích xuất Extractive / Cypher trả kết quả trực tiếp.' }
  ];

  const [steps, setSteps] = useState<TimelineStep[]>(initialSteps);

  const currentConv = conversations.find((c) => c.id === activeConvId) || conversations[0];

  const handleSaveToken = (newToken: string) => {
    setToken(newToken);
    localStorage.setItem(STORAGE_KEY_TOKEN, newToken);
  };

  const handleNewConversation = () => {
    const newId = `conv-${Date.now()}`;
    const newConv: Conversation = {
      id: newId,
      title: 'Hội thoại mới',
      createdAt: Date.now(),
      messages: []
    };
    setConversations((prev) => [newConv, ...prev]);
    setActiveConvId(newId);
  };

  const handleDeleteConversation = (id: string, e: React.MouseEvent) => {
    e.stopPropagation();
    if (conversations.length <= 1) {
      // Giữ ít nhất 1 hội thoại
      const resetConv: Conversation = {
        id: `conv-${Date.now()}`,
        title: 'Hội thoại mới',
        createdAt: Date.now(),
        messages: []
      };
      setConversations([resetConv]);
      setActiveConvId(resetConv.id);
      return;
    }

    const filtered = conversations.filter((c) => c.id !== id);
    setConversations(filtered);
    if (activeConvId === id) {
      setActiveConvId(filtered[0].id);
    }
  };

  // 5. Gửi câu hỏi qua SSE stream (kèm JWT Header)
  const sendQuestion = async (questionText: string) => {
    const cleanText = questionText.trim();
    if (!cleanText || loading) return;

    if (!token) {
      setIsAuthModalOpen(true);
      return;
    }

    const userMsgId = `user-${Date.now()}`;
    const assistantMsgId = `asst-${Date.now()}`;

    const userMessage: ChatMessage = {
      id: userMsgId,
      sender: 'user',
      text: cleanText,
      timestamp: Date.now()
    };

    const assistantPlaceholder: ChatMessage = {
      id: assistantMsgId,
      sender: 'assistant',
      text: '',
      timestamp: Date.now(),
      status: 'streaming'
    };

    // Cập nhật tin nhắn và tiêu đề hội thoại nếu là câu hỏi đầu
    setConversations((prev) =>
      prev.map((c) => {
        if (c.id === activeConvId) {
          const isFirst = c.messages.length === 0;
          return {
            ...c,
            title: isFirst ? cleanText.slice(0, 30) + (cleanText.length > 30 ? '...' : '') : c.title,
            messages: [...c.messages, userMessage, assistantPlaceholder]
          };
        }
        return c;
      })
    );

    setInputText('');
    setLoading(true);
    setSteps(initialSteps.map((s) => ({ ...s, status: 'pending' })));
    setActiveNode('client');
    setActiveEdge('client_to_gateway');

    const updateAssistant = (patch: Partial<ChatMessage>) => {
      setConversations((prev) =>
        prev.map((c) => {
          if (c.id === activeConvId) {
            return {
              ...c,
              messages: c.messages.map((m) => (m.id === assistantMsgId ? { ...m, ...patch } : m))
            };
          }
          return c;
        })
      );
    };

    try {
      // Sử dụng fetch với SSE stream để gửi kèm header Authorization: Bearer <token>
      const sseUrl = `${API_BASE}/api/debug/ask/stream?question=${encodeURIComponent(cleanText)}&top_k=3`;
      const response = await fetch(sseUrl, {
        headers: {
          'Authorization': `Bearer ${token}`
        }
      });

      if (!response.ok) {
        if (response.status === 401) {
          throw new Error('401 Unauthorized: Token xác thực không hợp lệ hoặc đã hết hạn. Vui lòng cập nhật JWT Token.');
        }
        if (response.status === 403) {
          throw new Error('403 Forbidden: Tài khoản không có quyền truy cập vào tài nguyên này.');
        }
        throw new Error(`HTTP Error ${response.status}: ${response.statusText}`);
      }

      if (!response.body) {
        throw new Error('Không nhận được dữ liệu stream từ server.');
      }

      const reader = response.body.getReader();
      const decoder = new TextDecoder('utf-8');
      let buffer = '';

      while (true) {
        const { value, done } = await reader.read();
        if (done) break;

        buffer += decoder.decode(value, { stream: true });
        const events = buffer.split('\n\n');
        buffer = events.pop() || '';

        for (const rawEvent of events) {
          if (!rawEvent.trim()) continue;

          let eventType = 'message';
          let dataStr = '';

          const lines = rawEvent.split('\n');
          for (const line of lines) {
            if (line.startsWith('event: ')) {
              eventType = line.slice(7).trim();
            } else if (line.startsWith('data: ')) {
              dataStr = line.slice(6).trim();
            }
          }

          if (!dataStr) continue;

          try {
            const data = JSON.parse(dataStr);

            if (eventType === 'step_start') {
              setActiveNode(data.active_node);
              if (data.active_edge) setActiveEdge(data.active_edge);
              setSteps((prev) =>
                prev.map((s) =>
                  s.stepId === data.step_id
                    ? { ...s, status: 'running', details: data.details || s.details }
                    : s
                )
              );
            } else if (eventType === 'step_complete') {
              setActiveNode(data.active_node);
              setSteps((prev) =>
                prev.map((s) =>
                  s.stepId === data.step_id
                    ? {
                        ...s,
                        status: 'completed',
                        durationMs: data.duration_ms,
                        details: data.details,
                        metrics: data.metrics
                      }
                    : s
                )
              );
            } else if (eventType === 'pipeline_complete') {
              setActiveNode('result');
              setActiveEdge('gemini_to_result');
              setTotalDuration(data.total_duration_ms);

              // Map sources
              const mappedSources: CitationSource[] = (data.sources || []).map((s: any) => ({
                chunk_id: s.chunk_id || '',
                article_title: s.article_title || '',
                article_url: s.article_url,
                score: s.score || 0,
                text: s.text || '',
                related_categories: s.related_categories || [],
                mentioned_articles: s.mentioned_articles || [],
                source_table: s.source_table,
                source_id: s.source_id,
                project_code: s.project_code,
                date: s.date
              }));

              updateAssistant({
                text: data.answer || 'Không có câu trả lời từ hệ thống.',
                sources: mappedSources,
                durationMs: data.total_duration_ms,
                status: 'done'
              });
            } else if (eventType === 'pipeline_error') {
              throw new Error(data.error || 'Lỗi xử lý từ pipeline');
            }
          } catch (pe) {
            console.error('Lỗi parse event SSE:', pe);
          }
        }
      }
    } catch (err: any) {
      console.error('Lỗi truy vấn:', err);
      updateAssistant({
        text: `⚠️ **Không thể hoàn tất truy vấn:**\n${err.message || 'Lỗi kết nối tới Gateway.'}`,
        status: 'error',
        errorMessage: err.message || 'Lỗi không xác định'
      });
    } finally {
      setLoading(false);
    }
  };

  const handleRetry = (msg: ChatMessage) => {
    // Tìm tin nhắn người dùng liền trước để hỏi lại
    const msgs = currentConv.messages;
    const errIdx = msgs.findIndex((m) => m.id === msg.id);
    if (errIdx > 0 && msgs[errIdx - 1].sender === 'user') {
      sendQuestion(msgs[errIdx - 1].text);
    }
  };

  return (
    <div className="flex h-screen w-screen bg-[#070b12] text-slate-100 font-sans overflow-hidden">
      {/* 1. SIDEBAR: LỊCH SỬ HỘI THOẠI */}
      {sidebarOpen && (
        <ConversationSidebar
          conversations={conversations}
          activeId={activeConvId}
          onSelect={(id) => setActiveConvId(id)}
          onNew={handleNewConversation}
          onDelete={handleDeleteConversation}
        />
      )}

      {/* 2. MAIN WORKSPACE */}
      <div className="flex-1 flex flex-col h-full min-w-0">
        {/* HEADER */}
        <header className="flex items-center justify-between px-4 py-3 border-b border-slate-800 bg-slate-900/60 backdrop-blur-md z-20">
          <div className="flex items-center gap-3">
            <button
              onClick={() => setSidebarOpen(!sidebarOpen)}
              className="p-1.5 rounded-lg border border-slate-700 bg-slate-800 hover:bg-slate-700 text-slate-300 transition cursor-pointer"
              title={sidebarOpen ? 'Thu gọn sidebar' : 'Mở sidebar'}
            >
              {sidebarOpen ? <PanelLeftClose className="w-4 h-4" /> : <PanelLeftOpen className="w-4 h-4" />}
            </button>

            <div className="flex items-center gap-2">
              <div className="w-8 h-8 rounded-lg bg-gradient-to-tr from-cyan-500 to-indigo-600 flex items-center justify-center shadow-lg shadow-cyan-500/20">
                <Cpu className="w-4 h-4 text-white" />
              </div>
              <div>
                <h1 className="text-sm font-bold tracking-tight text-white flex items-center gap-2">
                  Synapse Project Copilot
                  <span className="text-[10px] px-1.5 py-0.2 rounded-full bg-cyan-950 text-cyan-400 border border-cyan-800">
                    Live RBAC
                  </span>
                </h1>
                <p className="text-[11px] text-slate-400 truncate max-w-sm">
                  {currentConv.title}
                </p>
              </div>
            </div>
          </div>

          <div className="flex items-center gap-2">
            {/* Nút bật tắt 3D Timeline Visualizer */}
            <button
              onClick={() => setShowArchitecture(!showArchitecture)}
              className="flex items-center gap-1.5 px-2.5 py-1.5 rounded-lg border border-slate-700 bg-slate-800 hover:bg-slate-700 text-slate-300 text-xs transition cursor-pointer"
            >
              {showArchitecture ? <EyeOff className="w-3.5 h-3.5 text-cyan-400" /> : <Eye className="w-3.5 h-3.5 text-cyan-400" />}
              <span>{showArchitecture ? 'Ẩn 3D Visualizer' : 'Hiện 3D Visualizer'}</span>
            </button>

            {/* Nút cấu hình Token JWT */}
            <button
              onClick={() => setIsAuthModalOpen(true)}
              className={`flex items-center gap-1.5 px-3 py-1.5 rounded-lg text-xs font-medium border transition cursor-pointer ${
                token
                  ? 'border-emerald-700/60 bg-emerald-950/40 text-emerald-300 hover:bg-emerald-900/50'
                  : 'border-rose-700/80 bg-rose-950/50 text-rose-300 hover:bg-rose-900/60 animate-pulse'
              }`}
            >
              <KeyRound className="w-3.5 h-3.5" />
              <span>{token ? 'JWT Đã cấu hình' : 'Thiếu JWT Token'}</span>
            </button>

            {/* Developer link */}
            <a
              href={`${API_BASE}/graphql`}
              target="_blank"
              rel="noreferrer"
              className="hidden md:flex items-center gap-1 px-2.5 py-1.5 rounded-lg border border-indigo-800 bg-indigo-950/60 text-indigo-300 hover:bg-indigo-900 text-xs transition"
              title="Dành riêng cho Lập trình viên"
            >
              <ExternalLink className="w-3.5 h-3.5" />
              <span>Playground</span>
            </a>
          </div>
        </header>

        {/* BODY: SPLIT VIEW (CHAT + OPTIONAL 3D VISUALIZER) */}
        <div className="flex-1 flex flex-col lg:flex-row min-h-0 overflow-hidden">
          {/* CHAT PANE */}
          <div className="flex-1 flex flex-col h-full min-w-0 bg-[#070b12]">
            <ChatMessageList
              messages={currentConv.messages}
              loading={loading}
              onRetry={handleRetry}
              onSelectSuggested={(q) => sendQuestion(q)}
            />

            {/* INPUT FOOTER */}
            <div className="p-4 border-t border-slate-800/80 bg-slate-950/80 backdrop-blur-md">
              <form
                onSubmit={(e) => {
                  e.preventDefault();
                  sendQuestion(inputText);
                }}
                className="flex items-center gap-2 max-w-4xl mx-auto"
              >
                <input
                  type="text"
                  value={inputText}
                  onChange={(e) => setInputText(e.target.value)}
                  placeholder="Nhập câu hỏi bằng tiếng Việt (Ví dụ: 'Đang có bao nhiêu công việc của tất cả dự án?')..."
                  disabled={loading}
                  className="flex-1 bg-slate-900 border border-slate-700 focus:border-cyan-500 rounded-xl px-4 py-3 text-sm text-slate-100 placeholder-slate-500 focus:outline-none transition shadow-inner"
                />
                <button
                  type="submit"
                  disabled={loading || !inputText.trim()}
                  className="flex items-center justify-center w-11 h-11 bg-gradient-to-r from-cyan-500 to-indigo-600 hover:from-cyan-400 hover:to-indigo-500 disabled:opacity-40 text-white rounded-xl shadow-lg shadow-cyan-500/25 transition cursor-pointer flex-shrink-0"
                >
                  <Send className="w-4 h-4" />
                </button>
              </form>
            </div>
          </div>

          {/* 3D GRAPH & TIMELINE TRACE PANE (CAN BE TOGGLED) */}
          {showArchitecture && (
            <div className="w-full lg:w-[480px] xl:w-[540px] border-t lg:border-t-0 lg:border-l border-slate-800 bg-slate-950/50 flex flex-col h-[320px] lg:h-full flex-shrink-0 overflow-hidden">
              <div className="flex items-center justify-between px-3 py-2 border-b border-slate-800 text-xs">
                <span className="font-semibold text-slate-300 flex items-center gap-1.5">
                  <Layers className="w-3.5 h-3.5 text-cyan-400" />
                  Visualizer & Dấu vết Pipeline
                </span>
                <button
                  onClick={() => setViewMode2D(!viewMode2D)}
                  className="text-[11px] text-cyan-400 hover:underline cursor-pointer"
                >
                  {viewMode2D ? 'Xem 3D' : 'Xem 2D'}
                </button>
              </div>

              {/* 3D / 2D Canvas */}
              <div className="h-44 lg:h-52 relative border-b border-slate-800 bg-[#040711]">
                {viewMode2D ? (
                  <div className="p-3 text-[11px] text-slate-300 flex flex-col justify-around h-full">
                    <div className="font-semibold text-cyan-300 uppercase">Luồng xử lý câu hỏi:</div>
                    <div className="flex items-center justify-between gap-1 text-[10px] font-mono">
                      <span className={`px-2 py-1 rounded ${activeNode === 'client' ? 'bg-cyan-500 text-black font-bold' : 'bg-slate-800'}`}>1. Client</span>
                      <span>➔</span>
                      <span className={`px-2 py-1 rounded ${activeNode === 'gateway' ? 'bg-purple-500 text-black font-bold' : 'bg-slate-800'}`}>2. Gateway</span>
                      <span>➔</span>
                      <span className={`px-2 py-1 rounded ${activeNode === 'neo4j' ? 'bg-cyan-400 text-black font-bold' : 'bg-slate-800'}`}>3. Neo4j</span>
                      <span>➔</span>
                      <span className={`px-2 py-1 rounded ${activeNode === 'result' ? 'bg-amber-400 text-black font-bold' : 'bg-slate-800'}`}>4. Trả lời</span>
                    </div>
                  </div>
                ) : (
                  <Architecture3D activeNode={activeNode} activeEdge={activeEdge} />
                )}
              </div>

              {/* Timeline Steps */}
              <div className="flex-1 p-2 overflow-y-auto">
                <Timeline steps={steps} totalDurationMs={totalDuration} />
              </div>
            </div>
          )}
        </div>
      </div>

      {/* AUTH MODAL */}
      <AuthModal
        isOpen={isAuthModalOpen}
        onClose={() => setIsAuthModalOpen(false)}
        token={token}
        onSaveToken={handleSaveToken}
      />
    </div>
  );
};

export default App;
