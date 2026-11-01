import React, { useState } from 'react';
import { Architecture3D } from './components/Architecture3D';
import { Timeline } from './components/Timeline';
import type { TimelineStep } from './components/Timeline';
import { Search, RotateCcw, Trash2, Cpu, ExternalLink, Sparkles, BookOpen, Layers } from 'lucide-react';

interface Source {
  chunk_id: string;
  article_title: string;
  article_url?: string;
  score: number;
  text: string;
  related_categories: string[];
  mentioned_articles: string[];
}

export const App: React.FC = () => {
  const [question, setQuestion] = useState('Trí tuệ nhân tạo là gì và ứng dụng ra sao?');
  const [topK, setTopK] = useState(3);
  const [loading, setLoading] = useState(false);
  const [activeNode, setActiveNode] = useState<string | null>(null);
  const [activeEdge, setActiveEdge] = useState<string | null>(null);
  const [answer, setAnswer] = useState<string | null>(null);
  const [sources, setSources] = useState<Source[]>([]);
  const [totalDuration, setTotalDuration] = useState<number | undefined>(undefined);
  const [viewMode2D, setViewMode2D] = useState(false);

  const initialSteps: TimelineStep[] = [
    { stepId: 1, name: '1. Question Embedding', status: 'pending', details: 'Sinh vector 384 chiều bên trong GraphQL Gateway.' },
    { stepId: 2, name: '2. Vector Search & Graph Traversal', status: 'pending', details: 'Truy vấn Neo4j Index Vector & mở rộng quan hệ đồ thị.' },
    { stepId: 3, name: '3. Answer Generation (LLM)', status: 'pending', details: 'Google Gemini (gemini-2.5-flash-lite) tổng hợp câu trả lời tự nhiên.' },
  ];

  const [steps, setSteps] = useState<TimelineStep[]>(initialSteps);

  const handleAsk = () => {
    if (!question.trim() || loading) return;

    setLoading(true);
    setAnswer(null);
    setSources([]);
    setTotalDuration(undefined);
    setSteps(initialSteps.map((s) => ({ ...s, status: 'pending' })));
    setActiveNode('client');
    setActiveEdge('client_to_gateway');

    const url = `http://127.0.0.1:8000/api/debug/ask/stream?question=${encodeURIComponent(question)}&top_k=${topK}`;
    const eventSource = new EventSource(url);

    eventSource.addEventListener('pipeline_start', (e: any) => {
      const data = JSON.parse(e.data);
      setActiveNode(data.active_node || 'gateway');
    });

    eventSource.addEventListener('step_start', (e: any) => {
      const data = JSON.parse(e.data);
      setActiveNode(data.active_node);
      if (data.active_edge) setActiveEdge(data.active_edge);

      setSteps((prev) =>
        prev.map((s) =>
          s.stepId === data.step_id
            ? { ...s, status: 'running', details: data.details || s.details }
            : s
        )
      );
    });

    eventSource.addEventListener('step_complete', (e: any) => {
      const data = JSON.parse(e.data);
      setActiveNode(data.active_node);

      setSteps((prev) =>
        prev.map((s) =>
          s.stepId === data.step_id
            ? {
                ...s,
                status: 'completed',
                durationMs: data.duration_ms,
                details: data.details,
                metrics: data.metrics,
              }
            : s
        )
      );
    });

    eventSource.addEventListener('step_failed', (e: any) => {
      const data = JSON.parse(e.data);
      setSteps((prev) =>
        prev.map((s) =>
          s.stepId === data.step_id
            ? { ...s, status: 'failed', durationMs: data.duration_ms, details: data.error }
            : s
        )
      );
    });

    eventSource.addEventListener('pipeline_complete', (e: any) => {
      const data = JSON.parse(e.data);
      setAnswer(data.answer);
      setSources(data.sources || []);
      setTotalDuration(data.total_duration_ms);
      setActiveNode('result');
      setActiveEdge('gemini_to_result');
      setLoading(false);
      eventSource.close();
    });

    eventSource.addEventListener('pipeline_error', (e: any) => {
      const data = JSON.parse(e.data);
      setAnswer(`Lỗi: ${data.error}`);
      setLoading(false);
      eventSource.close();
    });

    eventSource.onerror = (err) => {
      console.error('SSE connection error:', err);
      setLoading(false);
      eventSource.close();
    };
  };

  const handleClear = () => {
    setAnswer(null);
    setSources([]);
    setTotalDuration(undefined);
    setActiveNode(null);
    setActiveEdge(null);
    setSteps(initialSteps);
  };

  return (
    <div className="flex flex-col min-h-screen bg-[#070b12] text-slate-100 font-sans">
      {/* HEADER */}
      <header className="flex items-center justify-between px-6 py-4 border-b border-slate-800 bg-slate-900/60 backdrop-blur-md sticky top-0 z-50">
        <div className="flex items-center gap-3">
          <div className="w-9 h-9 rounded-lg bg-gradient-to-tr from-cyan-500 to-indigo-600 flex items-center justify-center shadow-lg shadow-cyan-500/20">
            <Cpu className="w-5 h-5 text-white" />
          </div>
          <div>
            <h1 className="text-base font-bold tracking-tight text-white flex items-center gap-2">
              SynapseRAG 3D Developer Explorer
              <span className="text-[10px] px-2 py-0.5 rounded-full bg-cyan-950 text-cyan-400 border border-cyan-800">
                v1.0 Live
              </span>
            </h1>
            <p className="text-xs text-slate-400">
              Event-Driven GraphRAG Microservices Real-time Pipeline Visualizer
            </p>
          </div>
        </div>

        <div className="flex items-center gap-3">
          <button
            onClick={() => setViewMode2D(!viewMode2D)}
            className="flex items-center gap-1.5 px-3 py-1.5 rounded-lg border border-slate-700 bg-slate-800 hover:bg-slate-700 text-xs transition"
          >
            <Layers className="w-3.5 h-3.5 text-cyan-400" />
            {viewMode2D ? 'Chuyển sang 3D View' : 'Chuyển sang 2D View'}
          </button>
          <a
            href="http://localhost:8000/graphql"
            target="_blank"
            rel="noreferrer"
            className="flex items-center gap-1.5 px-3 py-1.5 rounded-lg border border-indigo-700 bg-indigo-950 hover:bg-indigo-900 text-indigo-300 text-xs transition"
          >
            <ExternalLink className="w-3.5 h-3.5" />
            GraphQL Playground
          </a>
        </div>
      </header>

      {/* MAIN CONTAINER */}
      <main className="flex-1 flex flex-col p-6 gap-6 max-w-[1600px] w-full mx-auto">
        {/* INPUT BOX */}
        <section className="bg-slate-900/80 border border-slate-800 rounded-xl p-4 shadow-xl">
          <div className="flex flex-col md:flex-row items-center gap-3">
            <div className="relative flex-1 w-full">
              <Search className="w-5 h-5 text-slate-500 absolute left-3.5 top-1/2 -translate-y-1/2" />
              <input
                type="text"
                value={question}
                onChange={(e) => setQuestion(e.target.value)}
                onKeyDown={(e) => e.key === 'Enter' && handleAsk()}
                placeholder="Nhập câu hỏi để quan sát luồng GraphRAG xử lý..."
                className="w-full bg-slate-950 border border-slate-700 focus:border-cyan-500 rounded-lg pl-11 pr-4 py-2.5 text-sm text-slate-100 placeholder-slate-500 focus:outline-none transition shadow-inner"
              />
            </div>

            <div className="flex items-center gap-2 w-full md:w-auto">
              <div className="flex items-center gap-2 bg-slate-950 px-3 py-2 rounded-lg border border-slate-700 text-xs">
                <span className="text-slate-400">Top-K:</span>
                <select
                  value={topK}
                  onChange={(e) => setTopK(Number(e.target.value))}
                  className="bg-transparent text-cyan-300 font-bold focus:outline-none"
                >
                  <option value={1} className="bg-slate-900">1</option>
                  <option value={2} className="bg-slate-900">2</option>
                  <option value={3} className="bg-slate-900">3</option>
                  <option value={5} className="bg-slate-900">5</option>
                </select>
              </div>

              <button
                onClick={handleAsk}
                disabled={loading || !question.trim()}
                className="flex items-center gap-2 px-5 py-2.5 bg-gradient-to-r from-cyan-500 to-indigo-600 hover:from-cyan-400 hover:to-indigo-500 disabled:opacity-50 text-white text-sm font-semibold rounded-lg shadow-lg shadow-cyan-500/25 transition cursor-pointer"
              >
                <Sparkles className="w-4 h-4" />
                {loading ? 'Đang xử lý...' : 'Gửi câu hỏi'}
              </button>

              <button
                onClick={handleAsk}
                title="Hỏi lại"
                className="p-2.5 rounded-lg border border-slate-700 hover:bg-slate-800 text-slate-300 transition"
              >
                <RotateCcw className="w-4 h-4" />
              </button>

              <button
                onClick={handleClear}
                title="Xóa kết quả"
                className="p-2.5 rounded-lg border border-slate-700 hover:bg-rose-950/60 text-slate-400 hover:text-rose-400 transition"
              >
                <Trash2 className="w-4 h-4" />
              </button>
            </div>
          </div>
        </section>

        {/* MIDDLE SECTION: 3D GRAPH + TIMELINE */}
        <section className="grid grid-cols-1 lg:grid-cols-12 gap-6 min-h-[460px]">
          {/* 3D / 2D Graph Display */}
          <div className="lg:col-span-8 flex flex-col">
            {viewMode2D ? (
              <div className="flex-1 bg-slate-900/90 border border-slate-800 rounded-xl p-6 flex flex-col justify-between">
                <div>
                  <h3 className="text-sm font-semibold text-cyan-400 mb-4">Sơ đồ luồng 2D (Query Pipeline)</h3>
                  <div className="flex items-center justify-between gap-2 p-4 bg-slate-950 rounded-lg border border-slate-800">
                    <span className={`px-3 py-1.5 rounded font-mono text-xs ${activeNode === 'client' ? 'bg-cyan-500 text-black font-bold' : 'bg-slate-800 text-slate-400'}`}>1. Client UI</span>
                    <span className="text-slate-600">➔</span>
                    <span className={`px-3 py-1.5 rounded font-mono text-xs ${activeNode === 'gateway' ? 'bg-purple-500 text-black font-bold' : 'bg-slate-800 text-slate-400'}`}>2. Gateway (Embedding)</span>
                    <span className="text-slate-600">➔</span>
                    <span className={`px-3 py-1.5 rounded font-mono text-xs ${activeNode === 'neo4j' ? 'bg-cyan-400 text-black font-bold' : 'bg-slate-800 text-slate-400'}`}>3. Neo4j Graph & Vector</span>
                    <span className="text-slate-600">➔</span>
                    <span className={`px-3 py-1.5 rounded font-mono text-xs ${activeNode === 'gemini' ? 'bg-emerald-400 text-black font-bold' : 'bg-slate-800 text-slate-400'}`}>4. Gemini LLM</span>
                    <span className="text-slate-600">➔</span>
                    <span className={`px-3 py-1.5 rounded font-mono text-xs ${activeNode === 'result' ? 'bg-amber-400 text-black font-bold' : 'bg-slate-800 text-slate-400'}`}>5. Answer & Citations</span>
                  </div>
                </div>

                <div className="p-4 bg-slate-950/80 rounded-lg border border-slate-800/80 text-xs text-slate-400 mt-6">
                  <span className="text-rose-400 font-bold block mb-1">Luồng nạp dữ liệu nền độc lập (Background Ingestion):</span>
                  Wikipedia Producer ➔ Kafka topic <code className="text-slate-300">wiki-raw-articles</code> ➔ Cassandra (Raw Lake) & AI Ingestion Worker ➔ Neo4j Store.
                </div>
              </div>
            ) : (
              <Architecture3D activeNode={activeNode} activeEdge={activeEdge} />
            )}
          </div>

          {/* Timeline Trace */}
          <div className="lg:col-span-4 flex flex-col">
            <Timeline steps={steps} totalDurationMs={totalDuration} />
          </div>
        </section>

        {/* BOTTOM SECTION: ANSWER & CITATIONS */}
        {answer && (
          <section className="flex flex-col gap-4 p-6 bg-slate-900/90 border border-slate-800 rounded-xl shadow-2xl animate-in fade-in duration-300">
            <div className="flex items-center gap-2 pb-3 border-b border-slate-800">
              <Sparkles className="w-5 h-5 text-amber-400" />
              <h2 className="text-base font-bold text-white">Câu trả lời tổng hợp từ Gemini LLM & Graph Context</h2>
            </div>

            <div className="text-slate-200 text-sm leading-relaxed whitespace-pre-wrap bg-slate-950/70 p-4 rounded-lg border border-slate-800/80">
              {answer}
            </div>

            {sources.length > 0 && (
              <div className="flex flex-col gap-3 mt-2">
                <div className="flex items-center gap-2 text-xs font-semibold text-slate-400">
                  <BookOpen className="w-4 h-4 text-cyan-400" />
                  Nguồn trích dẫn & Ngữ cảnh đồ thị ({sources.length} chunks):
                </div>

                <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-3 gap-3">
                  {sources.map((s, idx) => (
                    <div key={idx} className="p-3.5 bg-slate-950/80 border border-slate-800 rounded-lg flex flex-col justify-between hover:border-slate-700 transition">
                      <div>
                        <div className="flex items-center justify-between gap-2 mb-1.5">
                          <span className="text-xs font-bold text-cyan-300 truncate">
                            {s.article_title}
                          </span>
                          <span className="text-[10px] font-mono px-1.5 py-0.5 rounded bg-cyan-950 text-cyan-400 border border-cyan-800">
                            Score: {s.score}
                          </span>
                        </div>
                        <p className="text-[12px] text-slate-400 line-clamp-3 italic">
                          "{s.text}"
                        </p>
                      </div>

                      <div className="mt-3 pt-2 border-t border-slate-800/60 flex items-center justify-between text-[11px]">
                        <div className="flex flex-wrap gap-1">
                          {s.related_categories.map((c, i) => (
                            <span key={i} className="px-1.5 py-0.5 rounded bg-slate-900 text-slate-400 text-[10px]">
                              #{c}
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
                            Wiki <ExternalLink className="w-3 h-3" />
                          </a>
                        )}
                      </div>
                    </div>
                  ))}
                </div>
              </div>
            )}
          </section>
        )}
      </main>
    </div>
  );
};

export default App;
