export interface CitationSource {
  chunk_id: string;
  article_title: string;
  article_url?: string;
  score: number;
  text: string;
  related_categories: string[];
  mentioned_articles: string[];
  source_table?: string;
  source_id?: string;
  project_code?: string;
  date?: string;
}

export interface ChatMessage {
  id: string;
  sender: 'user' | 'assistant' | 'system';
  text: string;
  timestamp: number;
  sources?: CitationSource[];
  durationMs?: number;
  status?: 'sending' | 'streaming' | 'done' | 'error';
  errorMessage?: string;
}

export interface Conversation {
  id: string;
  title: string;
  createdAt: number;
  messages: ChatMessage[];
}

export interface AuthState {
  token: string;
  userId?: string;
  role?: string;
  allowedProjects?: string[];
}
