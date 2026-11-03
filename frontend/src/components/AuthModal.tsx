import React, { useState } from 'react';
import { KeyRound, ShieldAlert, Check, X } from 'lucide-react';

interface AuthModalProps {
  isOpen: boolean;
  onClose: () => void;
  token: string;
  onSaveToken: (newToken: string) => void;
}

export const AuthModal: React.FC<AuthModalProps> = ({
  isOpen,
  onClose,
  token,
  onSaveToken
}) => {
  const [inputVal, setInputVal] = useState(token);
  const [error, setError] = useState<string | null>(null);

  if (!isOpen) return null;

  const handleSave = () => {
    const trimmed = inputVal.trim();
    if (!trimmed) {
      setError('Token không được để trống.');
      return;
    }
    // Basic JWT structure validation (3 parts separated by dot)
    const cleanToken = trimmed.startsWith('Bearer ') ? trimmed.slice(7).trim() : trimmed;
    const parts = cleanToken.split('.');
    if (parts.length !== 3) {
      setError('Định dạng JWT không hợp lệ (cần đủ 3 phần header.payload.signature).');
      return;
    }

    onSaveToken(cleanToken);
    setError(null);
    onClose();
  };

  return (
    <div className="fixed inset-0 z-50 flex items-center justify-center bg-black/70 backdrop-blur-sm p-4">
      <div className="w-full max-w-lg rounded-2xl bg-slate-900 border border-slate-700 shadow-2xl p-6 flex flex-col gap-4 animate-in fade-in zoom-in-95 duration-200">
        <div className="flex items-center justify-between pb-3 border-b border-slate-800">
          <div className="flex items-center gap-2 text-cyan-400 font-bold text-base">
            <KeyRound className="w-5 h-5" />
            <span>Cấu hình Token Xác thực (JWT RBAC)</span>
          </div>
          <button
            onClick={onClose}
            className="p-1 rounded-lg text-slate-400 hover:text-white hover:bg-slate-800 transition"
          >
            <X className="w-5 h-5" />
          </button>
        </div>

        <div className="text-xs text-slate-300 space-y-2">
          <p>
            API Gateway yêu cầu xác thực nghiêm ngặt qua <strong>Authorization: Bearer &lt;JWT&gt;</strong> để thực thi phân quyền đa dự án (RBAC).
          </p>
          <div className="p-3 rounded-lg bg-amber-950/40 border border-amber-800/60 text-amber-300 flex items-start gap-2">
            <ShieldAlert className="w-4 h-4 text-amber-400 flex-shrink-0 mt-0.5" />
            <span>
              Token được lưu tạm an toàn trong <code>localStorage</code> trình duyệt của bạn, tuyệt đối không hardcode trong mã nguồn frontend.
            </span>
          </div>
        </div>

        <div className="flex flex-col gap-1.5">
          <label className="text-xs font-semibold text-slate-300">
            Dán chuỗi JWT Access Token:
          </label>
          <textarea
            rows={4}
            value={inputVal}
            onChange={(e) => {
              setInputVal(e.target.value);
              setError(null);
            }}
            placeholder="eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9..."
            className="w-full p-3 rounded-lg bg-slate-950 border border-slate-700 focus:border-cyan-500 text-xs font-mono text-slate-200 focus:outline-none transition resize-none"
          />
          {error && <span className="text-xs text-rose-400">{error}</span>}
        </div>

        <div className="flex items-center justify-end gap-2 pt-2 border-t border-slate-800">
          <button
            onClick={onClose}
            className="px-4 py-2 rounded-lg bg-slate-800 hover:bg-slate-700 text-slate-300 text-xs font-medium transition cursor-pointer"
          >
            Đóng
          </button>
          <button
            onClick={handleSave}
            className="flex items-center gap-1.5 px-4 py-2 rounded-lg bg-cyan-600 hover:bg-cyan-500 text-white text-xs font-semibold shadow-md transition cursor-pointer"
          >
            <Check className="w-4 h-4" />
            Lưu token
          </button>
        </div>
      </div>
    </div>
  );
};
