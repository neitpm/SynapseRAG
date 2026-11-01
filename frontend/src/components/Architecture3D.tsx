import React, { useEffect, useRef, useState } from 'react';
import * as THREE from 'three';
import { Info } from 'lucide-react';

interface Architecture3DProps {
  activeNode: string | null;
  activeEdge: string | null;
}

export interface NodeMetadata {
  id: string;
  stepNumber?: number;
  viName: string;
  techName: string;
  techSub: string;
  pos: [number, number, number];
  color: number;
  flow: 'query' | 'ingest';
  input: string;
  action: string;
  output: string;
  note?: string;
}

export const ARCHITECTURE_NODES: NodeMetadata[] = [
  // ==========================================
  // LUỒNG HỎI ĐÁP (SYNAPSE QUERY PIPELINE)
  // ==========================================
  {
    id: 'client',
    stepNumber: 1,
    viName: 'Giao diện Người dùng',
    techName: 'Client UI (Browser / Dashboard)',
    techSub: 'React + TypeScript + Three.js',
    pos: [-4.2, 1.3, 0],
    color: 0x38bdf8,
    flow: 'query',
    input: 'Câu hỏi bằng ngôn ngữ tự nhiên từ người dùng (VD: "Trí tuệ nhân tạo là gì?")',
    action: 'Hiển thị giao diện nhập câu hỏi, mở kết nối SSE và lắng nghe tiến trình xử lý trực tiếp.',
    output: 'Gửi request HTTP GET/SSE kèm tham số question và top_k đến Gateway.',
  },
  {
    id: 'gateway',
    stepNumber: 2,
    viName: 'Cổng Điều phối & Embedding',
    techName: 'GraphQL Gateway + Local Embedding',
    techSub: 'FastAPI + all-MiniLM-L6-v2',
    pos: [-1.4, 1.3, 0],
    color: 0xa855f7,
    flow: 'query',
    input: 'Nhận câu hỏi từ Client UI qua SSE Stream / GraphQL mutation.',
    action: 'Tiếp nhận câu hỏi, sau đó NHÚNG VECTOR NGAY TẠI GATEWAY (không phải microservice độc lập) biến văn bản thành vector 384 chiều. Nếu Neo4j chưa có bài viết tương ứng, Gateway tự động cào bài mới từ Wikipedia.',
    output: 'Vector truy vấn 384 chiều, câu lệnh Cypher và SSE progress events tới Client.',
    note: '⚠️ Chú ý: Embedding là hàm nội bộ chạy trên tiến trình Gateway, không phải là một microservice riêng lẻ qua mạng.',
  },
  {
    id: 'neo4j',
    stepNumber: 3,
    viName: 'Cơ sở Dữ liệu Đồ thị & Vector',
    techName: 'Neo4j 5.20 Graph Database',
    techSub: 'HNSW Vector Index + Cypher Traversal',
    pos: [1.6, 2.1, 0],
    color: 0x06b6d4,
    flow: 'query',
    input: 'Vector embedding 384 chiều và tham số Top-K từ Gateway.',
    action: 'Thực thi Vector Index Search tìm Top-K chunk tương đồng nhất, sau đó mở rộng quan hệ đồ thị (HAS_CHUNK, IN_CATEGORY, MENTIONS) để lấy ngữ cảnh phân loại và bài viết liên quan.',
    output: 'Danh sách các đoạn văn bản (chunks), độ tương đồng score (0.0 - 1.0), danh mục (categories) và liên kết liên quan.',
  },
  {
    id: 'gemini',
    stepNumber: 4,
    viName: 'Mô hình Ngôn ngữ AI',
    techName: 'Google Gemini LLM',
    techSub: 'gemini-2.5-flash-lite API',
    pos: [4.4, 1.3, 0],
    color: 0x10b981,
    flow: 'query',
    input: 'Câu hỏi ban đầu và toàn bộ ngữ cảnh đồ thị (context snippets) từ Neo4j.',
    action: 'Đọc hiểu ngữ cảnh được cung cấp, suy luận và tổng hợp thành câu trả lời mạch lạc, chính xác bằng tiếng Việt, kèm ghi chú nếu ngữ cảnh thiếu thông tin.',
    output: 'Văn bản câu trả lời tự nhiên hoàn chỉnh gửi về Gateway.',
  },
  {
    id: 'result',
    stepNumber: 5,
    viName: 'Kết quả & Nguồn Trích dẫn',
    techName: 'Citations & Final Answer',
    techSub: 'Verified Response Payload',
    pos: [1.6, 0.4, 0],
    color: 0xf59e0b,
    flow: 'query',
    input: 'Câu trả lời từ Gemini kết hợp thông tin nguồn bài viết từ Neo4j.',
    action: 'Tổng hợp hoàn chỉnh câu trả lời, gắn thẻ các nguồn bài viết gốc (Wikipedia URLs) và số liệu latency đo đạc.',
    output: 'Trình diễn lên màn hình người dùng với đầy đủ thẻ trích dẫn và thời gian thực thi.',
  },

  // ==========================================
  // LUỒNG NẠP DỮ LIỆU NỀN (BACKGROUND INGESTION)
  // ==========================================
  {
    id: 'producer',
    viName: 'Bộ Thu thập Wikipedia',
    techName: 'Wiki Producer Service',
    techSub: 'Background Crawler Worker',
    pos: [-4.2, -2.1, 0],
    color: 0x64748b,
    flow: 'ingest',
    input: 'Danh sách chủ đề từ khóa hoặc lịch thu thập định kỳ từ Wikipedia.',
    action: 'Cào nội dung thô (tiêu đề, văn bản, thể loại, URL) từ Wikipedia API tiếng Việt.',
    output: 'Đóng gói JSON bài viết và gửi vào hàng đợi thông điệp Kafka.',
    note: 'Độc lập với luồng hỏi đáp: Chỉ chạy ngầm khi nạp dữ liệu ban đầu hoặc bổ sung dữ liệu.',
  },
  {
    id: 'kafka',
    viName: 'Hàng đợi Thông điệp Phân tán',
    techName: 'Kafka / Redpanda Broker',
    techSub: 'Topic: wiki-raw-articles',
    pos: [-1.4, -2.1, 0],
    color: 0xef4444,
    flow: 'ingest',
    input: 'Thông điệp sự kiện bài viết thô từ Wiki Producer.',
    action: 'Lưu trữ đệm bền vững, phân phối thông điệp dạng publish/subscribe song song cho Data Lake và Ingestion Worker.',
    output: 'Phát dòng sự kiện độc lập tới Cassandra Sink và AI Ingestion Worker.',
    note: 'Độc lập: Câu hỏi của người dùng không bao giờ đi qua Kafka!',
  },
  {
    id: 'cassandra',
    viName: 'Kho Lưu trữ Hồ Dữ liệu Thô',
    techName: 'Cassandra 4.1 Data Lake',
    techSub: 'synapse_lake.raw_articles',
    pos: [1.6, -2.9, 0],
    color: 0x0284c7,
    flow: 'ingest',
    input: 'Thông điệp bài viết thô từ Kafka topic.',
    action: 'Lưu trữ lâu dài dữ liệu nguyên bản để phục vụ sao lưu, phân tích dữ liệu lớn và huấn luyện mô hình sau này.',
    output: 'Dữ liệu được lưu trữ an toàn (Cold/Warm Data Lake).',
    note: 'Độc lập: Hoàn toàn không tham gia vào luồng phản hồi câu hỏi trực tiếp.',
  },
  {
    id: 'worker',
    viName: 'Công nhân Tiền xử lý & Nạp Đồ thị',
    techName: 'AI Ingestion Worker',
    techSub: 'Text Chunking + Embedder + Cypher Loader',
    pos: [1.6, -1.3, 0],
    color: 0x8b5cf6,
    flow: 'ingest',
    input: 'Sự kiện bài viết từ Kafka topic.',
    action: 'Cắt văn bản thành các đoạn (chunks 500 ký tự), tính vector embedding 384 chiều, trích xuất thực thể và tạo nút Article, Chunk, Category vào Neo4j.',
    output: 'Cập nhật cấu trúc đồ thị và Vector Index trên Neo4j.',
    note: 'Độc lập: Làm giàu tri thức cho Neo4j ở chế độ nền.',
  },
];

export const Architecture3D: React.FC<Architecture3DProps> = ({ activeNode, activeEdge }) => {
  const mountRef = useRef<HTMLDivElement>(null);
  const [selectedNode, setSelectedNode] = useState<NodeMetadata | null>(null);
  const [hoveredNode, setHoveredNode] = useState<NodeMetadata | null>(null);

  // Active node to display in modal (prioritize hover, then click, then backend active)
  const inspectedNode = hoveredNode || selectedNode || ARCHITECTURE_NODES.find((n) => n.id === activeNode) || null;

  useEffect(() => {
    if (!mountRef.current) return;
    const container = mountRef.current;
    const width = container.clientWidth;
    const height = container.clientHeight;

    // 1. Scene, Camera, Renderer
    const scene = new THREE.Scene();
    scene.background = new THREE.Color(0x070b14);

    const camera = new THREE.PerspectiveCamera(45, width / height, 0.1, 100);
    camera.position.set(0.3, -0.3, 8.9);
    camera.lookAt(0.3, -0.3, 0);

    const renderer = new THREE.WebGLRenderer({ antialias: true, alpha: true });
    renderer.setSize(width, height);
    renderer.setPixelRatio(Math.min(window.devicePixelRatio, 2));
    container.appendChild(renderer.domElement);

    // 2. Lights
    const ambientLight = new THREE.AmbientLight(0xffffff, 0.85);
    scene.add(ambientLight);

    const pointLight = new THREE.PointLight(0x38bdf8, 2.5, 25);
    pointLight.position.set(0, 3, 6);
    scene.add(pointLight);

    const ingestPointLight = new THREE.PointLight(0xef4444, 1.5, 20);
    ingestPointLight.position.set(0, -3, 5);
    scene.add(ingestPointLight);

    // 3. Grid Plane Background
    const gridHelper = new THREE.GridHelper(22, 22, 0x1e293b, 0x0f172a);
    gridHelper.position.y = -3.8;
    scene.add(gridHelper);

    // 4. Create Nodes
    const nodeMeshes: { [id: string]: THREE.Mesh } = {};
    const glowMeshes: { [id: string]: THREE.Mesh } = {};
    const interactiveObjects: THREE.Object3D[] = [];

    ARCHITECTURE_NODES.forEach((node) => {
      const isQuery = node.flow === 'query';
      const geometry = isQuery
        ? new THREE.BoxGeometry(1.7, 0.75, 0.45)
        : new THREE.CylinderGeometry(0.7, 0.7, 0.45, 24);

      const material = new THREE.MeshStandardMaterial({
        color: node.color,
        roughness: 0.25,
        metalness: 0.65,
        emissive: node.color,
        emissiveIntensity: 0.15,
      });

      const mesh = new THREE.Mesh(geometry, material);
      mesh.position.set(...node.pos);
      if (!isQuery) mesh.rotation.x = Math.PI / 6;
      mesh.userData = { nodeId: node.id };
      scene.add(mesh);
      nodeMeshes[node.id] = mesh;
      interactiveObjects.push(mesh);

      // Glow halo
      const glowGeo = isQuery
        ? new THREE.BoxGeometry(1.85, 0.9, 0.5)
        : new THREE.CylinderGeometry(0.8, 0.8, 0.5, 24);
      const glowMat = new THREE.MeshBasicMaterial({
        color: node.color,
        transparent: true,
        opacity: 0.0,
        wireframe: true,
      });
      const glowMesh = new THREE.Mesh(glowGeo, glowMat);
      glowMesh.position.set(...node.pos);
      if (!isQuery) glowMesh.rotation.x = Math.PI / 6;
      scene.add(glowMesh);
      glowMeshes[node.id] = glowMesh;
    });

    // 5. Connect Edges
    const edgesList: { id: string; from: [number, number, number]; to: [number, number, number]; color: number; label: string }[] = [
      // Luồng Hỏi Đáp (Query Flow)
      { id: 'client_to_gateway', from: [-4.2, 1.3, 0], to: [-1.4, 1.3, 0], color: 0x38bdf8, label: 'Câu hỏi (text)' },
      { id: 'gateway_to_neo4j', from: [-1.4, 1.3, 0], to: [1.6, 2.1, 0], color: 0xa855f7, label: 'Vector 384D + Cypher' },
      { id: 'neo4j_to_gemini', from: [1.6, 2.1, 0], to: [4.4, 1.3, 0], color: 0x10b981, label: 'Chunks + Categories' },
      { id: 'gemini_to_result', from: [4.4, 1.3, 0], to: [1.6, 0.4, 0], color: 0xf59e0b, label: 'Câu trả lời tự nhiên' },
      { id: 'result_to_client', from: [1.6, 0.4, 0], to: [-4.2, 1.3, 0], color: 0x38bdf8, label: 'Phản hồi + Citations' },

      // Luồng Nạp Dữ Liệu Nền (Background Ingestion)
      { id: 'producer_to_kafka', from: [-4.2, -2.1, 0], to: [-1.4, -2.1, 0], color: 0xef4444, label: 'Wiki Raw JSON' },
      { id: 'kafka_to_cassandra', from: [-1.4, -2.1, 0], to: [1.6, -2.9, 0], color: 0x0284c7, label: 'Lưu trữ hồ Data Lake' },
      { id: 'kafka_to_worker', from: [-1.4, -2.1, 0], to: [1.6, -1.3, 0], color: 0x8b5cf6, label: 'Tiêu thụ thông điệp' },
      { id: 'worker_to_neo4j', from: [1.6, -1.3, 0], to: [1.6, 2.1, 0], color: 0x64748b, label: 'Tạo Nút & Index Vector' },
    ];

    const edgeLines: { id: string; line: THREE.Line; mat: THREE.LineBasicMaterial }[] = [];

    edgesList.forEach((e) => {
      const points = [new THREE.Vector3(...e.from), new THREE.Vector3(...e.to)];
      const geometry = new THREE.BufferGeometry().setFromPoints(points);
      const isIngest = e.id.includes('producer') || e.id.includes('kafka') || e.id.includes('worker') || e.id.includes('cassandra');
      const mat = new THREE.LineBasicMaterial({
        color: e.color,
        transparent: true,
        opacity: isIngest ? 0.35 : 0.6,
      });
      const line = new THREE.Line(geometry, mat);
      scene.add(line);
      edgeLines.push({ id: e.id, line, mat });
    });

    // 6. Raycaster for Mouse Interactivity
    const raycaster = new THREE.Raycaster();
    const mouse = new THREE.Vector2();

    const onPointerMove = (evt: MouseEvent) => {
      const rect = renderer.domElement.getBoundingClientRect();
      mouse.x = ((evt.clientX - rect.left) / rect.width) * 2 - 1;
      mouse.y = -((evt.clientY - rect.top) / rect.height) * 2 + 1;

      raycaster.setFromCamera(mouse, camera);
      const intersects = raycaster.intersectObjects(interactiveObjects);

      if (intersects.length > 0) {
        const nodeId = intersects[0].object.userData.nodeId;
        const found = ARCHITECTURE_NODES.find((n) => n.id === nodeId) || null;
        setHoveredNode(found);
        container.style.cursor = 'pointer';
      } else {
        setHoveredNode(null);
        container.style.cursor = 'default';
      }
    };

    const onPointerClick = (evt: MouseEvent) => {
      const rect = renderer.domElement.getBoundingClientRect();
      mouse.x = ((evt.clientX - rect.left) / rect.width) * 2 - 1;
      mouse.y = -((evt.clientY - rect.top) / rect.height) * 2 + 1;

      raycaster.setFromCamera(mouse, camera);
      const intersects = raycaster.intersectObjects(interactiveObjects);

      if (intersects.length > 0) {
        const nodeId = intersects[0].object.userData.nodeId;
        const found = ARCHITECTURE_NODES.find((n) => n.id === nodeId) || null;
        setSelectedNode(found);
      }
    };

    container.addEventListener('mousemove', onPointerMove);
    container.addEventListener('click', onPointerClick);

    // 7. Animation loop
    let animId: number;
    const clock = new THREE.Clock();

    const animate = () => {
      animId = requestAnimationFrame(animate);
      const elapsed = clock.getElapsedTime();

      // Floating & highlight effects
      ARCHITECTURE_NODES.forEach((node) => {
        const mesh = nodeMeshes[node.id];
        const glow = glowMeshes[node.id];
        if (mesh && glow) {
          const isLiveActive = activeNode === node.id;
          const isInspected = inspectedNode?.id === node.id;

          mesh.position.y = node.pos[1] + Math.sin(elapsed * 2 + node.pos[0]) * 0.05;
          glow.position.y = mesh.position.y;

          if (isLiveActive) {
            // Pulse fast when backend actually reports it is executing
            glow.scale.setScalar(1.0 + Math.sin(elapsed * 8) * 0.14);
            (glow.material as THREE.MeshBasicMaterial).opacity = 0.9;
            (mesh.material as THREE.MeshStandardMaterial).emissiveIntensity = 0.85;
          } else if (isInspected) {
            glow.scale.setScalar(1.06);
            (glow.material as THREE.MeshBasicMaterial).opacity = 0.55;
            (mesh.material as THREE.MeshStandardMaterial).emissiveIntensity = 0.45;
          } else {
            glow.scale.setScalar(1.0);
            (glow.material as THREE.MeshBasicMaterial).opacity = 0.0;
            (mesh.material as THREE.MeshStandardMaterial).emissiveIntensity = 0.15;
          }
        }
      });

      // Highlight active edge (ONLY when backend stream indicates)
      edgeLines.forEach((e) => {
        if (activeEdge === e.id) {
          e.mat.opacity = 1.0;
        } else {
          e.mat.opacity = e.id.startsWith('kafka') || e.id.startsWith('producer') ? 0.25 : 0.45;
        }
      });

      renderer.render(scene, camera);
    };

    animate();

    const handleResize = () => {
      if (!mountRef.current) return;
      const w = mountRef.current.clientWidth;
      const h = mountRef.current.clientHeight;
      camera.aspect = w / h;
      camera.updateProjectionMatrix();
      renderer.setSize(w, h);
    };

    window.addEventListener('resize', handleResize);

    return () => {
      window.removeEventListener('resize', handleResize);
      container.removeEventListener('mousemove', onPointerMove);
      container.removeEventListener('click', onPointerClick);
      cancelAnimationFrame(animId);
      if (container.contains(renderer.domElement)) {
        container.removeChild(renderer.domElement);
      }
      renderer.dispose();
    };
  }, [activeNode, activeEdge, inspectedNode?.id]);

  return (
    <div className="relative w-full h-full min-h-[500px] rounded-xl overflow-hidden border border-slate-800 bg-[#070b14] select-none">
      <div ref={mountRef} className="w-full h-full" />

      {/* 1. HEADER CHÚ THÍCH PHÂN TÁCH 2 LUỒNG RÕ RÀNG */}
      <div className="absolute top-3 left-3 right-3 flex flex-wrap items-center justify-between gap-2 pointer-events-none">
        <div className="flex flex-col gap-1 bg-slate-900/90 backdrop-blur-md px-3.5 py-2 rounded-lg border border-cyan-500/40 shadow-lg pointer-events-auto">
          <div className="flex items-center gap-2">
            <span className="w-2.5 h-2.5 rounded-full bg-cyan-400 animate-pulse"></span>
            <span className="text-cyan-300 font-bold text-xs uppercase tracking-wide">
              LUỒNG TRẢ LỜI CÂU HỎI (HỎI ĐÁP ĐỒNG BỘ)
            </span>
          </div>
          <div className="text-[11px] text-slate-300 flex items-center gap-1.5 flex-wrap">
            <span className="bg-slate-800 px-1.5 py-0.5 rounded text-cyan-300 font-mono">1. UI</span>
            <span className="text-slate-500">→</span>
            <span className="bg-slate-800 px-1.5 py-0.5 rounded text-purple-300 font-mono">2. Gateway (Embedding)</span>
            <span className="text-slate-500">→</span>
            <span className="bg-slate-800 px-1.5 py-0.5 rounded text-cyan-400 font-mono">3. Neo4j</span>
            <span className="text-slate-500">→</span>
            <span className="bg-slate-800 px-1.5 py-0.5 rounded text-emerald-300 font-mono">4. Gemini</span>
            <span className="text-slate-500">→</span>
            <span className="bg-slate-800 px-1.5 py-0.5 rounded text-amber-300 font-mono">5. Trích dẫn</span>
          </div>
        </div>

        <div className="flex items-center gap-2 bg-slate-900/80 backdrop-blur-md px-3 py-1.5 rounded-lg border border-slate-800 text-[11px] text-slate-400">
          <Info className="w-3.5 h-3.5 text-cyan-400" />
          <span>Bấm hoặc rê chuột vào từng khối để xem chi tiết nghiệp vụ</span>
        </div>
      </div>

      {/* 2. CHÚ THÍCH LUỒNG NẠP DỮ LIỆU NỀN Ở DƯỚI */}
      <div className="absolute bottom-3 left-3 flex flex-col gap-1 bg-slate-900/90 backdrop-blur-md px-3.5 py-2 rounded-lg border border-rose-500/30 text-[11px] pointer-events-auto shadow-lg max-w-md">
        <div className="flex items-center gap-2">
          <span className="w-2.5 h-2.5 rounded-full bg-rose-500"></span>
          <span className="text-rose-300 font-bold uppercase tracking-wide">
            LUỒNG NẠP DỮ LIỆU NỀN (CHẠY NGẦM ĐỘC LẬP)
          </span>
        </div>
        <p className="text-slate-400 text-[10.5px] leading-relaxed">
          Wiki Producer → Kafka topic → Cassandra (Hồ thô) & AI Ingestion Worker → Neo4j.
          <span className="text-amber-400 font-semibold block mt-0.5">
            ⚠️ Lưu ý: Luồng này không chạy lại khi người dùng hỏi câu hỏi.
          </span>
        </p>
      </div>

      {/* 3. BẢNG GIẢI THÍCH CHI TIẾT KHỐI ĐƯỢC CHỌN/RÊ CHUỘT (INSPECTION PANEL) */}
      {inspectedNode && (
        <div className="absolute top-20 right-3 w-80 max-w-[calc(100vw-2rem)] bg-slate-900/95 backdrop-blur-lg border border-slate-700/80 rounded-xl p-4 shadow-2xl z-20 animate-in fade-in slide-in-from-right-4 duration-150">
          <div className="flex items-start justify-between gap-2 pb-2.5 border-b border-slate-800">
            <div>
              <div className="flex items-center gap-2">
                {inspectedNode.stepNumber && (
                  <span className="w-5 h-5 rounded-full bg-cyan-500 text-black font-extrabold text-[11px] flex items-center justify-center">
                    {inspectedNode.stepNumber}
                  </span>
                )}
                <h4 className="text-sm font-bold text-white tracking-wide">
                  {inspectedNode.viName}
                </h4>
              </div>
              <p className="text-[11px] text-cyan-400 font-mono mt-0.5">
                {inspectedNode.techName}
              </p>
              <p className="text-[10px] text-slate-500">
                {inspectedNode.techSub}
              </p>
            </div>
            <button
              onClick={() => setSelectedNode(null)}
              className="text-slate-400 hover:text-white text-xs px-1.5 py-0.5 rounded bg-slate-800"
            >
              ✕
            </button>
          </div>

          <div className="mt-3 flex flex-col gap-2.5 text-xs">
            <div>
              <span className="text-[10.5px] font-bold text-slate-400 uppercase tracking-wider block">
                📥 Nhận dữ liệu gì (Input):
              </span>
              <p className="text-slate-200 text-[11px] mt-0.5 leading-relaxed bg-slate-950/70 p-2 rounded border border-slate-800/80">
                {inspectedNode.input}
              </p>
            </div>

            <div>
              <span className="text-[10.5px] font-bold text-slate-400 uppercase tracking-wider block">
                ⚙️ Xử lý những gì (Action):
              </span>
              <p className="text-slate-200 text-[11px] mt-0.5 leading-relaxed bg-slate-950/70 p-2 rounded border border-slate-800/80">
                {inspectedNode.action}
              </p>
            </div>

            <div>
              <span className="text-[10.5px] font-bold text-slate-400 uppercase tracking-wider block">
                📤 Kết quả gửi đi đâu (Output):
              </span>
              <p className="text-slate-200 text-[11px] mt-0.5 leading-relaxed bg-slate-950/70 p-2 rounded border border-slate-800/80">
                {inspectedNode.output}
              </p>
            </div>

            {inspectedNode.note && (
              <div className="bg-amber-950/40 border border-amber-600/40 rounded p-2 text-[10.5px] text-amber-200 leading-relaxed">
                {inspectedNode.note}
              </div>
            )}
          </div>
        </div>
      )}

      {/* 4. CHÚ THÍCH CÁC ĐƯỜNG DỮ LIỆU ĐANG NỐI */}
      <div className="absolute bottom-3 right-3 bg-slate-900/90 backdrop-blur-md px-3 py-2 rounded-lg border border-slate-800 text-[10.5px] text-slate-300 hidden md:flex flex-col gap-1 max-w-xs shadow-lg">
        <span className="text-[10px] uppercase font-bold text-slate-400 tracking-wider">
          MÀU SẮC ĐƯỜNG DỮ LIỆU (EDGES):
        </span>
        <div className="grid grid-cols-2 gap-x-2 gap-y-0.5 text-[10px]">
          <span className="text-cyan-400">● Xanh lơ: Client ↔ Gateway</span>
          <span className="text-purple-400">● Tím: Vector 384D</span>
          <span className="text-emerald-400">● Xanh lá: Chunks → Gemini</span>
          <span className="text-amber-400">● Vàng: Câu trả lời</span>
          <span className="text-rose-400">● Đỏ: Kafka (Dữ liệu nền)</span>
          <span className="text-sky-400">● Xanh biển: Hồ Cassandra</span>
        </div>
      </div>
    </div>
  );
};
