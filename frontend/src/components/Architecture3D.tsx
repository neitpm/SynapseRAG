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
  hexColor: string;
  flow: 'query' | 'ingest';
  input: string;
  action: string;
  output: string;
  note?: string;
}

export const ARCHITECTURE_NODES: NodeMetadata[] = [
  // ==========================================
  // LUỒNG HỎI ĐÁP (SYNAPSE QUERY PIPELINE - PHẦN TRÊN)
  // ==========================================
  {
    id: 'client',
    stepNumber: 1,
    viName: 'Giao diện Người dùng',
    techName: 'Client UI (Browser)',
    techSub: 'React + TypeScript + SSE',
    pos: [-4.6, 1.4, 0],
    color: 0x0284c7, // Sky
    hexColor: '#38bdf8',
    flow: 'query',
    input: 'Câu hỏi bằng ngôn ngữ tự nhiên từ người dùng (VD: "Trí tuệ nhân tạo là gì?")',
    action: 'Hiển thị giao diện nhập câu hỏi, kết nối SSE Stream thời gian thực tới Gateway.',
    output: 'Request HTTP GET kèm tham số question và top_k.',
  },
  {
    id: 'gateway',
    stepNumber: 2,
    viName: 'Cổng Gateway & Embedding',
    techName: 'GraphQL Gateway + Model Nhúng',
    techSub: 'FastAPI + all-MiniLM-L6-v2',
    pos: [-1.6, 1.4, 0],
    color: 0x9333ea, // Purple
    hexColor: '#c084fc',
    flow: 'query',
    input: 'Nhận câu hỏi người dùng từ Client UI.',
    action: 'Điều phối pipeline và NHÚNG VECTOR NGAY TẠI GATEWAY (all-MiniLM-L6-v2, 384 chiều). Nếu đồ thị chưa có bài viết tương ứng, Gateway tự động cào bài mới từ Wikipedia.',
    output: 'Vector 384 chiều, câu truy vấn Cypher và phát SSE events.',
    note: '⚠️ Chú ý: Embedding là hàm nội bộ chạy trên tiến trình Gateway, không phải là microservice riêng lẻ qua mạng.',
  },
  {
    id: 'neo4j',
    stepNumber: 3,
    viName: 'Cơ sở Dữ liệu Đồ thị & Vector',
    techName: 'Neo4j 5.20 Graph Database',
    techSub: 'Vector Index + Graph Traversal',
    pos: [1.6, 2.2, 0],
    color: 0x0d9488, // Teal
    hexColor: '#2dd4bf',
    flow: 'query',
    input: 'Vector embedding 384 chiều từ Gateway.',
    action: 'Tìm kiếm Top-K chunks tương đồng nhất qua index vector, sau đó duyệt quan hệ đồ thị (HAS_CHUNK, IN_CATEGORY) để lấy ngữ cảnh phân loại & liên kết.',
    output: 'Danh sách đoạn văn bản (chunks), độ tương đồng score và danh mục liên quan.',
  },
  {
    id: 'gemini',
    stepNumber: 4,
    viName: 'Mô hình Ngôn ngữ AI',
    techName: 'Google Gemini LLM',
    techSub: 'gemini-2.5-flash-lite API',
    pos: [4.6, 1.4, 0],
    color: 0x16a34a, // Emerald
    hexColor: '#4ade80',
    flow: 'query',
    input: 'Câu hỏi và toàn bộ ngữ cảnh đồ thị (context snippets) từ Neo4j.',
    action: 'Đọc hiểu ngữ cảnh được cung cấp, suy luận và tổng hợp thành câu trả lời tự nhiên, chính xác bằng tiếng Việt.',
    output: 'Văn bản câu trả lời hoàn chỉnh kèm chú giải.',
  },
  {
    id: 'result',
    stepNumber: 5,
    viName: 'Kết quả & Nguồn Trích dẫn',
    techName: 'Citations & Final Answer',
    techSub: 'Verified Response Card',
    pos: [1.6, 0.4, 0],
    color: 0xd97706, // Amber
    hexColor: '#fbbf24',
    flow: 'query',
    input: 'Câu trả lời từ Gemini kết hợp thông tin nguồn bài viết từ Neo4j.',
    action: 'Tổng hợp hoàn chỉnh câu trả lời, gắn thẻ các nguồn bài viết gốc (Wikipedia URLs) và số liệu latency đo đạc.',
    output: 'Trình diễn lên màn hình người dùng với đầy đủ thẻ trích dẫn.',
  },

  // ==========================================
  // LUỒNG NẠP DỮ LIỆU NỀN (BACKGROUND INGESTION - PHẦN DƯỚI)
  // ==========================================
  {
    id: 'producer',
    viName: 'Bộ Thu thập Wikipedia',
    techName: 'Wiki Producer Service',
    techSub: 'Background Crawler Worker',
    pos: [-4.6, -2.2, 0],
    color: 0x475569, // Slate
    hexColor: '#94a3b8',
    flow: 'ingest',
    input: 'Danh sách chủ đề từ khóa hoặc lịch thu thập định kỳ.',
    action: 'Cào bài viết thô (tiêu đề, nội dung, thể loại) từ Wikipedia API tiếng Việt.',
    output: 'Đóng gói JSON bài viết gửi vào topic Kafka.',
    note: 'Độc lập: Luồng này chỉ chạy ngầm khi nạp dữ liệu, không chạy khi người dùng hỏi.',
  },
  {
    id: 'kafka',
    viName: 'Hàng đợi Thông điệp Phân tán',
    techName: 'Kafka / Redpanda Broker',
    techSub: 'Topic: wiki-raw-articles',
    pos: [-1.6, -2.2, 0],
    color: 0xdc2626, // Red
    hexColor: '#f87171',
    flow: 'ingest',
    input: 'Thông điệp sự kiện bài viết thô từ Wiki Producer.',
    action: 'Lưu trữ đệm bền vững, phân phối thông điệp song song cho Cassandra Sink và Ingestion Worker.',
    output: 'Phát dòng sự kiện độc lập tới Cassandra Sink và Ingestion Worker.',
    note: 'Độc lập: Câu hỏi của người dùng không bao giờ đi qua Kafka!',
  },
  {
    id: 'cassandra',
    viName: 'Kho Hồ Dữ liệu Thô',
    techName: 'Cassandra 4.1 Data Lake',
    techSub: 'synapse_lake.raw_articles',
    pos: [1.6, -3.1, 0],
    color: 0x0284c7, // Blue
    hexColor: '#38bdf8',
    flow: 'ingest',
    input: 'Thông điệp bài viết thô từ Kafka topic.',
    action: 'Lưu trữ lâu dài dữ liệu nguyên bản phục vụ phân tích dữ liệu lớn và sao lưu.',
    output: 'Dữ liệu được lưu trữ an toàn trong Data Lake.',
    note: 'Độc lập: Hoàn toàn không tham gia vào luồng phản hồi câu hỏi trực tiếp.',
  },
  {
    id: 'worker',
    viName: 'Công nhân Tiền xử lý & Nạp Đồ thị',
    techName: 'AI Ingestion Worker',
    techSub: 'Text Chunking + Embedder + Neo4j',
    pos: [1.6, -1.3, 0],
    color: 0x7c3aed, // Violet
    hexColor: '#a78bfa',
    flow: 'ingest',
    input: 'Sự kiện bài viết từ Kafka topic.',
    action: 'Cắt văn bản thành chunks, nhúng vector 384 chiều, tạo nút Article, Chunk, Category vào Neo4j.',
    output: 'Cập nhật cấu trúc đồ thị và Vector Index trên Neo4j.',
    note: 'Độc lập: Làm giàu tri thức cho Neo4j ở chế độ nền.',
  },
];

export const Architecture3D: React.FC<Architecture3DProps> = ({ activeNode, activeEdge }) => {
  const mountRef = useRef<HTMLDivElement>(null);
  const [selectedNode, setSelectedNode] = useState<NodeMetadata | null>(null);
  const [hoveredNode, setHoveredNode] = useState<NodeMetadata | null>(null);

  // Vị trí 2D trên màn hình của từng Node (tính toán qua camera.project)
  const [nodeScreenPos, setNodeScreenPos] = useState<{ [id: string]: { x: number; y: number } }>({});

  const inspectedNode = hoveredNode || selectedNode || ARCHITECTURE_NODES.find((n) => n.id === activeNode) || null;

  useEffect(() => {
    if (!mountRef.current) return;
    const container = mountRef.current;
    let width = container.clientWidth;
    let height = container.clientHeight;

    // 1. Three.js Scene Setup
    const scene = new THREE.Scene();
    scene.background = new THREE.Color(0x060911);

    const camera = new THREE.PerspectiveCamera(45, width / height, 0.1, 100);
    camera.position.set(0.0, -0.4, 9.2);
    camera.lookAt(0.0, -0.4, 0);

    const renderer = new THREE.WebGLRenderer({ antialias: true, alpha: true });
    renderer.setSize(width, height);
    renderer.setPixelRatio(Math.min(window.devicePixelRatio, 2));
    container.appendChild(renderer.domElement);

    // 2. Lights
    const ambientLight = new THREE.AmbientLight(0xffffff, 0.9);
    scene.add(ambientLight);

    const dirLight = new THREE.DirectionalLight(0xffffff, 1.2);
    dirLight.position.set(0, 5, 8);
    scene.add(dirLight);

    // Grid Floor
    const gridHelper = new THREE.GridHelper(24, 24, 0x1e293b, 0x0c1322);
    gridHelper.position.y = -4.0;
    scene.add(gridHelper);

    // 3. Meshes
    const nodeMeshes: { [id: string]: THREE.Mesh } = {};
    const glowMeshes: { [id: string]: THREE.Mesh } = {};
    const interactiveObjects: THREE.Object3D[] = [];

    ARCHITECTURE_NODES.forEach((node) => {
      const isQuery = node.flow === 'query';
      const geometry = isQuery
        ? new THREE.BoxGeometry(2.1, 0.85, 0.4)
        : new THREE.CylinderGeometry(0.85, 0.85, 0.45, 32);

      const material = new THREE.MeshStandardMaterial({
        color: node.color,
        roughness: 0.2,
        metalness: 0.8,
        emissive: node.color,
        emissiveIntensity: 0.2,
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
        ? new THREE.BoxGeometry(2.25, 1.0, 0.45)
        : new THREE.CylinderGeometry(0.95, 0.95, 0.5, 32);
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

    // 4. Edges
    const edgesList: { id: string; from: [number, number, number]; to: [number, number, number]; color: number }[] = [
      // Luồng Hỏi Đáp
      { id: 'client_to_gateway', from: [-4.6, 1.4, 0], to: [-1.6, 1.4, 0], color: 0x38bdf8 },
      { id: 'gateway_to_neo4j', from: [-1.6, 1.4, 0], to: [1.6, 2.2, 0], color: 0xc084fc },
      { id: 'neo4j_to_gemini', from: [1.6, 2.2, 0], to: [4.6, 1.4, 0], color: 0x4ade80 },
      { id: 'gemini_to_result', from: [4.6, 1.4, 0], to: [1.6, 0.4, 0], color: 0xfbbf24 },
      { id: 'result_to_client', from: [1.6, 0.4, 0], to: [-4.6, 1.4, 0], color: 0x38bdf8 },

      // Luồng Nạp Nền
      { id: 'producer_to_kafka', from: [-4.6, -2.2, 0], to: [-1.6, -2.2, 0], color: 0xf87171 },
      { id: 'kafka_to_cassandra', from: [-1.6, -2.2, 0], to: [1.6, -3.1, 0], color: 0x38bdf8 },
      { id: 'kafka_to_worker', from: [-1.6, -2.2, 0], to: [1.6, -1.3, 0], color: 0xa78bfa },
      { id: 'worker_to_neo4j', from: [1.6, -1.3, 0], to: [1.6, 2.2, 0], color: 0x64748b },
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

    // 5. Raycaster
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
      } else {
        setHoveredNode(null);
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

    // 6. Animation loop & Screen Projection
    let animId: number;
    const clock = new THREE.Clock();

    const animate = () => {
      animId = requestAnimationFrame(animate);
      const elapsed = clock.getElapsedTime();

      // Cập nhật vị trí tọa độ 2D của các khối để render nhãn nổi (HTML Overlays)
      const positions: { [id: string]: { x: number; y: number } } = {};
      const tempVec = new THREE.Vector3();

      ARCHITECTURE_NODES.forEach((node) => {
        const mesh = nodeMeshes[node.id];
        const glow = glowMeshes[node.id];
        if (mesh && glow) {
          const isLiveActive = activeNode === node.id;
          const isInspected = inspectedNode?.id === node.id;

          mesh.position.y = node.pos[1] + Math.sin(elapsed * 2 + node.pos[0]) * 0.05;
          glow.position.y = mesh.position.y;

          if (isLiveActive) {
            glow.scale.setScalar(1.0 + Math.sin(elapsed * 8) * 0.15);
            (glow.material as THREE.MeshBasicMaterial).opacity = 0.95;
            (mesh.material as THREE.MeshStandardMaterial).emissiveIntensity = 0.95;
          } else if (isInspected) {
            glow.scale.setScalar(1.06);
            (glow.material as THREE.MeshBasicMaterial).opacity = 0.6;
            (mesh.material as THREE.MeshStandardMaterial).emissiveIntensity = 0.5;
          } else {
            glow.scale.setScalar(1.0);
            (glow.material as THREE.MeshBasicMaterial).opacity = 0.0;
            (mesh.material as THREE.MeshStandardMaterial).emissiveIntensity = 0.2;
          }

          // Chiếu 3D sang 2D Viewport
          tempVec.set(mesh.position.x, mesh.position.y, mesh.position.z);
          tempVec.project(camera);
          const x = (tempVec.x * 0.5 + 0.5) * width;
          const y = (-(tempVec.y * 0.5) + 0.5) * height;
          positions[node.id] = { x, y };
        }
      });

      setNodeScreenPos(positions);

      // Highlight active edge (Chỉ khi backend stream báo)
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
      width = mountRef.current.clientWidth;
      height = mountRef.current.clientHeight;
      camera.aspect = width / height;
      camera.updateProjectionMatrix();
      renderer.setSize(width, height);
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
    <div className="relative w-full h-full min-h-[520px] rounded-xl overflow-hidden border border-slate-800 bg-[#060911] select-none">
      {/* Three.js Canvas */}
      <div ref={mountRef} className="w-full h-full" />

      {/* NHÃN CHỮ RÕ NÉT NỔI NGAY TRÊN TỪNG KHỐI 3D */}
      {ARCHITECTURE_NODES.map((node) => {
        const pos = nodeScreenPos[node.id];
        if (!pos) return null;
        const isLive = activeNode === node.id;
        const isHovered = hoveredNode?.id === node.id;

        return (
          <div
            key={node.id}
            onClick={() => setSelectedNode(node)}
            onMouseEnter={() => setHoveredNode(node)}
            onMouseLeave={() => setHoveredNode(null)}
            style={{
              left: `${pos.x}px`,
              top: `${pos.y}px`,
              transform: 'translate(-50%, -50%)',
            }}
            className={`absolute flex flex-col items-center justify-center pointer-events-auto cursor-pointer transition-all duration-200 ${
              isLive ? 'scale-110 z-30' : isHovered ? 'scale-105 z-20' : 'z-10'
            }`}
          >
            <div
              className={`px-3 py-1.5 rounded-lg border backdrop-blur-md text-center shadow-lg transition-all ${
                isLive
                  ? 'bg-slate-900/95 border-cyan-400 shadow-cyan-500/40 ring-2 ring-cyan-400'
                  : isHovered
                  ? 'bg-slate-900/90 border-slate-500 shadow-white/10'
                  : 'bg-slate-950/85 border-slate-800'
              }`}
            >
              <div className="flex items-center gap-1.5 justify-center">
                {node.stepNumber && (
                  <span
                    className="w-4 h-4 rounded-full text-black font-extrabold text-[10px] flex items-center justify-center shrink-0"
                    style={{ backgroundColor: node.hexColor }}
                  >
                    {node.stepNumber}
                  </span>
                )}
                <span className="text-xs font-bold text-white tracking-wide whitespace-nowrap">
                  {node.viName}
                </span>
              </div>
              <div
                className="text-[10px] font-mono tracking-tight mt-0.5 truncate max-w-[150px]"
                style={{ color: node.hexColor }}
              >
                {node.techName}
              </div>
            </div>
          </div>
        );
      })}

      {/* 1. HEADER CHÚ THÍCH PHÂN TÁCH 2 LUỒNG */}
      <div className="absolute top-3 left-3 right-3 flex flex-wrap items-center justify-between gap-2 pointer-events-none">
        <div className="flex flex-col gap-1 bg-slate-900/95 backdrop-blur-md px-4 py-2.5 rounded-lg border border-cyan-500/40 shadow-xl pointer-events-auto">
          <div className="flex items-center gap-2">
            <span className="w-2.5 h-2.5 rounded-full bg-cyan-400 animate-pulse"></span>
            <span className="text-cyan-300 font-bold text-xs uppercase tracking-wide">
              LUỒNG TRẢ LỜI CÂU HỎI (HỎI ĐÁP ĐỒNG BỘ)
            </span>
          </div>
          <div className="text-[11px] text-slate-300 flex items-center gap-1.5 flex-wrap font-medium">
            <span className="bg-slate-800 px-1.5 py-0.5 rounded text-cyan-300">1. UI</span>
            <span className="text-slate-500">➔</span>
            <span className="bg-slate-800 px-1.5 py-0.5 rounded text-purple-300">2. Gateway (Embedding)</span>
            <span className="text-slate-500">➔</span>
            <span className="bg-slate-800 px-1.5 py-0.5 rounded text-teal-300">3. Neo4j</span>
            <span className="text-slate-500">➔</span>
            <span className="bg-slate-800 px-1.5 py-0.5 rounded text-emerald-300">4. Gemini</span>
            <span className="text-slate-500">➔</span>
            <span className="bg-slate-800 px-1.5 py-0.5 rounded text-amber-300">5. Trích dẫn</span>
          </div>
        </div>

        <div className="flex items-center gap-1.5 bg-slate-900/90 backdrop-blur-md px-3 py-1.5 rounded-lg border border-slate-700 text-[11px] text-slate-300 pointer-events-auto">
          <Info className="w-3.5 h-3.5 text-cyan-400" />
          <span>Bấm hoặc rê chuột vào khối để xem Input / Action / Output</span>
        </div>
      </div>

      {/* 2. CHÚ THÍCH LUỒNG NẠP DỮ LIỆU NỀN */}
      <div className="absolute bottom-3 left-3 flex flex-col gap-1 bg-slate-900/95 backdrop-blur-md px-3.5 py-2 rounded-lg border border-rose-500/30 text-[11px] pointer-events-auto shadow-xl max-w-sm">
        <div className="flex items-center gap-2">
          <span className="w-2.5 h-2.5 rounded-full bg-rose-500"></span>
          <span className="text-rose-300 font-bold uppercase tracking-wide">
            LUỒNG NẠP DỮ LIỆU NỀN (CHẠY NGẦM ĐỘC LẬP)
          </span>
        </div>
        <p className="text-slate-400 text-[10.5px] leading-relaxed">
          Wiki Producer → Kafka → Cassandra & Ingestion Worker → Neo4j.
          <span className="text-amber-400 font-semibold block mt-0.5">
            ⚠️ Lưu ý: Không chạy lại mỗi khi bạn đặt câu hỏi.
          </span>
        </p>
      </div>

      {/* 3. BẢNG GIẢI THÍCH CHI TIẾT KHỐI ĐƯỢC CHỌN (INSPECTION CARD) */}
      {inspectedNode && (
        <div className="absolute top-20 right-3 w-80 max-w-[calc(100vw-2rem)] bg-slate-900/95 backdrop-blur-lg border border-slate-700 rounded-xl p-4 shadow-2xl z-40 animate-in fade-in slide-in-from-right-4 duration-150">
          <div className="flex items-start justify-between gap-2 pb-2.5 border-b border-slate-800">
            <div>
              <div className="flex items-center gap-2">
                {inspectedNode.stepNumber && (
                  <span
                    className="w-5 h-5 rounded-full text-black font-extrabold text-[11px] flex items-center justify-center shrink-0"
                    style={{ backgroundColor: inspectedNode.hexColor }}
                  >
                    {inspectedNode.stepNumber}
                  </span>
                )}
                <h4 className="text-sm font-bold text-white tracking-wide">
                  {inspectedNode.viName}
                </h4>
              </div>
              <p className="text-[11px] font-mono mt-0.5" style={{ color: inspectedNode.hexColor }}>
                {inspectedNode.techName}
              </p>
              <p className="text-[10px] text-slate-400">
                {inspectedNode.techSub}
              </p>
            </div>
            <button
              onClick={() => setSelectedNode(null)}
              className="text-slate-400 hover:text-white text-xs px-2 py-0.5 rounded bg-slate-800"
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
              <div className="bg-amber-950/50 border border-amber-600/50 rounded p-2 text-[10.5px] text-amber-200 leading-relaxed">
                {inspectedNode.note}
              </div>
            )}
          </div>
        </div>
      )}

      {/* 4. CHÚ THÍCH CÁC ĐƯỜNG DỮ LIỆU ĐANG NỐI */}
      <div className="absolute bottom-3 right-3 bg-slate-900/90 backdrop-blur-md px-3 py-2 rounded-lg border border-slate-800 text-[10px] text-slate-300 hidden md:flex flex-col gap-1 max-w-xs shadow-lg">
        <span className="uppercase font-bold text-slate-400 tracking-wider">
          MÀU SẮC ĐƯỜNG DỮ LIỆU (EDGES):
        </span>
        <div className="grid grid-cols-2 gap-x-2 gap-y-0.5">
          <span className="text-cyan-400">● Xanh lơ: Client ↔ Gateway</span>
          <span className="text-purple-400">● Tím: Vector 384D</span>
          <span className="text-emerald-400">● Xanh lá: Chunks → Gemini</span>
          <span className="text-amber-400">● Vàng: Câu trả lời</span>
          <span className="text-rose-400">● Đỏ: Kafka (Nạp nền)</span>
          <span className="text-sky-400">● Xanh biển: Hồ Cassandra</span>
        </div>
      </div>
    </div>
  );
};
