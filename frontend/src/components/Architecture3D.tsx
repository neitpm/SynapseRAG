import React, { useEffect, useRef } from 'react';
import * as THREE from 'three';

interface Architecture3DProps {
  activeNode: string | null;
  activeEdge: string | null;
}

interface NodeDef {
  id: string;
  name: string;
  sub: string;
  pos: [number, number, number];
  color: number;
  category: 'query' | 'ingest';
}

const NODES: NodeDef[] = [
  // LUỒNG HỎI ĐÁP ĐỒNG BỘ (QUERY PIPELINE)
  { id: 'client', name: 'Client UI', sub: 'Browser / Dashboard', pos: [-4, 1.2, 0], color: 0x38bdf8, category: 'query' },
  { id: 'gateway', name: 'GraphQL Gateway', sub: 'FastAPI + Local Embedding', pos: [-1.2, 1.2, 0], color: 0xa855f7, category: 'query' },
  { id: 'neo4j', name: 'Neo4j 5.20', sub: 'Vector Index + Graph Traversal', pos: [1.6, 2.0, 0], color: 0x06b6d4, category: 'query' },
  { id: 'gemini', name: 'Google Gemini', sub: 'LLM Generation (2.5-flash-lite)', pos: [4.4, 1.2, 0], color: 0x10b981, category: 'query' },
  { id: 'result', name: 'Citations & Answer', sub: 'Synthesized Response', pos: [1.6, 0.4, 0], color: 0xf59e0b, category: 'query' },

  // LUỒNG NẠP DỮ LIỆU NỀN (BACKGROUND INGESTION - ĐỘC LẬP)
  { id: 'producer', name: 'Wiki Producer', sub: 'Wikipedia API Crawler', pos: [-4, -2.0, 0], color: 0x64748b, category: 'ingest' },
  { id: 'kafka', name: 'Kafka / Redpanda', sub: 'Topic: wiki-raw-articles', pos: [-1.2, -2.0, 0], color: 0xef4444, category: 'ingest' },
  { id: 'cassandra', name: 'Cassandra 4.1', sub: 'Raw Data Lake Storage', pos: [1.6, -2.8, 0], color: 0x0284c7, category: 'ingest' },
  { id: 'worker', name: 'AI Ingestion Worker', sub: 'Clean, Chunk & Graph Loader', pos: [1.6, -1.2, 0], color: 0x8b5cf6, category: 'ingest' },
];

export const Architecture3D: React.FC<Architecture3DProps> = ({ activeNode, activeEdge }) => {
  const mountRef = useRef<HTMLDivElement>(null);

  useEffect(() => {
    if (!mountRef.current) return;
    const container = mountRef.current;
    const width = container.clientWidth;
    const height = container.clientHeight;

    // 1. Scene, Camera, Renderer
    const scene = new THREE.Scene();
    scene.background = new THREE.Color(0x090d16);

    const camera = new THREE.PerspectiveCamera(45, width / height, 0.1, 100);
    camera.position.set(0.5, -0.2, 8.8);
    camera.lookAt(0.5, -0.2, 0);

    const renderer = new THREE.WebGLRenderer({ antialias: true });
    renderer.setSize(width, height);
    renderer.setPixelRatio(Math.min(window.devicePixelRatio, 2));
    container.appendChild(renderer.domElement);

    // 2. Lights
    const ambientLight = new THREE.AmbientLight(0xffffff, 0.8);
    scene.add(ambientLight);

    const pointLight = new THREE.PointLight(0x38bdf8, 2, 20);
    pointLight.position.set(0, 3, 5);
    scene.add(pointLight);

    // 3. Grid Plane Background
    const gridHelper = new THREE.GridHelper(20, 20, 0x1e293b, 0x0f172a);
    gridHelper.position.y = -3.8;
    scene.add(gridHelper);

    // 4. Create Nodes
    const nodeMeshes: { [id: string]: THREE.Mesh } = {};
    const glowMeshes: { [id: string]: THREE.Mesh } = {};

    NODES.forEach((node) => {
      const isQuery = node.category === 'query';
      const geometry = isQuery
        ? new THREE.BoxGeometry(1.6, 0.7, 0.4)
        : new THREE.CylinderGeometry(0.65, 0.65, 0.4, 24);

      const material = new THREE.MeshStandardMaterial({
        color: node.color,
        roughness: 0.3,
        metalness: 0.7,
        emissive: node.color,
        emissiveIntensity: 0.15,
      });

      const mesh = new THREE.Mesh(geometry, material);
      mesh.position.set(...node.pos);
      if (!isQuery) mesh.rotation.x = Math.PI / 6;
      scene.add(mesh);
      nodeMeshes[node.id] = mesh;

      // Glow halo
      const glowGeo = isQuery
        ? new THREE.BoxGeometry(1.75, 0.85, 0.45)
        : new THREE.CylinderGeometry(0.75, 0.75, 0.45, 24);
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
    const edgesList: { id: string; from: [number, number, number]; to: [number, number, number]; color: number }[] = [
      // Query Flow
      { id: 'client_to_gateway', from: [-4, 1.2, 0], to: [-1.2, 1.2, 0], color: 0x38bdf8 },
      { id: 'gateway_to_neo4j', from: [-1.2, 1.2, 0], to: [1.6, 2.0, 0], color: 0xa855f7 },
      { id: 'neo4j_to_gemini', from: [1.6, 2.0, 0], to: [4.4, 1.2, 0], color: 0x10b981 },
      { id: 'gemini_to_result', from: [4.4, 1.2, 0], to: [1.6, 0.4, 0], color: 0xf59e0b },
      { id: 'result_to_client', from: [1.6, 0.4, 0], to: [-4, 1.2, 0], color: 0x38bdf8 },

      // Ingestion Flow (Background)
      { id: 'producer_to_kafka', from: [-4, -2.0, 0], to: [-1.2, -2.0, 0], color: 0xef4444 },
      { id: 'kafka_to_cassandra', from: [-1.2, -2.0, 0], to: [1.6, -2.8, 0], color: 0x0284c7 },
      { id: 'kafka_to_worker', from: [-1.2, -2.0, 0], to: [1.6, -1.2, 0], color: 0x8b5cf6 },
      { id: 'worker_to_neo4j', from: [1.6, -1.2, 0], to: [1.6, 2.0, 0], color: 0x64748b },
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

    // 6. Animation loop
    let animId: number;
    const clock = new THREE.Clock();

    const animate = () => {
      animId = requestAnimationFrame(animate);
      const elapsed = clock.getElapsedTime();

      // Gentle floating animation
      NODES.forEach((node) => {
        const mesh = nodeMeshes[node.id];
        const glow = glowMeshes[node.id];
        if (mesh && glow) {
          const isCurrentActive = activeNode === node.id;
          mesh.position.y = node.pos[1] + Math.sin(elapsed * 2 + node.pos[0]) * 0.05;
          glow.position.y = mesh.position.y;

          if (isCurrentActive) {
            glow.scale.setScalar(1.0 + Math.sin(elapsed * 8) * 0.12);
            (glow.material as THREE.MeshBasicMaterial).opacity = 0.8;
            (mesh.material as THREE.MeshStandardMaterial).emissiveIntensity = 0.8;
          } else {
            glow.scale.setScalar(1.0);
            (glow.material as THREE.MeshBasicMaterial).opacity = 0.0;
            (mesh.material as THREE.MeshStandardMaterial).emissiveIntensity = 0.15;
          }
        }
      });

      // Highlight active edge
      edgeLines.forEach((e) => {
        if (activeEdge === e.id) {
          e.mat.opacity = 1.0;
          e.mat.linewidth = 3;
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
      cancelAnimationFrame(animId);
      if (container.contains(renderer.domElement)) {
        container.removeChild(renderer.domElement);
      }
      renderer.dispose();
    };
  }, [activeNode, activeEdge]);

  return (
    <div className="relative w-full h-full min-h-[420px] rounded-xl overflow-hidden border border-slate-800 bg-[#090d16]">
      <div ref={mountRef} className="w-full h-full" />
      {/* Overlay chú thích phân tách 2 luồng */}
      <div className="absolute top-3 left-3 flex flex-col gap-1.5 pointer-events-none">
        <div className="flex items-center gap-2 bg-slate-900/90 backdrop-blur-md px-3 py-1 rounded-md border border-cyan-500/30 text-xs">
          <span className="w-2.5 h-2.5 rounded-full bg-cyan-400 animate-pulse"></span>
          <span className="text-cyan-300 font-semibold tracking-wide">LUỒNG HỎI ĐÁP (SYNAPSE QUERY PIPELINE)</span>
        </div>
        <div className="text-[11px] text-slate-400 pl-1">
          Client → Gateway (Embedding) → Neo4j Vector/Graph → Gemini LLM → Citations
        </div>
      </div>

      <div className="absolute bottom-3 left-3 flex flex-col gap-1 bg-slate-900/85 backdrop-blur-md px-3 py-1.5 rounded-md border border-slate-700/50 text-[11px] pointer-events-none">
        <div className="flex items-center gap-2">
          <span className="w-2 h-2 rounded-full bg-rose-500"></span>
          <span className="text-slate-300 font-medium">LUỒNG NẠP DỮ LIỆU NỀN (BACKGROUND INGESTION):</span>
        </div>
        <span className="text-slate-500 pl-4">Wikipedia → Kafka topic → Cassandra (Raw Lake) & Neo4j Ingestion Worker</span>
      </div>
    </div>
  );
};
