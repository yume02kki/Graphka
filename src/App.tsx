import { useCallback, useEffect, useMemo, useRef, useState } from 'react';
import type { PointerEvent as ReactPointerEvent } from 'react';
import exampleProdYaml from '../configmaps.yaml?raw';
import exampleStageYaml from '../configmaps-staging.yaml?raw';
import databaseIcon from './assets/icons/database.svg';
import dotnetIcon from './assets/icons/dotnet.svg';
import elasticsearchIcon from './assets/icons/elasticsearch.svg';
import flinkIcon from './assets/icons/flink.svg';
import kafkaIcon from './assets/icons/kafka.svg';
import nifiIcon from './assets/icons/nifi.svg';
import oracleIcon from './assets/icons/oracle.svg';
import serviceIcon from './assets/icons/service.svg';
import unknownIcon from './assets/icons/unknown.svg';
import { getNodeKindLabel, parseConfigMaps } from './parser';
import type { GraphEdge, GraphNode, PipelineGraph } from './types';

const nodePalette: Record<GraphNode['kind'], string> = {
  service: '#7dd3fc',
  kafka: '#34d399',
  database: '#f59e0b',
  unknown: '#94a3b8',
};

const edgeColor: Record<GraphEdge['kind'], string> = {
  consume: '#60a5fa',
  produce: '#7dd3fc',
  output: '#34d399',
  write: '#f59e0b',
  dependency: '#a78bfa',
};

const NODE_SIZE: Record<GraphNode['kind'], { width: number; height: number }> = {
  service: { width: 184, height: 116 },
  kafka: { width: 164, height: 104 },
  database: { width: 170, height: 108 },
  unknown: { width: 156, height: 100 },
};

const nodeIcons = {
  kafka: kafkaIcon,
  flink: flinkIcon,
  nifi: nifiIcon,
  dotnet: dotnetIcon,
  elasticsearch: elasticsearchIcon,
  oracle: oracleIcon,
  service: serviceIcon,
  database: databaseIcon,
  unknown: unknownIcon,
} as const;

const MIN_SCALE = 0.45;
const MAX_SCALE = 1.8;
const exampleYaml = [exampleProdYaml, exampleStageYaml].join('\n---\n');

const getGraphKey = (graph: PipelineGraph) =>
  `${graph.source.project}|${graph.source.environment}|${graph.source.configMapName ?? 'configmap'}|${graph.id}`;

const clamp = (value: number, min: number, max: number) => Math.min(max, Math.max(min, value));

const formatMetaValue = (value: unknown) => {
  if (Array.isArray(value)) return value.join(', ');
  if (typeof value === 'object' && value !== null) return JSON.stringify(value);
  return String(value);
};

const wrapLabel = (value: string, size = 16) => {
  const tokens = value.split(/[-._/\s]+/).filter(Boolean);
  if (!tokens.length) return [value];
  const lines: string[] = [];
  let current = '';
  for (const token of tokens) {
    const next = current ? `${current} ${token}` : token;
    if (next.length > size && current) {
      lines.push(current);
      current = token;
    } else {
      current = next;
    }
  }
  if (current) lines.push(current);
  return lines.slice(0, 3);
};

const pathForEdge = (from: GraphNode, to: GraphNode, offset = 0) => {
  const fromSize = NODE_SIZE[from.kind];
  const toSize = NODE_SIZE[to.kind];
  const fx = from.x ?? 0;
  const fy = from.y ?? 0;
  const tx = to.x ?? 0;
  const ty = to.y ?? 0;
  const direction = tx >= fx ? 1 : -1;

  const startX = fx + (fromSize.width / 2) * direction;
  const startY = fy + offset * 10;
  const endX = tx - (toSize.width / 2) * direction;
  const endY = ty + offset * 10;

  const dx = Math.abs(endX - startX);
  const cp = Math.max(60, dx * 0.35);

  return [
    `M ${startX} ${startY}`,
    `C ${startX + cp * direction} ${startY + offset * 20},`,
    `${endX - cp * direction} ${endY + offset * 20},`,
    `${endX} ${endY}`,
  ].join(' ');
};

const edgeMidpoint = (from: GraphNode, to: GraphNode, offset = 0) => {
  const fx = from.x ?? 0;
  const fy = from.y ?? 0;
  const tx = to.x ?? 0;
  const ty = to.y ?? 0;
  return {
    x: (fx + tx) / 2,
    y: (fy + ty) / 2 + offset * 18 - 12,
  };
};

const graphDimensions = (graph: PipelineGraph) => {
  const width = Math.max(...graph.nodes.map((n) => (n.x ?? 0) + NODE_SIZE[n.kind].width / 2 + 180), 1400);
  const height = Math.max(...graph.nodes.map((n) => (n.y ?? 0) + NODE_SIZE[n.kind].height / 2 + 180), 900);
  return { width, height };
};

const fitGraphToViewport = (graph: PipelineGraph, vw: number, vh: number) => {
  const { width, height } = graphDimensions(graph);
  const scale = clamp(Math.min((vw - 80) / width, (vh - 80) / height, 1), MIN_SCALE, 1);
  return { scale, x: (vw - width * scale) / 2, y: (vh - height * scale) / 2 };
};

const getNodeIconKey = (node: GraphNode) => {
  const iconKey = typeof node.meta.iconKey === 'string' ? node.meta.iconKey : node.kind;
  return iconKey in nodeIcons ? (iconKey as keyof typeof nodeIcons) : 'unknown';
};

/* ─── GraphView ──────────────────────────────────────── */

const GraphView = ({
  graph,
  selectedNodeId,
  hoveredNodeId,
  onSelectNode,
  onHoverNode,
}: {
  graph: PipelineGraph;
  selectedNodeId: string | null;
  hoveredNodeId: string | null;
  onSelectNode: (nodeId: string | null) => void;
  onHoverNode: (nodeId: string | null) => void;
}) => {
  const containerRef = useRef<HTMLDivElement | null>(null);
  const [transform, setTransform] = useState({ x: 0, y: 0, scale: 1 });
  const [isDragging, setIsDragging] = useState(false);
  const [nodePositions, setNodePositions] = useState<Record<string, { x: number; y: number }>>({});
  const dragRef = useRef<{ startX: number; startY: number; originX: number; originY: number } | null>(null);
  const nodeDragRef = useRef<{
    nodeId: string;
    startX: number;
    startY: number;
    originX: number;
    originY: number;
  } | null>(null);

  const positionedNodes = useMemo(
    () =>
      graph.nodes.map((node) => ({
        ...node,
        x: nodePositions[node.id]?.x ?? node.x,
        y: nodePositions[node.id]?.y ?? node.y,
      })),
    [graph.nodes, nodePositions],
  );

  const nodeIndex = useMemo(
    () => Object.fromEntries(positionedNodes.map((n) => [n.id, n])),
    [positionedNodes],
  );

  const routedEdges = useMemo(() => {
    const pairGroups = new Map<string, GraphEdge[]>();
    for (const edge of graph.edges) {
      const a = edge.from < edge.to ? edge.from : edge.to;
      const b = edge.from < edge.to ? edge.to : edge.from;
      const key = `${a}|${b}`;
      const bucket = pairGroups.get(key) ?? [];
      bucket.push(edge);
      pairGroups.set(key, bucket);
    }

    const offsets = new Map<string, number>();
    const assignOffsets = (edges: GraphEdge[], sideBias: number) => {
      const ordered = [...edges].sort((l, r) => l.channel.localeCompare(r.channel));
      const center = (ordered.length - 1) / 2;
      ordered.forEach((edge, i) => {
        const local = (i - center) * 0.8;
        const offset = sideBias === 0 ? local : sideBias * (1.35 + ordered.length * 0.15) + local;
        offsets.set(edge.id, offset);
      });
    };

    for (const edges of pairGroups.values()) {
      const forward: GraphEdge[] = [];
      const reverse: GraphEdge[] = [];
      for (const edge of edges) {
        const from = nodeIndex[edge.from];
        const to = nodeIndex[edge.to];
        if (!from || !to) continue;
        if ((from.x ?? 0) <= (to.x ?? 0)) forward.push(edge);
        else reverse.push(edge);
      }
      if (forward.length && reverse.length) {
        assignOffsets(forward, -1);
        assignOffsets(reverse, 1);
      } else if (forward.length) {
        assignOffsets(forward, 0);
      } else if (reverse.length) {
        assignOffsets(reverse, 0);
      }
    }

    return graph.edges.map((edge) => ({ edge, offset: offsets.get(edge.id) ?? 0 }));
  }, [graph.edges, nodeIndex]);

  const selectedNeighbors = useMemo(() => {
    const focusId = selectedNodeId ?? hoveredNodeId;
    if (!focusId) return new Set<string>();
    const neighbors = new Set<string>([focusId]);
    for (const edge of graph.edges) {
      if (edge.from === focusId || edge.to === focusId) {
        neighbors.add(edge.from);
        neighbors.add(edge.to);
      }
    }
    return neighbors;
  }, [graph.edges, hoveredNodeId, selectedNodeId]);

  useEffect(() => {
    const container = containerRef.current;
    if (!container) return;
    const applyFit = () => setTransform(fitGraphToViewport(graph, container.clientWidth, container.clientHeight));
    applyFit();
    const observer = new ResizeObserver(applyFit);
    observer.observe(container);
    return () => observer.disconnect();
  }, [graph]);

  useEffect(() => {
    setNodePositions({});
  }, [graph.id, graph.source.project, graph.source.environment]);

  useEffect(() => {
    if (isDragging || !selectedNodeId) return;
    const container = containerRef.current;
    if (!container) return;
    const node = nodeIndex[selectedNodeId];
    if (!node) return;
    setTransform((cur) => ({
      ...cur,
      x: container.clientWidth / 2 - (node.x ?? 0) * cur.scale,
      y: container.clientHeight / 2 - (node.y ?? 0) * cur.scale,
    }));
  }, [selectedNodeId]);

  const adjustScale = useCallback((nextScale: number) => {
    const container = containerRef.current;
    if (!container) return;
    setTransform((cur) => {
      const cx = container.clientWidth / 2;
      const cy = container.clientHeight / 2;
      const wx = (cx - cur.x) / cur.scale;
      const wy = (cy - cur.y) / cur.scale;
      return { scale: nextScale, x: cx - wx * nextScale, y: cy - wy * nextScale };
    });
  }, []);

  useEffect(() => {
    const container = containerRef.current;
    if (!container) return;
    const onWheel = (e: WheelEvent) => {
      e.preventDefault();
      const bounds = container.getBoundingClientRect();
      const cx = e.clientX - bounds.left;
      const cy = e.clientY - bounds.top;
      setTransform((cur) => {
        const nextScale = clamp(cur.scale * (e.deltaY > 0 ? 0.92 : 1.08), MIN_SCALE, MAX_SCALE);
        const wx = (cx - cur.x) / cur.scale;
        const wy = (cy - cur.y) / cur.scale;
        return { scale: nextScale, x: cx - wx * nextScale, y: cy - wy * nextScale };
      });
    };
    container.addEventListener('wheel', onWheel, { passive: false });
    return () => container.removeEventListener('wheel', onWheel);
  }, []);

  const handlePointerDown = (e: ReactPointerEvent<HTMLDivElement>) => {
    if (e.target instanceof Element && e.target.closest('.graph-node')) return;
    e.currentTarget.setPointerCapture(e.pointerId);
    dragRef.current = { startX: e.clientX, startY: e.clientY, originX: transform.x, originY: transform.y };
    setIsDragging(true);
    onSelectNode(null);
  };

  const handlePointerMove = (e: ReactPointerEvent<HTMLDivElement>) => {
    const nd = nodeDragRef.current;
    if (nd) {
      setNodePositions((cur) => ({
        ...cur,
        [nd.nodeId]: {
          x: nd.originX + (e.clientX - nd.startX) / transform.scale,
          y: nd.originY + (e.clientY - nd.startY) / transform.scale,
        },
      }));
      return;
    }
    const d = dragRef.current;
    if (!d) return;
    setTransform((cur) => ({
      ...cur,
      x: d.originX + (e.clientX - d.startX),
      y: d.originY + (e.clientY - d.startY),
    }));
  };

  const handlePointerUp = (e: ReactPointerEvent<HTMLDivElement>) => {
    if (e.currentTarget.hasPointerCapture(e.pointerId)) {
      e.currentTarget.releasePointerCapture(e.pointerId);
    }
    dragRef.current = null;
    nodeDragRef.current = null;
    setIsDragging(false);
  };

  const { width, height } = graphDimensions({ ...graph, nodes: positionedNodes });

  const handleFit = useCallback(() => {
    const container = containerRef.current;
    if (!container) return;
    setTransform(fitGraphToViewport(graph, container.clientWidth, container.clientHeight));
  }, [graph]);

  return (
    <div className="graph-stage">
      <div
        ref={containerRef}
        className={`graph-shell ${isDragging ? 'is-dragging' : ''}`}
        onPointerDown={handlePointerDown}
        onPointerMove={handlePointerMove}
        onPointerUp={handlePointerUp}
        onPointerCancel={handlePointerUp}
      >
        <svg className="graph-canvas" role="img" aria-label={`${graph.id} topology`}>
          <defs>
            <pattern id="grid" width="48" height="48" patternUnits="userSpaceOnUse">
              <path d="M 48 0 L 0 0 0 48" fill="none" stroke="rgba(148,163,184,0.06)" strokeWidth="1" />
            </pattern>
            <marker id="flow-arrow" markerWidth="7" markerHeight="7" markerUnits="userSpaceOnUse" refX="6" refY="3.5" orient="auto">
              <path d="M 0 0 L 7 3.5 L 0 7 z" fill="context-stroke" />
            </marker>
          </defs>

          <g transform={`translate(${transform.x} ${transform.y}) scale(${transform.scale})`}>
            <rect width={width} height={height} fill="url(#grid)" />

            {routedEdges.map(({ edge, offset }) => {
              const from = nodeIndex[edge.from];
              const to = nodeIndex[edge.to];
              if (!from || !to) return null;
              const focusId = selectedNodeId ?? hoveredNodeId;
              const related = !focusId || edge.from === focusId || edge.to === focusId;
              const mid = edgeMidpoint(from, to, offset);
              return (
                <g key={edge.id} className={related ? 'edge-group is-related' : 'edge-group is-muted'}>
                  <path d={pathForEdge(from, to, offset)} className="flow-edge" stroke={edgeColor[edge.kind]} markerEnd="url(#flow-arrow)" />
                  <text className={`edge-label ${related ? 'is-visible' : 'is-hidden'}`} x={mid.x} y={mid.y}>
                    {edge.channel.split('\n').map((line, i) => (
                      <tspan key={`${edge.id}-${line}`} x={mid.x} dy={i === 0 ? 0 : 14}>
                        {line}
                      </tspan>
                    ))}
                  </text>
                </g>
              );
            })}

            {positionedNodes.map((node) => {
              const active = selectedNodeId === node.id;
              const muted = selectedNodeId ? !selectedNeighbors.has(node.id) : false;
              const size = NODE_SIZE[node.kind];
              const iconHref = nodeIcons[getNodeIconKey(node)];
              const lines = wrapLabel(node.label, node.kind === 'service' ? 16 : 14);
              return (
                <g
                  className={`graph-node ${active ? 'is-active' : ''} ${muted ? 'is-muted' : ''}`}
                  key={node.id}
                  onMouseEnter={() => onHoverNode(node.id)}
                  onMouseLeave={() => onHoverNode(null)}
                  onClick={(ev) => {
                    ev.stopPropagation();
                    onSelectNode(node.id);
                  }}
                  onPointerDown={(ev) => {
                    ev.stopPropagation();
                    containerRef.current?.setPointerCapture(ev.pointerId);
                    nodeDragRef.current = {
                      nodeId: node.id,
                      startX: ev.clientX,
                      startY: ev.clientY,
                      originX: node.x ?? 0,
                      originY: node.y ?? 0,
                    };
                    setIsDragging(true);
                  }}
                  transform={`translate(${node.x ?? 0}, ${node.y ?? 0})`}
                >
                  <rect
                    className="node-halo"
                    x={-(size.width / 2) - 10}
                    y={-(size.height / 2) - 10}
                    width={size.width + 20}
                    height={size.height + 20}
                    rx="24"
                  />
                  <rect
                    className="node-core"
                    x={-(size.width / 2)}
                    y={-(size.height / 2)}
                    width={size.width}
                    height={size.height}
                    rx="20"
                    fill="rgba(10, 16, 30, 0.98)"
                    stroke={nodePalette[node.kind]}
                  />
                  <image className="node-icon" href={iconHref} x="-16" y={-(size.height / 2) + 14} width="32" height="32" />
                  <text className="node-kind" x="0" y={-(size.height / 2) + 58}>
                    {getNodeKindLabel(node.kind)}
                  </text>
                  <text className="node-label center-text" x="0" y={14}>
                    {lines.map((line, i) => (
                      <tspan key={`${node.id}-${line}`} x="0" dy={i === 0 ? 0 : 17}>
                        {line}
                      </tspan>
                    ))}
                  </text>
                </g>
              );
            })}
          </g>
        </svg>
      </div>

      <div className="canvas-controls">
        <button className="ctrl-btn" type="button" onClick={() => adjustScale(clamp(transform.scale * 1.15, MIN_SCALE, MAX_SCALE))}>
          +
        </button>
        <button className="ctrl-btn" type="button" onClick={() => adjustScale(clamp(transform.scale * 0.87, MIN_SCALE, MAX_SCALE))}>
          -
        </button>
        <button className="ctrl-btn wide" type="button" onClick={handleFit}>
          Fit
        </button>
      </div>

      <div className="canvas-legend">
        <span className="legend-chip"><img alt="" src={kafkaIcon} /> Kafka</span>
        <span className="legend-chip"><img alt="" src={flinkIcon} /> Flink</span>
        <span className="legend-chip"><img alt="" src={dotnetIcon} /> .NET</span>
        <span className="legend-chip"><img alt="" src={nifiIcon} /> NiFi</span>
        <span className="legend-chip"><img alt="" src={elasticsearchIcon} /> Elastic</span>
        <span className="legend-chip"><img alt="" src={oracleIcon} /> Oracle</span>
      </div>
    </div>
  );
};

/* ─── App ────────────────────────────────────────────── */

export function App() {
  const [rawInput, setRawInput] = useState(exampleYaml);
  const [graphs, setGraphs] = useState<PipelineGraph[]>([]);
  const [selectedGraphId, setSelectedGraphId] = useState('');
  const [selectedNodeId, setSelectedNodeId] = useState<string | null>(null);
  const [error, setError] = useState('');
  const [sidebarOpen, setSidebarOpen] = useState(true);
  const [dataSourceOpen, setDataSourceOpen] = useState(false);
  const [yamlExpanded, setYamlExpanded] = useState(false);
  const [selectedEnvironment, setSelectedEnvironment] = useState('all');
  const [hoveredNodeId, setHoveredNodeId] = useState<string | null>(null);
  const [nodeQuery, setNodeQuery] = useState('');
  const searchRef = useRef<HTMLDivElement | null>(null);

  // Parse YAML
  useEffect(() => {
    try {
      const parsed = parseConfigMaps(rawInput);
      setGraphs(parsed.pipelineGraphs);
      setSelectedGraphId((cur) =>
        parsed.pipelineGraphs.some((g) => getGraphKey(g) === cur) ? cur : parsed.pipelineGraphs[0] ? getGraphKey(parsed.pipelineGraphs[0]) : '',
      );
      setSelectedNodeId(null);
      setError('');
    } catch (caught) {
      setGraphs([]);
      setSelectedGraphId('');
      setSelectedNodeId(null);
      setError(caught instanceof Error ? caught.message : 'Unable to parse YAML input.');
    }
  }, [rawInput]);

  const availableEnvironments = useMemo(
    () => [...new Set(graphs.map((g) => g.source.environment))].sort(),
    [graphs],
  );

  useEffect(() => {
    if (selectedEnvironment !== 'all' && !availableEnvironments.includes(selectedEnvironment)) {
      setSelectedEnvironment('all');
    }
  }, [availableEnvironments, selectedEnvironment]);

  const filteredGraphs = useMemo(
    () => graphs.filter((g) => selectedEnvironment === 'all' || g.source.environment === selectedEnvironment),
    [graphs, selectedEnvironment],
  );

  useEffect(() => {
    if (!filteredGraphs.length) {
      setSelectedGraphId('');
      setSelectedNodeId(null);
      return;
    }
    if (!filteredGraphs.some((g) => getGraphKey(g) === selectedGraphId)) {
      setSelectedGraphId(getGraphKey(filteredGraphs[0]));
      setSelectedNodeId(null);
    }
  }, [filteredGraphs, selectedGraphId]);

  const activeGraph = filteredGraphs.find((g) => getGraphKey(g) === selectedGraphId) ?? filteredGraphs[0] ?? null;
  const activeNode = activeGraph?.nodes.find((n) => n.id === selectedNodeId) ?? null;

  const quickFindNodes = useMemo(() => {
    if (!activeGraph || !nodeQuery.trim()) return [];
    const q = nodeQuery.trim().toLowerCase();
    return activeGraph.nodes
      .filter((n) => {
        const meta = Object.values(n.meta).flatMap((v) => (Array.isArray(v) ? v : [v])).join(' ').toLowerCase();
        return n.label.toLowerCase().includes(q) || meta.includes(q);
      })
      .slice(0, 8);
  }, [activeGraph, nodeQuery]);

  // Keyboard: Escape to deselect
  useEffect(() => {
    const onKey = (e: KeyboardEvent) => {
      if (e.key === 'Escape') {
        if (selectedNodeId) setSelectedNodeId(null);
        else if (nodeQuery) setNodeQuery('');
      }
    };
    window.addEventListener('keydown', onKey);
    return () => window.removeEventListener('keydown', onKey);
  }, [selectedNodeId, nodeQuery]);

  // Close search dropdown on outside click
  useEffect(() => {
    if (!quickFindNodes.length) return;
    const onClick = (e: MouseEvent) => {
      if (searchRef.current && !searchRef.current.contains(e.target as Node)) {
        setNodeQuery('');
      }
    };
    document.addEventListener('pointerdown', onClick);
    return () => document.removeEventListener('pointerdown', onClick);
  }, [quickFindNodes.length]);

  const handleFile = async (file: File) => {
    setRawInput(await file.text());
  };

  const visibleProjects = useMemo(() => new Set(filteredGraphs.map((g) => g.source.project)).size, [filteredGraphs]);

  return (
    <div className="app-shell">
      {/* ── Top bar ── */}
      <header className="topbar">
        <button
          className={`topbar-toggle ${sidebarOpen ? 'is-active' : ''}`}
          type="button"
          onClick={() => setSidebarOpen((c) => !c)}
          title="Toggle sidebar"
        >
          ☰
        </button>

        <div className="topbar-brand">
          <div className="topbar-logo">FK</div>
          <h1>{activeGraph?.id ?? 'Pipeline Graph'}</h1>
        </div>

        {activeGraph?.description ? (
          <span style={{ color: 'var(--text-dim)', fontSize: '12px', whiteSpace: 'nowrap', overflow: 'hidden', textOverflow: 'ellipsis', minWidth: 0 }}>
            {activeGraph.description}
          </span>
        ) : null}

        <div className="topbar-stats">
          <span className="stat-chip">{filteredGraphs.length} pipelines</span>
          <span className="stat-chip">{visibleProjects} projects</span>
          {activeGraph ? <span className="stat-chip">{activeGraph.summary.channels} channels</span> : null}
        </div>
      </header>

      {/* ── Left sidebar ── */}
      <nav className={`sidebar ${sidebarOpen ? '' : 'is-collapsed'}`}>
        <div className="sidebar-section" ref={searchRef}>
          <label className="sidebar-label">Search</label>
          <div className="search-box">
            <input
              className="search-input"
              type="text"
              value={nodeQuery}
              onChange={(e) => setNodeQuery(e.target.value)}
              placeholder="Nodes, topics, stores..."
            />
            {quickFindNodes.length > 0 && (
              <div className="search-dropdown">
                {quickFindNodes.map((n) => (
                  <button
                    key={n.id}
                    className="search-hit"
                    type="button"
                    onClick={() => {
                      setSelectedNodeId(n.id);
                      setNodeQuery('');
                    }}
                  >
                    <span className="search-hit-label">{n.label}</span>
                    <span className="search-hit-kind">{getNodeKindLabel(n.kind)}</span>
                  </button>
                ))}
              </div>
            )}
          </div>
        </div>

        <div className="sidebar-section">
          <label className="sidebar-label">Environment</label>
          <select className="env-select" value={selectedEnvironment} onChange={(e) => setSelectedEnvironment(e.target.value)}>
            <option value="all">All environments</option>
            {availableEnvironments.map((env) => (
              <option key={env} value={env}>{env}</option>
            ))}
          </select>
        </div>

        <div className="sidebar-section" style={{ flex: 1, overflow: 'hidden', display: 'flex', flexDirection: 'column', padding: 0 }}>
          <div style={{ padding: '10px 12px 6px' }}>
            <span className="sidebar-label">Pipelines</span>
          </div>
          <div className="pipeline-list">
            {filteredGraphs.map((g) => {
              const key = getGraphKey(g);
              return (
                <button
                  key={key}
                  className={`pipeline-item ${activeGraph && key === getGraphKey(activeGraph) ? 'is-active' : ''}`}
                  type="button"
                  onClick={() => {
                    setSelectedGraphId(key);
                    setSelectedNodeId(null);
                  }}
                >
                  <span className="pipeline-item-name">{g.id}</span>
                  <span className="pipeline-item-meta">{g.source.project} / {g.source.environment}</span>
                </button>
              );
            })}
            {!filteredGraphs.length && (
              <div style={{ padding: '12px', color: 'var(--text-dim)', fontSize: '12px' }}>
                No pipelines match the selected environment.
              </div>
            )}
          </div>
        </div>

        <div className="data-source">
          <button className="btn-sm" type="button" onClick={() => setDataSourceOpen((c) => !c)} style={{ width: '100%', textAlign: 'center' }}>
            {dataSourceOpen ? 'Hide YAML Editor' : 'Edit YAML Data'}
          </button>
          {dataSourceOpen && (
            <div style={{ marginTop: '8px' }}>
              <div className="data-actions">
                <label className="btn-sm" style={{ cursor: 'pointer' }}>
                  Upload
                  <input
                    type="file"
                    accept=".yaml,.yml,text/yaml,text/plain"
                    hidden
                    onChange={(e) => {
                      const file = e.target.files?.[0];
                      if (file) void handleFile(file);
                    }}
                  />
                </label>
                <button className="btn-sm" type="button" onClick={() => setRawInput(exampleYaml)}>
                  Load examples
                </button>
                <button className="btn-sm" type="button" onClick={() => setYamlExpanded((c) => !c)}>
                  {yamlExpanded ? 'Compact' : 'Expand'}
                </button>
              </div>
              <textarea
                className={`yaml-editor ${yamlExpanded ? 'is-expanded' : ''}`}
                value={rawInput}
                onChange={(e) => setRawInput(e.target.value)}
                spellCheck={false}
              />
            </div>
          )}
        </div>
      </nav>

      {/* ── Canvas ── */}
      <section className="canvas-area">
        {error ? (
          <div className="empty-state error-state">
            <div>
              <h2>YAML parse failed</h2>
              <p>{error}</p>
            </div>
          </div>
        ) : activeGraph ? (
          <GraphView
            graph={activeGraph}
            selectedNodeId={selectedNodeId}
            hoveredNodeId={hoveredNodeId}
            onSelectNode={setSelectedNodeId}
            onHoverNode={setHoveredNodeId}
          />
        ) : (
          <div className="empty-state">
            <div>
              <h2>{graphs.length ? 'No pipelines match environment' : 'No pipelines found'}</h2>
              <p>
                {graphs.length
                  ? 'Choose another environment or load different ConfigMap YAML.'
                  : 'Provide a ConfigMap YAML document with an embedded top-level "pipelines" key.'}
              </p>
            </div>
          </div>
        )}
      </section>

      {/* ── Right inspector ── */}
      <aside className={`inspector ${activeNode ? '' : 'is-hidden'}`}>
        {activeNode && (
          <>
            <div className="inspector-header">
              <div>
                <h2>{activeNode.label}</h2>
                <p>{getNodeKindLabel(activeNode.kind)}</p>
              </div>
              <button className="inspector-close" type="button" onClick={() => setSelectedNodeId(null)}>
                ✕
              </button>
            </div>

            {activeGraph && (
              <div className="inspector-section">
                <div className="detail-row">
                  <span className="detail-key">Project</span>
                  <span className="detail-value">{activeGraph.source.project}</span>
                </div>
                <div className="detail-row">
                  <span className="detail-key">Environment</span>
                  <span className="detail-value">{activeGraph.source.environment}</span>
                </div>
                {activeGraph.source.configMapName && (
                  <div className="detail-row">
                    <span className="detail-key">ConfigMap</span>
                    <span className="detail-value">{activeGraph.source.configMapName}</span>
                  </div>
                )}
              </div>
            )}

            <div className="inspector-section">
              {Object.entries(activeNode.meta)
                .filter(([, v]) => v !== undefined && v !== null && v !== '')
                .map(([key, value]) => (
                  <div key={key} className="detail-row">
                    <span className="detail-key">{key}</span>
                    <span className="detail-value">{formatMetaValue(value)}</span>
                  </div>
                ))}
            </div>
          </>
        )}
      </aside>
    </div>
  );
}
