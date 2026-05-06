import { useEffect, useMemo, useRef, useState } from 'react';
import type { PointerEvent as ReactPointerEvent, WheelEvent as ReactWheelEvent } from 'react';
import exampleProdYaml from '../conffigmaps.yaml?raw';
import exampleStageYaml from '../configmaps-staging.yaml?raw';
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

const NODE_RADIUS: Record<GraphNode['kind'], number> = {
  service: 68,
  kafka: 58,
  database: 60,
  unknown: 52,
};

const MIN_SCALE = 0.45;
const MAX_SCALE = 1.8;
const exampleYaml = [exampleProdYaml, exampleStageYaml].join('\n---\n');
const getGraphKey = (graph: PipelineGraph) =>
  `${graph.source.project}|${graph.source.environment}|${graph.source.configMapName ?? 'configmap'}|${graph.id}`;

const clamp = (value: number, min: number, max: number) => Math.min(max, Math.max(min, value));
const toggleValue = (values: string[], value: string) =>
  values.includes(value) ? values.filter((entry) => entry !== value) : [...values, value];

const formatMetaValue = (value: unknown) => {
  if (Array.isArray(value)) {
    return value.join(', ');
  }
  if (typeof value === 'object' && value !== null) {
    return JSON.stringify(value);
  }
  return String(value);
};

const wrapLabel = (value: string, size = 16) => {
  const tokens = value.split(/[-._/\s]+/).filter(Boolean);
  if (!tokens.length) {
    return [value];
  }

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
  if (current) {
    lines.push(current);
  }
  return lines.slice(0, 3);
};

const pathForEdge = (from: GraphNode, to: GraphNode) => {
  const fromRadius = NODE_RADIUS[from.kind];
  const toRadius = NODE_RADIUS[to.kind];
  const direction = (to.x ?? 0) >= (from.x ?? 0) ? 1 : -1;
  const startX = (from.x ?? 0) + fromRadius * direction;
  const startY = from.y ?? 0;
  const endX = (to.x ?? 0) - toRadius * direction;
  const endY = to.y ?? 0;
  const spread = Math.max(100, Math.abs(endX - startX) * 0.35);
  return `M ${startX} ${startY} C ${startX + spread * direction} ${startY}, ${endX - spread * direction} ${endY}, ${endX} ${endY}`;
};

const edgeMidpoint = (from: GraphNode, to: GraphNode) => ({
  x: ((from.x ?? 0) + (to.x ?? 0)) / 2,
  y: ((from.y ?? 0) + (to.y ?? 0)) / 2 - 18,
});

const graphDimensions = (graph: PipelineGraph) => {
  const width = Math.max(...graph.nodes.map((node) => (node.x ?? 0) + NODE_RADIUS[node.kind] + 180), 1400);
  const height = Math.max(...graph.nodes.map((node) => (node.y ?? 0) + NODE_RADIUS[node.kind] + 180), 900);
  return { width, height };
};

const fitGraphToViewport = (graph: PipelineGraph, viewportWidth: number, viewportHeight: number) => {
  const { width, height } = graphDimensions(graph);
  const scale = clamp(Math.min((viewportWidth - 180) / width, (viewportHeight - 180) / height, 1), MIN_SCALE, 1);
  return {
    scale,
    x: (viewportWidth - width * scale) / 2,
    y: (viewportHeight - height * scale) / 2,
  };
};

const FilterGroup = ({
  label,
  values,
  selected,
  onToggle,
  onClear,
}: {
  label: string;
  values: string[];
  selected: string[];
  onToggle: (value: string) => void;
  onClear: () => void;
}) => {
  if (!values.length) {
    return null;
  }

  return (
    <div className="filter-group">
      <span>{label}</span>
      <div className="filter-options">
        <button
          className={`filter-chip ${selected.length === 0 ? 'is-selected' : ''}`}
          type="button"
          onClick={onClear}
        >
          All
        </button>
        {values.map((value) => (
          <button
            key={value}
            className={`filter-chip ${selected.includes(value) ? 'is-selected' : ''}`}
            type="button"
            onClick={() => onToggle(value)}
          >
            {value}
          </button>
        ))}
      </div>
    </div>
  );
};

const GraphView = ({
  graph,
  selectedNodeId,
  onSelectNode,
}: {
  graph: PipelineGraph;
  selectedNodeId: string | null;
  onSelectNode: (nodeId: string | null) => void;
}) => {
  const containerRef = useRef<HTMLDivElement | null>(null);
  const [transform, setTransform] = useState({ x: 0, y: 0, scale: 1 });
  const [isDragging, setIsDragging] = useState(false);
  const [nodePositions, setNodePositions] = useState<Record<string, { x: number; y: number }>>({});
  const dragRef = useRef<{ startX: number; startY: number; originX: number; originY: number } | null>(null);
  const nodeDragRef = useRef<{ nodeId: string; startX: number; startY: number; originX: number; originY: number } | null>(
    null,
  );

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
    () => Object.fromEntries(positionedNodes.map((node) => [node.id, node])),
    [positionedNodes],
  );

  const selectedNeighbors = useMemo(() => {
    if (!selectedNodeId) {
      return new Set<string>();
    }
    const neighbors = new Set<string>([selectedNodeId]);
    for (const edge of graph.edges) {
      if (edge.from === selectedNodeId || edge.to === selectedNodeId) {
        neighbors.add(edge.from);
        neighbors.add(edge.to);
      }
    }
    return neighbors;
  }, [graph.edges, selectedNodeId]);

  useEffect(() => {
    const container = containerRef.current;
    if (!container) {
      return;
    }

    const applyFit = () => {
      setTransform(fitGraphToViewport(graph, container.clientWidth, container.clientHeight));
    };

    applyFit();
    const observer = new ResizeObserver(applyFit);
    observer.observe(container);
    return () => observer.disconnect();
  }, [graph]);

  useEffect(() => {
    setNodePositions({});
  }, [graph.id]);

  const adjustScale = (nextScale: number) => {
    const container = containerRef.current;
    if (!container) {
      return;
    }

    setTransform((current) => {
      const centerX = container.clientWidth / 2;
      const centerY = container.clientHeight / 2;
      const worldX = (centerX - current.x) / current.scale;
      const worldY = (centerY - current.y) / current.scale;
      return {
        scale: nextScale,
        x: centerX - worldX * nextScale,
        y: centerY - worldY * nextScale,
      };
    });
  };

  const handleWheel = (event: ReactWheelEvent<HTMLDivElement>) => {
    event.preventDefault();
    const container = containerRef.current;
    if (!container) {
      return;
    }

    const bounds = container.getBoundingClientRect();
    const cursorX = event.clientX - bounds.left;
    const cursorY = event.clientY - bounds.top;

    setTransform((current) => {
      const nextScale = clamp(current.scale * (event.deltaY > 0 ? 0.92 : 1.08), MIN_SCALE, MAX_SCALE);
      const worldX = (cursorX - current.x) / current.scale;
      const worldY = (cursorY - current.y) / current.scale;
      return {
        scale: nextScale,
        x: cursorX - worldX * nextScale,
        y: cursorY - worldY * nextScale,
      };
    });
  };

  const handlePointerDown = (event: ReactPointerEvent<HTMLDivElement>) => {
    if (event.target instanceof Element && event.target.closest('.graph-node')) {
      return;
    }
    event.currentTarget.setPointerCapture(event.pointerId);
    dragRef.current = {
      startX: event.clientX,
      startY: event.clientY,
      originX: transform.x,
      originY: transform.y,
    };
    setIsDragging(true);
    onSelectNode(null);
  };

  const handlePointerMove = (event: ReactPointerEvent<HTMLDivElement>) => {
    const nodeDrag = nodeDragRef.current;
    if (nodeDrag) {
      setNodePositions((current) => ({
        ...current,
        [nodeDrag.nodeId]: {
          x: nodeDrag.originX + (event.clientX - nodeDrag.startX) / transform.scale,
          y: nodeDrag.originY + (event.clientY - nodeDrag.startY) / transform.scale,
        },
      }));
      return;
    }

    const drag = dragRef.current;
    if (!drag) {
      return;
    }

    setTransform((current) => ({
      ...current,
      x: drag.originX + (event.clientX - drag.startX),
      y: drag.originY + (event.clientY - drag.startY),
    }));
  };

  const handlePointerUp = (event: ReactPointerEvent<HTMLDivElement>) => {
    if (event.currentTarget.hasPointerCapture(event.pointerId)) {
      event.currentTarget.releasePointerCapture(event.pointerId);
    }
    dragRef.current = null;
    nodeDragRef.current = null;
    setIsDragging(false);
  };

  const { width, height } = graphDimensions({ ...graph, nodes: positionedNodes });

  return (
    <div className="graph-stage">
      <div
        ref={containerRef}
        className={`graph-shell ${isDragging ? 'is-dragging' : ''}`}
        onWheel={handleWheel}
        onPointerDown={handlePointerDown}
        onPointerMove={handlePointerMove}
        onPointerUp={handlePointerUp}
        onPointerCancel={handlePointerUp}
      >
        <svg className="graph-canvas" viewBox={`0 0 ${width} ${height}`} role="img" aria-label={`${graph.id} topology`}>
          <defs>
            <pattern id="grid" width="48" height="48" patternUnits="userSpaceOnUse">
              <path d="M 48 0 L 0 0 0 48" fill="none" stroke="rgba(148,163,184,0.08)" strokeWidth="1" />
            </pattern>
            <marker
              id="flow-arrow"
              markerWidth="8"
              markerHeight="8"
              markerUnits="userSpaceOnUse"
              refX="7"
              refY="4"
              orient="auto"
            >
              <path d="M 0 0 L 8 4 L 0 8 z" fill="context-stroke" />
            </marker>
          </defs>

          <g transform={`translate(${transform.x} ${transform.y}) scale(${transform.scale})`}>
            <rect width={width} height={height} fill="url(#grid)" />

            {graph.edges.map((edge) => {
              const from = nodeIndex[edge.from];
              const to = nodeIndex[edge.to];
              if (!from || !to) {
                return null;
              }

              const related = !selectedNodeId || edge.from === selectedNodeId || edge.to === selectedNodeId;
              const mid = edgeMidpoint(from, to);
              return (
                <g key={edge.id} className={related ? 'edge-group is-related' : 'edge-group is-muted'}>
                  <path
                    d={pathForEdge(from, to)}
                    className="flow-edge"
                    stroke={edgeColor[edge.kind]}
                    markerEnd="url(#flow-arrow)"
                  />
                  <text className="edge-label" x={mid.x} y={mid.y}>
                    {edge.channel.split('\n').map((line, index) => (
                      <tspan key={`${edge.id}-${line}`} x={mid.x} dy={index === 0 ? 0 : 14}>
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
              const radius = NODE_RADIUS[node.kind];
              const lines = wrapLabel(node.label, node.kind === 'service' ? 16 : 14);

              return (
                <g
                  className={`graph-node ${active ? 'is-active' : ''} ${muted ? 'is-muted' : ''}`}
                  key={node.id}
                  onClick={(event) => {
                    event.stopPropagation();
                    onSelectNode(node.id);
                  }}
                  onPointerDown={(event) => {
                    event.stopPropagation();
                    containerRef.current?.setPointerCapture(event.pointerId);
                    nodeDragRef.current = {
                      nodeId: node.id,
                      startX: event.clientX,
                      startY: event.clientY,
                      originX: node.x ?? 0,
                      originY: node.y ?? 0,
                    };
                    setIsDragging(true);
                    onSelectNode(node.id);
                  }}
                  transform={`translate(${node.x ?? 0}, ${node.y ?? 0})`}
                >
                  <circle className="node-halo" r={radius + 16} />
                  <circle className="node-core" r={radius} fill="rgba(8, 13, 24, 0.98)" stroke={nodePalette[node.kind]} />
                  <text className="node-kind" x="0" y={-14}>
                    {getNodeKindLabel(node.kind)}
                  </text>
                  <text className="node-label center-text" x="0" y="6">
                    {lines.map((line, index) => (
                      <tspan key={`${node.id}-${line}`} x="0" dy={index === 0 ? 0 : 18}>
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

      <div className="graph-controls">
        <button className="icon-button" type="button" onClick={() => adjustScale(clamp(transform.scale * 1.12, MIN_SCALE, MAX_SCALE))}>
          +
        </button>
        <button className="icon-button" type="button" onClick={() => adjustScale(clamp(transform.scale * 0.9, MIN_SCALE, MAX_SCALE))}>
          -
        </button>
        <button
          className="icon-button wide"
          type="button"
          onClick={() => {
            const container = containerRef.current;
            if (!container) {
              return;
            }
            setTransform(fitGraphToViewport(graph, container.clientWidth, container.clientHeight));
          }}
        >
          Fit
        </button>
      </div>
    </div>
  );
};

export function App() {
  const [rawInput, setRawInput] = useState(exampleYaml);
  const [graphs, setGraphs] = useState<PipelineGraph[]>([]);
  const [selectedGraphId, setSelectedGraphId] = useState<string>('');
  const [selectedNodeId, setSelectedNodeId] = useState<string | null>(null);
  const [sources, setSources] = useState<string[]>([]);
  const [error, setError] = useState<string>('');
  const [leftPanelOpen, setLeftPanelOpen] = useState(false);
  const [yamlExpanded, setYamlExpanded] = useState(false);
  const [selectedProjects, setSelectedProjects] = useState<string[]>([]);
  const [selectedEnvironments, setSelectedEnvironments] = useState<string[]>([]);

  useEffect(() => {
    try {
      const parsed = parseConfigMaps(rawInput);
      setGraphs(parsed.pipelineGraphs);
      setSources(parsed.sources);
      setSelectedGraphId((current) =>
        parsed.pipelineGraphs.some((graph) => getGraphKey(graph) === current) ? current : parsed.pipelineGraphs[0] ? getGraphKey(parsed.pipelineGraphs[0]) : '',
      );
      setSelectedNodeId(null);
      setError('');
    } catch (caught) {
      const message = caught instanceof Error ? caught.message : 'Unable to parse YAML input.';
      setGraphs([]);
      setSources([]);
      setSelectedGraphId('');
      setSelectedNodeId(null);
      setError(message);
    }
  }, [rawInput]);

  const availableProjects = useMemo(
    () => [...new Set(graphs.map((graph) => graph.source.project))].sort((left, right) => left.localeCompare(right)),
    [graphs],
  );
  const availableEnvironments = useMemo(
    () => [...new Set(graphs.map((graph) => graph.source.environment))].sort((left, right) => left.localeCompare(right)),
    [graphs],
  );

  useEffect(() => {
    setSelectedProjects((current) => current.filter((value) => availableProjects.includes(value)));
  }, [availableProjects]);

  useEffect(() => {
    setSelectedEnvironments((current) => current.filter((value) => availableEnvironments.includes(value)));
  }, [availableEnvironments]);

  const filteredGraphs = useMemo(
    () =>
      graphs.filter((graph) => {
        const projectMatch = !selectedProjects.length || selectedProjects.includes(graph.source.project);
        const environmentMatch = !selectedEnvironments.length || selectedEnvironments.includes(graph.source.environment);
        return projectMatch && environmentMatch;
      }),
    [graphs, selectedProjects, selectedEnvironments],
  );

  useEffect(() => {
    if (!filteredGraphs.length) {
      setSelectedGraphId('');
      setSelectedNodeId(null);
      return;
    }

    if (!filteredGraphs.some((graph) => getGraphKey(graph) === selectedGraphId)) {
      setSelectedGraphId(getGraphKey(filteredGraphs[0]));
      setSelectedNodeId(null);
    }
  }, [filteredGraphs, selectedGraphId]);

  const activeGraph = filteredGraphs.find((graph) => getGraphKey(graph) === selectedGraphId) ?? filteredGraphs[0] ?? null;
  const activeNode = activeGraph?.nodes.find((node) => node.id === selectedNodeId) ?? null;
  const visibleSources = useMemo(() => new Set(filteredGraphs.map((graph) => graph.source.sourceLabel)).size, [filteredGraphs]);

  useEffect(() => {
    if (selectedNodeId) {
      setLeftPanelOpen(false);
    }
  }, [selectedNodeId]);

  const handleFile = async (file: File) => {
    const text = await file.text();
    setRawInput(text);
  };

  return (
    <main className="workspace-shell">
      <div className="graph-backdrop" />

      <header className="command-bar">
        <div className="brand-block">
          <div className="brand-mark">FK</div>
          <div>
            <h1>Pipeline Graph</h1>
            <p>
              {activeGraph
                ? `${activeGraph.source.project} / ${activeGraph.source.environment}`
                : 'Visualize OpenShift configmaps as live pipeline topology.'}
            </p>
          </div>
        </div>

        <div className="command-actions">
          <div className="field-stack compact">
            <span>Pipeline</span>
            <select
              value={activeGraph?.id ?? ''}
              onChange={(event) => {
                setSelectedGraphId(event.target.value);
                setSelectedNodeId(null);
              }}
              disabled={!filteredGraphs.length}
            >
              {filteredGraphs.map((graph) => (
                <option key={getGraphKey(graph)} value={getGraphKey(graph)}>
                  {graph.id}
                </option>
              ))}
            </select>
          </div>
          <button className="toolbar-button" type="button" onClick={() => setLeftPanelOpen((current) => !current)}>
            {leftPanelOpen ? 'Close Data' : 'Open Data'}
          </button>
        </div>
      </header>

      <div className="filter-dock">
        <FilterGroup
          label="Project"
          values={availableProjects}
          selected={selectedProjects}
          onToggle={(value) => setSelectedProjects((current) => toggleValue(current, value))}
          onClear={() => setSelectedProjects([])}
        />
        <FilterGroup
          label="Environment"
          values={availableEnvironments}
          selected={selectedEnvironments}
          onToggle={(value) => setSelectedEnvironments((current) => toggleValue(current, value))}
          onClear={() => setSelectedEnvironments([])}
        />
      </div>

      <div className="status-strip">
        <span>{filteredGraphs.length} pipelines</span>
        <span>{visibleSources} sources</span>
        {activeGraph ? <span>{activeGraph.summary.channels} channels</span> : null}
        {activeGraph ? <span>{activeGraph.summary.services} processors</span> : null}
        {activeGraph ? <span>{activeGraph.source.configMapName ?? 'configmap'}</span> : null}
      </div>

      {leftPanelOpen ? (
        <aside className="overlay-panel overlay-panel-left">
          <div className="panel-header">
            <div>
              <h2>Data Source</h2>
              <p>Paste or upload ConfigMap YAML.</p>
            </div>
            <button className="icon-button" type="button" onClick={() => setLeftPanelOpen(false)}>
              ×
            </button>
          </div>

          <div className="panel-section">
            <div className="summary-pills">
              <span>{filteredGraphs.length} pipelines</span>
              <span>{visibleSources} sources</span>
              {activeGraph ? <span>{activeGraph.summary.stores} stores</span> : null}
            </div>
          </div>

          <div className="panel-section">
            <FilterGroup
              label="Project"
              values={availableProjects}
              selected={selectedProjects}
              onToggle={(value) => setSelectedProjects((current) => toggleValue(current, value))}
              onClear={() => setSelectedProjects([])}
            />
          </div>

          <div className="panel-section">
            <FilterGroup
              label="Environment"
              values={availableEnvironments}
              selected={selectedEnvironments}
              onToggle={(value) => setSelectedEnvironments((current) => toggleValue(current, value))}
              onClear={() => setSelectedEnvironments([])}
            />
          </div>

          <div className="panel-section">
            <div className="field-stack">
              <span>Pipeline</span>
              <select
                value={activeGraph?.id ?? ''}
                onChange={(event) => {
                  setSelectedGraphId(event.target.value);
                  setSelectedNodeId(null);
                }}
                disabled={!filteredGraphs.length}
              >
                {filteredGraphs.map((graph) => (
                  <option key={getGraphKey(graph)} value={getGraphKey(graph)}>
                    {graph.id} ({graph.source.project})
                  </option>
                ))}
              </select>
            </div>
          </div>

          <div className="panel-section">
            <div className="panel-actions">
              <label className="toolbar-button secondary">
                Upload YAML
                <input
                  type="file"
                  accept=".yaml,.yml,text/yaml,text/plain"
                  hidden
                  onChange={(event) => {
                    const file = event.target.files?.[0];
                    if (file) {
                      void handleFile(file);
                    }
                  }}
                />
              </label>
              <button className="toolbar-button secondary" type="button" onClick={() => setRawInput(exampleYaml)}>
                Example
              </button>
              <button className="toolbar-button secondary" type="button" onClick={() => setYamlExpanded((current) => !current)}>
                {yamlExpanded ? 'Compact' : 'Expand'}
              </button>
            </div>
            <textarea
              className={`yaml-editor ${yamlExpanded ? 'is-expanded' : ''}`}
              value={rawInput}
              onChange={(event) => setRawInput(event.target.value)}
              spellCheck={false}
            />
          </div>
        </aside>
      ) : null}

      {activeNode ? (
        <aside className="overlay-panel overlay-panel-right">
          <div className="panel-header">
            <div>
              <h2>{activeNode.label}</h2>
              <p>{getNodeKindLabel(activeNode.kind)}</p>
            </div>
            <button className="icon-button" type="button" onClick={() => setSelectedNodeId(null)}>
              ×
            </button>
          </div>

          <div className="panel-section">
            <div className="detail-list">
              <div className="detail-row">
                <span>project</span>
                <strong>{activeGraph.source.project}</strong>
              </div>
              <div className="detail-row">
                <span>environment</span>
                <strong>{activeGraph.source.environment}</strong>
              </div>
              {activeGraph.source.configMapName ? (
                <div className="detail-row">
                  <span>configMap</span>
                  <strong>{activeGraph.source.configMapName}</strong>
                </div>
              ) : null}
            </div>
          </div>

          <div className="panel-section">
            <div className="detail-list">
              {Object.entries(activeNode.meta)
                .filter(([, value]) => value !== undefined && value !== null && value !== '')
                .map(([key, value]) => (
                  <div key={key} className="detail-row">
                    <span>{key}</span>
                    <strong>{formatMetaValue(value)}</strong>
                  </div>
                ))}
            </div>
          </div>
        </aside>
      ) : null}

      <section className="canvas-shell">
        {error ? (
          <div className="empty-state error-state">
            <h2>YAML parse failed</h2>
            <p>{error}</p>
          </div>
        ) : activeGraph ? (
          <>
            <GraphView graph={activeGraph} selectedNodeId={selectedNodeId} onSelectNode={setSelectedNodeId} />
            <div className="legend-dock">
              {Object.entries(nodePalette).map(([kind, color]) => (
                <span key={kind}>
                  <i style={{ backgroundColor: color }} />
                  {getNodeKindLabel(kind as GraphNode['kind'])}
                </span>
              ))}
            </div>
          </>
        ) : (
          <div className="empty-state">
            <h2>{graphs.length ? 'No pipelines match filters' : 'No pipelines found'}</h2>
            <p>
              {graphs.length
                ? 'Adjust the selected project or environment filters.'
                : 'Provide a ConfigMap YAML document with an embedded top-level `pipelines` object.'}
            </p>
          </div>
        )}
      </section>
    </main>
  );
}
