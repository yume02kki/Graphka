export type NodeKind = 'service' | 'kafka' | 'database' | 'unknown';

export interface GraphNode {
  id: string;
  label: string;
  kind: NodeKind;
  column: number;
  row: number;
  x?: number;
  y?: number;
  meta: Record<string, unknown>;
}

export interface GraphEdge {
  id: string;
  from: string;
  to: string;
  channel: string;
  kind: 'consume' | 'produce' | 'output' | 'write' | 'dependency';
}

export interface PipelineGraph {
  id: string;
  description?: string;
  nodes: GraphNode[];
  edges: GraphEdge[];
  source: {
    configMapName?: string;
    project: string;
    environment: string;
    sourceLabel: string;
  };
  summary: {
    services: number;
    kafkas: number;
    stores: number;
    channels: number;
  };
}

export interface ParsedConfig {
  pipelineGraphs: PipelineGraph[];
  sources: string[];
}
