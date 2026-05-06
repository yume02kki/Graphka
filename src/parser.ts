import { parseAllDocuments } from 'yaml';
import type { GraphEdge, GraphNode, NodeKind, ParsedConfig, PipelineGraph } from './types';

type AnyRecord = Record<string, unknown>;

interface PipelineDefinition {
  description?: string;
  services?: Record<string, AnyRecord>;
  kafkas?: Record<string, AnyRecord>;
  databases?: Record<string, AnyRecord>;
}

interface PipelineSourceContext {
  configMapName?: string;
  project?: string;
  environment?: string;
}

interface ParsedReference {
  scheme: string;
  target: string;
  name: string;
}

interface PipelineMatch {
  sourceLabel: string;
  pipelines: Record<string, PipelineDefinition>;
  context: PipelineSourceContext;
}

const isRecord = (value: unknown): value is AnyRecord =>
  typeof value === 'object' && value !== null && !Array.isArray(value);

const toRecord = (value: unknown): AnyRecord => (isRecord(value) ? value : {});

const asStringList = (value: unknown): string[] =>
  Array.isArray(value) ? value.filter((entry): entry is string => typeof entry === 'string') : [];

const safeSlug = (value: string): string =>
  value.toLowerCase().replace(/[^a-z0-9]+/g, '-').replace(/^-|-$/g, '');

const capitalize = (value: string): string => value.charAt(0).toUpperCase() + value.slice(1);

const average = (values: number[]) => (values.length ? values.reduce((sum, value) => sum + value, 0) / values.length : 0);

const pushMetaList = (meta: Record<string, unknown>, key: string, value: string) => {
  const existing = Array.isArray(meta[key]) ? (meta[key] as string[]) : [];
  if (!existing.includes(value)) {
    meta[key] = [...existing, value];
  }
};

const findPipelineDocuments = (
  input: unknown,
  sourceLabel: string,
  matches: PipelineMatch[],
  inheritedContext: PipelineSourceContext = {},
) => {
  if (Array.isArray(input)) {
    input.forEach((entry, index) => findPipelineDocuments(entry, `${sourceLabel}:list-${index + 1}`, matches, inheritedContext));
    return;
  }

  if (!isRecord(input)) {
    return;
  }

  if (isRecord(input.pipelines)) {
    matches.push({
      sourceLabel,
      pipelines: input.pipelines as Record<string, PipelineDefinition>,
      context: inheritedContext,
    });
  }

  if (input.kind === 'ConfigMap' && isRecord(input.data)) {
    const metadata = toRecord(input.metadata);
    const labels = toRecord(metadata.labels);
    const nextContext: PipelineSourceContext = {
      configMapName: typeof metadata.name === 'string' ? metadata.name : inheritedContext.configMapName,
      project: typeof metadata.namespace === 'string' ? metadata.namespace : inheritedContext.project,
      environment:
        typeof labels.environment === 'string'
          ? labels.environment
          : typeof labels['app.kubernetes.io/environment'] === 'string'
            ? (labels['app.kubernetes.io/environment'] as string)
            : inheritedContext.environment,
    };

    for (const [key, value] of Object.entries(input.data)) {
      if (typeof value !== 'string') {
        continue;
      }

      try {
        const nested = parseAllDocuments(value).map((document) => document.toJSON()).filter(Boolean);
        nested.forEach((entry, index) => findPipelineDocuments(entry, `${sourceLabel}:${key}:${index + 1}`, matches, nextContext));
      } catch {
        continue;
      }
    }
  }

  if (Array.isArray(input.items)) {
    input.items.forEach((entry, index) => findPipelineDocuments(entry, `${sourceLabel}:item-${index + 1}`, matches, inheritedContext));
  }

  for (const [key, value] of Object.entries(input)) {
    if (key === 'data' || key === 'items' || key === 'pipelines') {
      continue;
    }
    if (isRecord(value) || Array.isArray(value)) {
      findPipelineDocuments(value, `${sourceLabel}:${key}`, matches, inheritedContext);
    }
  }
};

const parseReference = (reference: string): ParsedReference | null => {
  const match = reference.match(/^([a-z0-9+.-]+):\/\/([^/]+)\/(.+)$/i);
  if (!match) {
    return null;
  }

  return {
    scheme: match[1],
    target: match[2],
    name: match[3],
  };
};

const inferNodeKind = (scheme: string): NodeKind => {
  if (scheme === 'kafka') {
    return 'kafka';
  }
  if (scheme === 'oracle' || scheme === 'elastic') {
    return 'database';
  }
  return 'unknown';
};

const inferServiceIconKey = (serviceConfig: AnyRecord) => {
  const type = typeof serviceConfig.type === 'string' ? serviceConfig.type.toLowerCase() : '';
  if (type.includes('flink')) {
    return 'flink';
  }
  if (type.includes('nifi')) {
    return 'nifi';
  }
  if (type.includes('dotnet') || type.includes('csharp')) {
    return 'dotnet';
  }
  return 'service';
};

const inferStoreIconKey = (databaseConfig: AnyRecord) => {
  const type = typeof databaseConfig.type === 'string' ? databaseConfig.type.toLowerCase() : '';
  if (type.includes('elastic')) {
    return 'elasticsearch';
  }
  if (type.includes('oracle')) {
    return 'oracle';
  }
  return 'database';
};

const inferReferenceIconKey = (parsed: ParsedReference) => {
  if (parsed.scheme === 'kafka') {
    return 'kafka';
  }
  if (parsed.scheme === 'elastic') {
    return 'elasticsearch';
  }
  if (parsed.scheme === 'oracle') {
    return 'oracle';
  }
  return 'database';
};

const buildPipelineGraph = (
  pipelineId: string,
  pipeline: PipelineDefinition,
  context: PipelineSourceContext,
  sourceLabel: string,
): PipelineGraph => {
  const nodeMap = new Map<string, GraphNode>();
  const edgeMap = new Map<string, GraphEdge>();
  const producedChannels = new Map<string, string[]>();
  const consumedChannels = new Map<string, string[]>();
  const channels = new Set<string>();

  const addNode = (node: GraphNode) => {
    const existing = nodeMap.get(node.id);
    if (existing) {
      existing.meta = { ...existing.meta, ...node.meta };
      existing.label = existing.label || node.label;
      return existing;
    }
    nodeMap.set(node.id, node);
    return node;
  };

  const addEdge = (edge: Omit<GraphEdge, 'id'>) => {
    const key = `${edge.from}|${edge.to}|${edge.kind}`;
    const existing = edgeMap.get(key);
    if (!existing) {
      edgeMap.set(key, {
        ...edge,
        id: `edge:${safeSlug(key)}`,
      });
      return;
    }

    const labels = new Set(
      existing.channel
        .split('\n')
        .map((entry) => entry.trim())
        .filter(Boolean),
    );
    labels.add(edge.channel);
    existing.channel = [...labels].join('\n');
  };

  const ensureReferenceNode = (reference: string) => {
    const parsed = parseReference(reference);
    if (!parsed) {
      return addNode({
        id: `ref:${reference}`,
        label: reference,
        kind: 'unknown',
        column: 0,
        row: 0,
        meta: { reference },
      });
    }

    const id = parsed.scheme === 'kafka' ? `kafka:${parsed.target}` : `database:${parsed.target}`;
    const node = addNode({
      id,
      label: parsed.target,
      kind: inferNodeKind(parsed.scheme),
      column: 0,
      row: 0,
      meta: {
        iconKey: inferReferenceIconKey(parsed),
        scheme: parsed.scheme,
        target: parsed.target,
      },
    });
    pushMetaList(node.meta, 'entities', parsed.name);
    pushMetaList(node.meta, 'references', reference);
    return node;
  };

  for (const [kafkaAlias, kafkaConfig] of Object.entries(toRecord(pipeline.kafkas))) {
    addNode({
      id: `kafka:${kafkaAlias}`,
      label: kafkaAlias,
      kind: 'kafka',
      column: 0,
      row: 0,
      meta: {
        iconKey: 'kafka',
        kafkaAlias,
        topics: asStringList(toRecord(kafkaConfig).topics),
        bootstrapServers: toRecord(kafkaConfig).bootstrapServers,
      },
    });
  }

  for (const [databaseId, databaseConfig] of Object.entries(toRecord(pipeline.databases))) {
    addNode({
      id: `database:${databaseId}`,
      label: databaseId,
      kind: 'database',
      column: 0,
      row: 0,
      meta: {
        iconKey: inferStoreIconKey(toRecord(databaseConfig)),
        ...toRecord(databaseConfig),
      },
    });
  }

  for (const [serviceId, serviceConfigRaw] of Object.entries(toRecord(pipeline.services))) {
    const serviceConfig = toRecord(serviceConfigRaw);
    addNode({
      id: `service:${serviceId}`,
      label: serviceId,
      kind: 'service',
      column: 0,
      row: 0,
      meta: {
        iconKey: inferServiceIconKey(serviceConfig),
        ...serviceConfig,
      },
    });

    for (const input of asStringList(serviceConfig.consumes)) {
      channels.add(input);
      consumedChannels.set(input, [...(consumedChannels.get(input) ?? []), `service:${serviceId}`]);
      const source = ensureReferenceNode(input);
      const parsed = parseReference(input);
      addEdge({
        from: source.id,
        to: `service:${serviceId}`,
        channel: parsed?.name ?? input,
        kind: 'consume',
      });
    }

    for (const output of asStringList(serviceConfig.produces)) {
      channels.add(output);
      producedChannels.set(output, [...(producedChannels.get(output) ?? []), `service:${serviceId}`]);
      const target = ensureReferenceNode(output);
      const parsed = parseReference(output);
      addEdge({
        from: `service:${serviceId}`,
        to: target.id,
        channel: parsed?.name ?? output,
        kind: 'produce',
      });
    }

    for (const output of asStringList(serviceConfig.outputs)) {
      channels.add(output);
      producedChannels.set(output, [...(producedChannels.get(output) ?? []), `service:${serviceId}`]);
      const target = ensureReferenceNode(output);
      const parsed = parseReference(output);
      addEdge({
        from: `service:${serviceId}`,
        to: target.id,
        channel: parsed?.name ?? output,
        kind: 'output',
      });
    }

    for (const sink of asStringList(serviceConfig.writesTo)) {
      channels.add(sink);
      const target = ensureReferenceNode(sink);
      const parsed = parseReference(sink);
      addEdge({
        from: `service:${serviceId}`,
        to: target.id,
        channel: parsed?.name ?? sink,
        kind: 'write',
      });
    }

    for (const dependency of asStringList(serviceConfig.dependencies)) {
      const parsed = parseReference(dependency);
      const source = parsed
        ? ensureReferenceNode(dependency)
        : addNode({
            id: `database:${dependency}`,
            label: dependency,
            kind: 'database',
            column: 0,
            row: 0,
            meta: {},
          });
      addEdge({
        from: source.id,
        to: `service:${serviceId}`,
        channel: parsed?.name ?? dependency,
        kind: 'dependency',
      });
    }
  }

  const edges = [...edgeMap.values()];
  const allNodes = [...nodeMap.values()];

  // ── Build full directed adjacency on ALL nodes (not just services) ──
  const predecessors = new Map<string, Set<string>>();
  const successors = new Map<string, Set<string>>();
  for (const edge of edges) {
    if (!successors.has(edge.from)) successors.set(edge.from, new Set());
    successors.get(edge.from)!.add(edge.to);
    if (!predecessors.has(edge.to)) predecessors.set(edge.to, new Set());
    predecessors.get(edge.to)!.add(edge.from);
  }

  // ── Column assignment via longest-path layering on all nodes ──
  // This gives each node a column based on its longest incoming path,
  // which naturally spreads the graph left-to-right along data flow.
  const colCache = new Map<string, number>();
  const computeCol = (id: string, trail = new Set<string>()): number => {
    if (colCache.has(id)) return colCache.get(id)!;
    if (trail.has(id)) return 0; // cycle
    trail.add(id);
    const preds = [...(predecessors.get(id) ?? [])];
    const col = preds.length
      ? Math.max(...preds.map((p) => computeCol(p, new Set(trail)))) + 1
      : 0;
    colCache.set(id, col);
    return col;
  };

  for (const node of allNodes) {
    node.column = computeCol(node.id);
  }

  // ── Row assignment ──
  // Strategy: use the graph structure to assign initial rows, then de-overlap.
  //
  // 1. Find all "root" nodes (no predecessors) — these seed the layout.
  // 2. BFS/topological order: assign each node a row based on where its
  //    predecessors are, fanning out children of the same parent.
  // 3. For nodes with multiple predecessors, use the median of parent rows.
  // 4. De-overlap columns.

  const rowAssignment = new Map<string, number>();

  // Topological order (Kahn's algorithm), handles cycles gracefully
  const inDegree = new Map<string, number>();
  for (const node of allNodes) inDegree.set(node.id, 0);
  for (const edge of edges) {
    inDegree.set(edge.to, (inDegree.get(edge.to) ?? 0) + 1);
  }

  const topoQueue: string[] = [];
  for (const node of allNodes) {
    if ((inDegree.get(node.id) ?? 0) === 0) topoQueue.push(node.id);
  }

  const topoOrder: string[] = [];
  const visited = new Set<string>();
  while (topoQueue.length) {
    // Sort queue so nodes with fewer connections come first (stable ordering)
    topoQueue.sort((a, b) => (nodeMap.get(a)?.label ?? a).localeCompare(nodeMap.get(b)?.label ?? b));
    const id = topoQueue.shift()!;
    if (visited.has(id)) continue;
    visited.add(id);
    topoOrder.push(id);
    for (const succ of (successors.get(id) ?? [])) {
      const deg = (inDegree.get(succ) ?? 1) - 1;
      inDegree.set(succ, deg);
      if (deg <= 0 && !visited.has(succ)) topoQueue.push(succ);
    }
  }
  // Add any remaining nodes (cycles)
  for (const node of allNodes) {
    if (!visited.has(node.id)) topoOrder.push(node.id);
  }

  // Assign rows in topological order
  // Track how many children each parent has placed, to fan out
  const childIndex = new Map<string, number>();

  for (const id of topoOrder) {
    const preds = [...(predecessors.get(id) ?? [])];
    const parentRows = preds
      .map((p) => rowAssignment.get(p))
      .filter((r): r is number => r !== undefined);

    if (!parentRows.length) {
      // Root node: stack roots vertically with generous spacing
      const existingRoots = [...rowAssignment.values()];
      const nextRow = existingRoots.length
        ? Math.max(...existingRoots) + 1.5
        : 0;
      // But first check — maybe other roots at same column already placed
      rowAssignment.set(id, nextRow);
    } else {
      // Place at median of parent rows
      const sorted = [...parentRows].sort((a, b) => a - b);
      const median = sorted[Math.floor(sorted.length / 2)];

      // Offset based on how many siblings already placed from same parents
      const parentKey = preds.sort().join(',');
      const siblingIdx = childIndex.get(parentKey) ?? 0;
      childIndex.set(parentKey, siblingIdx + 1);

      // Count total siblings from this parent set
      const totalSiblings = preds.length === 1
        ? (successors.get(preds[0])?.size ?? 1)
        : 1;
      const fanOffset = totalSiblings > 1
        ? (siblingIdx - (totalSiblings - 1) / 2) * 1.2
        : 0;

      rowAssignment.set(id, median + fanOffset);
    }
  }

  // De-overlap: within each column, enforce minimum vertical spacing
  const nodesByColumn = new Map<number, GraphNode[]>();
  for (const node of allNodes) {
    if (!nodesByColumn.has(node.column)) nodesByColumn.set(node.column, []);
    nodesByColumn.get(node.column)!.push(node);
  }

  // Multiple passes to let adjustments propagate
  for (let pass = 0; pass < 3; pass++) {
    for (const [, bucket] of nodesByColumn) {
      bucket.sort((a, b) => {
        const diff = (rowAssignment.get(a.id) ?? 0) - (rowAssignment.get(b.id) ?? 0);
        if (Math.abs(diff) > 0.001) return diff;
        return a.label.localeCompare(b.label);
      });

      for (let i = 1; i < bucket.length; i++) {
        const prevRow = rowAssignment.get(bucket[i - 1].id) ?? 0;
        const curRow = rowAssignment.get(bucket[i].id) ?? 0;
        if (curRow - prevRow < 1.0) {
          rowAssignment.set(bucket[i].id, prevRow + 1.0);
        }
      }
    }

    // After de-overlapping, pull nodes toward the median of their neighbors
    // to reduce edge lengths (but respect the de-overlap constraints)
    for (const id of topoOrder) {
      const neighbors: number[] = [];
      for (const p of (predecessors.get(id) ?? [])) {
        const r = rowAssignment.get(p);
        if (r !== undefined) neighbors.push(r);
      }
      for (const s of (successors.get(id) ?? [])) {
        const r = rowAssignment.get(s);
        if (r !== undefined) neighbors.push(r);
      }
      if (!neighbors.length) continue;

      neighbors.sort((a, b) => a - b);
      const median = neighbors[Math.floor(neighbors.length / 2)];
      const current = rowAssignment.get(id) ?? 0;
      // Gently pull toward median
      rowAssignment.set(id, current + (median - current) * 0.3);
    }
  }

  // Final de-overlap pass
  for (const [, bucket] of nodesByColumn) {
    bucket.sort((a, b) => {
      const diff = (rowAssignment.get(a.id) ?? 0) - (rowAssignment.get(b.id) ?? 0);
      if (Math.abs(diff) > 0.001) return diff;
      return a.label.localeCompare(b.label);
    });
    for (let i = 1; i < bucket.length; i++) {
      const prevRow = rowAssignment.get(bucket[i - 1].id) ?? 0;
      const curRow = rowAssignment.get(bucket[i].id) ?? 0;
      if (curRow - prevRow < 1.0) {
        rowAssignment.set(bucket[i].id, prevRow + 1.0);
      }
    }
  }

  // Center vertically: shift so min row = 0
  const allRows = [...rowAssignment.values()];
  const minRow = allRows.length ? Math.min(...allRows) : 0;
  if (minRow !== 0) {
    for (const [id, row] of rowAssignment) {
      rowAssignment.set(id, row - minRow);
    }
  }

  for (const node of allNodes) {
    node.row = rowAssignment.get(node.id) ?? 0;
  }

  const COL_WIDTH = 340;
  const ROW_HEIGHT = 200;
  const nodes = allNodes.map((node) => ({
    ...node,
    x: 220 + node.column * COL_WIDTH,
    y: 180 + node.row * ROW_HEIGHT,
  }));

  return {
    id: pipelineId,
    description: pipeline.description,
    nodes,
    edges,
    source: {
      configMapName: context.configMapName,
      project: context.project ?? 'unknown-project',
      environment: context.environment ?? 'unknown',
      sourceLabel,
    },
    summary: {
      services: nodes.filter((node) => node.kind === 'service').length,
      kafkas: nodes.filter((node) => node.kind === 'kafka').length,
      stores: nodes.filter((node) => node.kind === 'database').length,
      channels: channels.size,
    },
  };
};

export const parseConfigMaps = (rawText: string): ParsedConfig => {
  const documents = parseAllDocuments(rawText).map((document) => document.toJSON()).filter(Boolean);
  const matches: PipelineMatch[] = [];

  documents.forEach((document, index) => findPipelineDocuments(document, `document-${index + 1}`, matches));

  const pipelineGraphs: PipelineGraph[] = [];
  const sources: string[] = [];

  for (const match of matches) {
    sources.push(match.sourceLabel);
    for (const [pipelineId, pipeline] of Object.entries(match.pipelines)) {
      pipelineGraphs.push(buildPipelineGraph(pipelineId, pipeline, match.context, match.sourceLabel));
    }
  }

  if (!pipelineGraphs.length) {
    throw new Error('No embedded pipelines were found. Expected a ConfigMap data entry containing a YAML object with a top-level "pipelines" key.');
  }

  return {
    pipelineGraphs: pipelineGraphs.sort((left, right) => left.id.localeCompare(right.id)),
    sources: [...new Set(sources)],
  };
};

export const getNodeKindLabel = (kind: NodeKind) => {
  if (kind === 'service') {
    return 'Processor';
  }
  if (kind === 'kafka') {
    return 'Kafka';
  }
  if (kind === 'database') {
    return 'Store';
  }
  return capitalize(kind);
};
