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
  const serviceIds = allNodes.filter((n) => n.kind === 'service').map((n) => n.id);

  // Build adjacency for services through shared channels
  const upstreamServices = new Map<string, Set<string>>();
  const downstreamServices = new Map<string, Set<string>>();

  for (const [channel, consumers] of consumedChannels.entries()) {
    const producers = producedChannels.get(channel) ?? [];
    for (const consumerId of consumers) {
      for (const producerId of producers) {
        if (!upstreamServices.has(consumerId)) upstreamServices.set(consumerId, new Set());
        upstreamServices.get(consumerId)!.add(producerId);
        if (!downstreamServices.has(producerId)) downstreamServices.set(producerId, new Set());
        downstreamServices.get(producerId)!.add(consumerId);
      }
    }
  }

  // ── Column assignment (left-to-right depth) ──
  // Services get odd columns; intermediaries (kafka/db) get even columns between them.
  const depthCache = new Map<string, number>();
  const computeDepth = (id: string, trail = new Set<string>()): number => {
    if (depthCache.has(id)) return depthCache.get(id)!;
    if (trail.has(id)) return 0;
    trail.add(id);
    const parents = [...(upstreamServices.get(id) ?? [])];
    const depth = parents.length ? Math.max(...parents.map((p) => computeDepth(p, new Set(trail)))) + 1 : 0;
    depthCache.set(id, depth);
    return depth;
  };

  for (const sid of serviceIds) {
    nodeMap.get(sid)!.column = computeDepth(sid) * 2 + 1;
  }

  // Place non-service nodes between their producers and consumers
  for (const node of allNodes) {
    if (node.kind === 'service') continue;
    const inCols = edges.filter((e) => e.to === node.id).map((e) => nodeMap.get(e.from)?.column ?? 0);
    const outCols = edges.filter((e) => e.from === node.id).map((e) => nodeMap.get(e.to)?.column ?? 0);

    if (inCols.length && outCols.length) {
      // Place halfway between max producer column and min consumer column
      node.column = Math.round((Math.max(...inCols) + Math.min(...outCols)) / 2);
    } else if (inCols.length) {
      node.column = Math.max(...inCols) + 1;
    } else if (outCols.length) {
      node.column = Math.max(0, Math.min(...outCols) - 1);
    } else {
      node.column = 0;
    }
  }

  // ── Row assignment (vertical spread) ──
  // Goal: spread nodes vertically so the graph uses space well and data flow is readable.

  // 1. Assign rows to services using topological order + fan-out spreading
  const serviceDepths = serviceIds.map((id) => ({ id, depth: depthCache.get(id) ?? 0 }));
  serviceDepths.sort((a, b) => a.depth - b.depth || a.id.localeCompare(b.id));

  const rowAssignment = new Map<string, number>();

  // Group services by depth layer
  const layers = new Map<number, string[]>();
  for (const { id, depth } of serviceDepths) {
    if (!layers.has(depth)) layers.set(depth, []);
    layers.get(depth)!.push(id);
  }

  // For each layer, spread services vertically.
  // Downstream services are positioned near their upstream parents but fanned out.
  const sortedDepths = [...layers.keys()].sort((a, b) => a - b);

  for (const depth of sortedDepths) {
    const layerServices = layers.get(depth)!;

    if (depth === 0) {
      // Root services: spread evenly
      layerServices.sort((a, b) => {
        const aDown = downstreamServices.get(a)?.size ?? 0;
        const bDown = downstreamServices.get(b)?.size ?? 0;
        return bDown - aDown || a.localeCompare(b);
      });
      layerServices.forEach((id, i) => {
        rowAssignment.set(id, i);
      });
    } else {
      // Non-root: position based on upstream parents, then fan out
      const positioned: { id: string; target: number }[] = [];
      for (const id of layerServices) {
        const parents = [...(upstreamServices.get(id) ?? [])];
        const parentRows = parents.map((p) => rowAssignment.get(p)).filter((r): r is number => r !== undefined);
        const target = parentRows.length ? average(parentRows) : 0;
        positioned.push({ id, target });
      }

      // Sort by target row, then fan out siblings that share the same parent
      positioned.sort((a, b) => a.target - b.target || a.id.localeCompare(b.id));

      // Group by shared parent set for fan-out
      const parentKey = (id: string) => [...(upstreamServices.get(id) ?? [])].sort().join(',');
      const groups = new Map<string, typeof positioned>();
      for (const item of positioned) {
        const key = parentKey(item.id);
        if (!groups.has(key)) groups.set(key, []);
        groups.get(key)!.push(item);
      }

      for (const group of groups.values()) {
        if (group.length > 1) {
          const center = average(group.map((g) => g.target));
          group.forEach((item, i) => {
            item.target = center + (i - (group.length - 1) / 2) * 1.0;
          });
        }
      }

      // Re-sort after fan-out adjustment
      positioned.sort((a, b) => a.target - b.target || a.id.localeCompare(b.id));

      for (const item of positioned) {
        rowAssignment.set(item.id, item.target);
      }
    }
  }

  // 2. Assign rows to non-service nodes
  // Each non-service node is placed at the average row of its connected services,
  // but if multiple non-service nodes share a column, spread them out.
  for (const node of allNodes) {
    if (node.kind === 'service') continue;
    const neighborRows = edges
      .filter((e) => e.from === node.id || e.to === node.id)
      .map((e) => {
        const otherId = e.from === node.id ? e.to : e.from;
        return rowAssignment.get(otherId);
      })
      .filter((r): r is number => r !== undefined);

    if (neighborRows.length) {
      rowAssignment.set(node.id, average(neighborRows));
    } else {
      rowAssignment.set(node.id, 0);
    }
  }

  // 3. De-overlap: within each column, enforce minimum vertical spacing
  const nodesByColumn = new Map<number, GraphNode[]>();
  for (const node of allNodes) {
    const col = node.column;
    if (!nodesByColumn.has(col)) nodesByColumn.set(col, []);
    nodesByColumn.get(col)!.push(node);
  }

  for (const bucket of nodesByColumn.values()) {
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

  // 4. Center the whole layout vertically (shift so min row = 0)
  const allRows = [...rowAssignment.values()];
  const minRow = Math.min(...allRows);
  if (minRow !== 0) {
    for (const [id, row] of rowAssignment) {
      rowAssignment.set(id, row - minRow);
    }
  }

  // Apply column and row to nodes, convert to pixel positions
  for (const node of allNodes) {
    node.row = rowAssignment.get(node.id) ?? 0;
  }

  const COL_WIDTH = 320;
  const ROW_HEIGHT = 180;
  const nodes = allNodes.map((node) => ({
    ...node,
    x: 200 + node.column * COL_WIDTH,
    y: 160 + node.row * ROW_HEIGHT,
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
