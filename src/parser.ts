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
      meta: { ...toRecord(databaseConfig) },
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
      meta: { ...serviceConfig },
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
  const serviceIds = [...nodeMap.values()].filter((node) => node.kind === 'service').map((node) => node.id);
  const upstreamServices = new Map<string, string[]>();
  const downstreamServices = new Map<string, string[]>();

  for (const [channel, consumers] of consumedChannels.entries()) {
    const producers = producedChannels.get(channel) ?? [];
    for (const consumerId of consumers) {
      for (const producerId of producers) {
        upstreamServices.set(consumerId, [...(upstreamServices.get(consumerId) ?? []), producerId]);
        downstreamServices.set(producerId, [...(downstreamServices.get(producerId) ?? []), consumerId]);
      }
    }
  }

  const serviceDepthCache = new Map<string, number>();
  const visitDepth = (serviceId: string, trail = new Set<string>()): number => {
    if (serviceDepthCache.has(serviceId)) {
      return serviceDepthCache.get(serviceId)!;
    }
    if (trail.has(serviceId)) {
      return 0;
    }
    trail.add(serviceId);
    const parents = upstreamServices.get(serviceId) ?? [];
    const depth = parents.length ? Math.max(...parents.map((parent) => visitDepth(parent, new Set(trail)))) + 1 : 0;
    serviceDepthCache.set(serviceId, depth);
    return depth;
  };

  serviceIds.forEach((serviceId) => {
    const node = nodeMap.get(serviceId)!;
    node.column = visitDepth(serviceId) * 2 + 1;
  });

  for (const node of nodeMap.values()) {
    if (node.kind === 'kafka' || node.kind === 'database' || node.kind === 'unknown') {
      const inbound = edges.filter((edge) => edge.to === node.id).map((edge) => nodeMap.get(edge.from)?.column ?? 1);
      const outbound = edges.filter((edge) => edge.from === node.id).map((edge) => nodeMap.get(edge.to)?.column ?? 1);

      if (node.kind === 'kafka') {
        if (inbound.length) {
          node.column = Math.max(...inbound) + 1;
        } else if (outbound.length) {
          node.column = Math.max(0, Math.min(...outbound) - 1);
        } else {
          node.column = 0;
        }
      } else if (inbound.length) {
        node.column = Math.max(...inbound) + 1;
      } else if (outbound.length) {
        node.column = Math.max(0, Math.min(...outbound) - 1);
      } else {
        node.column = 0;
      }
    }
  }

  const preferredRows = new Map<string, number>();
  const rootServices = serviceIds
    .filter((serviceId) => !(upstreamServices.get(serviceId)?.length))
    .sort((left, right) => (nodeMap.get(left)?.label ?? left).localeCompare(nodeMap.get(right)?.label ?? right));

  rootServices.forEach((serviceId, index) => {
    preferredRows.set(serviceId, index * 2.25);
  });

  const serviceRowCache = new Map<string, number>();
  const visitRow = (serviceId: string, trail = new Set<string>()): number => {
    if (serviceRowCache.has(serviceId)) {
      return serviceRowCache.get(serviceId)!;
    }
    if (trail.has(serviceId)) {
      return preferredRows.get(serviceId) ?? 0;
    }

    trail.add(serviceId);
    const parents = upstreamServices.get(serviceId) ?? [];
    const row = parents.length
      ? average(parents.map((parent) => visitRow(parent, new Set(trail))))
      : (preferredRows.get(serviceId) ?? 0);

    serviceRowCache.set(serviceId, row);
    return row;
  };

  serviceIds.forEach((serviceId) => {
    preferredRows.set(serviceId, visitRow(serviceId));
  });

  for (const [parentId, childIds] of downstreamServices.entries()) {
    const parentRow = preferredRows.get(parentId) ?? 0;
    const siblings = [...new Set(childIds)].sort((left, right) =>
      (nodeMap.get(left)?.label ?? left).localeCompare(nodeMap.get(right)?.label ?? right),
    );
    if (siblings.length <= 1) {
      continue;
    }

    siblings.forEach((childId, index) => {
      const offset = index - (siblings.length - 1) / 2;
      preferredRows.set(childId, (preferredRows.get(childId) ?? parentRow) + offset * 0.9);
    });
  }

  for (const node of nodeMap.values()) {
    if (node.kind === 'service') {
      continue;
    }

    const neighborRows = edges
      .filter((edge) => edge.from === node.id || edge.to === node.id)
      .map((edge) => {
        const otherId = edge.from === node.id ? edge.to : edge.from;
        return preferredRows.get(otherId);
      })
      .filter((value): value is number => value !== undefined);

    if (neighborRows.length) {
      preferredRows.set(node.id, average(neighborRows));
    }
  }

  for (const serviceId of serviceIds) {
    const serviceRow = preferredRows.get(serviceId) ?? 0;
    const sinkTargets = [...new Set(edges.filter((edge) => edge.from === serviceId).map((edge) => edge.to))]
      .map((targetId) => nodeMap.get(targetId))
      .filter((node): node is GraphNode => node !== undefined && node.kind !== 'service');

    const groupedByColumn = new Map<number, GraphNode[]>();
    sinkTargets.forEach((node) => {
      const bucket = groupedByColumn.get(node.column) ?? [];
      bucket.push(node);
      groupedByColumn.set(node.column, bucket);
    });

    for (const bucket of groupedByColumn.values()) {
      if (bucket.length <= 1) {
        continue;
      }

      bucket
        .sort((left, right) => left.label.localeCompare(right.label))
        .forEach((node, index) => {
          preferredRows.set(node.id, serviceRow + index * 1.1);
        });
    }
  }

  const nodesByColumn = new Map<number, GraphNode[]>();
  for (const node of nodeMap.values()) {
    const bucket = nodesByColumn.get(node.column) ?? [];
    bucket.push(node);
    nodesByColumn.set(node.column, bucket);
  }

  for (const bucket of nodesByColumn.values()) {
    let lane = Number.NEGATIVE_INFINITY;
    bucket
      .sort((left, right) => {
        const rowDifference = (preferredRows.get(left.id) ?? 0) - (preferredRows.get(right.id) ?? 0);
        if (Math.abs(rowDifference) > 0.01) {
          return rowDifference;
        }
        return left.label.localeCompare(right.label);
      })
      .forEach((node) => {
        const targetLane = preferredRows.get(node.id) ?? 0;
        lane = Math.max(lane + 1, Math.round(targetLane));
        node.row = lane;
      });
  }

  const nodes = [...nodeMap.values()].map((node) => ({
    ...node,
    x: 180 + node.column * 280,
    y: 170 + node.row * 190,
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
