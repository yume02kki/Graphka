# OpenShift Pipeline Visualizer

React + TypeScript site for visualizing data-flow pipelines defined inside OpenShift ConfigMaps.

## Run

1. Install dependencies:

```bash
npm install
```

2. Start the app:

```bash
npm run dev
```

The bundled example is loaded from `conffigmaps.yaml`.

## Input format

The app expects YAML from `oc get configmap -o yaml` or a ConfigMap list where one of the `data` entries contains another YAML document with a top-level `pipelines` key.

Supported graph edges are inferred from:

- `services[*].consumes`
- `services[*].produces`
- `services[*].outputs`
- `services[*].writesTo`
- `services[*].dependencies`
