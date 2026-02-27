# dagster-coalesce-component

Custom Dagster components for orchestrating [Coalesce](https://coalesce.io) data transformations. Provides two components:

| Component | Pattern | Use case |
|---|---|---|
| `CoalesceStartARun` | Single asset | Trigger a Coalesce run for a set of nodes using a selector string |
| `CoalesceProjectComponent` | One asset per node | Full lineage visibility with dependency graph from your Coalesce environment |

---

## Installation

Requires [`uv`](https://docs.astral.sh/uv/getting-started/installation/).

```bash
uv sync
source .venv/bin/activate  # Windows: .venv\Scripts\activate
```

---

## Components

### `CoalesceProjectComponent` (recommended)

A [state-backed component](https://docs.dagster.io/guides/build/components/state-backed-components/configuring-state-backed-components) that fetches your Coalesce environment's node graph at CI/CD time and creates one Dagster asset per Coalesce node, with dependencies wired from the Coalesce project graph. Dagster controls execution order — each asset triggers a single-node run.

**What you get:**
- Full asset lineage matching your Coalesce project graph
- Column schema visible in the asset catalog (before any runs)
- Per-materialization metadata: rows inserted, warehouse, start/end times, primary SQL
- Custom asset keys, group names, and metadata via `DagsterCoalesceTranslator`

#### YAML configuration

```yaml
type: dagster_coalesce_component.components.coalesce_project_component.CoalesceProjectComponent

requirements:
  env:
    - COALESCE_BEARER_TOKEN
    - COALESCE_ENVIRONMENT_ID
    - SNOWFLAKE_USERNAME
    - SNOWFLAKE_KEYPAIR_KEY   # or SNOWFLAKE_PASSWORD for Basic auth
    - SNOWFLAKE_WAREHOUSE
    - SNOWFLAKE_ROLE

attributes:
  base_url: "app.coalescesoftware.io"
  bearer_token: "{{ env.COALESCE_BEARER_TOKEN }}"
  environment_id: "{{ env.COALESCE_ENVIRONMENT_ID }}"

  # Key/Pair auth
  snowflake_username: "{{ env.SNOWFLAKE_USERNAME }}"
  snowflake_keypair_key: "{{ env.SNOWFLAKE_KEYPAIR_KEY }}"
  snowflake_warehouse: "{{ env.SNOWFLAKE_WAREHOUSE }}"
  snowflake_role: "{{ env.SNOWFLAKE_ROLE }}"

  # Basic auth (alternative — use one or the other)
  # snowflake_password: "{{ env.SNOWFLAKE_PASSWORD }}"

  group_name: "coalesce"
  poll_interval_sec: 10
  max_wait_time_sec: 3600
```

#### Scaffold a new instance

```bash
dg scaffold defs dagster_coalesce_component.components.coalesce_project_component.CoalesceProjectComponent my_coalesce_project
```

#### State management

Node metadata (dependencies, columns) is fetched from the Coalesce API and stored locally in `.local_defs_state/`. This file should be committed to your repository or regenerated during CI/CD.

```bash
# Fetch latest node graph from Coalesce API (run during CI/CD or locally)
dg utils refresh-defs-state

# In local dev, state is refreshed automatically on startup
dg dev
```

Add to `.gitignore` if you prefer to regenerate state during CI/CD rather than committing it:
```
src/**/defs/.local_defs_state/
```

#### Customizing asset keys and metadata

There are three levels of customization, from simplest to most powerful:

| Level | Where | Best for |
|---|---|---|
| `translation` in YAML | `defs.yaml` | Key prefixes, group names, static overrides |
| `DagsterCoalesceTranslator` subclass | Python | Per-node logic, upstream key matching |
| `CoalesceProjectComponent` subclass | Python | Org-wide defaults baked into the component |

---

##### Option 1: YAML `translation` field

Add a `translation` block directly to `defs.yaml`. Supports static values or Jinja templates — `{{ node.* }}` exposes all Coalesce node fields, and `{{ spec.* }}` exposes the base `AssetSpec`.

```yaml
attributes:
  # ... connection config ...
  group_name: "coalesce"

  translation:
    # Prefix all asset keys with "coalesce"
    key_prefix: "coalesce"

    # Group by Coalesce node type (Stage, Fact, Dimension, Source)
    group_name: "{{ node.node_type.lower() }}"
```

**Available `translation` fields:** `key`, `key_prefix`, `group_name`, `description`, `metadata`, `tags`, `kinds`, `owners`

Templates support any Python string method — e.g. `{{ node.node_type.lower() }}` produces `"stage"` instead of `"Stage"`.

**Available `node` fields in templates:**

| Field | Example |
|---|---|
| `node.name` | `"STG_USERS"` |
| `node.location_name` | `"TARGET"` |
| `node.node_type` | `"Stage"`, `"Fact"`, `"Dimension"`, `"Source"` |
| `node.database` | `"MY_DATABASE"` |
| `node.schema` | `"MY_SCHEMA"` |
| `node.description` | `"Staged users from raw source"` (empty string if not set in Coalesce) |
| `node.node_selector` | `"{location: TARGET name: STG_USERS}"` |

---

##### Option 2: Python translator subclass

Use `DagsterCoalesceTranslator` for logic that can't be expressed as a template — most commonly, mapping Source node keys to match an upstream tool.

**Available methods:**

| Method | Default behavior |
|---|---|
| `get_asset_key(node)` | `AssetKey([location_name, name])` (lowercased) |
| `get_group_name(node)` | `None` (falls back to component `group_name`) |
| `get_description(node)` | Coalesce node description if set, otherwise `None` |
| `get_metadata(node)` | `node_id`, `node_type`, `database`, `schema`, `location`, `node_selector`, `column_schema` |
| `get_kinds(node)` | `{"coalesce", "snowflake"}` (Source nodes: `{"coalesce"}`) |
| `get_asset_spec(node, default_group)` | Calls all of the above — override for full control |

```python
from dagster_coalesce_component.components.coalesce_project_component import (
    CoalesceProjectComponent,
    CoalesceNodeData,
    DagsterCoalesceTranslator,
)
import dagster as dg

class MyTranslator(DagsterCoalesceTranslator):
    def get_group_name(self, node: CoalesceNodeData) -> str:
        return node.node_type.lower()

class MyCoalesceComponent(CoalesceProjectComponent):
    def get_translator(self) -> DagsterCoalesceTranslator:
        return MyTranslator()
```

Reference the subclass in `defs.yaml`:

```yaml
type: my_project.components.my_coalesce.MyCoalesceComponent

attributes:
  # ... same attributes as CoalesceProjectComponent ...
```

---

#### Matching upstream asset keys (Fivetran, Sling, etc.)

Coalesce `Source` nodes appear as external assets in Dagster with default keys like `["src", "users"]`. If your upstream ingestion tool (Fivetran, Sling, Airbyte, etc.) produces assets under different keys, the lineage graph will be disconnected.

Use `get_asset_key` in a translator to align Source node keys with whatever the upstream tool produces. Dagster will automatically consolidate matching keys into a single node in the asset graph — no extra configuration needed on the upstream side.

```python
class MyTranslator(DagsterCoalesceTranslator):
    def get_asset_key(self, node: CoalesceNodeData) -> dg.AssetKey:
        if node.node_type == "Source":
            # Match Fivetran asset keys: [connector_name, schema, table]
            return dg.AssetKey(["fivetran", "postgres_prod", node.schema.lower(), node.name.lower()])
        return super().get_asset_key(node)
```

For sources spread across multiple upstream tools, map them individually:

```python
_SOURCE_KEY_MAP = {
    "USERS":     dg.AssetKey(["fivetran", "postgres_prod", "public", "users"]),
    "ORDERS":    dg.AssetKey(["fivetran", "postgres_prod", "public", "orders"]),
    "LOCATIONS": dg.AssetKey(["sling", "locations"]),
}

class MyTranslator(DagsterCoalesceTranslator):
    def get_asset_key(self, node: CoalesceNodeData) -> dg.AssetKey:
        if node.node_type == "Source" and node.name in _SOURCE_KEY_MAP:
            return _SOURCE_KEY_MAP[node.name]
        return super().get_asset_key(node)
```

---

### `CoalesceStartARun`

Creates a single Dagster asset that triggers a Coalesce run for a set of nodes defined by a [selector string](https://docs.coalesce.io/docs/reference/selector). Use this when you want simple orchestration without per-node asset lineage.

#### YAML configuration

```yaml
type: dagster_coalesce_component.components.coalesce_start_a_run.CoalesceStartARun

requirements:
  env:
    - COALESCE_BEARER_TOKEN
    - COALESCE_ENVIRONMENT_ID
    - SNOWFLAKE_USERNAME
    - SNOWFLAKE_KEYPAIR_KEY
    - SNOWFLAKE_WAREHOUSE
    - SNOWFLAKE_ROLE

attributes:
  asset_key: "fct_orders_refresh"
  group_name: "coalesce"

  base_url: "app.coalescesoftware.io"
  bearer_token: "{{ env.COALESCE_BEARER_TOKEN }}"
  environment_id: "{{ env.COALESCE_ENVIRONMENT_ID }}"

  snowflake_username: "{{ env.SNOWFLAKE_USERNAME }}"
  snowflake_keypair_key: "{{ env.SNOWFLAKE_KEYPAIR_KEY }}"
  snowflake_warehouse: "{{ env.SNOWFLAKE_WAREHOUSE }}"
  snowflake_role: "{{ env.SNOWFLAKE_ROLE }}"

  # Selector: run FCT_ORDERS and all its upstream dependencies
  include_nodes_selector: "+{name:FCT_ORDERS}"

  poll_interval_sec: 10
  max_wait_time_sec: 3600
```

#### Scaffold a new instance

```bash
dg scaffold defs dagster_coalesce_component.components.coalesce_start_a_run.CoalesceStartARun my_coalesce_run
```

#### Coalesce selector syntax

```
{name:MY_NODE}                          # single node
+{name:FCT_ORDERS}                      # node + all upstream dependencies
{location:TARGET}                       # all nodes in a location
{nodeType:Fact}                         # all nodes of a type
+{location:TARGET} AND {nodeType:Fact}  # combined
```

See the [Coalesce selector documentation](https://docs.coalesce.io/docs/reference/selector) for the full syntax.

---

## Authentication

Both components support Snowflake **Basic auth** (username + password) and **Key/Pair auth** (username + PEM private key). Key/Pair is recommended.

| Field | Auth type |
|---|---|
| `snowflake_password` | Basic |
| `snowflake_keypair_key` | Key/Pair (PEM-encoded private key) |
| `snowflake_keypair_pass` | Key/Pair (optional passphrase to decrypt key) |

If `snowflake_keypair_key` is provided, Key/Pair auth is used. Otherwise, Basic auth is used.

---

## Environment variables

Create a `.env` file in the project root:

```bash
COALESCE_BEARER_TOKEN=your-bearer-token
COALESCE_ENVIRONMENT_ID=your-environment-id

SNOWFLAKE_USERNAME=your-username
SNOWFLAKE_KEYPAIR_KEY=-----BEGIN PRIVATE KEY-----\n...
SNOWFLAKE_WAREHOUSE=YOUR_WAREHOUSE
SNOWFLAKE_ROLE=YOUR_ROLE

# Basic auth alternative
# SNOWFLAKE_PASSWORD=your-password
```

---

## Validation

```bash
# Validate YAML and load all definitions
dg check defs

# List all registered assets
dg list defs

# Launch local UI
dg dev
```

---

## Learn more

- [Dagster Components Guide](https://docs.dagster.io/guides/build/components/creating-new-components/creating-and-registering-a-component)
- [Dagster State-Backed Components](https://docs.dagster.io/guides/build/components/state-backed-components/configuring-state-backed-components)
- [Coalesce API Documentation](https://docs.coalesce.io/docs/api/coalesce/)
- [Coalesce Selector Reference](https://docs.coalesce.io/docs/reference/selector)
