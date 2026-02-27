"""Coalesce.io State-Backed Component for Dagster.

This component fetches node metadata from a Coalesce environment at CI/CD time,
stores it as a state file, and creates one Dagster asset per Coalesce node with
full dependency lineage. Each asset triggers a single-node Coalesce run when
materialized, letting Dagster control execution order.
"""

import json
import time
from dataclasses import dataclass
from enum import Enum
from pathlib import Path
from typing import Annotated, Any, Optional

import dagster as dg
import requests
from dagster.components.component.state_backed_component import StateBackedComponent
from dagster.components.resolved.model import Resolver
from dagster.components.utils.defs_state import (
    DefsStateConfig,
    DefsStateConfigArgs,
    ResolvedDefsStateConfig,
)
from dagster.components.utils.translation import TranslationFn, TranslationFnResolver
from pydantic import field_validator


class CoalesceRunStatus(Enum):
    """Coalesce run status values from the run results endpoint."""
    RUNNING = "running"
    COMPLETE = "complete"
    FAILED = "failed"
    CANCELED = "canceled"


# -----------------------------------------------------------------------------
# Node data model and translator
# -----------------------------------------------------------------------------

@dataclass(frozen=True)
class CoalesceColumnData:
    """Represents a column from a Coalesce node."""
    name: str
    data_type: str


@dataclass(frozen=True)
class CoalesceNodeData:
    """Represents a Coalesce node as read from the state file.

    Passed to DagsterCoalesceTranslator to generate AssetSpecs.
    """
    id: str
    name: str
    location_name: str
    node_type: str
    database: str
    schema: str
    dep_asset_keys: list[dg.AssetKey]
    columns: list[CoalesceColumnData]

    @property
    def node_selector(self) -> str:
        """Single-node selector string for the Coalesce API."""
        return f"{{location: {self.location_name} name: {self.name}}}"


class DagsterCoalesceTranslator:
    """Translates Coalesce node metadata into Dagster AssetSpecs.

    Override methods in a subclass to customize asset keys, group names,
    descriptions, metadata, or kinds for your organization's conventions.

    Example — prefix all asset keys with a custom namespace::

        class MyTranslator(DagsterCoalesceTranslator):
            def get_asset_key(self, node: CoalesceNodeData) -> dg.AssetKey:
                return dg.AssetKey(["coalesce", node.location_name.lower(), node.name.lower()])

        # Pass to the component:
        CoalesceProjectComponent(
            ...,
            translator=MyTranslator(),
        )
    """

    def get_asset_key(self, node: CoalesceNodeData) -> dg.AssetKey:
        """Return the AssetKey for a Coalesce node. Defaults to [location, name]."""
        return dg.AssetKey([node.location_name.lower(), node.name.lower()])

    def get_group_name(self, _node: CoalesceNodeData) -> Optional[str]:
        """Return the Dagster group name for this node. Defaults to None (uses component default)."""
        return None

    def get_description(self, node: CoalesceNodeData) -> Optional[str]:
        """Return the asset description."""
        return f"Coalesce {node.node_type}: {node.name} in {node.location_name}"

    def get_metadata(self, node: CoalesceNodeData) -> dict[str, Any]:
        """Return metadata dict attached to the asset definition."""
        metadata: dict[str, Any] = {
            "node_id": node.id,
            "node_type": node.node_type,
            "database": node.database,
            "schema": node.schema,
            "location": node.location_name,
            "node_selector": node.node_selector,
        }
        if node.columns:
            metadata["column_schema"] = dg.MetadataValue.table_schema(
                dg.TableSchema(
                    columns=[
                        dg.TableColumn(name=col.name, type=col.data_type)
                        for col in node.columns
                    ]
                )
            )
        return metadata

    def get_kinds(self, node: CoalesceNodeData) -> set[str]:
        """Return the set of kinds to attach to the asset.

        Source nodes are external references and get no snowflake kind since
        they are not executed by Coalesce.
        """
        if node.node_type == "Source":
            return {"coalesce"}
        return {"coalesce", "snowflake"}

    def get_asset_spec(self, node: CoalesceNodeData, default_group: Optional[str]) -> dg.AssetSpec:
        """Compose a full AssetSpec for the given node.

        Calls the individual getter methods — override those for targeted
        customizations, or override this method entirely for full control.
        """
        group = self.get_group_name(node) or default_group
        return dg.AssetSpec(
            key=self.get_asset_key(node),
            deps=node.dep_asset_keys,
            description=self.get_description(node),
            group_name=group,
            kinds=self.get_kinds(node),
            metadata=self.get_metadata(node),
        )


# -----------------------------------------------------------------------------
# Component
# -----------------------------------------------------------------------------

class CoalesceProjectComponent(StateBackedComponent, dg.Model, dg.Resolvable):
    """Execute Coalesce nodes as individual Dagster assets with full dependency lineage.

    This component fetches node metadata from a Coalesce environment at CI/CD time
    (via `dg utils refresh-defs-state`) and stores it locally as a state file. At
    definition load time, one Dagster asset is created per Coalesce node, with
    dependencies wired from the Coalesce project graph. When materialized,
    each asset triggers a single-node Coalesce run.

    Dagster controls execution order (no `+` selector prefix), giving full
    lineage visibility and per-node observability in the Dagster UI.

    Asset keys, group names, descriptions, metadata, and kinds can all be
    customized by passing a subclassed DagsterCoalesceTranslator.

    Example YAML usage:
        type: dagster_coalesce_component.components.coalesce_project_component.CoalesceProjectComponent
        attributes:
          base_url: "app.coalescesoftware.io"
          bearer_token: "{{ env.COALESCE_BEARER_TOKEN }}"
          environment_id: "{{ env.COALESCE_ENVIRONMENT_ID }}"
          snowflake_username: "{{ env.SNOWFLAKE_USERNAME }}"
          snowflake_keypair_key: "{{ env.SNOWFLAKE_KEYPAIR_KEY }}"
          snowflake_warehouse: "{{ env.SNOWFLAKE_WAREHOUSE }}"
          snowflake_role: "{{ env.SNOWFLAKE_ROLE }}"
          group_name: "coalesce"
    """

    # Coalesce API Configuration
    base_url: str  # e.g., "app.coalescesoftware.io"
    bearer_token: str = ""
    environment_id: str

    # Snowflake Credentials — supports Basic auth (password) or Key/Pair auth (keypair_key)
    snowflake_username: str = ""
    snowflake_password: str = ""
    snowflake_keypair_key: str = ""
    snowflake_keypair_pass: str = ""
    snowflake_warehouse: str = ""
    snowflake_role: str = ""

    # Default Dagster Asset Configuration (can be overridden per-node via translator)
    group_name: Optional[str] = "coalesce"

    # Optional YAML-driven asset attribute overrides.
    # Supports static values or Jinja templates with `node` (CoalesceNodeData) and
    # `spec` (the base AssetSpec) in scope. Applied on top of the translator output.
    # Supported fields: key, key_prefix, group_name, description, metadata, tags, kinds, owners.
    # Example:
    #   translation:
    #     key_prefix: "coalesce"
    #     group_name: "{{ node.node_type.lower() }}"
    translation: Annotated[
        Optional[TranslationFn[CoalesceNodeData]],
        TranslationFnResolver(
            template_vars_for_translation_fn=lambda node: {"node": node}
        ),
    ] = None

    # Polling Configuration (used at asset execution time)
    poll_interval_sec: int = 10
    max_wait_time_sec: int = 3600

    # State management configuration — defaults to LOCAL_FILESYSTEM
    defs_state: Annotated[
        ResolvedDefsStateConfig,
        Resolver.default(
            description=(
                "Configuration for how component state is stored and refreshed. "
                "Defaults to LOCAL_FILESYSTEM. Run `dg utils refresh-defs-state` "
                "during CI/CD to update the state file with the latest Coalesce nodes."
            ),
        ),
    ] = DefsStateConfigArgs.local_filesystem()

    @field_validator("environment_id", mode="before")
    @classmethod
    def convert_environment_id_to_string(cls, v):
        """Convert environment_id to string if it's an int."""
        return str(v) if isinstance(v, int) else v

    def get_translator(self) -> DagsterCoalesceTranslator:
        """Return the translator used to convert Coalesce nodes into Dagster AssetSpecs.

        Override this method in a subclass to use a custom translator::

            class MyCoalesceComponent(CoalesceProjectComponent):
                def get_translator(self) -> DagsterCoalesceTranslator:
                    return MyCustomTranslator()
        """
        return DagsterCoalesceTranslator()

    @classmethod
    def get_spec(cls) -> dg.ComponentTypeSpec:
        return dg.ComponentTypeSpec(
            owners=["data-engineering@example.com"],
            tags=["coalesce", "snowflake", "etl"],
        )

    # -------------------------------------------------------------------------
    # StateBackedComponent interface
    # -------------------------------------------------------------------------

    @property
    def defs_state_config(self) -> DefsStateConfig:
        return DefsStateConfig.from_args(
            self.defs_state,
            default_key=f"CoalesceProjectComponent[{self.environment_id}]",
        )

    def write_state_to_path(self, state_path: Path) -> None:
        """Fetch all nodes and their dependencies from Coalesce and write to state_path.

        Called during CI/CD via `dg utils refresh-defs-state` and automatically
        on `dagster dev` (when refresh_if_dev=True). Paginates the list-nodes
        endpoint, then fetches each node individually to capture dependency
        information from sourceMapping.
        """
        print(f"Fetching nodes from Coalesce environment: {self.environment_id}")

        nodes_summary = self._fetch_all_nodes()
        print(f"Fetched {len(nodes_summary)} nodes, now fetching dependency details...")

        nodes_with_deps = []
        for node in nodes_summary:
            node_id = node["id"]
            node_name = node.get("name", node_id)
            try:
                detailed = self._fetch_node_detail(node_id)
                deps = _extract_dependencies(detailed)
                columns = _extract_columns(detailed)
                nodes_with_deps.append({
                    "id": node_id,
                    "name": node.get("name"),
                    "locationName": node.get("locationName"),
                    "nodeType": node.get("nodeType"),
                    "database": node.get("database"),
                    "schema": node.get("schema"),
                    "dependencies": deps,
                    "columns": columns,
                })
            except Exception as e:
                print(f"Warning: Failed to fetch details for node {node_name} ({node_id}): {e}")
                nodes_with_deps.append({
                    "id": node_id,
                    "name": node.get("name"),
                    "locationName": node.get("locationName"),
                    "nodeType": node.get("nodeType"),
                    "database": node.get("database"),
                    "schema": node.get("schema"),
                    "dependencies": [],
                    "columns": [],
                })

        state = {
            "environment_id": self.environment_id,
            "nodes": nodes_with_deps,
        }

        state_path.write_text(json.dumps(state, indent=2))
        print(f"Wrote state for {len(nodes_with_deps)} nodes to {state_path}")

    def build_defs_from_state(
        self, _context: dg.ComponentLoadContext, state_path: Optional[Path]
    ) -> dg.Definitions:
        """Build one Dagster asset per Coalesce node, with dependencies wired from state."""
        if state_path is None:
            print(
                "Warning: No Coalesce state file found. Run `dg utils refresh-defs-state` "
                "to fetch node metadata from the Coalesce API."
            )
            return dg.Definitions()

        state = json.loads(state_path.read_text())
        nodes = state.get("nodes", [])

        if not nodes:
            print(f"Warning: No nodes found in Coalesce state for environment {self.environment_id}")
            return dg.Definitions()

        translator = self.get_translator()

        # Build lookup of (location, name) -> AssetKey using the translator,
        # so dependency resolution respects any custom key logic.
        node_key_map: dict[tuple[str, str], dg.AssetKey] = {}
        for n in nodes:
            if n.get("locationName") and n.get("name"):
                stub = CoalesceNodeData(
                    id=n["id"],
                    name=n["name"],
                    location_name=n["locationName"],
                    node_type=n.get("nodeType", ""),
                    database=n.get("database", ""),
                    schema=n.get("schema", ""),
                    dep_asset_keys=[],
                    columns=[],
                )
                node_key_map[(n["locationName"].lower(), n["name"].lower())] = (
                    translator.get_asset_key(stub)
                )

        assets = []
        for node in nodes:
            asset = self._create_asset_for_node(node, node_key_map, translator)
            assets.append(asset)

        print(f"Created {len(assets)} Dagster assets from Coalesce state")
        return dg.Definitions(assets=assets)

    # -------------------------------------------------------------------------
    # Internal helpers
    # -------------------------------------------------------------------------

    def _fetch_all_nodes(self) -> list[dict[str, Any]]:
        """Fetch all nodes from the environment, handling pagination via the `next` cursor."""
        url = f"https://{self.base_url}/api/v1/environments/{self.environment_id}/nodes"
        headers = {
            "Accept": "application/json",
            "Authorization": f"Bearer {self.bearer_token}",
        }

        all_nodes: list[dict[str, Any]] = []
        params: dict[str, Any] = {}

        while True:
            response = requests.get(url, headers=headers, params=params)
            response.raise_for_status()
            data = response.json()

            batch = data.get("data", []) if isinstance(data, dict) else []
            all_nodes.extend(batch)

            total = data.get("total") if isinstance(data, dict) else None
            next_cursor = data.get("next") if isinstance(data, dict) else None

            # Stop if no next cursor, empty batch, or we've received all nodes
            if not next_cursor or not batch:
                break
            if total is not None and len(all_nodes) >= total:
                break

            params = {"after": next_cursor}

        return all_nodes

    def _fetch_node_detail(self, node_id: str) -> dict[str, Any]:
        """Fetch detailed metadata for a single node, including sourceMapping dependencies."""
        url = f"https://{self.base_url}/api/v1/environments/{self.environment_id}/nodes/{node_id}"
        headers = {
            "Accept": "application/json",
            "Authorization": f"Bearer {self.bearer_token}",
        }
        response = requests.get(url, headers=headers)
        response.raise_for_status()
        return response.json()

    def _create_asset_for_node(
        self,
        node: dict[str, Any],
        node_key_map: dict[tuple[str, str], dg.AssetKey],
        translator: DagsterCoalesceTranslator,
    ) -> "dg.AssetsDefinition | dg.AssetSpec":
        """Create a Dagster asset for a single Coalesce node."""
        node_name = node.get("name", "unknown")
        node_location = node.get("locationName", "")

        # Resolve dependency references using the translator's key logic
        dep_keys: list[dg.AssetKey] = []
        for dep in node.get("dependencies", []):
            dep_loc = dep.get("locationName", "").lower()
            dep_name = dep.get("nodeName", "").lower()
            resolved = node_key_map.get((dep_loc, dep_name))
            if resolved:
                dep_keys.append(resolved)
            else:
                print(
                    f"Warning: Could not resolve dependency ({dep_loc}, {dep_name}) "
                    f"for node {node_name}. It may be an external source."
                )

        node_data = CoalesceNodeData(
            id=node.get("id", node_name),
            name=node_name,
            location_name=node_location,
            node_type=node.get("nodeType", "Unknown"),
            database=node.get("database", ""),
            schema=node.get("schema", ""),
            dep_asset_keys=dep_keys,
            columns=[
                CoalesceColumnData(name=c["name"], data_type=c.get("dataType", ""))
                for c in node.get("columns", [])
                if c.get("name")
            ],
        )

        spec = translator.get_asset_spec(node_data, default_group=self.group_name)

        # Apply YAML-driven translation overrides on top of the translator output.
        if self.translation:
            spec = self.translation(spec, node_data)

        # Source nodes are external references — they cannot be executed in Coalesce.
        # Return a bare AssetSpec so they appear in the lineage graph as observable/
        # external assets. Downstream nodes that depend on them will still have their
        # deps wired correctly, and any upstream Dagster asset with a matching key
        # (e.g. from Sling or Fivetran) will be automatically consolidated into the graph.
        if node_data.node_type == "Source":
            return spec

        # Capture execution-time config in locals for closure
        base_url = self.base_url
        bearer_token = self.bearer_token
        environment_id = self.environment_id
        snowflake_username = self.snowflake_username
        snowflake_password = self.snowflake_password
        snowflake_keypair_key = self.snowflake_keypair_key
        snowflake_keypair_pass = self.snowflake_keypair_pass
        snowflake_warehouse = self.snowflake_warehouse
        snowflake_role = self.snowflake_role
        poll_interval_sec = self.poll_interval_sec
        max_wait_time_sec = self.max_wait_time_sec
        node_selector = node_data.node_selector

        @dg.asset(
            key=spec.key,
            deps=spec.deps,
            description=spec.description,
            group_name=spec.group_name,
            kinds=spec.kinds,
            metadata=spec.metadata,
        )
        def coalesce_node_asset(context: dg.AssetExecutionContext) -> None:
            """Trigger a single-node Coalesce run for this node."""
            context.log.info(f"Starting Coalesce run for node: {node_location}.{node_name}")

            run_counter = _start_coalesce_run(
                context=context,
                base_url=base_url,
                bearer_token=bearer_token,
                environment_id=environment_id,
                snowflake_username=snowflake_username,
                snowflake_password=snowflake_password,
                snowflake_keypair_key=snowflake_keypair_key,
                snowflake_keypair_pass=snowflake_keypair_pass,
                snowflake_warehouse=snowflake_warehouse,
                snowflake_role=snowflake_role,
                node_selector=node_selector,
            )

            _run_completed = False
            try:
                _poll_run_results(
                    context=context,
                    base_url=base_url,
                    bearer_token=bearer_token,
                    run_counter=run_counter,
                    poll_interval_sec=poll_interval_sec,
                    max_wait_time_sec=max_wait_time_sec,
                )
                _run_completed = True
            finally:
                if not _run_completed:
                    context.log.warning(
                        f"Dagster op interrupted — canceling Coalesce run {run_counter}"
                    )
                    _cancel_coalesce_run(
                        context=context,
                        base_url=base_url,
                        bearer_token=bearer_token,
                        run_counter=run_counter,
                        environment_id=environment_id,
                    )

        return coalesce_node_asset


# -----------------------------------------------------------------------------
# Module-level execution helpers
# -----------------------------------------------------------------------------

def _extract_dependencies(node_detail: dict[str, Any]) -> list[dict[str, str]]:
    """Extract upstream node dependencies from a node's sourceMapping."""
    deps: list[dict[str, str]] = []
    source_mappings = node_detail.get("metadata", {}).get("sourceMapping", [])
    for mapping in source_mappings:
        for dep in mapping.get("dependencies", []):
            loc = dep.get("locationName")
            name = dep.get("nodeName")
            if loc and name:
                deps.append({"locationName": loc, "nodeName": name})
    # Deduplicate while preserving order
    seen: set[tuple[str, str]] = set()
    unique_deps = []
    for d in deps:
        key = (d["locationName"], d["nodeName"])
        if key not in seen:
            seen.add(key)
            unique_deps.append(d)
    return unique_deps


def _start_coalesce_run(
    context: dg.AssetExecutionContext,
    base_url: str,
    bearer_token: str,
    environment_id: str,
    snowflake_username: str,
    snowflake_password: str,
    snowflake_keypair_key: str,
    snowflake_keypair_pass: str,
    snowflake_warehouse: str,
    snowflake_role: str,
    node_selector: str,
) -> str:
    """Start a single-node Coalesce run and return the run counter."""
    url = f"https://{base_url}/scheduler/startRun"

    if snowflake_keypair_key:
        user_credentials: dict[str, Any] = {
            "snowflakeUsername": snowflake_username,
            "snowflakeKeyPairKey": snowflake_keypair_key,
            "snowflakeWarehouse": snowflake_warehouse,
            "snowflakeRole": snowflake_role,
            "snowflakeAuthType": "KeyPair",
        }
        if snowflake_keypair_pass:
            user_credentials["snowflakeKeyPairPass"] = snowflake_keypair_pass
    else:
        user_credentials = {
            "snowflakeUsername": snowflake_username,
            "snowflakePassword": snowflake_password,
            "snowflakeWarehouse": snowflake_warehouse,
            "snowflakeRole": snowflake_role,
            "snowflakeAuthType": "Basic",
        }

    payload = {
        "runDetails": {
            "environmentID": environment_id,
            "includeNodesSelector": node_selector,
        },
        "userCredentials": user_credentials,
    }

    headers = {
        "Content-Type": "application/json",
        "Accept": "application/json",
        "Authorization": f"Bearer {bearer_token}",
    }

    context.log.info(f"Starting Coalesce run with selector: {node_selector}")
    context.log.debug(f"POST {url}")

    try:
        response = requests.post(url, json=payload, headers=headers)
        response.raise_for_status()
        result = response.json()
        run_counter = result.get("runCounter", result.get("id"))
        context.log.info(f"Coalesce run started. Run counter: {run_counter}")
        return run_counter
    except requests.exceptions.RequestException as e:
        context.log.error(
            f"Failed to start Coalesce run.\n"
            f"  URL: {url}\n"
            f"  Selector: {node_selector}\n"
            f"  Status: {getattr(e.response, 'status_code', 'N/A')}\n"
            f"  Response: {getattr(e.response, 'text', 'N/A')}\n"
            f"  Error: {e}"
        )
        raise


def _poll_run_results(
    context: dg.AssetExecutionContext,
    base_url: str,
    bearer_token: str,
    run_counter: str,
    poll_interval_sec: int,
    max_wait_time_sec: int,
) -> None:
    """Poll the run results endpoint until the node completes, then emit materialization metadata."""
    url = f"https://{base_url}/api/v1/runs/{run_counter}/results"
    headers = {
        "Accept": "application/json",
        "Authorization": f"Bearer {bearer_token}",
    }

    start_time = time.time()

    while True:
        elapsed = time.time() - start_time
        if elapsed >= max_wait_time_sec:
            raise Exception(
                f"Coalesce run {run_counter} timed out after {max_wait_time_sec}s"
            )

        try:
            response = requests.get(url, headers=headers)
            response.raise_for_status()
            data = response.json().get("data", [])
        except requests.exceptions.RequestException as e:
            context.log.error(f"Failed to get run results: {e}")
            raise

        if not data:
            # Run hasn't produced any results yet — still initializing
            context.log.info(
                f"Coalesce run {run_counter}: waiting for results... (elapsed: {elapsed:.1f}s)"
            )
            time.sleep(poll_interval_sec)
            continue

        # Single-node runs return exactly one entry
        node_result = data[0]
        run_state = node_result.get("runState", "").lower()
        is_running = node_result.get("isRunning", True)

        context.log.info(
            f"Coalesce run {run_counter} state: {run_state} (elapsed: {elapsed:.1f}s)"
        )

        if is_running:
            time.sleep(poll_interval_sec)
            continue

        # Run has finished — handle terminal states
        query_results = node_result.get("queryResults", [])

        if run_state == CoalesceRunStatus.COMPLETE.value:
            context.log.info(f"Coalesce run {run_counter} completed successfully")
            _emit_run_metadata(context, run_counter, query_results)
            return

        elif run_state == CoalesceRunStatus.CANCELED.value:
            error_detail = _extract_error_detail(query_results)
            raise Exception(
                f"Coalesce run {run_counter} was canceled. {error_detail}"
            )

        elif run_state == CoalesceRunStatus.FAILED.value:
            error_detail = _extract_error_detail(query_results)
            raise Exception(
                f"Coalesce run {run_counter} failed. {error_detail}"
            )

        else:
            # Unknown terminal state
            raise Exception(
                f"Coalesce run {run_counter} ended with unexpected state: {run_state}"
            )


def _emit_run_metadata(
    context: dg.AssetExecutionContext,
    _run_counter: str,
    query_results: list[dict[str, Any]],
) -> None:
    """Surface key query result fields as Dagster materialization metadata."""
    total_rows_inserted = sum(
        q.get("rowsInserted", 0) for q in query_results if q.get("rowsInserted")
    )
    warehouses = list({q["warehouse"] for q in query_results if q.get("warehouse")})

    start_times = [q["startTime"] for q in query_results if q.get("startTime")]
    end_times = [q["endTime"] for q in query_results if q.get("endTime")]

    total_rows_updated = sum(
        q.get("rowsUpdated", 0) for q in query_results if q.get("rowsUpdated")
    )

    metadata: dict[str, Any] = {}

    if total_rows_inserted:
        metadata["rows_inserted"] = total_rows_inserted

    if total_rows_updated:
        metadata["rows_updated"] = total_rows_updated

    if warehouses:
        metadata["warehouse"] = warehouses[0] if len(warehouses) == 1 else warehouses

    if start_times and end_times:
        metadata["started_at"] = min(start_times)
        metadata["completed_at"] = max(end_times)

    # Surface the primary DML SQL as markdown — skip utility queries like truncates.
    # The primary query is the last non-truncate query with SQL present.
    primary_sql = _extract_primary_sql(query_results)
    if primary_sql:
        metadata["sql"] = dg.MetadataValue.md(f"```sql\n{primary_sql.strip()}\n```")

    context.add_output_metadata(metadata)


def _extract_error_detail(query_results: list[dict[str, Any]]) -> str:
    """Extract a human-readable error string from query results."""
    for q in query_results:
        error = q.get("error", {})
        if error:
            error_string = error.get("errorString", "")
            query_name = q.get("name", "")
            return f"Query '{query_name}' error: {error_string}"
    return ""


def _extract_primary_sql(query_results: list[dict[str, Any]]) -> Optional[str]:
    """Return the primary DML SQL from query results.

    Skips utility queries (TRUNCATE, CREATE STAGE, etc.) and returns the last
    query with meaningful SQL — typically the INSERT, MERGE, or SELECT statement
    that represents the node's core transformation logic.
    """
    _UTILITY_PREFIXES = ("TRUNCATE", "CREATE", "DROP", "ALTER", "GRANT", "COMMENT")

    candidates = []
    for q in query_results:
        sql = (q.get("sql") or "").strip()
        if not sql:
            continue
        if any(sql.upper().startswith(prefix) for prefix in _UTILITY_PREFIXES):
            continue
        candidates.append(sql)

    return candidates[-1] if candidates else None


def _cancel_coalesce_run(
    context: dg.AssetExecutionContext,
    base_url: str,
    bearer_token: str,
    run_counter: str,
    environment_id: str,
) -> None:
    """Cancel an active Coalesce run.

    Called in the finally block when the Dagster op is interrupted (user cancel,
    timeout, or unexpected error) to ensure the Coalesce run doesn't continue
    running in the background after Dagster has given up.

    Errors are logged but not re-raised — we're already in a failure/cancel path
    and don't want to mask the original exception.
    """
    url = f"https://{base_url}/scheduler/cancelRun"
    headers = {
        "Content-Type": "application/json",
        "Accept": "application/json",
        "Authorization": f"Bearer {bearer_token}",
    }
    payload = {
        "runID": int(run_counter),
        "environmentID": environment_id,
    }
    try:
        response = requests.post(url, json=payload, headers=headers, timeout=10)
        response.raise_for_status()
        context.log.info(f"Coalesce run {run_counter} canceled successfully")
    except requests.exceptions.RequestException as e:
        context.log.error(
            f"Failed to cancel Coalesce run {run_counter}: {e}\n"
            f"The run may still be active in Coalesce — cancel it manually."
        )


def _extract_columns(node_detail: dict[str, Any]) -> list[dict[str, str]]:
    """Extract column names and data types from a node detail response."""
    columns = node_detail.get("metadata", {}).get("columns", [])
    result = []
    for col in columns:
        name = col.get("name")
        data_type = col.get("dataType", "")
        if name:
            result.append({"name": name, "dataType": data_type})
    return result
