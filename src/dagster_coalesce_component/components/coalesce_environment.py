"""Coalesce.io Component for Dagster.

This component allows you to execute Coalesce nodes as Dagster assets.
Each node in a Coalesce environment becomes a distinct Dagster asset.
"""

import dagster as dg
import requests
import time
from typing import Optional, List, Dict, Any
from enum import Enum
from pydantic import field_validator


class CoalesceRunStatus(Enum):
    """Coalesce run status values."""
    RUNNING = "running"
    COMPLETED = "completed"
    FAILED = "failed"
    CANCELED = "canceled"


class CoalesceEnvironment(dg.Component, dg.Model, dg.Resolvable):
    """Execute Coalesce nodes as Dagster assets.

    This component fetches nodes from a Coalesce environment and creates
    a Dagster asset for each node. When materialized, assets trigger
    Coalesce runs and poll for completion.

    Example YAML usage:
        type: dagster_coalesce_component.components.coalesce_environment.CoalesceEnvironment
        params:
          base_url: app.us.coalescesoftware.io
          bearer_token: your-bearer-token
          environment_id: your-environment-id
          snowflake_username: your-username
          snowflake_password: your-password
          snowflake_warehouse: your-warehouse
          snowflake_role: your-role
          group_name: coalesce_etl
          poll_interval_sec: 10
          max_wait_time_sec: 3600
    """

    # Coalesce API Configuration
    base_url: str  # e.g., "app.us.coalescesoftware.io"
    bearer_token: str = ""
    environment_id: str

    # Snowflake Credentials for Coalesce runs
    snowflake_username: str = ""
    snowflake_password: str = ""
    snowflake_warehouse: str
    snowflake_role: str

    # Optional: Node selection
    include_nodes_selector: Optional[str] = None
    exclude_nodes_selector: Optional[str] = None
    job_id: Optional[str] = None

    # Dagster Asset Configuration
    group_name: Optional[str] = "coalesce"

    # Polling Configuration
    poll_interval_sec: int = 10
    max_wait_time_sec: int = 3600

    @field_validator('environment_id', mode='before')
    @classmethod
    def convert_environment_id_to_string(cls, v):
        """Convert environment_id to string if it's an int."""
        return str(v) if isinstance(v, int) else v

    @classmethod
    def get_spec(cls) -> dg.ComponentTypeSpec:
        return dg.ComponentTypeSpec(
            owners=["data-engineering@example.com"],
            tags=["coalesce", "snowflake", "etl"],
        )

    def build_defs(self, context: dg.ComponentLoadContext) -> dg.Definitions:
        """Build Dagster definitions for Coalesce nodes."""

        # Fetch nodes from Coalesce environment
        nodes = self._fetch_nodes()

        if not nodes:
            print(f"Warning: No nodes found in Coalesce environment {self.environment_id}")
            return dg.Definitions()

        # Create assets for each node
        assets = []
        for node in nodes:
            asset = self._create_asset_for_node(node)
            assets.append(asset)

        print(f"Created {len(assets)} Dagster assets from Coalesce nodes")

        return dg.Definitions(assets=assets)

    def _fetch_nodes(self) -> List[Dict[str, Any]]:
        """Fetch nodes from Coalesce environment using the API."""

        url = f"https://{self.base_url}/api/v1/environments/{str(self.environment_id)}/nodes"

        headers = {
            'Accept': 'application/json',
            'Authorization': f'Bearer {self.bearer_token}'
        }

        print(f"Fetching nodes from Coalesce environment: {self.environment_id}")

        try:
            response = requests.get(url, headers=headers)
            response.raise_for_status()

            data = response.json()
            nodes = data.get('data', []) if isinstance(data, dict) else []

            print(f"Fetched {len(nodes)} nodes from Coalesce")
            return nodes

        except requests.exceptions.RequestException as e:
            print(f"Error: Failed to fetch nodes from Coalesce: {str(e)}")
            raise

    def _create_asset_for_node(self, node: Dict[str, Any]) -> dg.AssetsDefinition:
        """Create a Dagster asset for a Coalesce node."""

        # Extract node information
        node_name = node.get('name', 'unknown_node')
        node_location = node.get('locationName', '')
        node_id = node.get('id', node_name)
        node_type = node.get('nodeType', 'Unknown')
        database = node.get('database', '')
        schema = node.get('schema', '')

        # Create a clean asset key
        asset_key = f"coalesce_{node_location}_{node_name}".lower().replace(' ', '_')

        # Build node selector for this specific node
        node_selector = f"{{ location: {node_location} name: {node_name} }}"

        # Capture component configuration
        base_url = self.base_url
        bearer_token = self.bearer_token
        environment_id = self.environment_id
        snowflake_username = self.snowflake_username
        snowflake_password = self.snowflake_password
        snowflake_warehouse = self.snowflake_warehouse
        snowflake_role = self.snowflake_role
        job_id = self.job_id
        group_name = self.group_name
        poll_interval_sec = self.poll_interval_sec
        max_wait_time_sec = self.max_wait_time_sec

        @dg.asset(
            key=asset_key,
            description=f"Coalesce {node_type}: {node_name} in {node_location}",
            group_name=group_name,
            kinds={"coalesce"},
            metadata={
                "node_name": node_name,
                "node_location": node_location,
                "node_id": node_id,
                "node_type": node_type,
                "database": database,
                "schema": schema,
            }
        )
        def coalesce_node_asset(context: dg.AssetExecutionContext) -> Dict[str, Any]:
            """Execute Coalesce node."""

            context.log.info(f"Starting Coalesce run for node: {node_name}")

            # Start the run
            run_counter = _start_coalesce_run(
                context=context,
                base_url=base_url,
                bearer_token=bearer_token,
                environment_id=environment_id,
                snowflake_username=snowflake_username,
                snowflake_password=snowflake_password,
                snowflake_warehouse=snowflake_warehouse,
                snowflake_role=snowflake_role,
                node_selector=node_selector,
                job_id=job_id,
            )

            # Poll for completion
            result = _poll_run_status(
                context=context,
                base_url=base_url,
                bearer_token=bearer_token,
                run_counter=run_counter,
                poll_interval_sec=poll_interval_sec,
                max_wait_time_sec=max_wait_time_sec,
            )

            return result

        return coalesce_node_asset


def _start_coalesce_run(
    context: dg.AssetExecutionContext,
    base_url: str,
    bearer_token: str,
    environment_id: str,
    snowflake_username: str,
    snowflake_password: str,
    snowflake_warehouse: str,
    snowflake_role: str,
    node_selector: str,
    job_id: Optional[str],
) -> str:
    """Start a Coalesce run and return the run counter."""

    url = f"https://{base_url}/scheduler/startRun"

    payload = {
        "runDetails": {
            "environmentID": environment_id,
            "includeNodesSelector": node_selector,
        },
        "userCredentials": {
            "snowflakeUsername": snowflake_username,
            "snowflakePassword": snowflake_password,
            "snowflakeWarehouse": snowflake_warehouse,
            "snowflakeRole": snowflake_role,
            "snowflakeAuthType": "Basic"
        }
    }

    # Add job_id if provided
    if job_id:
        payload["runDetails"]["jobID"] = job_id

    headers = {
        'Content-Type': 'application/json',
        'Accept': 'application/json',
        'Authorization': f'Bearer {bearer_token}'
    }

    context.log.info(f"Starting Coalesce run with selector: {node_selector}")

    try:
        response = requests.post(url, json=payload, headers=headers)
        response.raise_for_status()

        result = response.json()
        run_counter = result.get('runCounter', result.get('id'))

        context.log.info(f"Coalesce run started. Run counter: {run_counter}")
        return run_counter

    except requests.exceptions.RequestException as e:
        context.log.error(f"Failed to start Coalesce run: {str(e)}")
        raise


def _poll_run_status(
    context: dg.AssetExecutionContext,
    base_url: str,
    bearer_token: str,
    run_counter: str,
    poll_interval_sec: int,
    max_wait_time_sec: int,
) -> Dict[str, Any]:
    """Poll Coalesce run status until completion."""

    url = f"https://{base_url}/api/v1/runs/{run_counter}"

    headers = {
        'Accept': 'application/json',
        'Authorization': f'Bearer {bearer_token}'
    }

    start_time = time.time()
    elapsed_time = 0

    while elapsed_time < max_wait_time_sec:
        try:
            response = requests.get(url, headers=headers)
            response.raise_for_status()

            result = response.json()
            status = result.get('runStatus', '').lower()

            context.log.info(
                f"Coalesce run {run_counter} status: {status} "
                f"(elapsed: {elapsed_time:.1f}s)"
            )

            if status == CoalesceRunStatus.COMPLETED.value:
                context.log.info(f"Coalesce run {run_counter} completed successfully")
                return {
                    "run_counter": run_counter,
                    "status": status,
                    "result": result,
                }

            elif status == CoalesceRunStatus.FAILED.value:
                error_msg = result.get('error', 'Unknown error')
                raise Exception(
                    f"Coalesce run {run_counter} failed: {error_msg}"
                )

            elif status == CoalesceRunStatus.CANCELED.value:
                raise Exception(
                    f"Coalesce run {run_counter} was canceled"
                )

            # Still running, wait before next poll
            time.sleep(poll_interval_sec)
            elapsed_time = time.time() - start_time

        except requests.exceptions.RequestException as e:
            context.log.error(f"Failed to get run status: {str(e)}")
            raise

    # Timeout reached
    raise Exception(
        f"Coalesce run {run_counter} timed out after {max_wait_time_sec}s"
    )
