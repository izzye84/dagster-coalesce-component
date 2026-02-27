"""Coalesce.io Component for Dagster - Start and Monitor Runs.

This component creates a single Dagster asset that triggers a Coalesce run
for specified nodes using node selectors. Simple integration for running
Coalesce jobs from Dagster.
"""

import dagster as dg
import requests
import time
from typing import Optional, List, Dict, Any
from enum import Enum
from pydantic import field_validator
from pathlib import Path


class CoalesceRunStatus(Enum):
    """Coalesce run status values."""
    RUNNING = "running"
    COMPLETED = "completed"
    FAILED = "failed"
    CANCELED = "canceled"


class CoalesceStartARunScaffolder(dg.Scaffolder):
    """Custom scaffolder that pre-populates component parameters."""

    def scaffold(self, request: dg.ScaffoldRequest) -> None:
        """Create a pre-populated defs.yaml or component.py file with all parameters filled in."""

        # Default values for scaffolding
        default_values = {
            "asset_key": "my_coalesce_asset",
            "group_name": "coalesce",
            "base_url": "app.coalescesoftware.io",
            "bearer_token": '{{ env.COALESCE_BEARER_TOKEN }}',
            "environment_id": '{{ env.COALESCE_ENVIRONMENT_ID }}',
            "snowflake_username": '{{ env.SNOWFLAKE_USERNAME }}',
            "snowflake_keypair_key": '{{ env.SNOWFLAKE_KEYPAIR_KEY }}',
            "snowflake_warehouse": '{{ env.SNOWFLAKE_WAREHOUSE }}',
            "snowflake_role": '{{ env.SNOWFLAKE_ROLE }}',
            "include_nodes_selector": "+{name:MY_NODE}",
            "poll_interval_sec": 10,
            "max_wait_time_sec": 3600,
        }

        target_path = Path(request.target_path)

        # Check if we're scaffolding YAML or Python format
        is_python_format = request.scaffold_format == "python"

        if is_python_format:
            # Python format - create pre-populated component.py
            target_path.mkdir(parents=True, exist_ok=True)
            component_file = target_path / "component.py"

            # Generate the component.py content with all parameters
            content = f'''"""Coalesce component instance with pre-configured parameters.

Edit the parameters below to match your Coalesce environment and nodes.
"""

import dagster as dg
from dagster_coalesce_component.components.coalesce_start_a_run import CoalesceStartARun


@dg.component_instance
def load(_context: dg.ComponentLoadContext) -> CoalesceStartARun:
    """Load the Coalesce component instance.

    To add dynamic dependencies from a list of assets:

    from my_package.upstream_assets import my_asset_list
    upstream_deps = [[asset.key.to_user_string()] for asset in my_asset_list]

    Then pass: deps=upstream_deps
    """

    return CoalesceStartARun(
        # Dagster Asset Configuration
        asset_key="{default_values['asset_key']}",
        group_name="{default_values['group_name']}",

        # Coalesce API Configuration
        base_url=dg.EnvVar("COALESCE_BASE_URL").get_value("{default_values['base_url']}"),
        bearer_token=dg.EnvVar("COALESCE_BEARER_TOKEN").get_value(""),
        environment_id=dg.EnvVar("COALESCE_ENVIRONMENT_ID").get_value(""),

        # Snowflake Credentials (Key/Pair Auth)
        snowflake_username=dg.EnvVar("SNOWFLAKE_USERNAME").get_value(""),
        snowflake_keypair_key=dg.EnvVar("SNOWFLAKE_KEYPAIR_KEY").get_value(""),
        snowflake_warehouse=dg.EnvVar("SNOWFLAKE_WAREHOUSE").get_value(""),
        snowflake_role=dg.EnvVar("SNOWFLAKE_ROLE").get_value(""),

        # For Basic Auth instead of Key/Pair, use:
        # snowflake_password=dg.EnvVar("SNOWFLAKE_PASSWORD").get_value(""),

        # Coalesce Run Configuration
        include_nodes_selector="{default_values['include_nodes_selector']}",

        # Optional: Exclude specific nodes
        # exclude_nodes_selector="{{name:TEST_TABLE}}",

        # Optional: Define upstream dependencies
        # deps=[["upstream_asset_1"], ["upstream_asset_2"]],

        # Polling Configuration
        poll_interval_sec={default_values['poll_interval_sec']},
        max_wait_time_sec={default_values['max_wait_time_sec']},
    )
'''

            component_file.write_text(content)
            print(f"✓ Created pre-populated component.py at {component_file}")
        else:
            # YAML format - use default Dagster scaffolding with our values
            dg.scaffold_component(request, default_values)
            print(f"✓ Created pre-populated defs.yaml")


@dg.scaffold_with(CoalesceStartARunScaffolder)
class CoalesceStartARun(dg.Component, dg.Model, dg.Resolvable):
    """Execute Coalesce nodes as a single Dagster asset.

    This component creates a single Dagster asset that triggers a Coalesce run
    for the specified nodes. Use node selectors to control which Coalesce nodes
    to execute. Supports upstream dependencies from other Dagster assets.

    Example YAML usage:
        type: dagster_coalesce_component.components.coalesce_start_a_run.CoalesceStartARun
        attributes:
          asset_key: "coalesce_staging_layer"
          base_url: "app.coalescesoftware.io"
          bearer_token: "{{ env.COALESCE_BEARER_TOKEN }}"
          environment_id: "{{ env.COALESCE_ENVIRONMENT_ID }}"
          snowflake_username: "{{ env.SNOWFLAKE_USERNAME }}"
          snowflake_password: "{{ env.SNOWFLAKE_PASSWORD }}"
          snowflake_keypair_key: "{{ env.SNOWFLAKE_KEYPAIR_KEY }}"
          snowflake_keypair_pass: "{{ env.SNOWFLAKE_KEYPAIR_PASS }}"
          snowflake_warehouse: "{{ env.SNOWFLAKE_WAREHOUSE }}"
          snowflake_role: "{{ env.SNOWFLAKE_ROLE }}"
          include_nodes_selector: "{ location: TARGET name: STG_USERS }"
          group_name: "coalesce_etl"
          deps:
            - ["upstream_asset_1"]
            - ["upstream_asset_2"]
    """

    # Dagster Asset Configuration
    asset_key: str  # Name for the Dagster asset
    group_name: Optional[str] = "coalesce"

    # Coalesce API Configuration
    base_url: str  # e.g., "app.coalescesoftware.io"
    bearer_token: str = ""
    environment_id: str

    # Snowflake Credentials for Coalesce runs
    snowflake_username: str = ""

    # Basic Auth (username/password)
    snowflake_password: str = ""

    # Key/Pair Auth
    snowflake_keypair_key: str = ""  # PEM-encoded private key
    snowflake_keypair_pass: str = ""  # Password to decrypt key (optional, only if key is encrypted)

    snowflake_warehouse: str
    snowflake_role: str

    # Coalesce Run Configuration
    include_nodes_selector: Optional[str] = None  # Nodes to include for the run
    exclude_nodes_selector: Optional[str] = None  # Nodes to exclude from the run
    job_id: Optional[str] = None  # The ID of a job being run
    parallelism: int = 16  # Maximum number of parallel nodes to run

    # Optional: Upstream dependencies from other Dagster assets
    # Format: [["upstream_group", "upstream_asset"], ...]
    deps: Optional[List[List[str]]] = None

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
        """Build Dagster definition for Coalesce run."""

        # Capture component configuration in closure
        asset_key = self.asset_key
        base_url = self.base_url
        bearer_token = self.bearer_token
        environment_id = self.environment_id
        snowflake_username = self.snowflake_username
        snowflake_password = self.snowflake_password
        snowflake_keypair_key = self.snowflake_keypair_key
        snowflake_keypair_pass = self.snowflake_keypair_pass
        snowflake_warehouse = self.snowflake_warehouse
        snowflake_role = self.snowflake_role
        include_nodes_selector = self.include_nodes_selector
        exclude_nodes_selector = self.exclude_nodes_selector
        job_id = self.job_id
        parallelism = self.parallelism
        group_name = self.group_name
        poll_interval_sec = self.poll_interval_sec
        max_wait_time_sec = self.max_wait_time_sec

        # Parse upstream dependencies if configured
        deps_list = None
        if self.deps:
            deps_list = [dg.AssetKey(dep) for dep in self.deps]
            print(f"Asset {asset_key} has {len(deps_list)} configured dependencies")

        # Build metadata
        metadata = {
            "environment_id": environment_id,
        }
        if include_nodes_selector:
            metadata["include_nodes_selector"] = include_nodes_selector
        if exclude_nodes_selector:
            metadata["exclude_nodes_selector"] = exclude_nodes_selector
        if job_id:
            metadata["job_id"] = job_id

        @dg.asset(
            key=asset_key,
            description=f"Coalesce run in environment {environment_id}",
            group_name=group_name,
            kinds={"coalesce", "snowflake"},
            deps=deps_list,
            metadata=metadata,
        )
        def coalesce_run_asset(context: dg.AssetExecutionContext) -> Dict[str, Any]:
            """Execute Coalesce run and wait for completion."""

            context.log.info(f"Starting Coalesce run in environment {environment_id}")
            if include_nodes_selector:
                context.log.info(f"Node selector: {include_nodes_selector}")

            # Start the run
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
                include_nodes_selector=include_nodes_selector,
                exclude_nodes_selector=exclude_nodes_selector,
                job_id=job_id,
                parallelism=parallelism,
            )

            # Poll for completion — cancel Coalesce run if Dagster op is interrupted
            _run_completed = False
            try:
                result = _poll_run_status(
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

            return result

        print(f"Created Dagster asset: {asset_key}")
        return dg.Definitions(assets=[coalesce_run_asset])


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
    include_nodes_selector: Optional[str] = None,
    exclude_nodes_selector: Optional[str] = None,
    job_id: Optional[str] = None,
    parallelism: int = 16,
) -> str:
    """Start a Coalesce run and return the run counter.

    Supports both Basic authentication (username/password) and Key/Pair authentication.
    """

    url = f"https://{base_url}/scheduler/startRun"

    # Build run details
    run_details = {
        "environmentID": environment_id,
        "parallelism": parallelism,
    }

    # Add node selectors if provided
    if include_nodes_selector:
        run_details["includeNodesSelector"] = include_nodes_selector
        context.log.info(f"Including nodes: {include_nodes_selector}")

    if exclude_nodes_selector:
        run_details["excludeNodesSelector"] = exclude_nodes_selector
        context.log.info(f"Excluding nodes: {exclude_nodes_selector}")

    if job_id:
        run_details["jobID"] = job_id
        context.log.info(f"Using job ID: {job_id}")

    # Determine authentication method and build userCredentials
    if snowflake_keypair_key:
        # Use Key/Pair authentication
        context.log.info("Using Key/Pair authentication")
        user_credentials = {
            "snowflakeUsername": snowflake_username,
            "snowflakeKeyPairKey": snowflake_keypair_key,
            "snowflakeWarehouse": snowflake_warehouse,
            "snowflakeRole": snowflake_role,
            "snowflakeAuthType": "KeyPair"
        }
        # Add keypair password only if provided (for encrypted keys)
        if snowflake_keypair_pass:
            user_credentials["snowflakeKeyPairPass"] = snowflake_keypair_pass
    else:
        # Use Basic authentication (username/password)
        context.log.info("Using Basic authentication")
        user_credentials = {
            "snowflakeUsername": snowflake_username,
            "snowflakePassword": snowflake_password,
            "snowflakeWarehouse": snowflake_warehouse,
            "snowflakeRole": snowflake_role,
            "snowflakeAuthType": "Basic"
        }

    payload = {
        "runDetails": run_details,
        "userCredentials": user_credentials
    }

    headers = {
        'Content-Type': 'application/json',
        'Accept': 'application/json',
        'Authorization': f'Bearer {bearer_token}'
    }

    context.log.info(f"Starting Coalesce run in environment {environment_id}")
    context.log.debug(f"Payload: {payload}")

    try:
        response = requests.post(url, json=payload, headers=headers)
        response.raise_for_status()

        result = response.json()
        run_counter = result.get('runCounter', result.get('id'))

        context.log.info(f"Coalesce run started. Run counter: {run_counter}")
        return run_counter

    except requests.exceptions.RequestException as e:
        context.log.error(f"Failed to start Coalesce run: {str(e)}")
        context.log.error(f"Request URL: {url}")
        context.log.error(f"Request payload: {payload}")
        if hasattr(e, 'response') and e.response is not None:
            context.log.error(f"Response status: {e.response.status_code}")
            context.log.error(f"Response body: {e.response.text}")
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
