"""Utility functions for Dagster components."""

import dagster as dg
import inspect
from typing import List


def get_asset_keys_by_group(module, group_name: str) -> List[List[str]]:
    """Programmatically get all asset keys in a module with a given group name.

    This uses Python introspection to find assets at component load time,
    since AssetSelection.resolve() requires an asset graph which isn't
    available during build_defs().
    """
    asset_keys = []

    # Get all attributes from the module
    for name, obj in inspect.getmembers(module):
        # Check if it's an AssetsDefinition (single or multi-asset)
        if isinstance(obj, dg.AssetsDefinition):
            # Check specs for group_name (works for both @asset and @multi_asset)
            if hasattr(obj, 'specs'):
                for spec in obj.specs:
                    if spec.group_name == group_name:
                        asset_keys.append([spec.key.to_user_string()])

            # Fallback: check group_names_by_key dict
            elif hasattr(obj, 'group_names_by_key'):
                for key, grp in obj.group_names_by_key.items():
                    if grp == group_name:
                        asset_keys.append([key.to_user_string()])

    return asset_keys


def get_asset_keys_from_list(assets: List[dg.AssetsDefinition]) -> List[List[str]]:
    """Convert a list of AssetsDefinition objects to asset keys.

    Convenience function for when you have an explicit list of assets.

    Args:
        assets: List of AssetsDefinition objects

    Returns:
        List of asset keys in the format expected by deps parameter: [["asset_key"], ...]

    Example:
        >>> from my_package.upstream_assets import asset_a, asset_b, asset_c
        >>> all_assets = [asset_a, asset_b, asset_c]
        >>> deps = get_asset_keys_from_list(all_assets)
        >>> # Returns: [["asset_a"], ["asset_b"], ["asset_c"]]
    """
    return [[asset.key.to_user_string()] for asset in assets]
