import logging
import json
import os
import requests
from datetime import datetime
import pendulum
import dlt

CONFIG_DIR = os.path.join(os.path.dirname(__file__), "../../config")

def load_api_config(pipeline_name):
    """
    Loads the API configuration from a JSON file based on the pipeline name.
    The config may include:
      - endpoint_url: the API URL (if not passed manually)
      - auth: a dictionary with authentication headers for private APIs.
      - incremental_type: "FULL" or "INCREMENTAL"
      - primary_key, delta_column, delta_value for incremental loads.
    """
    config_path = os.path.join(
        CONFIG_DIR, f"{pipeline_name.replace(' ', '_').lower()}_config.json"
    )
    if os.path.exists(config_path):
        with open(config_path, "r") as f:
            return json.load(f)
    return {}

def fetch_data_from_api(api_url, pipeline_name):
    api_config = load_api_config(pipeline_name)
    if not api_url and "endpoint_url" in api_config:
        api_url = api_config["endpoint_url"]

    if not api_url:
        logging.error("❌ No API URL provided!")
        return []

    headers = {}
    if "auth" in api_config:
        headers.update(api_config["auth"])

    response = requests.get(api_url, headers=headers, timeout=10)
    response.raise_for_status()
    data = response.json()

    # Use data_selector if provided.
    if isinstance(data, dict) and "data_selector" in api_config:
        data = data.get(api_config["data_selector"], [])
    elif not isinstance(data, list):
        data = [data]

    for item in data:
        item["extracted_at"] = datetime.utcnow().isoformat()
    return data

def get_api_resource(pipeline_name, table_name, api_url):
    """
    Returns a dlt resource function for API data.
    If the config indicates an incremental load, applies incremental hints.
    
    If the delta_column in the config contains a dot (e.g. "meta.updatedAt"),
    the function will extract that nested value and promote it to a new top-level key
    (e.g. "meta_updatedAt") that will be used as the incremental cursor.
    
    Additionally, this function uses add_map to cast the cursor field to a pendulum datetime
    so that it can be compared with the initial_value.
    """
    data = fetch_data_from_api(api_url, pipeline_name)
    api_config = load_api_config(pipeline_name)
    incremental_config = api_config.get("incremental", {})
    incremental_type = incremental_config.get("cursor_path") or api_config.get("incremental_type", "FULL").upper()
    
    if isinstance(incremental_type, str):
        incremental_type = incremental_type.upper()
    
    if incremental_type == "INCREMENTAL":
        primary_key = api_config.get("primary_key")
        # Get the cursor_path from config; default to the simple delta_column if not nested.
        cursor_path = incremental_config.get("cursor_path") or api_config.get("delta_column")
        delta_value = incremental_config.get("initial_value") or api_config.get("delta_value")
        # Parse the initial value if provided.
        initial_value = pendulum.parse(delta_value) if delta_value else None

        # Determine effective field name
        if "." in cursor_path:
            effective_delta_field = cursor_path.replace(".", "_")
            # For each record, traverse the nested keys to extract the value, and set it as a top-level field.
            for item in data:
                value = item
                for key in cursor_path.split("."):
                    if isinstance(value, dict) and key in value:
                        value = value[key]
                    else:
                        value = None
                        break
                if value is not None:
                    item[effective_delta_field] = value
        else:
            effective_delta_field = cursor_path

        # Define the resource that yields the data.
        @dlt.resource(name=table_name, write_disposition="append")
        def resource():
            yield from data

        # Use add_map to convert the effective_delta_field from string to a pendulum datetime.
        resource_with_mapping = resource.add_map(
            lambda record: {**record,
                            effective_delta_field: pendulum.parse(record[effective_delta_field])
                           } if record.get(effective_delta_field) else record
        )
        return resource_with_mapping.apply_hints(
            primary_key=primary_key,
            incremental=dlt.sources.incremental(effective_delta_field, initial_value=initial_value)
        )
    else:
        @dlt.resource(name=table_name, write_disposition="replace")
        def resource():
            yield from data
        return resource
