import logging
import json
import os
import requests
from datetime import datetime
import pendulum
import dlt
from itertools import islice

CONFIG_DIR = os.path.join(os.path.dirname(__file__), "../../config")

def paginate_generator(gen, chunk_size=50000):
    """Yield chunks (lists) of rows from the generator."""
    while True:
        chunk = list(islice(gen, chunk_size))
        if not chunk:
            break
        yield chunk

def load_api_config(pipeline_name):
    """
    Loads the API configuration from a JSON file based on the pipeline name.
    The config may include:
      - endpoint_url: the API URL (if not passed manually)
      - auth: a dictionary with authentication headers for private APIs.
      - incremental_type: "FULL" or "INCREMENTAL"
      - primary_key, delta_column, delta_value for incremental loads.
      - data_selector: key in the response that holds the records
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
    elif "auth_type" in api_config:
        if api_config["auth_type"] == "api_key":
            headers[api_config.get("api_key_header", "X-API-Key")] = api_config["api_key"]
        elif api_config["auth_type"] == "bearer":
            headers["Authorization"] = f"Bearer {api_config['bearer_token']}"
        elif api_config["auth_type"] == "basic":
            import base64
            auth_string = f"{api_config['username']}:{api_config['password']}"
            auth_bytes = auth_string.encode('ascii')
            base64_bytes = base64.b64encode(auth_bytes)
            base64_auth = base64_bytes.decode('ascii')
            headers["Authorization"] = f"Basic {base64_auth}"

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

# For API, we'll simply yield chunks in the resource functions.
def get_api_resource(pipeline_name, table_name, api_url):
    data = fetch_data_from_api(api_url, pipeline_name)
    api_config = load_api_config(pipeline_name)
    incremental_config = api_config.get("incremental", {})
    incremental_type = incremental_config.get("cursor_path") or api_config.get("incremental_type", "FULL").upper()
    
    if isinstance(incremental_type, str):
        incremental_type = incremental_type.upper()
    
    if incremental_type == "INCREMENTAL":
        primary_key = api_config.get("primary_key")
        cursor_path = incremental_config.get("cursor_path") or api_config.get("delta_column")
        delta_value = incremental_config.get("initial_value") or api_config.get("delta_value")
        initial_value = pendulum.parse(delta_value) if delta_value else None

        if "." in cursor_path:
            effective_delta_field = cursor_path.replace(".", "_")
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

        @dlt.resource(name=table_name, write_disposition="append")
        def resource():
            yield from paginate_generator(iter(data), 50000)

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
            yield from paginate_generator(iter(data), 50000)
        return resource
