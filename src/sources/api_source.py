import logging
import json
import os
from datetime import datetime
import pendulum
import dlt
from itertools import islice
from dlt.sources.helpers.rest_client import RESTClient
from dlt.sources.helpers.rest_client.auth import BearerTokenAuth
from dlt.sources.helpers.rest_client.paginators import PageNumberPaginator, OffsetPaginator, JSONResponseCursorPaginator

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
    
    # Get pagination settings from config
    pagination_type = api_config.get("pagination", {}).get("type", "none")
    page_size = api_config.get("pagination", {}).get("page_size", 100)
    max_pages = api_config.get("pagination", {}).get("max_pages", None)
    
    # Configure paginator based on API type
    paginator = None
    if pagination_type == "page_number":
        paginator = PageNumberPaginator(
            page_size=page_size,
            page_param="page",
            max_pages=max_pages
        )
    elif pagination_type == "offset":
        paginator = OffsetPaginator(
            page_size=page_size,
            offset_param="offset",
            max_pages=max_pages
        )
    elif pagination_type == "cursor":
        paginator = JSONResponseCursorPaginator(
            cursor_path=api_config.get("cursor_path"),
            max_pages=max_pages
        )

    # Configure REST client
    auth = None
    if api_config.get("auth", {}).get("type") == "bearer":
        auth = BearerTokenAuth(token=api_config["auth"]["token"])

    client = RESTClient(
        base_url=api_url,
        headers=api_config.get("headers", {}),
        auth=auth,
        paginator=paginator,
        data_selector=api_config.get("data_selector")
    )

    all_data = []
    try:
        # Use the client's paginate method to handle pagination
        for page in client.paginate(""):  # Empty string since we already have full URL
            all_data.extend(page)
    except Exception as e:
        logging.error(f"Error fetching data: {str(e)}")
        raise

    # Add extraction timestamp
    for item in all_data:
        item["extracted_at"] = datetime.utcnow().isoformat()
    
    return all_data

# For API, we'll simply yield chunks in the resource functions.
def get_api_resource(pipeline_name, table_name, api_url):
    api_config = load_api_config(pipeline_name)
    incremental_config = api_config.get("incremental", {})
    
    @dlt.resource(
        name=table_name,
        write_disposition="append" if api_config.get("incremental_load", {}).get("enabled") else "replace",
        primary_key=api_config.get("primary_key"),
        pagination=api_config.get("pagination", {}),
        retry_count=3  # Add retry for resilience
    )
    def resource():
        yield from paginate_generator(
            fetch_data_from_api(api_url, pipeline_name),
            chunk_size=api_config.get("pagination", {}).get("page_size", 50000)
        )

    if api_config.get("incremental_load", {}).get("enabled"):
        field = api_config["incremental_load"]["field"]
        resource = resource.add_map(
            lambda record: {
                **record,
                field: pendulum.parse(record[field])
            } if record.get(field) else record
        ).apply_hints(
            incremental=dlt.sources.incremental(field)
        )

    return resource
