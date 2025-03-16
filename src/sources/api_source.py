import logging
import json
import os
import requests
from datetime import datetime

CONFIG_DIR = os.path.join(os.path.dirname(__file__), "../../config")


# ✅ Load API Config (for private APIs)
def load_api_config(pipeline_name):
    config_path = os.path.join(
        CONFIG_DIR, f"{pipeline_name.replace(' ', '_').lower()}_config.json"
    )
    if os.path.exists(config_path):
        with open(config_path, "r") as f:
            return json.load(f)
    return {}


# ✅ Fetch Data (Handles Public & Private APIs)
def fetch_data_from_api(api_url, pipeline_name):
    if not api_url:
        logging.error("❌ No API URL provided!")
        return []

    headers = {}
    api_config = load_api_config(pipeline_name)

    if "auth" in api_config:
        logging.info(f"🔑 Applying authentication for `{pipeline_name}`")
        headers.update(api_config["auth"])

    logging.info(f"Fetching data from API: {api_url}")

    try:
        response = requests.get(api_url, headers=headers, timeout=10)
        response.raise_for_status()
        data = response.json()
    except Exception as e:
        logging.error(f"❌ API Request Failed: {e}")
        return []

    if isinstance(data, dict):
        data = [data]

    for item in data:
        item["extracted_at"] = datetime.utcnow().isoformat()

    return data
