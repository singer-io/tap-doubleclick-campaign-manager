import os
import json
from datetime import datetime
from singer.catalog import Catalog
from singer.transform import Transformer
from singer import metadata, get_logger
from tap_doubleclick_campaign_manager.schema import get_schema, SINGER_REPORT_FIELD, REPORT_ID_FIELD

LOGGER = get_logger()


def explain_transform(record, schema, metadata_map):
    transformed = Transformer().transform(record, schema, metadata_map)
    included = list(transformed.keys())
    excluded = list(set(record.keys()) - set(included))
    return included, excluded


def extract_selected_fields(metadata_map):
    selected_fields = []
    for breadcrumb, meta in metadata_map.items():
        if len(breadcrumb) == 2 and breadcrumb[0] == "properties":
            field = breadcrumb[1]
            if meta.get("selected", True):  # Treat missing 'selected' as True
                selected_fields.append(field)
    return selected_fields

def extract_explicitly_unselected_fields(metadata_map):
    explicitly_unselected = []
    for breadcrumb, meta in metadata_map.items():
        if len(breadcrumb) == 2 and breadcrumb[0] == "properties":
            field = breadcrumb[1]
            if meta.get("selected") is False:
                explicitly_unselected.append(field)
    return explicitly_unselected

def find_catalog_json(start_dir):
    """
    Walk up and also check common subfolders for catalog.json
    """
    current_dir = start_dir
    while True:
        # Check in current directory
        direct_path = os.path.join(current_dir, 'catalog.json')
        if os.path.exists(direct_path):
            print(f"[DEBUG] Found catalog.json at: {direct_path}")
            return direct_path

        # Check tap_doubleclick_campaign_manager subfolder
        nested_path = os.path.join(current_dir, 'tap_doubleclick_campaign_manager', 'catalog.json')
        if os.path.exists(nested_path):
            print(f"[DEBUG] Found catalog.json at: {nested_path}")
            return nested_path

        # Go up
        parent = os.path.dirname(current_dir)
        if parent == current_dir:
            break
        current_dir = parent

    raise FileNotFoundError("catalog.json not found in any parent directory or subfolder.")


def test_transform_all_selected_streams_from_catalog():
    current_dir = os.path.dirname(__file__)
    catalog_path = find_catalog_json(current_dir)

    LOGGER.info("Found catalog.json at: %s", catalog_path)

    with open(catalog_path) as f:
        catalog_dict = json.load(f)

    catalog = Catalog.from_dict(catalog_dict)

    for stream in catalog.streams:
        stream_id = stream.tap_stream_id
        metadata_map = metadata.to_map(stream.metadata)

        # Check if stream itself is selected
        stream_selected = metadata_map.get((), {}).get("selected", False)
        if not stream_selected:
            continue  # Skip unselected streams

        LOGGER.info("Stream '%s' is selected", stream_id)

        # Extract schema and field info
        schema_dict = stream.schema.to_dict()
        properties = schema_dict.get("properties", {})
        fieldmap = [{"name": name, "type": "string"} for name in properties]
        schema = get_schema(stream_id, fieldmap)

        # Create mock record with all fields
        record = {name: "mock_value" for name in properties}
        record[SINGER_REPORT_FIELD] = datetime.utcnow().isoformat() + "Z"
        record[REPORT_ID_FIELD] = 123456

        included, excluded = explain_transform(record, schema, metadata_map)
        explicitly_unselected = extract_explicitly_unselected_fields(metadata_map)
        selected_fields = extract_selected_fields(metadata_map)

        LOGGER.info("Selected fields in stream '%s': %s", stream_id, selected_fields)
        LOGGER.info("Explicitly unselected fields in stream '%s': %s", stream_id, explicitly_unselected)

        for field in explicitly_unselected:
            assert field not in included, f"Field '{field}' is unselected but found in output of stream '{stream_id}'"
