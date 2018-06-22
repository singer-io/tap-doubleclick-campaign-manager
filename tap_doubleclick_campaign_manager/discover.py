import re

from singer.catalog import Catalog, CatalogEntry, Schema

from tap_doubleclick_campaign_manager.schema import (
    SINGER_REPORT_FIELD,
    get_fields,
    get_schema,
    get_field_type_lookup
)

def sanitize_name(report_name):
    report_name = re.sub(r'[\s\-\/]', '_', report_name.lower())
    return re.sub(r'[^a-z0-9_]', '', report_name)

def handle_duplicate(report_configs, stream_name, number=1):
    new_stream_name = stream_name + '_' + str(number)
    if number == 1 and stream_name in report_configs:
        report_configs[new_stream_name] = report_configs[stream_name]
        del report_configs[stream_name]
    elif new_stream_name not in report_configs:
        return new_stream_name
    return handle_duplicate(report_configs, stream_name, number=number + 1)

def discover_streams(service, config):
    profile_id = config.get('profile_id')

    reports = (
        service
        .reports()
        .list(profileId=profile_id)
        .execute()
        .get('items')
    )

    reports = sorted(reports, key=lambda x: x['id'])
    report_configs = {}
    for report in reports:
        stream_name = sanitize_name(report['name'])
        if stream_name in report_configs:
            stream_name = handle_duplicate(report_configs, stream_name)
        report_configs[stream_name] = report

    field_type_lookup = get_field_type_lookup()
    catalog = Catalog([])

    for stream_name, report in report_configs.items():
        fieldmap = get_fields(field_type_lookup, report)
        schema_dict = get_schema(stream_name, fieldmap)
        schema = Schema.from_dict(schema_dict)

        metadata = []
        metadata.append({
            'metadata': {
                'tap-doubleclick-campaign-manager.report-id': report['id']
            },
            'breadcrumb': []
        })

        for prop in schema_dict['properties'].keys():
            metadata.append({
                'metadata': {
                    'inclusion': 'automatic'
                },
                'breadcrumb': ['properties', prop]
            })

        catalog.streams.append(CatalogEntry(
            stream=stream_name,
            tap_stream_id=stream_name,
            key_properties=[],
            schema=schema,
            metadata=metadata
        ))

    return catalog.to_dict()
