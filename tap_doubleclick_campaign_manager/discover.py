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
        tap_stream_id = '{}_{}'.format(stream_name, report['id'])
        report_configs[(stream_name, tap_stream_id)] = report

    field_type_lookup = get_field_type_lookup()
    catalog = Catalog([])

    for (stream_name, tap_stream_id), report in report_configs.items():
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
            stream_alias=stream_name,
            tap_stream_id=tap_stream_id,
            key_properties=[],
            schema=schema,
            metadata=metadata
        ))

    return catalog.to_dict()
