from singer.catalog import Catalog, CatalogEntry, Schema

from tap_doubleclick_campaign_manager.schema import (
    SINGER_REPORT_FIELD,
    get_fields,
    get_schema,
    get_field_type_lookup
)

def discover_streams(service, config):
    profile_id = config.get('profile_id')
    reports = config.get('reports')

    field_type_lookup = get_field_type_lookup()
    catalog = Catalog([])

    for report_config in reports:
        report_id = report_config['report_id']
        stream_name = report_config['stream_name']

        report = (
            service
            .reports()
            .get(profileId=profile_id,
                 reportId=report_id)
            .execute()
        )

        fieldmap = get_fields(field_type_lookup, report)
        schema_dict = get_schema(stream_name, fieldmap)
        schema = Schema.from_dict(schema_dict)

        catalog.streams.append(CatalogEntry(
            stream=stream_name,
            tap_stream_id=stream_name,
            key_properties=[],
            schema=schema,
        ))

    return catalog.to_dict()
