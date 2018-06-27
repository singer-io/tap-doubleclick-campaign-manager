import os
import json

SINGER_REPORT_FIELD = '_sdc_report_time'
REPORT_ID_FIELD = '_sdc_report_id'

def get_field_type_lookup():
    path = os.path.join(os.path.dirname(os.path.realpath(__file__)),
                        'report_field_type_lookup.json')
    with open(path) as file:
        return json.load(file)

def get_fields(field_type_lookup, report):
    report_type = report['type']
    if report_type == 'STANDARD':
        criteria_obj = report['criteria']
        dimensions = criteria_obj['dimensions']
        metric_names = criteria_obj['metricNames']
    elif report_type == 'FLOODLIGHT':
        criteria_obj = report['floodlightCriteria']
        dimensions = criteria_obj['dimensions']
        metric_names = criteria_obj['metricNames']
    elif report_type == 'CROSS_DIMENSION_REACH':
        criteria_obj = report['crossDimensionReachCriteria']
        dimensions = criteria_obj['breakdown']
        metric_names = criteria_obj['metricNames'] + criteria_obj['overlapMetricNames']
    elif report_type == 'PATH_TO_CONVERSION':
        criteria_obj = report['pathToConversionCriteria']
        dimensions = criteria_obj['conversionDimensions'] + criteria_obj['customFloodlightVariables']
        metric_names = criteria_obj['metricNames'] + criteria_obj['perInteractionDimensions']
    elif report_type == 'REACH':
        criteria_obj = report['reachCriteria']
        dimensions = criteria_obj['dimensions']
        metric_names = criteria_obj['metricNames'] + criteria_obj['reachByFrequencyMetricNames']

    dimensions = list(map(lambda x: x['name'], dimensions))
    columns = dimensions + metric_names

    fieldmap = []
    for column in columns:
        fieldmap.append({
            'name': column.replace('dfa:', ''),
            'type': field_type_lookup.get(column, 'string')
        })

    return fieldmap

def get_schema(stream_name, fieldmap):
    properties = {}

    properties[SINGER_REPORT_FIELD] = {
        'type': 'string',
        'format': 'date-time'
    }

    properties[REPORT_ID_FIELD] = {
        'type': 'integer'
    }

    for field in fieldmap:
        _type = field['type']
        if _type == 'long':
            _type = 'integer'
        elif _type == 'double':
            _type = 'number'
        properties[field['name']] = {
            'type': ['null', _type]
        }

    schema = {
        'type': 'object',
        'properties': properties,
        'addtionalProperties': False
    }

    return schema
