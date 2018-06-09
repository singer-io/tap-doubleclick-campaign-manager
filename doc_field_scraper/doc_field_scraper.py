import sys
import json
import collections

import requests
from bs4 import BeautifulSoup

FIELD_DOC_URL = 'https://developers.google.com/doubleclick-advertisers/v3.1/dimensions'

FIELD_TABLES = [
    'standard-dimensions',
    'standard-metrics',
    'standard-filters',
    'activity-filters',
    'activity-metrics',
    'custom',
    'reach-dimensions',
    'reach-metrics',
    'reach-by-frequency-metrics',
    'reach-filters',
    'p2c-conversion-dimensions',
    'p2c-per-interaction-dimensions',
    'p2c-metrics',
    'p2c-custom-floodlight-variables',
    'cdr-breakdown',
    'cdr-filters',
    'cdr-metrics',
    'cdr-overlap-metrics',
    'floodlight-dimensions',
    'floodlight-metrics',
    'floodlight-filters'
]

def main():
    raw_html = requests.get(FIELD_DOC_URL).text
    soup = BeautifulSoup(raw_html, 'html.parser')

    field_type_lookup = {}
    for field_table in FIELD_TABLES:
        table = soup.find(id=field_table).next_sibling.next_sibling
        cells = table.find_all('td')

        for i in range(len(cells)):
            if i % 3 == 0:
                field = cells[i].get_text().strip()
                _type = cells[i + 2].get_text().strip()
                field_type_lookup[field] = _type

    json.dump(field_type_lookup, sys.stdout, indent=2)

if __name__ == '__main__':
    main()
