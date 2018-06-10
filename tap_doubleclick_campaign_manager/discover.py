## TODO: need to discover report columns
## TODO: combine all report columns into one large table?
## https://developers.google.com/doubleclick-advertisers/v3.1/dimensions

def discover_streams(client):
    streams = []
    for s in STREAMS.values():
        s = s(client)
        streams.append({
            'stream': s.name,
            'tap_stream_id': s.name,
            'schema': s.load_schema(),
            'metadata': s.load_metadata()
        })
    return streams
