# tap-doubleclick-campaign-manager

This is a [Singer](https://singer.io) tap that produces JSON-formatted data following the [Singer spec](https://github.com/singer-io/getting-started/blob/master/SPEC.md).

This tap:

- Pulls [reports](https://developers.google.com/doubleclick-advertisers/guides/run_reports) from the [DoubleClick for Campaign Manager API](https://developers.google.com/doubleclick-advertisers/getting_started)
- Requires users to create reports and specify a date range (usually relative) in the DoubleClick for Campaign Managers user interface. The report ID of the created report along with a unique stream name (becomes the table name) is placed in the tap configuration file. 
- Outputs the schema for each resource

## Configuration

This tap requires a `config.json` which specifies details regarding [Google OAuth authentication](https://developers.google.com/identity/protocols/OAuth2WebServer), which reports to pull, and the stream names for each report. See [config.sample.json](config.sample.json) for an example.

In the config, the `reports` property contains a delimited string to keep it compatible with the Stitch environment. It uses semicolons to separate reports (like rows) and commas to separate report IDs and report stream names (like columns).

Example `123456789,activities_and_clicks;123456790,dfa1_report`

To run `tap-doubleclick-campaign-manager` with the configuration file, use this command:

```bash
› tap-doubleclick-campaign-manager -c my-config.json
```

---

Copyright &copy; 2018 Stitch