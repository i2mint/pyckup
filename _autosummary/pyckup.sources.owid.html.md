# pyckup.sources.owid

OWID data source.

You can see a list of datas here: [https://ourworldindata.org/data](https://ourworldindata.org/data)

See: [https://docs.owid.io/projects/etl/api/chart-api/](https://docs.owid.io/projects/etl/api/chart-api/)

### Functions

| [`acquire_owid_data`](#pyckup.sources.owid.acquire_owid_data)(slug, \*[, refresh])          | Download OWID CSV + JSON metadata for each chart slug.             |
|--------------------------------------------------------------------------------------------------|--------------------------------------------------------------------|
| [`slug_to_url_and_filename`](#pyckup.sources.owid.slug_to_url_and_filename)(slug[, kind, rootdir]) | The OWID url for a chart `slug` and the local path to store it at. |

### pyckup.sources.owid.acquire_owid_data(slug, , refresh=False)

Download OWID CSV + JSON metadata for each chart slug.

Returns list of tuples: (json_path_or_None, csv_path_or_None)

* **Return type:**
  [`list`](https://docs.python.org/3/builtins/stdtypes.html#list)[[`tuple`](https://docs.python.org/3/builtins/stdtypes.html#tuple)[[`str`](https://docs.python.org/3/builtins/stdtypes.html#str), [`Path`](https://docs.python.org/3/library/pathlib.html#pathlib.Path) | [`None`](https://docs.python.org/3/builtins/constants.html#None), [`Path`](https://docs.python.org/3/library/pathlib.html#pathlib.Path) | [`None`](https://docs.python.org/3/builtins/constants.html#None)]]

### pyckup.sources.owid.slug_to_url_and_filename(slug, kind='json', rootdir='/home/runner/.config/pyckup/downloads/owid')

The OWID url for a chart `slug` and the local path to store it at.

`kind` is one of ‘json’ (metadata), ‘csv’, ‘zip’ or ‘html’.

```pycon
>>> url, path = slug_to_url_and_filename('co2', 'csv', rootdir='/tmp')
>>> url
'https://ourworldindata.org/grapher/co2.csv'
```
