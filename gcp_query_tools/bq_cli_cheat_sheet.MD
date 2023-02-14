### BQ CLI Cheat Sheet

#### Get list of tables w/ resource name, displayName and state

`bq ls --transfer_config --transfer_location='us' --filter='dataSourceIds:scheduled_query' --project_id=nih-nci-dceg-connect-prod-6d04`

#### List tables in FlatConnect data set

Simple table

`bq ls FlatConnect`

JSON with more detail

`bq ls --format=prettyjson FlatConnect`

#### Get table schema

Display only

`bq show --schema --format=prettyjson FlatConnect.participants_JP`

Write to file

`bq show --schema --format=prettyjson FlatConnect.participants_JP > participants_schema.json`
