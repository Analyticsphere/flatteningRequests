### BQ CLI Cheat Sheet

#### Set default project (on Mac or Linux)

`bq ls -j --project_id nih-nci-dceg-connect-prod-6d04 > /dev/null`

#### Get list of tables w/ resource name, displayName and state

`bq ls --transfer_config --transfer_location='us' --filter='dataSourceIds:scheduled_query' --project_id=nih-nci-dceg-connect-prod-6d04`

`bq ls --transfer_config --transfer_location='us' --filter='dataSourceIds:scheduled_query' --project_id=nih-nci-dceg-connect-stg-5519`

`bq ls --transfer_config --transfer_location='us' --filter='dataSourceIds:scheduled_query' --project_id=nih-nci-dceg-connect-dev`

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
