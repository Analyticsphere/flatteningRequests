-- FYI: This query was automatically generated using R code written by Jake 
-- Peters. The code references a the source table schema and a configuration
-- file specifying various parameters. This style of query was developed by 
-- Warren Lu to flatten the Connect4Cancer datasets in 2022. See GitHub repo
-- for further documentation.
-- 
-- Repository: https://github.com/Analyticsphere/flatteningRequests
-- Relavent functions: generate_flattening_query.R
-- 
-- source_table: nih-nci-dceg-connect-prod-6d04.Connect.participants
-- destination table: nih-nci-dceg-connect-prod-6d04.FlatConnect.notifications_JP -- notes
    
----- User-defined JavaScript functions used in BigQuery -----
CREATE TEMP FUNCTION
  handleRow(input_row STRING)
  RETURNS STRING
  LANGUAGE js AS r"""
  
  function getNestedObjectValue(obj, pathString) {
    function getNestedObjectValueFromPathArray(obj, pathArray) {
      let currKey = pathArray[0];
      if (
        typeof obj[currKey] === "undefined" || obj[currKey] === null ||
        pathArray.length === 1
      )
        return obj[currKey];
      return getNestedObjectValueFromPathArray(obj[currKey], pathArray.slice(1));
    }
    return getNestedObjectValueFromPathArray(obj, pathString.split("."));
  }
  
  function setNestedObjectValue(obj, pathString, value) {
    function setNestedObjectValueFromPathArray(obj, pathArray, value) {
      let currKey = pathArray[0];
      if (pathArray.length === 1) {
        obj[currKey] = value;
        return;
      }
      if (
        typeof obj[currKey] === "undefined" ||
        typeof obj[currKey] !== "object"
      ) {
        obj[currKey] = {};
      }
      return setNestedObjectValueFromPathArray(
        obj[currKey],
        pathArray.slice(1),
        value
      );
    }
    return setNestedObjectValueFromPathArray(obj, pathString.split("."), value);
  }
  
  const arraysToBeFlattened= [

]
 
  
  function handleRowJS(row) {
    for (let arrPath of Object.keys(arraysToBeFlattened)) {
      let currObj = {};
      let inputConceptIdList = getNestedObjectValue(row, arrPath);
      if (!inputConceptIdList || inputConceptIdList.length === 0) continue;
      inputConceptIdList = inputConceptIdList.map(v => +v);
      for (let cid of arraysToBeFlattened[arrPath]) {
        if (inputConceptIdList.indexOf(cid) >= 0) {
          currObj["" + cid] = 1;
        } else currObj["" + cid] = 0;
      }
      setNestedObjectValue(row, arrPath, currObj);
    }
    return JSON.stringify(row);
  }
  const row = JSON.parse(input_row);
  return handleRowJS(row);
""";
----- End of user-defined JavaScript functions -----


----- Beginning of query body -----
CREATE OR REPLACE TABLE
  `nih-nci-dceg-connect-prod-6d04.FlatConnect.notifications_JP` -- destination_table
  OPTIONS (description="Source table: Connect.notifications; Scheduled Query: FlatConnect.notifications_JP; GitHub: https://github.com/Analyticsphere/flatteningRequests/tree/main/queryGenerators/FlatConnect_queries/notifications; Team: Analytics; Maintainer: Jake Peters; Super Users: Jing, Kelsey; Notes: This table is a flattened version of Connect.notifications. It is used for QAQC/metrics purposes.") -- table_description
  AS (
  WITH
    json_data AS (
    SELECT
      Connect_ID,
      [handleRow(TO_JSON_STRING(input_row))] AS body
    FROM
      `nih-nci-dceg-connect-prod-6d04.Connect.participants` AS input_row -- source_table
    ), -- filter_statement
    flattened_data AS (
    SELECT
      	REPLACE(JSON_QUERY(row,'$.attempt'), '\"', '') AS attempt,
	REPLACE(JSON_QUERY(row,'$.category'), '\"', '') AS category,
	REPLACE(JSON_QUERY(row,'$.email'), '\"', '') AS email,
	REPLACE(JSON_QUERY(row,'$.id'), '\"', '') AS id,
	REPLACE(JSON_QUERY(row,'$.notification.body'), '\"', '') AS notification_body,
	REPLACE(JSON_QUERY(row,'$.notification.time'), '\"', '') AS notification_time,
	REPLACE(JSON_QUERY(row,'$.notification.title'), '\"', '') AS notification_title,
	REPLACE(JSON_QUERY(row,'$.notificationId'), '\"', '') AS notificationId,
	REPLACE(JSON_QUERY(row,'$.notificationSpecificationsID'), '\"', '') AS notificationSpecificationsID,
	REPLACE(JSON_QUERY(row,'$.notificationType'), '\"', '') AS notificationType,
	REPLACE(JSON_QUERY(row,'$.read'), '\"', '') AS read,
	REPLACE(JSON_QUERY(row,'$.token'), '\"', '') AS token,
	REPLACE(JSON_QUERY(row,'$.uid'), '\"', '') AS uid -- selects
    FROM
      json_data,
      UNNEST(body) AS ROW )
  SELECT
    *,
    FORMAT_TIMESTAMP("%Y%m%d", DATETIME(CURRENT_TIMESTAMP(), "America/New_York")) AS date --date_format
  FROM
    flattened_data 
   -- order statement
  );
    

