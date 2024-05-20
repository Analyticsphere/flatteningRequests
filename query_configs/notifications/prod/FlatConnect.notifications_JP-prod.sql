-- FYI: This query was automatically generated using R code written by Jake 
-- Peters. The code references a the source table schema and a configuration
-- file specifying various parameters. This style of query was developed by 
-- Warren Lu to flatten the Connect4Cancer datasets in 2022. See GitHub repo
-- for further documentation.
-- 
-- Repository: https://github.com/Analyticsphere/flatteningRequests
-- Relavent functions: generate_flattening_query.R
-- 
-- source_table: nih-nci-dceg-connect-prod-6d04.Connect.notifications
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
      `nih-nci-dceg-connect-prod-6d04.Connect.notifications` AS input_row -- source_table
    ), -- filter_statement
    flattened_data AS (
    SELECT
      	REPLACE(JSON_QUERY(row,'$.accepted_date'), '\"', '') AS accepted_date,
	REPLACE(JSON_QUERY(row,'$.attempt'), '\"', '') AS attempt,
	REPLACE(JSON_QUERY(row,'$.bounce_date'), '\"', '') AS bounce_date,
	REPLACE(JSON_QUERY(row,'$.bounce_reason'), '\"', '') AS bounce_reason,
	REPLACE(JSON_QUERY(row,'$.bounce_status'), '\"', '') AS bounce_status,
	REPLACE(JSON_QUERY(row,'$.bounce_timestamp'), '\"', '') AS bounce_timestamp,
	REPLACE(JSON_QUERY(row,'$.category'), '\"', '') AS category,
	REPLACE(JSON_QUERY(row,'$.deferred_date'), '\"', '') AS deferred_date,
	REPLACE(JSON_QUERY(row,'$.deferred_status'), '\"', '') AS deferred_status,
	REPLACE(JSON_QUERY(row,'$.deferred_timestamp'), '\"', '') AS deferred_timestamp,
	REPLACE(JSON_QUERY(row,'$.delivered_date'), '\"', '') AS delivered_date,
	REPLACE(JSON_QUERY(row,'$.delivered_status'), '\"', '') AS delivered_status,
	REPLACE(JSON_QUERY(row,'$.delivered_timestamp'), '\"', '') AS delivered_timestamp,
	REPLACE(JSON_QUERY(row,'$.deliveredDate'), '\"', '') AS deliveredDate,
	REPLACE(JSON_QUERY(row,'$.dropped_date'), '\"', '') AS dropped_date,
	REPLACE(JSON_QUERY(row,'$.dropped_reason'), '\"', '') AS dropped_reason,
	REPLACE(JSON_QUERY(row,'$.dropped_status'), '\"', '') AS dropped_status,
	REPLACE(JSON_QUERY(row,'$.dropped_timestamp'), '\"', '') AS dropped_timestamp,
	REPLACE(JSON_QUERY(row,'$.email'), '\"', '') AS email,
	REPLACE(JSON_QUERY(row,'$.error_code'), '\"', '') AS error_code,
	REPLACE(JSON_QUERY(row,'$.error_message'), '\"', '') AS error_message,
	REPLACE(JSON_QUERY(row,'$.errorCode'), '\"', '') AS errorCode,
	REPLACE(JSON_QUERY(row,'$.errorMessage'), '\"', '') AS errorMessage,
	REPLACE(JSON_QUERY(row,'$.failedDate'), '\"', '') AS failedDate,
	REPLACE(JSON_QUERY(row,'$.fullname'), '\"', '') AS fullname,
	REPLACE(JSON_QUERY(row,'$.id'), '\"', '') AS id,
	REPLACE(JSON_QUERY(row,'$.id	'), '\"', '') AS id	,
	REPLACE(JSON_QUERY(row,'$.messageSid'), '\"', '') AS messageSid,
	REPLACE(JSON_QUERY(row,'$.notification'), '\"', '') AS notification,
	REPLACE(JSON_QUERY(row,'$.notification.body'), '\"', '') AS notification_body,
	REPLACE(JSON_QUERY(row,'$.notification.body	'), '\"', '') AS notification_body	,
	REPLACE(JSON_QUERY(row,'$.notification.time'), '\"', '') AS notification_time,
	REPLACE(JSON_QUERY(row,'$.notification.title'), '\"', '') AS notification_title,
	REPLACE(JSON_QUERY(row,'$.notificationId'), '\"', '') AS notificationId,
	REPLACE(JSON_QUERY(row,'$.notificationSpecificationsID'), '\"', '') AS notificationSpecificationsID,
	REPLACE(JSON_QUERY(row,'$.notificationType'), '\"', '') AS notificationType,
	REPLACE(JSON_QUERY(row,'$.open_date'), '\"', '') AS open_date,
	REPLACE(JSON_QUERY(row,'$.open_status'), '\"', '') AS open_status,
	REPLACE(JSON_QUERY(row,'$.open_timestamp'), '\"', '') AS open_timestamp,
	REPLACE(JSON_QUERY(row,'$.phone'), '\"', '') AS phone,
	REPLACE(JSON_QUERY(row,'$.processed_date'), '\"', '') AS processed_date,
	REPLACE(JSON_QUERY(row,'$.processed_status'), '\"', '') AS processed_status,
	REPLACE(JSON_QUERY(row,'$.processed_timestamp'), '\"', '') AS processed_timestamp,
	REPLACE(JSON_QUERY(row,'$.queued_date'), '\"', '') AS queued_date,
	REPLACE(JSON_QUERY(row,'$.read'), '\"', '') AS read,
	REPLACE(JSON_QUERY(row,'$.read	'), '\"', '') AS read	,
	REPLACE(JSON_QUERY(row,'$.sent_date'), '\"', '') AS sent_date,
	REPLACE(JSON_QUERY(row,'$.status'), '\"', '') AS status,
	REPLACE(JSON_QUERY(row,'$.token'), '\"', '') AS token,
	REPLACE(JSON_QUERY(row,'$.token	'), '\"', '') AS token	,
	REPLACE(JSON_QUERY(row,'$.uid'), '\"', '') AS uid,
	REPLACE(JSON_QUERY(row,'$.uid	'), '\"', '') AS uid	,
	REPLACE(JSON_QUERY(row,'$.undeliveredDate'), '\"', '') AS undeliveredDate,
	REPLACE(JSON_QUERY(row,'$.unsubscribe_date'), '\"', '') AS unsubscribe_date,
	REPLACE(JSON_QUERY(row,'$.unsubscribe_status'), '\"', '') AS unsubscribe_status,
	REPLACE(JSON_QUERY(row,'$.unsubscribe_timestamp'), '\"', '') AS unsubscribe_timestamp -- selects
    FROM
      json_data,
      UNNEST(body) AS ROW )
  SELECT
    *
  FROM
    flattened_data 
   -- order statement
  );
    

