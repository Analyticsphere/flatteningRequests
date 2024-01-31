-- FYI: This query was automatically generated using R code written by Jake 
-- Peters. The code references a the source table schema and a configuration
-- file specifying various parameters. This style of query was developed by 
-- Warren Lu to flatten the Connect4Cancer datasets in 2022. See GitHub repo
-- for further documentation.
-- 
-- Repository: https://github.com/Analyticsphere/flatteningRequests
-- Relavent functions: generate_flattening_query.R
-- 
-- source_table: nih-nci-dceg-connect-stg-5519.Connect.cancerOccurrence
-- destination table: FlatConnect.cancerOccurrence_JP -- notes
    
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
          currObj["d_" + cid] = 1;
        } else currObj["d_" + cid] = 0;
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
  FlatConnect.cancerOccurrence_JP -- destination_table
  OPTIONS (description="Source table: Connect.cancerOccurrence; Scheduled Query: FlatConnect.cancerOccurrence_JP; GitHub: https://github.com/Analyticsphere/flatteningRequests/tree/main/queryGenerators/FlatConnect_queries/cancerOccurrence; Team: Analytics; Maintainer: Jake Peters; Super Users: Kelsey, Jing; Notes: This table is a flattened version of Connect.cancerOccurrence.") -- table_description
  AS (
  WITH
    json_data AS (
    SELECT
      Connect_ID,
      [handleRow(TO_JSON_STRING(input_row))] AS body
    FROM
      `nih-nci-dceg-connect-stg-5519.Connect.cancerOccurrence` AS input_row -- source_table
    WHERE Connect_ID IS NOT NULL), -- filter_statement
    flattened_data AS (
    SELECT
      	REPLACE(JSON_QUERY(row,'$.Connect_ID'), '\"', '') AS Connect_ID,
	REPLACE(JSON_QUERY(row,'$.d_345545422'), '\"', '') AS d_345545422,
	REPLACE(JSON_QUERY(row,'$.d_525972260'), '\"', '') AS d_525972260,
	REPLACE(JSON_QUERY(row,'$.d_740819233.d_149205077'), '\"', '') AS d_740819233_d_149205077,
	REPLACE(JSON_QUERY(row,'$.d_740819233.d_868006655'), '\"', '') AS d_740819233_d_868006655,
	REPLACE(JSON_QUERY(row,'$.d_793981056'), '\"', '') AS d_793981056,
	REPLACE(JSON_QUERY(row,'$.token'), '\"', '') AS token -- selects
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
    

