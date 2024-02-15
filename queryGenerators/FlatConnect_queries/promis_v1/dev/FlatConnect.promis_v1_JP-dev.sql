-- FYI: This query was automatically generated using R code written by Jake 
-- Peters. The code references a the source table schema and a configuration
-- file specifying various parameters. This style of query was developed by 
-- Warren Lu to flatten the Connect4Cancer datasets in 2022. See GitHub repo
-- for further documentation.
-- 
-- Repository: https://github.com/Analyticsphere/flatteningRequests
-- Relavent functions: generate_flattening_query.R
-- 
-- source_table: nih-nci-dceg-connect-dev.Connect.promis_v1
-- destination table: nih-nci-dceg-connect-dev.FlatConnect.promis_v1_JP -- notes
    
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
  `nih-nci-dceg-connect-dev.FlatConnect.promis_v1_JP` -- destination_table
  OPTIONS (description="Source table: Connect.promis_v1; Scheduled Query: FlatConnect.promis_v1_JP; GitHub: https://github.com/Analyticsphere/flatteningRequests/tree/main/queryGenerators/FlatConnect_queries/promis_v1; Team: Analytics; Maintainer: Jake Peters; Super Users: Kelsey, Jing; Notes: This table is a flattened version of Connect.promis_v1.") -- table_description
  AS (
  WITH
    json_data AS (
    SELECT
      Connect_ID,
      [handleRow(TO_JSON_STRING(input_row))] AS body
    FROM
      `nih-nci-dceg-connect-dev.Connect.promis_v1` AS input_row -- source_table
    WHERE Connect_ID IS NOT NULL), -- filter_statement
    flattened_data AS (
    SELECT
      	REPLACE(JSON_QUERY(row,'$.Connect_ID'), '\"', '') AS Connect_ID,
	REPLACE(JSON_QUERY(row,'$.D_284353934.D_559540891'), '\"', '') AS D_284353934_D_559540891,
	REPLACE(JSON_QUERY(row,'$.D_284353934.D_780866928'), '\"', '') AS D_284353934_D_780866928,
	REPLACE(JSON_QUERY(row,'$.D_284353934.D_783201540'), '\"', '') AS D_284353934_D_783201540,
	REPLACE(JSON_QUERY(row,'$.D_284353934.D_917425212'), '\"', '') AS D_284353934_D_917425212,
	REPLACE(JSON_QUERY(row,'$.D_404946076.D_179665441'), '\"', '') AS D_404946076_D_179665441,
	REPLACE(JSON_QUERY(row,'$.D_404946076.D_195292223'), '\"', '') AS D_404946076_D_195292223,
	REPLACE(JSON_QUERY(row,'$.D_404946076.D_429680247'), '\"', '') AS D_404946076_D_429680247,
	REPLACE(JSON_QUERY(row,'$.D_404946076.D_829976839'), '\"', '') AS D_404946076_D_829976839,
	REPLACE(JSON_QUERY(row,'$.d_490327747'), '\"', '') AS d_490327747,
	REPLACE(JSON_QUERY(row,'$.D_715048033.D_168582270'), '\"', '') AS D_715048033_D_168582270,
	REPLACE(JSON_QUERY(row,'$.D_715048033.D_361835532'), '\"', '') AS D_715048033_D_361835532,
	REPLACE(JSON_QUERY(row,'$.D_715048033.D_468039454'), '\"', '') AS D_715048033_D_468039454,
	REPLACE(JSON_QUERY(row,'$.D_715048033.D_803322918'), '\"', '') AS D_715048033_D_803322918,
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
    

