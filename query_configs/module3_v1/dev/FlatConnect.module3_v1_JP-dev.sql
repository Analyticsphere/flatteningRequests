-- FYI: This query was automatically generated using R code written by Jake 
-- Peters. The code references a the source table schema and a configuration
-- file specifying various parameters. This style of query was developed by 
-- Warren Lu to flatten the Connect4Cancer datasets in 2022. See GitHub repo
-- for further documentation.
-- 
-- Repository: https://github.com/Analyticsphere/flatteningRequests
-- Relavent functions: generate_flattening_query.R
-- 
-- source_table: nih-nci-dceg-connect-dev.Connect.module3_v1
-- destination table: nih-nci-dceg-connect-dev.FlatConnect.module3_v1_JP -- notes
    
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
  
  const arraysToBeFlattened= {
    "D_830608495": [
        535003378
    ],
    "D_947205597": [
        535003378
    ]
}
 
  
  function handleRowJS(row) {
    for (let arrPath of Object.keys(arraysToBeFlattened)) {
      let currObj = {};
      let inputConceptIdList = getNestedObjectValue(row, arrPath);
      if (!inputConceptIdList || inputConceptIdList.length === 0) continue;
      inputConceptIdList = inputConceptIdList.map(v => +v);
      for (let cid of arraysToBeFlattened[arrPath]) {
        if (inputConceptIdList.indexOf(cid) >= 0) {
          currObj["D_" + cid] = 1;
        } else currObj["D_" + cid] = 0;
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
  `nih-nci-dceg-connect-dev.FlatConnect.module3_v1_JP` -- destination_table
  OPTIONS (description="Source table: Connect.module4_v1; Scheduled Query: FlatConnect.module3_v1_JP; GitHub: https://github.com/Analyticsphere/flatteningRequests/tree/main/queryGenerators/FlatConnect_queries/module3_v1; Team: Analytics; Maintainer: Jake Peters; Super Users: Kelsey; Notes: This table is a flattened version of Connect.module3_v1.") -- table_description
  AS (
  WITH
    json_data AS (
    SELECT
      [handleRow(TO_JSON_STRING(input_row))] AS body
    FROM
      `nih-nci-dceg-connect-dev.Connect.module3_v1` AS input_row -- source_table
    WHERE Connect_ID IS NOT NULL), -- filter_statement
    flattened_data AS (
    SELECT
      	REPLACE(JSON_QUERY(row,'$.Connect_ID'), '\"', '') AS Connect_ID,
	REPLACE(JSON_QUERY(row,'$.D_146319128'), '\"', '') AS D_146319128,
	REPLACE(JSON_QUERY(row,'$.D_252017075.D_106272739'), '\"', '') AS D_252017075_D_106272739,
	REPLACE(JSON_QUERY(row,'$.D_252017075.D_257564920'), '\"', '') AS D_252017075_D_257564920,
	REPLACE(JSON_QUERY(row,'$.D_252017075.D_359443374'), '\"', '') AS D_252017075_D_359443374,
	REPLACE(JSON_QUERY(row,'$.D_252017075.D_670036093'), '\"', '') AS D_252017075_D_670036093,
	REPLACE(JSON_QUERY(row,'$.D_337810964'), '\"', '') AS D_337810964,
	REPLACE(JSON_QUERY(row,'$.D_345457702'), '\"', '') AS D_345457702,
	REPLACE(JSON_QUERY(row,'$.D_352986465'), '\"', '') AS D_352986465,
	REPLACE(JSON_QUERY(row,'$.D_384562783'), '\"', '') AS D_384562783,
	REPLACE(JSON_QUERY(row,'$.D_394410256'), '\"', '') AS D_394410256,
	REPLACE(JSON_QUERY(row,'$.D_429228540'), '\"', '') AS D_429228540,
	REPLACE(JSON_QUERY(row,'$.D_448968121'), '\"', '') AS D_448968121,
	REPLACE(JSON_QUERY(row,'$.D_467751479'), '\"', '') AS D_467751479,
	REPLACE(JSON_QUERY(row,'$.D_524971659'), '\"', '') AS D_524971659,
	REPLACE(JSON_QUERY(row,'$.D_525152373'), '\"', '') AS D_525152373,
	REPLACE(JSON_QUERY(row,'$.D_662762705'), '\"', '') AS D_662762705,
	REPLACE(JSON_QUERY(row,'$.D_711858159'), '\"', '') AS D_711858159,
	REPLACE(JSON_QUERY(row,'$.D_738219521.D_575237218'), '\"', '') AS D_738219521_D_575237218,
	REPLACE(JSON_QUERY(row,'$.D_738219521.D_666840938'), '\"', '') AS D_738219521_D_666840938,
	REPLACE(JSON_QUERY(row,'$.D_738219521.D_976863738'), '\"', '') AS D_738219521_D_976863738,
	REPLACE(JSON_QUERY(row,'$.D_795761911'), '\"', '') AS D_795761911,
	REPLACE(JSON_QUERY(row,'$.D_830608495.D_535003378'), '\"', '') AS D_830608495_D_535003378,
	REPLACE(JSON_QUERY(row,'$.D_857219333'), '\"', '') AS D_857219333,
	REPLACE(JSON_QUERY(row,'$.D_858858333'), '\"', '') AS D_858858333,
	REPLACE(JSON_QUERY(row,'$.D_863593410'), '\"', '') AS D_863593410,
	REPLACE(JSON_QUERY(row,'$.D_910501158'), '\"', '') AS D_910501158,
	REPLACE(JSON_QUERY(row,'$.D_947205597.D_535003378'), '\"', '') AS D_947205597_D_535003378,
	REPLACE(JSON_QUERY(row,'$.d_951528844'), '\"', '') AS d_951528844,
	REPLACE(JSON_QUERY(row,'$.uid'), '\"', '') AS uid -- selects
    FROM
      json_data,
      UNNEST(body) AS ROW )
  SELECT
    *
  FROM
    flattened_data 
   -- order statement
  );
    

