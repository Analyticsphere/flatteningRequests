-- FYI: This query was automatically generated using R code written by Jake 
-- Peters. The code references a the source table schema and a configuration
-- file specifying various parameters. This style of query was developed by 
-- Warren Lu to flatten the Connect4Cancer datasets in 2022. See GitHub repo
-- for further documentation.
-- 
-- Repository: https://github.com/Analyticsphere/flatteningRequests
-- Relavent functions: generate_flattening_query.R
-- 
-- source_table: nih-nci-dceg-connect-dev.Connect.bioSurvey_v1
-- destination table: nih-nci-dceg-connect-dev.FlatConnect.bioSurvey_v1_JP -- notes
    
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
    "D_470484596": [
        535003378
    ],
    "D_479143504": [
        203919683
    ],
    "D_542661394": [
        167336253
    ],
    "D_899251483_V2": [
        551489317
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
  `nih-nci-dceg-connect-dev.FlatConnect.bioSurvey_v1_JP` -- destination_table
  OPTIONS (description="Source table: Connect.bioSurvey_v1; Scheduled Query: FlatConnect.bioSurvey_v1_JP; GitHub: https://github.com/Analyticsphere/flatteningRequests/tree/main/queryGenerators/FlatConnect_queries/bioSurvey_v1; Team: Analytics; Maintainer: Jake Peters; Super Users: Kelsey; Notes: This table is a flattened version of Connect.bioSurvey_v1.") -- table_description
  AS (
  WITH
    json_data AS (
    SELECT
      [handleRow(TO_JSON_STRING(input_row))] AS body
    FROM
      `nih-nci-dceg-connect-dev.Connect.bioSurvey_v1` AS input_row -- source_table
    WHERE Connect_ID IS NOT NULL), -- filter_statement
    flattened_data AS (
    SELECT
      	REPLACE(JSON_QUERY(row,'$.Connect_ID'), '\"', '') AS Connect_ID,
	REPLACE(JSON_QUERY(row,'$.D_112151599'), '\"', '') AS D_112151599,
	REPLACE(JSON_QUERY(row,'$.D_130311122'), '\"', '') AS D_130311122,
	REPLACE(JSON_QUERY(row,'$.D_191057574_V2'), '\"', '') AS D_191057574_V2,
	REPLACE(JSON_QUERY(row,'$.d_227848247'), '\"', '') AS d_227848247,
	REPLACE(JSON_QUERY(row,'$.D_234714655'), '\"', '') AS D_234714655,
	REPLACE(JSON_QUERY(row,'$.D_294886836'), '\"', '') AS D_294886836,
	REPLACE(JSON_QUERY(row,'$.D_299417266_V2'), '\"', '') AS D_299417266_V2,
	REPLACE(JSON_QUERY(row,'$.D_339570897'), '\"', '') AS D_339570897,
	REPLACE(JSON_QUERY(row,'$.D_350251057'), '\"', '') AS D_350251057,
	REPLACE(JSON_QUERY(row,'$.D_380603392'), '\"', '') AS D_380603392,
	REPLACE(JSON_QUERY(row,'$.D_470484596.D_535003378'), '\"', '') AS D_470484596_D_535003378,
	REPLACE(JSON_QUERY(row,'$.D_479143504.D_203919683'), '\"', '') AS D_479143504_D_203919683,
	REPLACE(JSON_QUERY(row,'$.D_487532606.D_520755310'), '\"', '') AS D_487532606_D_520755310,
	REPLACE(JSON_QUERY(row,'$.D_487532606.D_619765650'), '\"', '') AS D_487532606_D_619765650,
	REPLACE(JSON_QUERY(row,'$.D_487532606.D_839329467'), '\"', '') AS D_487532606_D_839329467,
	REPLACE(JSON_QUERY(row,'$.D_498984275'), '\"', '') AS D_498984275,
	REPLACE(JSON_QUERY(row,'$.D_518916981'), '\"', '') AS D_518916981,
	REPLACE(JSON_QUERY(row,'$.D_522008539'), '\"', '') AS D_522008539,
	REPLACE(JSON_QUERY(row,'$.D_542661394.D_167336253'), '\"', '') AS D_542661394_D_167336253,
	REPLACE(JSON_QUERY(row,'$.D_563539159'), '\"', '') AS D_563539159,
	REPLACE(JSON_QUERY(row,'$.D_642044281'), '\"', '') AS D_642044281,
	REPLACE(JSON_QUERY(row,'$.D_644459734'), '\"', '') AS D_644459734,
	REPLACE(JSON_QUERY(row,'$.D_689861450_V2'), '\"', '') AS D_689861450_V2,
	REPLACE(JSON_QUERY(row,'$.D_736028153'), '\"', '') AS D_736028153,
	REPLACE(JSON_QUERY(row,'$.D_736393021'), '\"', '') AS D_736393021,
	REPLACE(JSON_QUERY(row,'$.D_792134396'), '\"', '') AS D_792134396,
	REPLACE(JSON_QUERY(row,'$.D_798452445'), '\"', '') AS D_798452445,
	REPLACE(JSON_QUERY(row,'$.D_800703566'), '\"', '') AS D_800703566,
	REPLACE(JSON_QUERY(row,'$.D_867307558'), '\"', '') AS D_867307558,
	REPLACE(JSON_QUERY(row,'$.D_875535246'), '\"', '') AS D_875535246,
	REPLACE(JSON_QUERY(row,'$.D_877878167'), '\"', '') AS D_877878167,
	REPLACE(JSON_QUERY(row,'$.D_899251483_V2.D_551489317'), '\"', '') AS D_899251483_V2_D_551489317,
	REPLACE(JSON_QUERY(row,'$.D_983043203'), '\"', '') AS D_983043203,
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
    

