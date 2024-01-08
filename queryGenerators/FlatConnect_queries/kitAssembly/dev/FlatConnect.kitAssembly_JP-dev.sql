-- FYI: This query was automatically generated using R code written by Jake 
-- Peters. The code references a the source table schema and a configuration
-- file specifying various parameters. This style of query was developed by 
-- Warren Lu to flatten the Connect4Cancer datasets in 2022. See GitHub repo
-- for further documentation.
-- 
-- Repository: https://github.com/Analyticsphere/flatteningRequests
-- Relavent functions: generate_flattening_query.R
-- 
-- source_table: nih-nci-dceg-connect-dev.Connect.kitAssembly
-- destination table: FlatConnect.kitAssembly_JP -- notes
    
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
  FlatConnect.kitAssembly_JP -- destination_table
  OPTIONS (description="Source table: Connect.kitAssembly; Scheduled Query: FlatConnect.kitAssembly_JP; GitHub: https://github.com/Analyticsphere/flatteningRequests/tree/main/queryGenerators/FlatConnect_queries/kitAssembly; Team: Analytics; Maintainer: Jake Peters; Super Users: Kelsey, Jing; Notes: This table is a flattened version of Connect.kitAssembly table.") -- table_description
  AS (
  WITH
    json_data AS (
    SELECT
      Connect_ID,
      [handleRow(TO_JSON_STRING(input_row))] AS body
    FROM
      `nih-nci-dceg-connect-dev.Connect.kitAssembly` AS input_row -- source_table
    WHERE Connect_ID IS NOT NULL), -- filter_statement
    flattened_data AS (
    SELECT
      	REPLACE(JSON_QUERY(row,'$.Connect_ID'), '\"', '') AS Connect_ID,
	REPLACE(JSON_QUERY(row,'$.d_101010101'), '\"', '') AS d_101010101,
	REPLACE(JSON_QUERY(row,'$.d_137401245'), '\"', '') AS d_137401245,
	REPLACE(JSON_QUERY(row,'$.d_194252513'), '\"', '') AS d_194252513,
	REPLACE(JSON_QUERY(row,'$.d_221592017'), '\"', '') AS d_221592017,
	REPLACE(JSON_QUERY(row,'$.d_259846815'), '\"', '') AS d_259846815,
	REPLACE(JSON_QUERY(row,'$.d_341636034'), '\"', '') AS d_341636034,
	REPLACE(JSON_QUERY(row,'$.d_379252329'), '\"', '') AS d_379252329,
	REPLACE(JSON_QUERY(row,'$.d_418571751'), '\"', '') AS d_418571751,
	REPLACE(JSON_QUERY(row,'$.d_531858099'), '\"', '') AS d_531858099,
	REPLACE(JSON_QUERY(row,'$.d_633640710.d_100618603'), '\"', '') AS d_633640710_d_100618603,
	REPLACE(JSON_QUERY(row,'$.d_633640710.d_205954477'), '\"', '') AS d_633640710_d_205954477,
	REPLACE(JSON_QUERY(row,'$.d_633640710.d_289239334'), '\"', '') AS d_633640710_d_289239334,
	REPLACE(JSON_QUERY(row,'$.d_633640710.d_427719697'), '\"', '') AS d_633640710_d_427719697,
	REPLACE(JSON_QUERY(row,'$.d_633640710.d_541085383'), '\"', '') AS d_633640710_d_541085383,
	REPLACE(JSON_QUERY(row,'$.d_633640710.d_545319575'), '\"', '') AS d_633640710_d_545319575,
	REPLACE(JSON_QUERY(row,'$.d_633640710.d_938338155'), '\"', '') AS d_633640710_d_938338155,
	REPLACE(JSON_QUERY(row,'$.d_633640710.d_950521660'), '\"', '') AS d_633640710_d_950521660,
	REPLACE(JSON_QUERY(row,'$.d_633640710.d_992420392'), '\"', '') AS d_633640710_d_992420392,
	REPLACE(JSON_QUERY(row,'$.d_661940160'), '\"', '') AS d_661940160,
	REPLACE(JSON_QUERY(row,'$.d_687158491'), '\"', '') AS d_687158491,
	REPLACE(JSON_QUERY(row,'$.d_690210658'), '\"', '') AS d_690210658,
	REPLACE(JSON_QUERY(row,'$.d_755095663'), '\"', '') AS d_755095663,
	REPLACE(JSON_QUERY(row,'$.d_786397882'), '\"', '') AS d_786397882,
	REPLACE(JSON_QUERY(row,'$.d_826941471'), '\"', '') AS d_826941471,
	REPLACE(JSON_QUERY(row,'$.d_972453354'), '\"', '') AS d_972453354 -- selects
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
    

