-- FYI: This query was automatically generated using R code written by Jake 
-- Peters. The code references a the source table schema and a configuration
-- file specifying various parameters. This style of query was developed by 
-- Warren Lu to flatten the Connect4Cancer datasets in 2022. See GitHub repo
-- for further documentation.
-- 
-- Repository: https://github.com/Analyticsphere/flatteningRequests
-- Relavent functions: generate_flattening_query.R
-- 
-- source_table: nih-nci-dceg-connect-dev.Connect.clinicalBioSurvey_v1
-- destination table: nih-nci-dceg-connect-dev.FlatConnect.clinicalBioSurvey_v1_JP -- notes
    
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
        406943303,
        535003378,
        756774083,
        811126581,
        955154600
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
  `nih-nci-dceg-connect-dev.FlatConnect.clinicalBioSurvey_v1_JP` -- destination_table
  OPTIONS (description="Source table: Connect.clinicalBioSurvey_v1; Scheduled Query: FlatConnect.clinicalBioSurvey_v1_JP; GitHub: https://github.com/Analyticsphere/flatteningRequests/tree/main/queryGenerators/FlatConnect_queries/clinicalBioSurvey_v1; Team: Analytics; Maintainer: Jake Peters; Super Users: Kelsey; Notes: This table is a flattened version of Connect.clinicalBioSurvey_v1.") -- table_description
  AS (
  WITH
    json_data AS (
    SELECT
      [handleRow(TO_JSON_STRING(input_row))] AS body
    FROM
      `nih-nci-dceg-connect-dev.Connect.clinicalBioSurvey_v1` AS input_row -- source_table
    WHERE Connect_ID IS NOT NULL), -- filter_statement
    flattened_data AS (
    SELECT
      	REPLACE(JSON_QUERY(row,'$.Connect_ID'), '\"', '') AS Connect_ID,
	REPLACE(JSON_QUERY(row,'$.D_112151599'), '\"', '') AS D_112151599,
	REPLACE(JSON_QUERY(row,'$.D_130311122'), '\"', '') AS D_130311122,
	REPLACE(JSON_QUERY(row,'$.D_191057574_V2'), '\"', '') AS D_191057574_V2,
	REPLACE(JSON_QUERY(row,'$.d_220355297'), '\"', '') AS d_220355297,
	REPLACE(JSON_QUERY(row,'$.D_234714655'), '\"', '') AS D_234714655,
	REPLACE(JSON_QUERY(row,'$.D_299417266_V2'), '\"', '') AS D_299417266_V2,
	REPLACE(JSON_QUERY(row,'$.D_380603392'), '\"', '') AS D_380603392,
	REPLACE(JSON_QUERY(row,'$.D_470484596.D_406943303'), '\"', '') AS D_470484596_D_406943303,
	REPLACE(JSON_QUERY(row,'$.D_470484596.D_535003378'), '\"', '') AS D_470484596_D_535003378,
	REPLACE(JSON_QUERY(row,'$.D_470484596.D_756774083'), '\"', '') AS D_470484596_D_756774083,
	REPLACE(JSON_QUERY(row,'$.D_470484596.D_811126581'), '\"', '') AS D_470484596_D_811126581,
	REPLACE(JSON_QUERY(row,'$.D_470484596.D_955154600'), '\"', '') AS D_470484596_D_955154600,
	REPLACE(JSON_QUERY(row,'$.D_487532606.D_520755310'), '\"', '') AS D_487532606_D_520755310,
	REPLACE(JSON_QUERY(row,'$.D_487532606.D_619765650'), '\"', '') AS D_487532606_D_619765650,
	REPLACE(JSON_QUERY(row,'$.D_487532606.D_839329467'), '\"', '') AS D_487532606_D_839329467,
	REPLACE(JSON_QUERY(row,'$.D_518916981'), '\"', '') AS D_518916981,
	REPLACE(JSON_QUERY(row,'$.D_522008539'), '\"', '') AS D_522008539,
	REPLACE(JSON_QUERY(row,'$.D_563539159'), '\"', '') AS D_563539159,
	REPLACE(JSON_QUERY(row,'$.D_644459734'), '\"', '') AS D_644459734,
	REPLACE(JSON_QUERY(row,'$.D_689861450_V2'), '\"', '') AS D_689861450_V2,
	REPLACE(JSON_QUERY(row,'$.d_784119588'), '\"', '') AS d_784119588,
	REPLACE(JSON_QUERY(row,'$.D_798452445'), '\"', '') AS D_798452445,
	REPLACE(JSON_QUERY(row,'$.D_867307558'), '\"', '') AS D_867307558,
	REPLACE(JSON_QUERY(row,'$.D_875535246'), '\"', '') AS D_875535246,
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
    

