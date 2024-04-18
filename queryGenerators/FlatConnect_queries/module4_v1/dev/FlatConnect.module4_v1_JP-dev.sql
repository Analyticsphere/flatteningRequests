-- FYI: This query was automatically generated using R code written by Jake 
-- Peters. The code references a the source table schema and a configuration
-- file specifying various parameters. This style of query was developed by 
-- Warren Lu to flatten the Connect4Cancer datasets in 2022. See GitHub repo
-- for further documentation.
-- 
-- Repository: https://github.com/Analyticsphere/flatteningRequests
-- Relavent functions: generate_flattening_query.R
-- 
-- source_table: nih-nci-dceg-connect-dev.Connect.module4_v1
-- destination table: nih-nci-dceg-connect-dev.FlatConnect.module4_v1_JP -- notes
    
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
    "D_728155643.D_728155643": [
        678602069
    ],
    "D_831438612.D_831438612": [
        531671855
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
  `nih-nci-dceg-connect-dev.FlatConnect.module4_v1_JP` -- destination_table
  OPTIONS (description="Source table: Connect.module4_v1; Scheduled Query: FlatConnect.module4_v1_JP; GitHub: https://github.com/Analyticsphere/flatteningRequests/tree/main/queryGenerators/FlatConnect_queries/module4_v1; Team: Analytics; Maintainer: Jake Peters; Super Users: Kelsey; Notes: This table is a flattened version of Connect.module4_v1.") -- table_description
  AS (
  WITH
    json_data AS (
    SELECT
      [handleRow(TO_JSON_STRING(input_row))] AS body
    FROM
      `nih-nci-dceg-connect-dev.Connect.module4_v1` AS input_row -- source_table
    WHERE Connect_ID IS NOT NULL), -- filter_statement
    flattened_data AS (
    SELECT
      	REPLACE(JSON_QUERY(row,'$.Connect_ID'), '\"', '') AS Connect_ID,
	REPLACE(JSON_QUERY(row,'$.D_121490150.D_195068098'), '\"', '') AS D_121490150_D_195068098,
	REPLACE(JSON_QUERY(row,'$.D_121490150.D_202784871'), '\"', '') AS D_121490150_D_202784871,
	REPLACE(JSON_QUERY(row,'$.D_121490150.D_255248624'), '\"', '') AS D_121490150_D_255248624,
	REPLACE(JSON_QUERY(row,'$.D_121490150.D_303500597'), '\"', '') AS D_121490150_D_303500597,
	REPLACE(JSON_QUERY(row,'$.D_121490150.D_469838242'), '\"', '') AS D_121490150_D_469838242,
	REPLACE(JSON_QUERY(row,'$.D_121490150.D_831127170'), '\"', '') AS D_121490150_D_831127170,
	REPLACE(JSON_QUERY(row,'$.D_121490150.D_945532934'), '\"', '') AS D_121490150_D_945532934,
	REPLACE(JSON_QUERY(row,'$.D_138116092.D_138116092'), '\"', '') AS D_138116092_D_138116092,
	REPLACE(JSON_QUERY(row,'$.d_180097038'), '\"', '') AS d_180097038,
	REPLACE(JSON_QUERY(row,'$.D_191221569.D_191221569'), '\"', '') AS D_191221569_D_191221569,
	REPLACE(JSON_QUERY(row,'$.D_277348878'), '\"', '') AS D_277348878,
	REPLACE(JSON_QUERY(row,'$.D_285545471.D_285545471'), '\"', '') AS D_285545471_D_285545471,
	REPLACE(JSON_QUERY(row,'$.D_290969423'), '\"', '') AS D_290969423,
	REPLACE(JSON_QUERY(row,'$.D_291959537'), '\"', '') AS D_291959537,
	REPLACE(JSON_QUERY(row,'$.D_302947409'), '\"', '') AS D_302947409,
	REPLACE(JSON_QUERY(row,'$.D_342677547'), '\"', '') AS D_342677547,
	REPLACE(JSON_QUERY(row,'$.D_374639590'), '\"', '') AS D_374639590,
	REPLACE(JSON_QUERY(row,'$.D_539909957.D_539909957'), '\"', '') AS D_539909957_D_539909957,
	REPLACE(JSON_QUERY(row,'$.D_590222838'), '\"', '') AS D_590222838,
	REPLACE(JSON_QUERY(row,'$.D_665593888.D_665593888'), '\"', '') AS D_665593888_D_665593888,
	REPLACE(JSON_QUERY(row,'$.D_694113343'), '\"', '') AS D_694113343,
	REPLACE(JSON_QUERY(row,'$.D_710449106'), '\"', '') AS D_710449106,
	REPLACE(JSON_QUERY(row,'$.D_714365367.D_714365367'), '\"', '') AS D_714365367_D_714365367,
	REPLACE(JSON_QUERY(row,'$.D_720221117'), '\"', '') AS D_720221117,
	REPLACE(JSON_QUERY(row,'$.D_728155643.D_728155643.D_678602069'), '\"', '') AS D_728155643_D_728155643_D_678602069,
	REPLACE(JSON_QUERY(row,'$.D_752015272.D_752015272'), '\"', '') AS D_752015272_D_752015272,
	REPLACE(JSON_QUERY(row,'$.D_774087638'), '\"', '') AS D_774087638,
	REPLACE(JSON_QUERY(row,'$.D_831438612.D_831438612.D_531671855'), '\"', '') AS D_831438612_D_831438612_D_531671855,
	REPLACE(JSON_QUERY(row,'$.D_869829679'), '\"', '') AS D_869829679,
	REPLACE(JSON_QUERY(row,'$.D_920576363.D_500100435'), '\"', '') AS D_920576363_D_500100435,
	REPLACE(JSON_QUERY(row,'$.D_920576363.D_725583683'), '\"', '') AS D_920576363_D_725583683,
	REPLACE(JSON_QUERY(row,'$.D_920576363.D_917021073'), '\"', '') AS D_920576363_D_917021073,
	REPLACE(JSON_QUERY(row,'$.D_920576363.D_970000442'), '\"', '') AS D_920576363_D_970000442,
	REPLACE(JSON_QUERY(row,'$.D_958419506.D_958419506'), '\"', '') AS D_958419506_D_958419506,
	REPLACE(JSON_QUERY(row,'$.D_980007418'), '\"', '') AS D_980007418,
	REPLACE(JSON_QUERY(row,'$.D_995620555'), '\"', '') AS D_995620555,
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
    

