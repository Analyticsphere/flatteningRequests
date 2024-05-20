-- FYI: This query was automatically generated using R code written by Jake 
-- Peters. The code references a the source table schema and a configuration
-- file specifying various parameters. This style of query was developed by 
-- Warren Lu to flatten the Connect4Cancer datasets in 2022. See GitHub repo
-- for further documentation.
-- 
-- Repository: https://github.com/Analyticsphere/flatteningRequests
-- Relavent functions: generate_flattening_query.R
-- 
-- source_table: nih-nci-dceg-connect-dev.Connect.mouthwash_v1
-- destination table: nih-nci-dceg-connect-dev.FlatConnect.mouthwash_v1_JP -- notes
    
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
    "D_406270109": [
        259744087,
        404389800,
        463302301,
        523660949,
        762727133,
        877842367,
        886771318,
        950773275
    ],
    "D_479143504": [
        165596977,
        184513726,
        203919683,
        390351864,
        578402172,
        807884576
    ],
    "D_542661394": [
        167336253,
        178420302,
        181769837,
        215662651,
        329536041,
        365685000,
        371726159,
        656498939
    ],
    "D_899251483": [
        452438775,
        551489317,
        812107266,
        886864375
    ],
    "D_899251483_V2": [
        452438775,
        551489317,
        812107266,
        886864375
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
  `nih-nci-dceg-connect-dev.FlatConnect.mouthwash_v1_JP` -- destination_table
  OPTIONS (description="Source table: Connect.mouthwash_v1; Scheduled Query: FlatConnect.mouthwash_v1_JP; GitHub: https://github.com/Analyticsphere/flatteningRequests/tree/main/queryGenerators/FlatConnect_queries/mouthwash_v1; Team: Analytics; Maintainer: Jake Peters; Super Users: Kelsey; Notes: This table is a flattened version of Connect.mouthwash_v1.") -- table_description
  AS (
  WITH
    json_data AS (
    SELECT
      [handleRow(TO_JSON_STRING(input_row))] AS body
    FROM
      `nih-nci-dceg-connect-dev.Connect.mouthwash_v1` AS input_row -- source_table
    WHERE Connect_ID IS NOT NULL), -- filter_statement
    flattened_data AS (
    SELECT
      	REPLACE(JSON_QUERY(row,'$.'), '\"', '') AS ,
	REPLACE(JSON_QUERY(row,'$.D_406270109.d_259744087'), '\"', '') AS D_406270109_d_259744087,
	REPLACE(JSON_QUERY(row,'$.D_406270109.d_404389800'), '\"', '') AS D_406270109_d_404389800,
	REPLACE(JSON_QUERY(row,'$.D_406270109.d_463302301'), '\"', '') AS D_406270109_d_463302301,
	REPLACE(JSON_QUERY(row,'$.D_406270109.d_523660949'), '\"', '') AS D_406270109_d_523660949,
	REPLACE(JSON_QUERY(row,'$.D_406270109.d_762727133'), '\"', '') AS D_406270109_d_762727133,
	REPLACE(JSON_QUERY(row,'$.D_406270109.d_877842367'), '\"', '') AS D_406270109_d_877842367,
	REPLACE(JSON_QUERY(row,'$.D_406270109.d_886771318'), '\"', '') AS D_406270109_d_886771318,
	REPLACE(JSON_QUERY(row,'$.D_406270109.d_950773275'), '\"', '') AS D_406270109_d_950773275,
	REPLACE(JSON_QUERY(row,'$.D_479143504.d_165596977'), '\"', '') AS D_479143504_d_165596977,
	REPLACE(JSON_QUERY(row,'$.D_479143504.d_184513726'), '\"', '') AS D_479143504_d_184513726,
	REPLACE(JSON_QUERY(row,'$.D_479143504.d_203919683'), '\"', '') AS D_479143504_d_203919683,
	REPLACE(JSON_QUERY(row,'$.D_479143504.d_390351864'), '\"', '') AS D_479143504_d_390351864,
	REPLACE(JSON_QUERY(row,'$.D_479143504.d_578402172'), '\"', '') AS D_479143504_d_578402172,
	REPLACE(JSON_QUERY(row,'$.D_479143504.d_807884576'), '\"', '') AS D_479143504_d_807884576,
	REPLACE(JSON_QUERY(row,'$.D_542661394.d_167336253'), '\"', '') AS D_542661394_d_167336253,
	REPLACE(JSON_QUERY(row,'$.D_542661394.d_178420302'), '\"', '') AS D_542661394_d_178420302,
	REPLACE(JSON_QUERY(row,'$.D_542661394.d_181769837'), '\"', '') AS D_542661394_d_181769837,
	REPLACE(JSON_QUERY(row,'$.D_542661394.d_215662651'), '\"', '') AS D_542661394_d_215662651,
	REPLACE(JSON_QUERY(row,'$.D_542661394.d_329536041'), '\"', '') AS D_542661394_d_329536041,
	REPLACE(JSON_QUERY(row,'$.D_542661394.d_365685000'), '\"', '') AS D_542661394_d_365685000,
	REPLACE(JSON_QUERY(row,'$.D_542661394.d_371726159'), '\"', '') AS D_542661394_d_371726159,
	REPLACE(JSON_QUERY(row,'$.D_542661394.d_656498939'), '\"', '') AS D_542661394_d_656498939,
	REPLACE(JSON_QUERY(row,'$.D_899251483_V2.d_452438775'), '\"', '') AS D_899251483_V2_d_452438775,
	REPLACE(JSON_QUERY(row,'$.D_899251483_V2.d_551489317'), '\"', '') AS D_899251483_V2_d_551489317,
	REPLACE(JSON_QUERY(row,'$.D_899251483_V2.d_812107266'), '\"', '') AS D_899251483_V2_d_812107266,
	REPLACE(JSON_QUERY(row,'$.D_899251483_V2.d_886864375'), '\"', '') AS D_899251483_V2_d_886864375,
	REPLACE(JSON_QUERY(row,'$.D_899251483.d_452438775'), '\"', '') AS D_899251483_d_452438775,
	REPLACE(JSON_QUERY(row,'$.D_899251483.d_551489317'), '\"', '') AS D_899251483_d_551489317,
	REPLACE(JSON_QUERY(row,'$.D_899251483.d_812107266'), '\"', '') AS D_899251483_d_812107266,
	REPLACE(JSON_QUERY(row,'$.D_899251483.d_886864375'), '\"', '') AS D_899251483_d_886864375 -- selects
    FROM
      json_data,
      UNNEST(body) AS ROW )
  SELECT
    *
  FROM
    flattened_data 
   -- order statement
  );
    

