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
      [handleRow(TO_JSON_STRING(input_row))] AS body
    FROM
      `nih-nci-dceg-connect-dev.Connect.promis_v1` AS input_row -- source_table
    WHERE Connect_ID IS NOT NULL), -- filter_statement
    flattened_data AS (
    SELECT
      	REPLACE(JSON_QUERY(row,'$.Connect_ID'), '\"', '') AS Connect_ID,
	REPLACE(JSON_QUERY(row,'$.D_101862879'), '\"', '') AS D_101862879,
	REPLACE(JSON_QUERY(row,'$.D_129202138'), '\"', '') AS D_129202138,
	REPLACE(JSON_QUERY(row,'$.D_132343154'), '\"', '') AS D_132343154,
	REPLACE(JSON_QUERY(row,'$.D_144446277'), '\"', '') AS D_144446277,
	REPLACE(JSON_QUERY(row,'$.D_218828500'), '\"', '') AS D_218828500,
	REPLACE(JSON_QUERY(row,'$.D_230486322.D_115480943'), '\"', '') AS D_230486322_D_115480943,
	REPLACE(JSON_QUERY(row,'$.D_230486322.D_650091514'), '\"', '') AS D_230486322_D_650091514,
	REPLACE(JSON_QUERY(row,'$.D_230486322.D_960308206'), '\"', '') AS D_230486322_D_960308206,
	REPLACE(JSON_QUERY(row,'$.D_230486322.D_974618086'), '\"', '') AS D_230486322_D_974618086,
	REPLACE(JSON_QUERY(row,'$.D_261177801.D_313287837'), '\"', '') AS D_261177801_D_313287837,
	REPLACE(JSON_QUERY(row,'$.D_261177801.D_542492755'), '\"', '') AS D_261177801_D_542492755,
	REPLACE(JSON_QUERY(row,'$.D_261177801.D_598001940'), '\"', '') AS D_261177801_D_598001940,
	REPLACE(JSON_QUERY(row,'$.D_261177801.D_733106290'), '\"', '') AS D_261177801_D_733106290,
	REPLACE(JSON_QUERY(row,'$.D_281351288'), '\"', '') AS D_281351288,
	REPLACE(JSON_QUERY(row,'$.D_284353934.D_559540891'), '\"', '') AS D_284353934_D_559540891,
	REPLACE(JSON_QUERY(row,'$.D_284353934.D_780866928'), '\"', '') AS D_284353934_D_780866928,
	REPLACE(JSON_QUERY(row,'$.D_284353934.D_783201540'), '\"', '') AS D_284353934_D_783201540,
	REPLACE(JSON_QUERY(row,'$.D_284353934.D_917425212'), '\"', '') AS D_284353934_D_917425212,
	REPLACE(JSON_QUERY(row,'$.D_297016093'), '\"', '') AS D_297016093,
	REPLACE(JSON_QUERY(row,'$.D_326712049.D_311938392'), '\"', '') AS D_326712049_D_311938392,
	REPLACE(JSON_QUERY(row,'$.D_326712049.D_387564567'), '\"', '') AS D_326712049_D_387564567,
	REPLACE(JSON_QUERY(row,'$.D_326712049.D_437905191'), '\"', '') AS D_326712049_D_437905191,
	REPLACE(JSON_QUERY(row,'$.D_326712049.D_479680555'), '\"', '') AS D_326712049_D_479680555,
	REPLACE(JSON_QUERY(row,'$.D_328149278'), '\"', '') AS D_328149278,
	REPLACE(JSON_QUERY(row,'$.D_336566965.D_526006101'), '\"', '') AS D_336566965_D_526006101,
	REPLACE(JSON_QUERY(row,'$.D_336566965.D_624200915'), '\"', '') AS D_336566965_D_624200915,
	REPLACE(JSON_QUERY(row,'$.D_336566965.D_644233792'), '\"', '') AS D_336566965_D_644233792,
	REPLACE(JSON_QUERY(row,'$.D_336566965.D_992194402'), '\"', '') AS D_336566965_D_992194402,
	REPLACE(JSON_QUERY(row,'$.D_338924834'), '\"', '') AS D_338924834,
	REPLACE(JSON_QUERY(row,'$.D_370227545'), '\"', '') AS D_370227545,
	REPLACE(JSON_QUERY(row,'$.D_393186700'), '\"', '') AS D_393186700,
	REPLACE(JSON_QUERY(row,'$.D_404946076.D_179665441'), '\"', '') AS D_404946076_D_179665441,
	REPLACE(JSON_QUERY(row,'$.D_404946076.D_195292223'), '\"', '') AS D_404946076_D_195292223,
	REPLACE(JSON_QUERY(row,'$.D_404946076.D_429680247'), '\"', '') AS D_404946076_D_429680247,
	REPLACE(JSON_QUERY(row,'$.D_404946076.D_829976839'), '\"', '') AS D_404946076_D_829976839,
	REPLACE(JSON_QUERY(row,'$.D_410751656'), '\"', '') AS D_410751656,
	REPLACE(JSON_QUERY(row,'$.D_420392309.D_226478149'), '\"', '') AS D_420392309_D_226478149,
	REPLACE(JSON_QUERY(row,'$.D_420392309.D_271090432'), '\"', '') AS D_420392309_D_271090432,
	REPLACE(JSON_QUERY(row,'$.D_420392309.D_828608766'), '\"', '') AS D_420392309_D_828608766,
	REPLACE(JSON_QUERY(row,'$.D_420392309.D_886047084'), '\"', '') AS D_420392309_D_886047084,
	REPLACE(JSON_QUERY(row,'$.D_420560514.D_380975443'), '\"', '') AS D_420560514_D_380975443,
	REPLACE(JSON_QUERY(row,'$.D_420560514.D_426764225'), '\"', '') AS D_420560514_D_426764225,
	REPLACE(JSON_QUERY(row,'$.D_420560514.D_693503159'), '\"', '') AS D_420560514_D_693503159,
	REPLACE(JSON_QUERY(row,'$.D_420560514.D_754781311'), '\"', '') AS D_420560514_D_754781311,
	REPLACE(JSON_QUERY(row,'$.D_424548783'), '\"', '') AS D_424548783,
	REPLACE(JSON_QUERY(row,'$.d_490327747'), '\"', '') AS d_490327747,
	REPLACE(JSON_QUERY(row,'$.D_582985641'), '\"', '') AS D_582985641,
	REPLACE(JSON_QUERY(row,'$.D_608953994'), '\"', '') AS D_608953994,
	REPLACE(JSON_QUERY(row,'$.D_633951291'), '\"', '') AS D_633951291,
	REPLACE(JSON_QUERY(row,'$.D_697423629'), '\"', '') AS D_697423629,
	REPLACE(JSON_QUERY(row,'$.D_715048033.D_168582270'), '\"', '') AS D_715048033_D_168582270,
	REPLACE(JSON_QUERY(row,'$.D_715048033.D_361835532'), '\"', '') AS D_715048033_D_361835532,
	REPLACE(JSON_QUERY(row,'$.D_715048033.D_468039454'), '\"', '') AS D_715048033_D_468039454,
	REPLACE(JSON_QUERY(row,'$.D_715048033.D_803322918'), '\"', '') AS D_715048033_D_803322918,
	REPLACE(JSON_QUERY(row,'$.D_755066850'), '\"', '') AS D_755066850,
	REPLACE(JSON_QUERY(row,'$.D_766974306'), '\"', '') AS D_766974306,
	REPLACE(JSON_QUERY(row,'$.D_801982443'), '\"', '') AS D_801982443,
	REPLACE(JSON_QUERY(row,'$.D_907490539.D_105063268'), '\"', '') AS D_907490539_D_105063268,
	REPLACE(JSON_QUERY(row,'$.D_907490539.D_467404576'), '\"', '') AS D_907490539_D_467404576,
	REPLACE(JSON_QUERY(row,'$.D_907490539.D_658945347'), '\"', '') AS D_907490539_D_658945347,
	REPLACE(JSON_QUERY(row,'$.D_907490539.D_787436735'), '\"', '') AS D_907490539_D_787436735,
	REPLACE(JSON_QUERY(row,'$.D_990793746'), '\"', '') AS D_990793746,
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
    

