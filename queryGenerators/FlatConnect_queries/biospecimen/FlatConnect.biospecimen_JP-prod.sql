-- FYI: This query was automatically generated using R code written by Jake 
-- Peters. The code references a the source table schema and a configuration
-- file specifying various parameters. This style of query was developed by 
-- Warren Lu to flatten the Connect4Cancer datasets in 2022. See GitHub repo
-- for further documentation.
-- 
-- Repository: https://github.com/Analyticsphere/flatteningRequests
-- Relavent functions: generate_flattening_query.R
-- 
-- source_table: nih-nci-dceg-connect-prod-6d04.Connect.biospecimen
-- destination table: FlatConnect.biospecimen_JP -- notes
    
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
  FlatConnect.biospecimen_JP -- destination_table
  OPTIONS (description="Source table: Connect.biospecimen; Scheduled Query: FlatConnect.biospecimen_JP; GitHub: https://github.com/Analyticsphere/flatteningRequests/tree/main/queryGenerators/FlatConnect_queries/biospecimen; Team: Analytics; Maintainer: Jake Peters; Super Users: Kelsey, Jing; Notes: This table is a flattened version of Connect.biospecimen.") -- table_description
  AS (
  WITH
    json_data AS (
    SELECT
      Connect_ID,
      [handleRow(TO_JSON_STRING(input_row))] AS body
    FROM
      `nih-nci-dceg-connect-prod-6d04.Connect.biospecimen` AS input_row -- source_table
    WHERE Connect_ID IS NOT NULL), -- filter_statement
    flattened_data AS (
    SELECT
      	REPLACE(JSON_QUERY(row,'$.Connect_ID'), '\"', '') AS Connect_ID,
	REPLACE(JSON_QUERY(row,'$.d_143615646.d_248868659.d_283900611'), '\"', '') AS d_143615646_d_248868659_d_283900611,
	REPLACE(JSON_QUERY(row,'$.d_143615646.d_248868659.d_313097539'), '\"', '') AS d_143615646_d_248868659_d_313097539,
	REPLACE(JSON_QUERY(row,'$.d_143615646.d_248868659.d_453343022'), '\"', '') AS d_143615646_d_248868659_d_453343022,
	REPLACE(JSON_QUERY(row,'$.d_143615646.d_248868659.d_472864016'), '\"', '') AS d_143615646_d_248868659_d_472864016,
	REPLACE(JSON_QUERY(row,'$.d_143615646.d_248868659.d_684617815'), '\"', '') AS d_143615646_d_248868659_d_684617815,
	REPLACE(JSON_QUERY(row,'$.d_143615646.d_248868659.d_728366619'), '\"', '') AS d_143615646_d_248868659_d_728366619,
	REPLACE(JSON_QUERY(row,'$.d_143615646.d_248868659.d_742806035'), '\"', '') AS d_143615646_d_248868659_d_742806035,
	REPLACE(JSON_QUERY(row,'$.d_143615646.d_248868659.d_757246707'), '\"', '') AS d_143615646_d_248868659_d_757246707,
	REPLACE(JSON_QUERY(row,'$.d_143615646.d_248868659.d_982885431'), '\"', '') AS d_143615646_d_248868659_d_982885431,
	REPLACE(JSON_QUERY(row,'$.d_143615646.d_338286049'), '\"', '') AS d_143615646_d_338286049,
	REPLACE(JSON_QUERY(row,'$.d_143615646.d_536710547'), '\"', '') AS d_143615646_d_536710547,
	REPLACE(JSON_QUERY(row,'$.d_143615646.d_593843561'), '\"', '') AS d_143615646_d_593843561,
	REPLACE(JSON_QUERY(row,'$.d_143615646.d_678857215'), '\"', '') AS d_143615646_d_678857215,
	REPLACE(JSON_QUERY(row,'$.d_143615646.d_762124027'), '\"', '') AS d_143615646_d_762124027,
	REPLACE(JSON_QUERY(row,'$.d_143615646.d_825582494'), '\"', '') AS d_143615646_d_825582494,
	REPLACE(JSON_QUERY(row,'$.d_143615646.d_883732523'), '\"', '') AS d_143615646_d_883732523,
	REPLACE(JSON_QUERY(row,'$.d_143615646.d_926457119'), '\"', '') AS d_143615646_d_926457119,
	REPLACE(JSON_QUERY(row,'$.d_210921343'), '\"', '') AS d_210921343,
	REPLACE(JSON_QUERY(row,'$.d_223999569.d_593843561'), '\"', '') AS d_223999569_d_593843561,
	REPLACE(JSON_QUERY(row,'$.d_223999569.d_825582494'), '\"', '') AS d_223999569_d_825582494,
	REPLACE(JSON_QUERY(row,'$.d_232343615.d_248868659.d_102695484'), '\"', '') AS d_232343615_d_248868659_d_102695484,
	REPLACE(JSON_QUERY(row,'$.d_232343615.d_248868659.d_242307474'), '\"', '') AS d_232343615_d_248868659_d_242307474,
	REPLACE(JSON_QUERY(row,'$.d_232343615.d_248868659.d_283900611'), '\"', '') AS d_232343615_d_248868659_d_283900611,
	REPLACE(JSON_QUERY(row,'$.d_232343615.d_248868659.d_313097539'), '\"', '') AS d_232343615_d_248868659_d_313097539,
	REPLACE(JSON_QUERY(row,'$.d_232343615.d_248868659.d_453343022'), '\"', '') AS d_232343615_d_248868659_d_453343022,
	REPLACE(JSON_QUERY(row,'$.d_232343615.d_248868659.d_472864016'), '\"', '') AS d_232343615_d_248868659_d_472864016,
	REPLACE(JSON_QUERY(row,'$.d_232343615.d_248868659.d_550088682'), '\"', '') AS d_232343615_d_248868659_d_550088682,
	REPLACE(JSON_QUERY(row,'$.d_232343615.d_248868659.d_561005927'), '\"', '') AS d_232343615_d_248868659_d_561005927,
	REPLACE(JSON_QUERY(row,'$.d_232343615.d_248868659.d_635875253'), '\"', '') AS d_232343615_d_248868659_d_635875253,
	REPLACE(JSON_QUERY(row,'$.d_232343615.d_248868659.d_654002184'), '\"', '') AS d_232343615_d_248868659_d_654002184,
	REPLACE(JSON_QUERY(row,'$.d_232343615.d_248868659.d_684617815'), '\"', '') AS d_232343615_d_248868659_d_684617815,
	REPLACE(JSON_QUERY(row,'$.d_232343615.d_248868659.d_690540566'), '\"', '') AS d_232343615_d_248868659_d_690540566,
	REPLACE(JSON_QUERY(row,'$.d_232343615.d_248868659.d_728366619'), '\"', '') AS d_232343615_d_248868659_d_728366619,
	REPLACE(JSON_QUERY(row,'$.d_232343615.d_248868659.d_757246707'), '\"', '') AS d_232343615_d_248868659_d_757246707,
	REPLACE(JSON_QUERY(row,'$.d_232343615.d_248868659.d_777486216'), '\"', '') AS d_232343615_d_248868659_d_777486216,
	REPLACE(JSON_QUERY(row,'$.d_232343615.d_248868659.d_810960823'), '\"', '') AS d_232343615_d_248868659_d_810960823,
	REPLACE(JSON_QUERY(row,'$.d_232343615.d_248868659.d_861162895'), '\"', '') AS d_232343615_d_248868659_d_861162895,
	REPLACE(JSON_QUERY(row,'$.d_232343615.d_248868659.d_912088602'), '\"', '') AS d_232343615_d_248868659_d_912088602,
	REPLACE(JSON_QUERY(row,'$.d_232343615.d_248868659.d_937362785'), '\"', '') AS d_232343615_d_248868659_d_937362785,
	REPLACE(JSON_QUERY(row,'$.d_232343615.d_248868659.d_982885431'), '\"', '') AS d_232343615_d_248868659_d_982885431,
	REPLACE(JSON_QUERY(row,'$.d_232343615.d_536710547'), '\"', '') AS d_232343615_d_536710547,
	REPLACE(JSON_QUERY(row,'$.d_232343615.d_593843561'), '\"', '') AS d_232343615_d_593843561,
	REPLACE(JSON_QUERY(row,'$.d_232343615.d_678857215'), '\"', '') AS d_232343615_d_678857215,
	REPLACE(JSON_QUERY(row,'$.d_232343615.d_762124027'), '\"', '') AS d_232343615_d_762124027,
	REPLACE(JSON_QUERY(row,'$.d_232343615.d_825582494'), '\"', '') AS d_232343615_d_825582494,
	REPLACE(JSON_QUERY(row,'$.d_232343615.d_926457119'), '\"', '') AS d_232343615_d_926457119,
	REPLACE(JSON_QUERY(row,'$.d_299553921.d_248868659.d_102695484'), '\"', '') AS d_299553921_d_248868659_d_102695484,
	REPLACE(JSON_QUERY(row,'$.d_299553921.d_248868659.d_242307474'), '\"', '') AS d_299553921_d_248868659_d_242307474,
	REPLACE(JSON_QUERY(row,'$.d_299553921.d_248868659.d_283900611'), '\"', '') AS d_299553921_d_248868659_d_283900611,
	REPLACE(JSON_QUERY(row,'$.d_299553921.d_248868659.d_313097539'), '\"', '') AS d_299553921_d_248868659_d_313097539,
	REPLACE(JSON_QUERY(row,'$.d_299553921.d_248868659.d_453343022'), '\"', '') AS d_299553921_d_248868659_d_453343022,
	REPLACE(JSON_QUERY(row,'$.d_299553921.d_248868659.d_472864016'), '\"', '') AS d_299553921_d_248868659_d_472864016,
	REPLACE(JSON_QUERY(row,'$.d_299553921.d_248868659.d_550088682'), '\"', '') AS d_299553921_d_248868659_d_550088682,
	REPLACE(JSON_QUERY(row,'$.d_299553921.d_248868659.d_561005927'), '\"', '') AS d_299553921_d_248868659_d_561005927,
	REPLACE(JSON_QUERY(row,'$.d_299553921.d_248868659.d_635875253'), '\"', '') AS d_299553921_d_248868659_d_635875253,
	REPLACE(JSON_QUERY(row,'$.d_299553921.d_248868659.d_654002184'), '\"', '') AS d_299553921_d_248868659_d_654002184,
	REPLACE(JSON_QUERY(row,'$.d_299553921.d_248868659.d_684617815'), '\"', '') AS d_299553921_d_248868659_d_684617815,
	REPLACE(JSON_QUERY(row,'$.d_299553921.d_248868659.d_690540566'), '\"', '') AS d_299553921_d_248868659_d_690540566,
	REPLACE(JSON_QUERY(row,'$.d_299553921.d_248868659.d_728366619'), '\"', '') AS d_299553921_d_248868659_d_728366619,
	REPLACE(JSON_QUERY(row,'$.d_299553921.d_248868659.d_757246707'), '\"', '') AS d_299553921_d_248868659_d_757246707,
	REPLACE(JSON_QUERY(row,'$.d_299553921.d_248868659.d_777486216'), '\"', '') AS d_299553921_d_248868659_d_777486216,
	REPLACE(JSON_QUERY(row,'$.d_299553921.d_248868659.d_810960823'), '\"', '') AS d_299553921_d_248868659_d_810960823,
	REPLACE(JSON_QUERY(row,'$.d_299553921.d_248868659.d_861162895'), '\"', '') AS d_299553921_d_248868659_d_861162895,
	REPLACE(JSON_QUERY(row,'$.d_299553921.d_248868659.d_912088602'), '\"', '') AS d_299553921_d_248868659_d_912088602,
	REPLACE(JSON_QUERY(row,'$.d_299553921.d_248868659.d_937362785'), '\"', '') AS d_299553921_d_248868659_d_937362785,
	REPLACE(JSON_QUERY(row,'$.d_299553921.d_248868659.d_982885431'), '\"', '') AS d_299553921_d_248868659_d_982885431,
	REPLACE(JSON_QUERY(row,'$.d_299553921.d_338286049'), '\"', '') AS d_299553921_d_338286049,
	REPLACE(JSON_QUERY(row,'$.d_299553921.d_536710547'), '\"', '') AS d_299553921_d_536710547,
	REPLACE(JSON_QUERY(row,'$.d_299553921.d_593843561'), '\"', '') AS d_299553921_d_593843561,
	REPLACE(JSON_QUERY(row,'$.d_299553921.d_678857215'), '\"', '') AS d_299553921_d_678857215,
	REPLACE(JSON_QUERY(row,'$.d_299553921.d_762124027'), '\"', '') AS d_299553921_d_762124027,
	REPLACE(JSON_QUERY(row,'$.d_299553921.d_825582494'), '\"', '') AS d_299553921_d_825582494,
	REPLACE(JSON_QUERY(row,'$.d_299553921.d_883732523'), '\"', '') AS d_299553921_d_883732523,
	REPLACE(JSON_QUERY(row,'$.d_299553921.d_926457119'), '\"', '') AS d_299553921_d_926457119,
	REPLACE(JSON_QUERY(row,'$.d_331584571'), '\"', '') AS d_331584571,
	REPLACE(JSON_QUERY(row,'$.d_338570265'), '\"', '') AS d_338570265,
	REPLACE(JSON_QUERY(row,'$.d_376960806.d_248868659.d_102695484'), '\"', '') AS d_376960806_d_248868659_d_102695484,
	REPLACE(JSON_QUERY(row,'$.d_376960806.d_248868659.d_242307474'), '\"', '') AS d_376960806_d_248868659_d_242307474,
	REPLACE(JSON_QUERY(row,'$.d_376960806.d_248868659.d_283900611'), '\"', '') AS d_376960806_d_248868659_d_283900611,
	REPLACE(JSON_QUERY(row,'$.d_376960806.d_248868659.d_313097539'), '\"', '') AS d_376960806_d_248868659_d_313097539,
	REPLACE(JSON_QUERY(row,'$.d_376960806.d_248868659.d_453343022'), '\"', '') AS d_376960806_d_248868659_d_453343022,
	REPLACE(JSON_QUERY(row,'$.d_376960806.d_248868659.d_472864016'), '\"', '') AS d_376960806_d_248868659_d_472864016,
	REPLACE(JSON_QUERY(row,'$.d_376960806.d_248868659.d_550088682'), '\"', '') AS d_376960806_d_248868659_d_550088682,
	REPLACE(JSON_QUERY(row,'$.d_376960806.d_248868659.d_561005927'), '\"', '') AS d_376960806_d_248868659_d_561005927,
	REPLACE(JSON_QUERY(row,'$.d_376960806.d_248868659.d_635875253'), '\"', '') AS d_376960806_d_248868659_d_635875253,
	REPLACE(JSON_QUERY(row,'$.d_376960806.d_248868659.d_654002184'), '\"', '') AS d_376960806_d_248868659_d_654002184,
	REPLACE(JSON_QUERY(row,'$.d_376960806.d_248868659.d_684617815'), '\"', '') AS d_376960806_d_248868659_d_684617815,
	REPLACE(JSON_QUERY(row,'$.d_376960806.d_248868659.d_690540566'), '\"', '') AS d_376960806_d_248868659_d_690540566,
	REPLACE(JSON_QUERY(row,'$.d_376960806.d_248868659.d_728366619'), '\"', '') AS d_376960806_d_248868659_d_728366619,
	REPLACE(JSON_QUERY(row,'$.d_376960806.d_248868659.d_757246707'), '\"', '') AS d_376960806_d_248868659_d_757246707,
	REPLACE(JSON_QUERY(row,'$.d_376960806.d_248868659.d_777486216'), '\"', '') AS d_376960806_d_248868659_d_777486216,
	REPLACE(JSON_QUERY(row,'$.d_376960806.d_248868659.d_810960823'), '\"', '') AS d_376960806_d_248868659_d_810960823,
	REPLACE(JSON_QUERY(row,'$.d_376960806.d_248868659.d_861162895'), '\"', '') AS d_376960806_d_248868659_d_861162895,
	REPLACE(JSON_QUERY(row,'$.d_376960806.d_248868659.d_912088602'), '\"', '') AS d_376960806_d_248868659_d_912088602,
	REPLACE(JSON_QUERY(row,'$.d_376960806.d_248868659.d_937362785'), '\"', '') AS d_376960806_d_248868659_d_937362785,
	REPLACE(JSON_QUERY(row,'$.d_376960806.d_248868659.d_982885431'), '\"', '') AS d_376960806_d_248868659_d_982885431,
	REPLACE(JSON_QUERY(row,'$.d_376960806.d_536710547'), '\"', '') AS d_376960806_d_536710547,
	REPLACE(JSON_QUERY(row,'$.d_376960806.d_593843561'), '\"', '') AS d_376960806_d_593843561,
	REPLACE(JSON_QUERY(row,'$.d_376960806.d_678857215'), '\"', '') AS d_376960806_d_678857215,
	REPLACE(JSON_QUERY(row,'$.d_376960806.d_762124027'), '\"', '') AS d_376960806_d_762124027,
	REPLACE(JSON_QUERY(row,'$.d_376960806.d_825582494'), '\"', '') AS d_376960806_d_825582494,
	REPLACE(JSON_QUERY(row,'$.d_376960806.d_926457119'), '\"', '') AS d_376960806_d_926457119,
	REPLACE(JSON_QUERY(row,'$.d_387108065'), '\"', '') AS d_387108065,
	REPLACE(JSON_QUERY(row,'$.d_398645039'), '\"', '') AS d_398645039,
	REPLACE(JSON_QUERY(row,'$.d_410912345'), '\"', '') AS d_410912345,
	REPLACE(JSON_QUERY(row,'$.d_454453939.d_248868659.d_242307474'), '\"', '') AS d_454453939_d_248868659_d_242307474,
	REPLACE(JSON_QUERY(row,'$.d_454453939.d_248868659.d_283900611'), '\"', '') AS d_454453939_d_248868659_d_283900611,
	REPLACE(JSON_QUERY(row,'$.d_454453939.d_248868659.d_313097539'), '\"', '') AS d_454453939_d_248868659_d_313097539,
	REPLACE(JSON_QUERY(row,'$.d_454453939.d_248868659.d_453343022'), '\"', '') AS d_454453939_d_248868659_d_453343022,
	REPLACE(JSON_QUERY(row,'$.d_454453939.d_248868659.d_472864016'), '\"', '') AS d_454453939_d_248868659_d_472864016,
	REPLACE(JSON_QUERY(row,'$.d_454453939.d_248868659.d_550088682'), '\"', '') AS d_454453939_d_248868659_d_550088682,
	REPLACE(JSON_QUERY(row,'$.d_454453939.d_248868659.d_684617815'), '\"', '') AS d_454453939_d_248868659_d_684617815,
	REPLACE(JSON_QUERY(row,'$.d_454453939.d_248868659.d_690540566'), '\"', '') AS d_454453939_d_248868659_d_690540566,
	REPLACE(JSON_QUERY(row,'$.d_454453939.d_248868659.d_728366619'), '\"', '') AS d_454453939_d_248868659_d_728366619,
	REPLACE(JSON_QUERY(row,'$.d_454453939.d_248868659.d_757246707'), '\"', '') AS d_454453939_d_248868659_d_757246707,
	REPLACE(JSON_QUERY(row,'$.d_454453939.d_248868659.d_777486216'), '\"', '') AS d_454453939_d_248868659_d_777486216,
	REPLACE(JSON_QUERY(row,'$.d_454453939.d_248868659.d_810960823'), '\"', '') AS d_454453939_d_248868659_d_810960823,
	REPLACE(JSON_QUERY(row,'$.d_454453939.d_248868659.d_982885431'), '\"', '') AS d_454453939_d_248868659_d_982885431,
	REPLACE(JSON_QUERY(row,'$.d_454453939.d_338286049'), '\"', '') AS d_454453939_d_338286049,
	REPLACE(JSON_QUERY(row,'$.d_454453939.d_536710547'), '\"', '') AS d_454453939_d_536710547,
	REPLACE(JSON_QUERY(row,'$.d_454453939.d_593843561'), '\"', '') AS d_454453939_d_593843561,
	REPLACE(JSON_QUERY(row,'$.d_454453939.d_678857215'), '\"', '') AS d_454453939_d_678857215,
	REPLACE(JSON_QUERY(row,'$.d_454453939.d_762124027'), '\"', '') AS d_454453939_d_762124027,
	REPLACE(JSON_QUERY(row,'$.d_454453939.d_825582494'), '\"', '') AS d_454453939_d_825582494,
	REPLACE(JSON_QUERY(row,'$.d_454453939.d_883732523'), '\"', '') AS d_454453939_d_883732523,
	REPLACE(JSON_QUERY(row,'$.d_454453939.d_926457119'), '\"', '') AS d_454453939_d_926457119,
	REPLACE(JSON_QUERY(row,'$.d_534041351'), '\"', '') AS d_534041351,
	REPLACE(JSON_QUERY(row,'$.d_541311218'), '\"', '') AS d_541311218,
	REPLACE(JSON_QUERY(row,'$.d_556788178'), '\"', '') AS d_556788178,
	REPLACE(JSON_QUERY(row,'$.d_589588440.d_248868659.d_102695484'), '\"', '') AS d_589588440_d_248868659_d_102695484,
	REPLACE(JSON_QUERY(row,'$.d_589588440.d_248868659.d_242307474'), '\"', '') AS d_589588440_d_248868659_d_242307474,
	REPLACE(JSON_QUERY(row,'$.d_589588440.d_248868659.d_283900611'), '\"', '') AS d_589588440_d_248868659_d_283900611,
	REPLACE(JSON_QUERY(row,'$.d_589588440.d_248868659.d_313097539'), '\"', '') AS d_589588440_d_248868659_d_313097539,
	REPLACE(JSON_QUERY(row,'$.d_589588440.d_248868659.d_453343022'), '\"', '') AS d_589588440_d_248868659_d_453343022,
	REPLACE(JSON_QUERY(row,'$.d_589588440.d_248868659.d_472864016'), '\"', '') AS d_589588440_d_248868659_d_472864016,
	REPLACE(JSON_QUERY(row,'$.d_589588440.d_248868659.d_550088682'), '\"', '') AS d_589588440_d_248868659_d_550088682,
	REPLACE(JSON_QUERY(row,'$.d_589588440.d_248868659.d_561005927'), '\"', '') AS d_589588440_d_248868659_d_561005927,
	REPLACE(JSON_QUERY(row,'$.d_589588440.d_248868659.d_635875253'), '\"', '') AS d_589588440_d_248868659_d_635875253,
	REPLACE(JSON_QUERY(row,'$.d_589588440.d_248868659.d_654002184'), '\"', '') AS d_589588440_d_248868659_d_654002184,
	REPLACE(JSON_QUERY(row,'$.d_589588440.d_248868659.d_684617815'), '\"', '') AS d_589588440_d_248868659_d_684617815,
	REPLACE(JSON_QUERY(row,'$.d_589588440.d_248868659.d_690540566'), '\"', '') AS d_589588440_d_248868659_d_690540566,
	REPLACE(JSON_QUERY(row,'$.d_589588440.d_248868659.d_728366619'), '\"', '') AS d_589588440_d_248868659_d_728366619,
	REPLACE(JSON_QUERY(row,'$.d_589588440.d_248868659.d_757246707'), '\"', '') AS d_589588440_d_248868659_d_757246707,
	REPLACE(JSON_QUERY(row,'$.d_589588440.d_248868659.d_777486216'), '\"', '') AS d_589588440_d_248868659_d_777486216,
	REPLACE(JSON_QUERY(row,'$.d_589588440.d_248868659.d_810960823'), '\"', '') AS d_589588440_d_248868659_d_810960823,
	REPLACE(JSON_QUERY(row,'$.d_589588440.d_248868659.d_861162895'), '\"', '') AS d_589588440_d_248868659_d_861162895,
	REPLACE(JSON_QUERY(row,'$.d_589588440.d_248868659.d_912088602'), '\"', '') AS d_589588440_d_248868659_d_912088602,
	REPLACE(JSON_QUERY(row,'$.d_589588440.d_248868659.d_937362785'), '\"', '') AS d_589588440_d_248868659_d_937362785,
	REPLACE(JSON_QUERY(row,'$.d_589588440.d_248868659.d_982885431'), '\"', '') AS d_589588440_d_248868659_d_982885431,
	REPLACE(JSON_QUERY(row,'$.d_589588440.d_536710547'), '\"', '') AS d_589588440_d_536710547,
	REPLACE(JSON_QUERY(row,'$.d_589588440.d_593843561'), '\"', '') AS d_589588440_d_593843561,
	REPLACE(JSON_QUERY(row,'$.d_589588440.d_678857215'), '\"', '') AS d_589588440_d_678857215,
	REPLACE(JSON_QUERY(row,'$.d_589588440.d_762124027'), '\"', '') AS d_589588440_d_762124027,
	REPLACE(JSON_QUERY(row,'$.d_589588440.d_825582494'), '\"', '') AS d_589588440_d_825582494,
	REPLACE(JSON_QUERY(row,'$.d_589588440.d_926457119'), '\"', '') AS d_589588440_d_926457119,
	REPLACE(JSON_QUERY(row,'$.d_611091485'), '\"', '') AS d_611091485,
	REPLACE(JSON_QUERY(row,'$.d_646899796'), '\"', '') AS d_646899796,
	REPLACE(JSON_QUERY(row,'$.d_650516960'), '\"', '') AS d_650516960,
	REPLACE(JSON_QUERY(row,'$.d_652357376.d_248868659.d_242307474'), '\"', '') AS d_652357376_d_248868659_d_242307474,
	REPLACE(JSON_QUERY(row,'$.d_652357376.d_248868659.d_283900611'), '\"', '') AS d_652357376_d_248868659_d_283900611,
	REPLACE(JSON_QUERY(row,'$.d_652357376.d_248868659.d_313097539'), '\"', '') AS d_652357376_d_248868659_d_313097539,
	REPLACE(JSON_QUERY(row,'$.d_652357376.d_248868659.d_453343022'), '\"', '') AS d_652357376_d_248868659_d_453343022,
	REPLACE(JSON_QUERY(row,'$.d_652357376.d_248868659.d_472864016'), '\"', '') AS d_652357376_d_248868659_d_472864016,
	REPLACE(JSON_QUERY(row,'$.d_652357376.d_248868659.d_550088682'), '\"', '') AS d_652357376_d_248868659_d_550088682,
	REPLACE(JSON_QUERY(row,'$.d_652357376.d_248868659.d_684617815'), '\"', '') AS d_652357376_d_248868659_d_684617815,
	REPLACE(JSON_QUERY(row,'$.d_652357376.d_248868659.d_690540566'), '\"', '') AS d_652357376_d_248868659_d_690540566,
	REPLACE(JSON_QUERY(row,'$.d_652357376.d_248868659.d_728366619'), '\"', '') AS d_652357376_d_248868659_d_728366619,
	REPLACE(JSON_QUERY(row,'$.d_652357376.d_248868659.d_757246707'), '\"', '') AS d_652357376_d_248868659_d_757246707,
	REPLACE(JSON_QUERY(row,'$.d_652357376.d_248868659.d_777486216'), '\"', '') AS d_652357376_d_248868659_d_777486216,
	REPLACE(JSON_QUERY(row,'$.d_652357376.d_248868659.d_810960823'), '\"', '') AS d_652357376_d_248868659_d_810960823,
	REPLACE(JSON_QUERY(row,'$.d_652357376.d_248868659.d_982885431'), '\"', '') AS d_652357376_d_248868659_d_982885431,
	REPLACE(JSON_QUERY(row,'$.d_652357376.d_338286049'), '\"', '') AS d_652357376_d_338286049,
	REPLACE(JSON_QUERY(row,'$.d_652357376.d_536710547'), '\"', '') AS d_652357376_d_536710547,
	REPLACE(JSON_QUERY(row,'$.d_652357376.d_593843561'), '\"', '') AS d_652357376_d_593843561,
	REPLACE(JSON_QUERY(row,'$.d_652357376.d_678857215'), '\"', '') AS d_652357376_d_678857215,
	REPLACE(JSON_QUERY(row,'$.d_652357376.d_762124027'), '\"', '') AS d_652357376_d_762124027,
	REPLACE(JSON_QUERY(row,'$.d_652357376.d_825582494'), '\"', '') AS d_652357376_d_825582494,
	REPLACE(JSON_QUERY(row,'$.d_652357376.d_883732523'), '\"', '') AS d_652357376_d_883732523,
	REPLACE(JSON_QUERY(row,'$.d_652357376.d_926457119'), '\"', '') AS d_652357376_d_926457119,
	REPLACE(JSON_QUERY(row,'$.d_677469051.d_248868659.d_242307474'), '\"', '') AS d_677469051_d_248868659_d_242307474,
	REPLACE(JSON_QUERY(row,'$.d_677469051.d_248868659.d_283900611'), '\"', '') AS d_677469051_d_248868659_d_283900611,
	REPLACE(JSON_QUERY(row,'$.d_677469051.d_248868659.d_313097539'), '\"', '') AS d_677469051_d_248868659_d_313097539,
	REPLACE(JSON_QUERY(row,'$.d_677469051.d_248868659.d_453343022'), '\"', '') AS d_677469051_d_248868659_d_453343022,
	REPLACE(JSON_QUERY(row,'$.d_677469051.d_248868659.d_472864016'), '\"', '') AS d_677469051_d_248868659_d_472864016,
	REPLACE(JSON_QUERY(row,'$.d_677469051.d_248868659.d_550088682'), '\"', '') AS d_677469051_d_248868659_d_550088682,
	REPLACE(JSON_QUERY(row,'$.d_677469051.d_248868659.d_684617815'), '\"', '') AS d_677469051_d_248868659_d_684617815,
	REPLACE(JSON_QUERY(row,'$.d_677469051.d_248868659.d_690540566'), '\"', '') AS d_677469051_d_248868659_d_690540566,
	REPLACE(JSON_QUERY(row,'$.d_677469051.d_248868659.d_728366619'), '\"', '') AS d_677469051_d_248868659_d_728366619,
	REPLACE(JSON_QUERY(row,'$.d_677469051.d_248868659.d_757246707'), '\"', '') AS d_677469051_d_248868659_d_757246707,
	REPLACE(JSON_QUERY(row,'$.d_677469051.d_248868659.d_777486216'), '\"', '') AS d_677469051_d_248868659_d_777486216,
	REPLACE(JSON_QUERY(row,'$.d_677469051.d_248868659.d_810960823'), '\"', '') AS d_677469051_d_248868659_d_810960823,
	REPLACE(JSON_QUERY(row,'$.d_677469051.d_248868659.d_982885431'), '\"', '') AS d_677469051_d_248868659_d_982885431,
	REPLACE(JSON_QUERY(row,'$.d_677469051.d_536710547'), '\"', '') AS d_677469051_d_536710547,
	REPLACE(JSON_QUERY(row,'$.d_677469051.d_593843561'), '\"', '') AS d_677469051_d_593843561,
	REPLACE(JSON_QUERY(row,'$.d_677469051.d_678857215'), '\"', '') AS d_677469051_d_678857215,
	REPLACE(JSON_QUERY(row,'$.d_677469051.d_762124027'), '\"', '') AS d_677469051_d_762124027,
	REPLACE(JSON_QUERY(row,'$.d_677469051.d_825582494'), '\"', '') AS d_677469051_d_825582494,
	REPLACE(JSON_QUERY(row,'$.d_677469051.d_926457119'), '\"', '') AS d_677469051_d_926457119,
	REPLACE(JSON_QUERY(row,'$.d_678166505'), '\"', '') AS d_678166505,
	REPLACE(JSON_QUERY(row,'$.d_683613884.d_248868659.d_242307474'), '\"', '') AS d_683613884_d_248868659_d_242307474,
	REPLACE(JSON_QUERY(row,'$.d_683613884.d_248868659.d_283900611'), '\"', '') AS d_683613884_d_248868659_d_283900611,
	REPLACE(JSON_QUERY(row,'$.d_683613884.d_248868659.d_313097539'), '\"', '') AS d_683613884_d_248868659_d_313097539,
	REPLACE(JSON_QUERY(row,'$.d_683613884.d_248868659.d_453343022'), '\"', '') AS d_683613884_d_248868659_d_453343022,
	REPLACE(JSON_QUERY(row,'$.d_683613884.d_248868659.d_472864016'), '\"', '') AS d_683613884_d_248868659_d_472864016,
	REPLACE(JSON_QUERY(row,'$.d_683613884.d_248868659.d_550088682'), '\"', '') AS d_683613884_d_248868659_d_550088682,
	REPLACE(JSON_QUERY(row,'$.d_683613884.d_248868659.d_684617815'), '\"', '') AS d_683613884_d_248868659_d_684617815,
	REPLACE(JSON_QUERY(row,'$.d_683613884.d_248868659.d_690540566'), '\"', '') AS d_683613884_d_248868659_d_690540566,
	REPLACE(JSON_QUERY(row,'$.d_683613884.d_248868659.d_728366619'), '\"', '') AS d_683613884_d_248868659_d_728366619,
	REPLACE(JSON_QUERY(row,'$.d_683613884.d_248868659.d_757246707'), '\"', '') AS d_683613884_d_248868659_d_757246707,
	REPLACE(JSON_QUERY(row,'$.d_683613884.d_248868659.d_777486216'), '\"', '') AS d_683613884_d_248868659_d_777486216,
	REPLACE(JSON_QUERY(row,'$.d_683613884.d_248868659.d_810960823'), '\"', '') AS d_683613884_d_248868659_d_810960823,
	REPLACE(JSON_QUERY(row,'$.d_683613884.d_248868659.d_982885431'), '\"', '') AS d_683613884_d_248868659_d_982885431,
	REPLACE(JSON_QUERY(row,'$.d_683613884.d_536710547'), '\"', '') AS d_683613884_d_536710547,
	REPLACE(JSON_QUERY(row,'$.d_683613884.d_593843561'), '\"', '') AS d_683613884_d_593843561,
	REPLACE(JSON_QUERY(row,'$.d_683613884.d_678857215'), '\"', '') AS d_683613884_d_678857215,
	REPLACE(JSON_QUERY(row,'$.d_683613884.d_762124027'), '\"', '') AS d_683613884_d_762124027,
	REPLACE(JSON_QUERY(row,'$.d_683613884.d_825582494'), '\"', '') AS d_683613884_d_825582494,
	REPLACE(JSON_QUERY(row,'$.d_683613884.d_926457119'), '\"', '') AS d_683613884_d_926457119,
	REPLACE(JSON_QUERY(row,'$.d_703954371.d_248868659.d_102695484'), '\"', '') AS d_703954371_d_248868659_d_102695484,
	REPLACE(JSON_QUERY(row,'$.d_703954371.d_248868659.d_242307474'), '\"', '') AS d_703954371_d_248868659_d_242307474,
	REPLACE(JSON_QUERY(row,'$.d_703954371.d_248868659.d_283900611'), '\"', '') AS d_703954371_d_248868659_d_283900611,
	REPLACE(JSON_QUERY(row,'$.d_703954371.d_248868659.d_313097539'), '\"', '') AS d_703954371_d_248868659_d_313097539,
	REPLACE(JSON_QUERY(row,'$.d_703954371.d_248868659.d_453343022'), '\"', '') AS d_703954371_d_248868659_d_453343022,
	REPLACE(JSON_QUERY(row,'$.d_703954371.d_248868659.d_472864016'), '\"', '') AS d_703954371_d_248868659_d_472864016,
	REPLACE(JSON_QUERY(row,'$.d_703954371.d_248868659.d_550088682'), '\"', '') AS d_703954371_d_248868659_d_550088682,
	REPLACE(JSON_QUERY(row,'$.d_703954371.d_248868659.d_561005927'), '\"', '') AS d_703954371_d_248868659_d_561005927,
	REPLACE(JSON_QUERY(row,'$.d_703954371.d_248868659.d_635875253'), '\"', '') AS d_703954371_d_248868659_d_635875253,
	REPLACE(JSON_QUERY(row,'$.d_703954371.d_248868659.d_654002184'), '\"', '') AS d_703954371_d_248868659_d_654002184,
	REPLACE(JSON_QUERY(row,'$.d_703954371.d_248868659.d_684617815'), '\"', '') AS d_703954371_d_248868659_d_684617815,
	REPLACE(JSON_QUERY(row,'$.d_703954371.d_248868659.d_690540566'), '\"', '') AS d_703954371_d_248868659_d_690540566,
	REPLACE(JSON_QUERY(row,'$.d_703954371.d_248868659.d_728366619'), '\"', '') AS d_703954371_d_248868659_d_728366619,
	REPLACE(JSON_QUERY(row,'$.d_703954371.d_248868659.d_757246707'), '\"', '') AS d_703954371_d_248868659_d_757246707,
	REPLACE(JSON_QUERY(row,'$.d_703954371.d_248868659.d_777486216'), '\"', '') AS d_703954371_d_248868659_d_777486216,
	REPLACE(JSON_QUERY(row,'$.d_703954371.d_248868659.d_810960823'), '\"', '') AS d_703954371_d_248868659_d_810960823,
	REPLACE(JSON_QUERY(row,'$.d_703954371.d_248868659.d_861162895'), '\"', '') AS d_703954371_d_248868659_d_861162895,
	REPLACE(JSON_QUERY(row,'$.d_703954371.d_248868659.d_912088602'), '\"', '') AS d_703954371_d_248868659_d_912088602,
	REPLACE(JSON_QUERY(row,'$.d_703954371.d_248868659.d_937362785'), '\"', '') AS d_703954371_d_248868659_d_937362785,
	REPLACE(JSON_QUERY(row,'$.d_703954371.d_248868659.d_982885431'), '\"', '') AS d_703954371_d_248868659_d_982885431,
	REPLACE(JSON_QUERY(row,'$.d_703954371.d_338286049'), '\"', '') AS d_703954371_d_338286049,
	REPLACE(JSON_QUERY(row,'$.d_703954371.d_536710547'), '\"', '') AS d_703954371_d_536710547,
	REPLACE(JSON_QUERY(row,'$.d_703954371.d_593843561'), '\"', '') AS d_703954371_d_593843561,
	REPLACE(JSON_QUERY(row,'$.d_703954371.d_678857215'), '\"', '') AS d_703954371_d_678857215,
	REPLACE(JSON_QUERY(row,'$.d_703954371.d_762124027'), '\"', '') AS d_703954371_d_762124027,
	REPLACE(JSON_QUERY(row,'$.d_703954371.d_825582494'), '\"', '') AS d_703954371_d_825582494,
	REPLACE(JSON_QUERY(row,'$.d_703954371.d_883732523'), '\"', '') AS d_703954371_d_883732523,
	REPLACE(JSON_QUERY(row,'$.d_703954371.d_926457119'), '\"', '') AS d_703954371_d_926457119,
	REPLACE(JSON_QUERY(row,'$.d_719427591'), '\"', '') AS d_719427591,
	REPLACE(JSON_QUERY(row,'$.d_787237543.d_593843561'), '\"', '') AS d_787237543_d_593843561,
	REPLACE(JSON_QUERY(row,'$.d_787237543.d_825582494'), '\"', '') AS d_787237543_d_825582494,
	REPLACE(JSON_QUERY(row,'$.d_820476880'), '\"', '') AS d_820476880,
	REPLACE(JSON_QUERY(row,'$.d_827220437'), '\"', '') AS d_827220437,
	REPLACE(JSON_QUERY(row,'$.d_838567176.d_248868659.d_242307474'), '\"', '') AS d_838567176_d_248868659_d_242307474,
	REPLACE(JSON_QUERY(row,'$.d_838567176.d_248868659.d_283900611'), '\"', '') AS d_838567176_d_248868659_d_283900611,
	REPLACE(JSON_QUERY(row,'$.d_838567176.d_248868659.d_313097539'), '\"', '') AS d_838567176_d_248868659_d_313097539,
	REPLACE(JSON_QUERY(row,'$.d_838567176.d_248868659.d_453343022'), '\"', '') AS d_838567176_d_248868659_d_453343022,
	REPLACE(JSON_QUERY(row,'$.d_838567176.d_248868659.d_472864016'), '\"', '') AS d_838567176_d_248868659_d_472864016,
	REPLACE(JSON_QUERY(row,'$.d_838567176.d_248868659.d_550088682'), '\"', '') AS d_838567176_d_248868659_d_550088682,
	REPLACE(JSON_QUERY(row,'$.d_838567176.d_248868659.d_684617815'), '\"', '') AS d_838567176_d_248868659_d_684617815,
	REPLACE(JSON_QUERY(row,'$.d_838567176.d_248868659.d_690540566'), '\"', '') AS d_838567176_d_248868659_d_690540566,
	REPLACE(JSON_QUERY(row,'$.d_838567176.d_248868659.d_728366619'), '\"', '') AS d_838567176_d_248868659_d_728366619,
	REPLACE(JSON_QUERY(row,'$.d_838567176.d_248868659.d_757246707'), '\"', '') AS d_838567176_d_248868659_d_757246707,
	REPLACE(JSON_QUERY(row,'$.d_838567176.d_248868659.d_777486216'), '\"', '') AS d_838567176_d_248868659_d_777486216,
	REPLACE(JSON_QUERY(row,'$.d_838567176.d_248868659.d_810960823'), '\"', '') AS d_838567176_d_248868659_d_810960823,
	REPLACE(JSON_QUERY(row,'$.d_838567176.d_248868659.d_982885431'), '\"', '') AS d_838567176_d_248868659_d_982885431,
	REPLACE(JSON_QUERY(row,'$.d_838567176.d_338286049'), '\"', '') AS d_838567176_d_338286049,
	REPLACE(JSON_QUERY(row,'$.d_838567176.d_536710547'), '\"', '') AS d_838567176_d_536710547,
	REPLACE(JSON_QUERY(row,'$.d_838567176.d_593843561'), '\"', '') AS d_838567176_d_593843561,
	REPLACE(JSON_QUERY(row,'$.d_838567176.d_678857215'), '\"', '') AS d_838567176_d_678857215,
	REPLACE(JSON_QUERY(row,'$.d_838567176.d_762124027'), '\"', '') AS d_838567176_d_762124027,
	REPLACE(JSON_QUERY(row,'$.d_838567176.d_825582494'), '\"', '') AS d_838567176_d_825582494,
	REPLACE(JSON_QUERY(row,'$.d_838567176.d_883732523'), '\"', '') AS d_838567176_d_883732523,
	REPLACE(JSON_QUERY(row,'$.d_838567176.d_926457119'), '\"', '') AS d_838567176_d_926457119,
	REPLACE(JSON_QUERY(row,'$.d_915838974'), '\"', '') AS d_915838974,
	REPLACE(JSON_QUERY(row,'$.d_926457119'), '\"', '') AS d_926457119,
	REPLACE(JSON_QUERY(row,'$.d_928693120.integer'), '\"', '') AS d_928693120_integer,
	REPLACE(JSON_QUERY(row,'$.d_928693120.provided'), '\"', '') AS d_928693120_provided,
	REPLACE(JSON_QUERY(row,'$.d_928693120.string'), '\"', '') AS d_928693120_string,
	REPLACE(JSON_QUERY(row,'$.d_951355211'), '\"', '') AS d_951355211,
	REPLACE(JSON_QUERY(row,'$.d_958646668.d_248868659.d_242307474'), '\"', '') AS d_958646668_d_248868659_d_242307474,
	REPLACE(JSON_QUERY(row,'$.d_958646668.d_248868659.d_283900611'), '\"', '') AS d_958646668_d_248868659_d_283900611,
	REPLACE(JSON_QUERY(row,'$.d_958646668.d_248868659.d_313097539'), '\"', '') AS d_958646668_d_248868659_d_313097539,
	REPLACE(JSON_QUERY(row,'$.d_958646668.d_248868659.d_453343022'), '\"', '') AS d_958646668_d_248868659_d_453343022,
	REPLACE(JSON_QUERY(row,'$.d_958646668.d_248868659.d_472864016'), '\"', '') AS d_958646668_d_248868659_d_472864016,
	REPLACE(JSON_QUERY(row,'$.d_958646668.d_248868659.d_550088682'), '\"', '') AS d_958646668_d_248868659_d_550088682,
	REPLACE(JSON_QUERY(row,'$.d_958646668.d_248868659.d_684617815'), '\"', '') AS d_958646668_d_248868659_d_684617815,
	REPLACE(JSON_QUERY(row,'$.d_958646668.d_248868659.d_690540566'), '\"', '') AS d_958646668_d_248868659_d_690540566,
	REPLACE(JSON_QUERY(row,'$.d_958646668.d_248868659.d_728366619'), '\"', '') AS d_958646668_d_248868659_d_728366619,
	REPLACE(JSON_QUERY(row,'$.d_958646668.d_248868659.d_757246707'), '\"', '') AS d_958646668_d_248868659_d_757246707,
	REPLACE(JSON_QUERY(row,'$.d_958646668.d_248868659.d_777486216'), '\"', '') AS d_958646668_d_248868659_d_777486216,
	REPLACE(JSON_QUERY(row,'$.d_958646668.d_248868659.d_810960823'), '\"', '') AS d_958646668_d_248868659_d_810960823,
	REPLACE(JSON_QUERY(row,'$.d_958646668.d_248868659.d_982885431'), '\"', '') AS d_958646668_d_248868659_d_982885431,
	REPLACE(JSON_QUERY(row,'$.d_958646668.d_536710547'), '\"', '') AS d_958646668_d_536710547,
	REPLACE(JSON_QUERY(row,'$.d_958646668.d_593843561'), '\"', '') AS d_958646668_d_593843561,
	REPLACE(JSON_QUERY(row,'$.d_958646668.d_678857215'), '\"', '') AS d_958646668_d_678857215,
	REPLACE(JSON_QUERY(row,'$.d_958646668.d_762124027'), '\"', '') AS d_958646668_d_762124027,
	REPLACE(JSON_QUERY(row,'$.d_958646668.d_825582494'), '\"', '') AS d_958646668_d_825582494,
	REPLACE(JSON_QUERY(row,'$.d_958646668.d_926457119'), '\"', '') AS d_958646668_d_926457119,
	REPLACE(JSON_QUERY(row,'$.d_973670172.d_248868659.d_283900611'), '\"', '') AS d_973670172_d_248868659_d_283900611,
	REPLACE(JSON_QUERY(row,'$.d_973670172.d_248868659.d_313097539'), '\"', '') AS d_973670172_d_248868659_d_313097539,
	REPLACE(JSON_QUERY(row,'$.d_973670172.d_248868659.d_453343022'), '\"', '') AS d_973670172_d_248868659_d_453343022,
	REPLACE(JSON_QUERY(row,'$.d_973670172.d_248868659.d_472864016'), '\"', '') AS d_973670172_d_248868659_d_472864016,
	REPLACE(JSON_QUERY(row,'$.d_973670172.d_248868659.d_550088682'), '\"', '') AS d_973670172_d_248868659_d_550088682,
	REPLACE(JSON_QUERY(row,'$.d_973670172.d_248868659.d_684617815'), '\"', '') AS d_973670172_d_248868659_d_684617815,
	REPLACE(JSON_QUERY(row,'$.d_973670172.d_248868659.d_690540566'), '\"', '') AS d_973670172_d_248868659_d_690540566,
	REPLACE(JSON_QUERY(row,'$.d_973670172.d_248868659.d_728366619'), '\"', '') AS d_973670172_d_248868659_d_728366619,
	REPLACE(JSON_QUERY(row,'$.d_973670172.d_248868659.d_757246707'), '\"', '') AS d_973670172_d_248868659_d_757246707,
	REPLACE(JSON_QUERY(row,'$.d_973670172.d_248868659.d_956345366'), '\"', '') AS d_973670172_d_248868659_d_956345366,
	REPLACE(JSON_QUERY(row,'$.d_973670172.d_248868659.d_982885431'), '\"', '') AS d_973670172_d_248868659_d_982885431,
	REPLACE(JSON_QUERY(row,'$.d_973670172.d_338286049'), '\"', '') AS d_973670172_d_338286049,
	REPLACE(JSON_QUERY(row,'$.d_973670172.d_536710547'), '\"', '') AS d_973670172_d_536710547,
	REPLACE(JSON_QUERY(row,'$.d_973670172.d_593843561'), '\"', '') AS d_973670172_d_593843561,
	REPLACE(JSON_QUERY(row,'$.d_973670172.d_678857215'), '\"', '') AS d_973670172_d_678857215,
	REPLACE(JSON_QUERY(row,'$.d_973670172.d_762124027'), '\"', '') AS d_973670172_d_762124027,
	REPLACE(JSON_QUERY(row,'$.d_973670172.d_825582494'), '\"', '') AS d_973670172_d_825582494,
	REPLACE(JSON_QUERY(row,'$.d_973670172.d_883732523'), '\"', '') AS d_973670172_d_883732523,
	REPLACE(JSON_QUERY(row,'$.d_973670172.d_926457119'), '\"', '') AS d_973670172_d_926457119,
	REPLACE(JSON_QUERY(row,'$.id'), '\"', '') AS id,
	REPLACE(JSON_QUERY(row,'$.siteAcronym'), '\"', '') AS siteAcronym,
	REPLACE(JSON_QUERY(row,'$.token'), '\"', '') AS token -- selects
    FROM
      json_data,
      UNNEST(body) AS ROW )
  SELECT
    *,
    FORMAT_TIMESTAMP("%Y%m%d", DATETIME(CURRENT_TIMESTAMP(), "America/New_York")) AS date --date_format
  FROM
    flattened_data
  ORDER BY
    Connect_ID )

