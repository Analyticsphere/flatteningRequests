-- FYI: This query was automatically generated using R code written by Jake 
-- Peters. The code references a the source table schema and a configuration
-- file specifying various parameters. This style of query was developed by 
-- Warren Lu to flatten the Connect4Cancer datasets in 2022. See GitHub repo
-- for further documentation.
-- 
-- Repository: https://github.com/Analyticsphere/flatteningRequests
-- Relavent functions: generate_flattening_query.R
-- 
-- source_table: nih-nci-dceg-connect-dev.Connect.experience2024
-- destination table: nih-nci-dceg-connect-dev.FlatConnect.experience2024_JP -- notes
    
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
    "D_145727599": [
        153962804,
        188067494,
        386252749
    ],
    "D_210120853": [
        191667117,
        217279879,
        299631230,
        572976454,
        916319911
    ],
    "D_210983898.D_210983898": [
        334491616
    ],
    "D_228922812.D_228922812": [
        334491616
    ],
    "D_355080680.D_355080680": [
        334491616
    ],
    "D_363878103.D_363878103": [
        334491616
    ],
    "D_403175318.D_403175318": [
        178420302,
        200962909,
        554920493,
        798682161,
        807835037
    ],
    "D_470013848.D_470013848": [
        178420302,
        358413399,
        482753957,
        807835037
    ],
    "D_549970786.D_549970786": [
        334491616
    ],
    "D_575160226.D_575160226": [
        334491616
    ],
    "D_579939641.D_579939641": [
        334491616
    ],
    "D_649713579": [
        147611720,
        354508982,
        588555669,
        834694858,
        899315984
    ],
    "D_674994176.D_674994176": [
        334491616
    ],
    "D_731524314.D_731524314": [
        115959973,
        132115595,
        178420302,
        238237869,
        299722216,
        515798638,
        807835037,
        985034149
    ],
    "D_960544981": [
        101837333,
        313446770,
        393996571,
        604524950,
        815468840,
        925993577,
        985468594
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
  `nih-nci-dceg-connect-dev.FlatConnect.experience2024_JP` -- destination_table
  OPTIONS (description="Source table: Connect.experience2024; Scheduled Query: FlatConnect.experience2024_JP; GitHub: https://github.com/Analyticsphere/flatteningRequests/tree/main/queryGenerators/FlatConnect_queries/experience2024; Team: Analytics; Maintainer: Jake Peters; Super Users: Kelsey, Jing; Notes: This table is a flattened version of Connect.experience2024.") -- table_description
  AS (
  WITH
    json_data AS (
    SELECT
      [handleRow(TO_JSON_STRING(input_row))] AS body
    FROM
      `nih-nci-dceg-connect-dev.Connect.experience2024` AS input_row -- source_table
    WHERE Connect_ID IS NOT NULL), -- filter_statement
    flattened_data AS (
    SELECT
      	REPLACE(JSON_QUERY(row,'$.Connect_ID'), '\"', '') AS Connect_ID,
	REPLACE(JSON_QUERY(row,'$.D_124830305'), '\"', '') AS D_124830305,
	REPLACE(JSON_QUERY(row,'$.D_145727599.d_153962804'), '\"', '') AS D_145727599_d_153962804,
	REPLACE(JSON_QUERY(row,'$.D_145727599.d_188067494'), '\"', '') AS D_145727599_d_188067494,
	REPLACE(JSON_QUERY(row,'$.D_145727599.d_386252749'), '\"', '') AS D_145727599_d_386252749,
	REPLACE(JSON_QUERY(row,'$.D_176469609'), '\"', '') AS D_176469609,
	REPLACE(JSON_QUERY(row,'$.D_210120853.d_191667117'), '\"', '') AS D_210120853_d_191667117,
	REPLACE(JSON_QUERY(row,'$.D_210120853.d_217279879'), '\"', '') AS D_210120853_d_217279879,
	REPLACE(JSON_QUERY(row,'$.D_210120853.d_299631230'), '\"', '') AS D_210120853_d_299631230,
	REPLACE(JSON_QUERY(row,'$.D_210120853.d_572976454'), '\"', '') AS D_210120853_d_572976454,
	REPLACE(JSON_QUERY(row,'$.D_210120853.d_916319911'), '\"', '') AS D_210120853_d_916319911,
	REPLACE(JSON_QUERY(row,'$.D_210983898.D_210983898.d_334491616'), '\"', '') AS D_210983898_D_210983898_d_334491616,
	REPLACE(JSON_QUERY(row,'$.D_210983898.D_766069692'), '\"', '') AS D_210983898_D_766069692,
	REPLACE(JSON_QUERY(row,'$.D_228922812.D_228922812.d_334491616'), '\"', '') AS D_228922812_D_228922812_d_334491616,
	REPLACE(JSON_QUERY(row,'$.D_228922812.D_587755868'), '\"', '') AS D_228922812_D_587755868,
	REPLACE(JSON_QUERY(row,'$.D_260186214'), '\"', '') AS D_260186214,
	REPLACE(JSON_QUERY(row,'$.D_307813936'), '\"', '') AS D_307813936,
	REPLACE(JSON_QUERY(row,'$.D_355080680.D_355080680.d_334491616'), '\"', '') AS D_355080680_D_355080680_d_334491616,
	REPLACE(JSON_QUERY(row,'$.D_355080680.D_607064749'), '\"', '') AS D_355080680_D_607064749,
	REPLACE(JSON_QUERY(row,'$.D_363878103.D_177096151'), '\"', '') AS D_363878103_D_177096151,
	REPLACE(JSON_QUERY(row,'$.D_363878103.D_363878103.d_334491616'), '\"', '') AS D_363878103_D_363878103_d_334491616,
	REPLACE(JSON_QUERY(row,'$.D_394328384.D_394328384'), '\"', '') AS D_394328384_D_394328384,
	REPLACE(JSON_QUERY(row,'$.D_394328384.D_887117937'), '\"', '') AS D_394328384_D_887117937,
	REPLACE(JSON_QUERY(row,'$.D_403175318.D_403175318.d_178420302'), '\"', '') AS D_403175318_D_403175318_d_178420302,
	REPLACE(JSON_QUERY(row,'$.D_403175318.D_403175318.d_200962909'), '\"', '') AS D_403175318_D_403175318_d_200962909,
	REPLACE(JSON_QUERY(row,'$.D_403175318.D_403175318.d_554920493'), '\"', '') AS D_403175318_D_403175318_d_554920493,
	REPLACE(JSON_QUERY(row,'$.D_403175318.D_403175318.d_798682161'), '\"', '') AS D_403175318_D_403175318_d_798682161,
	REPLACE(JSON_QUERY(row,'$.D_403175318.D_403175318.d_807835037'), '\"', '') AS D_403175318_D_403175318_d_807835037,
	REPLACE(JSON_QUERY(row,'$.D_403175318.D_507471937'), '\"', '') AS D_403175318_D_507471937,
	REPLACE(JSON_QUERY(row,'$.D_465287908'), '\"', '') AS D_465287908,
	REPLACE(JSON_QUERY(row,'$.D_470013848.D_470013848.d_178420302'), '\"', '') AS D_470013848_D_470013848_d_178420302,
	REPLACE(JSON_QUERY(row,'$.D_470013848.D_470013848.d_358413399'), '\"', '') AS D_470013848_D_470013848_d_358413399,
	REPLACE(JSON_QUERY(row,'$.D_470013848.D_470013848.d_482753957'), '\"', '') AS D_470013848_D_470013848_d_482753957,
	REPLACE(JSON_QUERY(row,'$.D_470013848.D_470013848.d_807835037'), '\"', '') AS D_470013848_D_470013848_d_807835037,
	REPLACE(JSON_QUERY(row,'$.D_470013848.D_495052121'), '\"', '') AS D_470013848_D_495052121,
	REPLACE(JSON_QUERY(row,'$.D_472709337'), '\"', '') AS D_472709337,
	REPLACE(JSON_QUERY(row,'$.D_476960744'), '\"', '') AS D_476960744,
	REPLACE(JSON_QUERY(row,'$.D_482763096'), '\"', '') AS D_482763096,
	REPLACE(JSON_QUERY(row,'$.D_496748977.D_492806629'), '\"', '') AS D_496748977_D_492806629,
	REPLACE(JSON_QUERY(row,'$.D_496748977.D_496748977'), '\"', '') AS D_496748977_D_496748977,
	REPLACE(JSON_QUERY(row,'$.D_549970786.D_549970786.d_334491616'), '\"', '') AS D_549970786_D_549970786_d_334491616,
	REPLACE(JSON_QUERY(row,'$.D_549970786.D_733294715'), '\"', '') AS D_549970786_D_733294715,
	REPLACE(JSON_QUERY(row,'$.D_575160226.D_108849491'), '\"', '') AS D_575160226_D_108849491,
	REPLACE(JSON_QUERY(row,'$.D_575160226.D_575160226.d_334491616'), '\"', '') AS D_575160226_D_575160226_d_334491616,
	REPLACE(JSON_QUERY(row,'$.D_579939641.D_303566606'), '\"', '') AS D_579939641_D_303566606,
	REPLACE(JSON_QUERY(row,'$.D_579939641.D_579939641.d_334491616'), '\"', '') AS D_579939641_D_579939641_d_334491616,
	REPLACE(JSON_QUERY(row,'$.D_586132480'), '\"', '') AS D_586132480,
	REPLACE(JSON_QUERY(row,'$.D_630940888'), '\"', '') AS D_630940888,
	REPLACE(JSON_QUERY(row,'$.D_646060480'), '\"', '') AS D_646060480,
	REPLACE(JSON_QUERY(row,'$.D_649713579.d_147611720'), '\"', '') AS D_649713579_d_147611720,
	REPLACE(JSON_QUERY(row,'$.D_649713579.d_354508982'), '\"', '') AS D_649713579_d_354508982,
	REPLACE(JSON_QUERY(row,'$.D_649713579.d_588555669'), '\"', '') AS D_649713579_d_588555669,
	REPLACE(JSON_QUERY(row,'$.D_649713579.d_834694858'), '\"', '') AS D_649713579_d_834694858,
	REPLACE(JSON_QUERY(row,'$.D_649713579.d_899315984'), '\"', '') AS D_649713579_d_899315984,
	REPLACE(JSON_QUERY(row,'$.D_674994176.D_124300201'), '\"', '') AS D_674994176_D_124300201,
	REPLACE(JSON_QUERY(row,'$.D_674994176.D_674994176.d_334491616'), '\"', '') AS D_674994176_D_674994176_d_334491616,
	REPLACE(JSON_QUERY(row,'$.D_731524314.D_638847244'), '\"', '') AS D_731524314_D_638847244,
	REPLACE(JSON_QUERY(row,'$.D_731524314.D_731524314.d_115959973'), '\"', '') AS D_731524314_D_731524314_d_115959973,
	REPLACE(JSON_QUERY(row,'$.D_731524314.D_731524314.d_132115595'), '\"', '') AS D_731524314_D_731524314_d_132115595,
	REPLACE(JSON_QUERY(row,'$.D_731524314.D_731524314.d_178420302'), '\"', '') AS D_731524314_D_731524314_d_178420302,
	REPLACE(JSON_QUERY(row,'$.D_731524314.D_731524314.d_238237869'), '\"', '') AS D_731524314_D_731524314_d_238237869,
	REPLACE(JSON_QUERY(row,'$.D_731524314.D_731524314.d_299722216'), '\"', '') AS D_731524314_D_731524314_d_299722216,
	REPLACE(JSON_QUERY(row,'$.D_731524314.D_731524314.d_515798638'), '\"', '') AS D_731524314_D_731524314_d_515798638,
	REPLACE(JSON_QUERY(row,'$.D_731524314.D_731524314.d_807835037'), '\"', '') AS D_731524314_D_731524314_d_807835037,
	REPLACE(JSON_QUERY(row,'$.D_731524314.D_731524314.d_985034149'), '\"', '') AS D_731524314_D_731524314_d_985034149,
	REPLACE(JSON_QUERY(row,'$.d_784119588'), '\"', '') AS d_784119588,
	REPLACE(JSON_QUERY(row,'$.D_800057241'), '\"', '') AS D_800057241,
	REPLACE(JSON_QUERY(row,'$.D_875017278'), '\"', '') AS D_875017278,
	REPLACE(JSON_QUERY(row,'$.D_886084185.D_126388230'), '\"', '') AS D_886084185_D_126388230,
	REPLACE(JSON_QUERY(row,'$.D_886084185.D_886084185'), '\"', '') AS D_886084185_D_886084185,
	REPLACE(JSON_QUERY(row,'$.D_890945599'), '\"', '') AS D_890945599,
	REPLACE(JSON_QUERY(row,'$.d_905236593'), '\"', '') AS d_905236593,
	REPLACE(JSON_QUERY(row,'$.D_943119849.D_943119849'), '\"', '') AS D_943119849_D_943119849,
	REPLACE(JSON_QUERY(row,'$.D_945546878'), '\"', '') AS D_945546878,
	REPLACE(JSON_QUERY(row,'$.D_956625094'), '\"', '') AS D_956625094,
	REPLACE(JSON_QUERY(row,'$.D_960544981.d_101837333'), '\"', '') AS D_960544981_d_101837333,
	REPLACE(JSON_QUERY(row,'$.D_960544981.d_313446770'), '\"', '') AS D_960544981_d_313446770,
	REPLACE(JSON_QUERY(row,'$.D_960544981.d_393996571'), '\"', '') AS D_960544981_d_393996571,
	REPLACE(JSON_QUERY(row,'$.D_960544981.d_604524950'), '\"', '') AS D_960544981_d_604524950,
	REPLACE(JSON_QUERY(row,'$.D_960544981.d_815468840'), '\"', '') AS D_960544981_d_815468840,
	REPLACE(JSON_QUERY(row,'$.D_960544981.d_925993577'), '\"', '') AS D_960544981_d_925993577,
	REPLACE(JSON_QUERY(row,'$.D_960544981.d_985468594'), '\"', '') AS D_960544981_d_985468594,
	REPLACE(JSON_QUERY(row,'$.D_972944307'), '\"', '') AS D_972944307,
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
    

