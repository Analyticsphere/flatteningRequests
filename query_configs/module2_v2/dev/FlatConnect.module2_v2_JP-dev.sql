-- FYI: This query was automatically generated using R code written by Jake 
-- Peters. The code references a the source table schema and a configuration
-- file specifying various parameters. This style of query was developed by 
-- Warren Lu to flatten the Connect4Cancer datasets in 2022. See GitHub repo
-- for further documentation.
-- 
-- Repository: https://github.com/Analyticsphere/flatteningRequests
-- Relavent functions: generate_flattening_query.R
-- 
-- source_table: nih-nci-dceg-connect-dev.Connect.module2_v2
-- destination table: nih-nci-dceg-connect-dev.FlatConnect.module2_v2_JP -- notes
    
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
    "D_204421360": [
        535003378
    ],
    "D_399982309": [
        555555502
    ],
    "D_479628567": [
        535003378
    ],
    "D_517976064": [
        181769837,
        904954920,
        936042582
    ],
    "D_543780863": [
        535003378
    ],
    "D_547633480": [
        535003378
    ],
    "D_614123836": [
        535003378
    ],
    "D_630231395": [
        191656389,
        243596698,
        727773491,
        769901710
    ],
    "D_643361372": [
        535003378
    ],
    "D_845164425": [
        249341444,
        917302906
    ],
    "D_894610280": [
        152773041,
        249341444
    ],
    "D_895837106": [
        152773041
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
  `nih-nci-dceg-connect-dev.FlatConnect.module2_v2_JP` -- destination_table
  OPTIONS (description="Source table: Connect.module2_v2; Scheduled Query: FlatConnect.module2_v2_JP; GitHub: https://github.com/Analyticsphere/flatteningRequests/tree/main/queryGenerators/FlatConnect_queries/module2_v2; Team: Analytics; Maintainer: Jake Peters; Super Users: Kelsey; Notes: This table is a flattened version of Connect.module2_v2.") -- table_description
  AS (
  WITH
    json_data AS (
    SELECT
      [handleRow(TO_JSON_STRING(input_row))] AS body
    FROM
      `nih-nci-dceg-connect-dev.Connect.module2_v2` AS input_row -- source_table
    WHERE Connect_ID IS NOT NULL), -- filter_statement
    flattened_data AS (
    SELECT
      	REPLACE(JSON_QUERY(row,'$.Connect_ID'), '\"', '') AS Connect_ID,
	REPLACE(JSON_QUERY(row,'$.D_111082535'), '\"', '') AS D_111082535,
	REPLACE(JSON_QUERY(row,'$.D_112024853'), '\"', '') AS D_112024853,
	REPLACE(JSON_QUERY(row,'$.D_128705365.D_491484323'), '\"', '') AS D_128705365_D_491484323,
	REPLACE(JSON_QUERY(row,'$.D_128705365.D_607323377'), '\"', '') AS D_128705365_D_607323377,
	REPLACE(JSON_QUERY(row,'$.D_128705365.D_986476579'), '\"', '') AS D_128705365_D_986476579,
	REPLACE(JSON_QUERY(row,'$.D_164595895'), '\"', '') AS D_164595895,
	REPLACE(JSON_QUERY(row,'$.D_167997205'), '\"', '') AS D_167997205,
	REPLACE(JSON_QUERY(row,'$.D_168431681'), '\"', '') AS D_168431681,
	REPLACE(JSON_QUERY(row,'$.D_204421360.D_535003378'), '\"', '') AS D_204421360_D_535003378,
	REPLACE(JSON_QUERY(row,'$.D_224681201'), '\"', '') AS D_224681201,
	REPLACE(JSON_QUERY(row,'$.D_267122668'), '\"', '') AS D_267122668,
	REPLACE(JSON_QUERY(row,'$.D_270254670'), '\"', '') AS D_270254670,
	REPLACE(JSON_QUERY(row,'$.D_272119228'), '\"', '') AS D_272119228,
	REPLACE(JSON_QUERY(row,'$.D_354980163'), '\"', '') AS D_354980163,
	REPLACE(JSON_QUERY(row,'$.D_358259099.D_707805344'), '\"', '') AS D_358259099_D_707805344,
	REPLACE(JSON_QUERY(row,'$.D_358259099.D_850393641'), '\"', '') AS D_358259099_D_850393641,
	REPLACE(JSON_QUERY(row,'$.D_392629868'), '\"', '') AS D_392629868,
	REPLACE(JSON_QUERY(row,'$.D_392658845.D_239572239'), '\"', '') AS D_392658845_D_239572239,
	REPLACE(JSON_QUERY(row,'$.D_392658845.D_589522790'), '\"', '') AS D_392658845_D_589522790,
	REPLACE(JSON_QUERY(row,'$.D_392658845.D_652561589'), '\"', '') AS D_392658845_D_652561589,
	REPLACE(JSON_QUERY(row,'$.D_392658845.D_699101535'), '\"', '') AS D_392658845_D_699101535,
	REPLACE(JSON_QUERY(row,'$.D_399982309.D_555555502'), '\"', '') AS D_399982309_D_555555502,
	REPLACE(JSON_QUERY(row,'$.D_414005872'), '\"', '') AS D_414005872,
	REPLACE(JSON_QUERY(row,'$.D_421859821.D_151006826'), '\"', '') AS D_421859821_D_151006826,
	REPLACE(JSON_QUERY(row,'$.D_421859821.D_220238240'), '\"', '') AS D_421859821_D_220238240,
	REPLACE(JSON_QUERY(row,'$.D_421859821.D_257137378'), '\"', '') AS D_421859821_D_257137378,
	REPLACE(JSON_QUERY(row,'$.D_421859821.D_453071098'), '\"', '') AS D_421859821_D_453071098,
	REPLACE(JSON_QUERY(row,'$.D_421859821.D_513065503'), '\"', '') AS D_421859821_D_513065503,
	REPLACE(JSON_QUERY(row,'$.D_421859821.D_781115321'), '\"', '') AS D_421859821_D_781115321,
	REPLACE(JSON_QUERY(row,'$.D_421859821.D_965290070'), '\"', '') AS D_421859821_D_965290070,
	REPLACE(JSON_QUERY(row,'$.D_424807655.D_707805344'), '\"', '') AS D_424807655_D_707805344,
	REPLACE(JSON_QUERY(row,'$.D_424807655.D_850393641'), '\"', '') AS D_424807655_D_850393641,
	REPLACE(JSON_QUERY(row,'$.D_471018082'), '\"', '') AS D_471018082,
	REPLACE(JSON_QUERY(row,'$.D_479628567.D_535003378'), '\"', '') AS D_479628567_D_535003378,
	REPLACE(JSON_QUERY(row,'$.D_496539718.D_969451837'), '\"', '') AS D_496539718_D_969451837,
	REPLACE(JSON_QUERY(row,'$.D_517976064.D_181769837'), '\"', '') AS D_517976064_D_181769837,
	REPLACE(JSON_QUERY(row,'$.D_517976064.D_904954920'), '\"', '') AS D_517976064_D_904954920,
	REPLACE(JSON_QUERY(row,'$.D_517976064.D_936042582'), '\"', '') AS D_517976064_D_936042582,
	REPLACE(JSON_QUERY(row,'$.D_522949496'), '\"', '') AS D_522949496,
	REPLACE(JSON_QUERY(row,'$.D_532767229.D_707805344'), '\"', '') AS D_532767229_D_707805344,
	REPLACE(JSON_QUERY(row,'$.D_534920374'), '\"', '') AS D_534920374,
	REPLACE(JSON_QUERY(row,'$.D_543780863.D_535003378'), '\"', '') AS D_543780863_D_535003378,
	REPLACE(JSON_QUERY(row,'$.D_547633480.D_535003378'), '\"', '') AS D_547633480_D_535003378,
	REPLACE(JSON_QUERY(row,'$.D_580367946'), '\"', '') AS D_580367946,
	REPLACE(JSON_QUERY(row,'$.D_591723682'), '\"', '') AS D_591723682,
	REPLACE(JSON_QUERY(row,'$.D_612068433'), '\"', '') AS D_612068433,
	REPLACE(JSON_QUERY(row,'$.D_614123836.D_535003378'), '\"', '') AS D_614123836_D_535003378,
	REPLACE(JSON_QUERY(row,'$.D_614446276.D_785412329'), '\"', '') AS D_614446276_D_785412329,
	REPLACE(JSON_QUERY(row,'$.D_621744463'), '\"', '') AS D_621744463,
	REPLACE(JSON_QUERY(row,'$.D_630231395.D_191656389'), '\"', '') AS D_630231395_D_191656389,
	REPLACE(JSON_QUERY(row,'$.D_630231395.D_243596698'), '\"', '') AS D_630231395_D_243596698,
	REPLACE(JSON_QUERY(row,'$.D_630231395.D_727773491'), '\"', '') AS D_630231395_D_727773491,
	REPLACE(JSON_QUERY(row,'$.D_630231395.D_769901710'), '\"', '') AS D_630231395_D_769901710,
	REPLACE(JSON_QUERY(row,'$.D_636757313'), '\"', '') AS D_636757313,
	REPLACE(JSON_QUERY(row,'$.D_643361372.D_535003378'), '\"', '') AS D_643361372_D_535003378,
	REPLACE(JSON_QUERY(row,'$.D_646042915.D_101310722'), '\"', '') AS D_646042915_D_101310722,
	REPLACE(JSON_QUERY(row,'$.D_649352910.D_263483736'), '\"', '') AS D_649352910_D_263483736,
	REPLACE(JSON_QUERY(row,'$.D_649352910.D_396649512'), '\"', '') AS D_649352910_D_396649512,
	REPLACE(JSON_QUERY(row,'$.D_649352910.D_654385712'), '\"', '') AS D_649352910_D_654385712,
	REPLACE(JSON_QUERY(row,'$.D_649352910.D_806746224'), '\"', '') AS D_649352910_D_806746224,
	REPLACE(JSON_QUERY(row,'$.D_650405110'), '\"', '') AS D_650405110,
	REPLACE(JSON_QUERY(row,'$.D_653906464.D_707805344'), '\"', '') AS D_653906464_D_707805344,
	REPLACE(JSON_QUERY(row,'$.D_653906464.D_850393641'), '\"', '') AS D_653906464_D_850393641,
	REPLACE(JSON_QUERY(row,'$.D_674976924'), '\"', '') AS D_674976924,
	REPLACE(JSON_QUERY(row,'$.D_698673038'), '\"', '') AS D_698673038,
	REPLACE(JSON_QUERY(row,'$.D_711365027.D_707805344'), '\"', '') AS D_711365027_D_707805344,
	REPLACE(JSON_QUERY(row,'$.D_733547268.D_115422925'), '\"', '') AS D_733547268_D_115422925,
	REPLACE(JSON_QUERY(row,'$.D_733547268.D_151161693'), '\"', '') AS D_733547268_D_151161693,
	REPLACE(JSON_QUERY(row,'$.D_733547268.D_980120253'), '\"', '') AS D_733547268_D_980120253,
	REPLACE(JSON_QUERY(row,'$.D_733547268.D_980196073'), '\"', '') AS D_733547268_D_980196073,
	REPLACE(JSON_QUERY(row,'$.D_733547268.D_993029890'), '\"', '') AS D_733547268_D_993029890,
	REPLACE(JSON_QUERY(row,'$.D_820694957'), '\"', '') AS D_820694957,
	REPLACE(JSON_QUERY(row,'$.D_823541843'), '\"', '') AS D_823541843,
	REPLACE(JSON_QUERY(row,'$.D_823919522'), '\"', '') AS D_823919522,
	REPLACE(JSON_QUERY(row,'$.D_825227968.D_707805344'), '\"', '') AS D_825227968_D_707805344,
	REPLACE(JSON_QUERY(row,'$.D_825227968.D_850393641'), '\"', '') AS D_825227968_D_850393641,
	REPLACE(JSON_QUERY(row,'$.D_845164425.D_249341444'), '\"', '') AS D_845164425_D_249341444,
	REPLACE(JSON_QUERY(row,'$.D_845164425.D_917302906'), '\"', '') AS D_845164425_D_917302906,
	REPLACE(JSON_QUERY(row,'$.D_849945160'), '\"', '') AS D_849945160,
	REPLACE(JSON_QUERY(row,'$.D_861824298'), '\"', '') AS D_861824298,
	REPLACE(JSON_QUERY(row,'$.D_874262904'), '\"', '') AS D_874262904,
	REPLACE(JSON_QUERY(row,'$.D_894610280.D_152773041'), '\"', '') AS D_894610280_D_152773041,
	REPLACE(JSON_QUERY(row,'$.D_894610280.D_249341444'), '\"', '') AS D_894610280_D_249341444,
	REPLACE(JSON_QUERY(row,'$.D_895837106.D_152773041'), '\"', '') AS D_895837106_D_152773041,
	REPLACE(JSON_QUERY(row,'$.D_901660173'), '\"', '') AS D_901660173,
	REPLACE(JSON_QUERY(row,'$.D_910856756'), '\"', '') AS D_910856756,
	REPLACE(JSON_QUERY(row,'$.d_932127832'), '\"', '') AS d_932127832,
	REPLACE(JSON_QUERY(row,'$.D_939897560'), '\"', '') AS D_939897560,
	REPLACE(JSON_QUERY(row,'$.D_940733450'), '\"', '') AS D_940733450,
	REPLACE(JSON_QUERY(row,'$.D_972593606'), '\"', '') AS D_972593606,
	REPLACE(JSON_QUERY(row,'$.D_981441822.D_179705366'), '\"', '') AS D_981441822_D_179705366,
	REPLACE(JSON_QUERY(row,'$.D_981441822.D_186488870'), '\"', '') AS D_981441822_D_186488870,
	REPLACE(JSON_QUERY(row,'$.D_981441822.D_354550061'), '\"', '') AS D_981441822_D_354550061,
	REPLACE(JSON_QUERY(row,'$.D_981441822.D_403155173'), '\"', '') AS D_981441822_D_403155173,
	REPLACE(JSON_QUERY(row,'$.D_981441822.D_576621769'), '\"', '') AS D_981441822_D_576621769,
	REPLACE(JSON_QUERY(row,'$.D_981441822.D_760686611'), '\"', '') AS D_981441822_D_760686611,
	REPLACE(JSON_QUERY(row,'$.D_981441822.D_922388461'), '\"', '') AS D_981441822_D_922388461,
	REPLACE(JSON_QUERY(row,'$.D_981441822.D_955609858'), '\"', '') AS D_981441822_D_955609858,
	REPLACE(JSON_QUERY(row,'$.D_981441822.D_988508028'), '\"', '') AS D_981441822_D_988508028,
	REPLACE(JSON_QUERY(row,'$.D_982448213'), '\"', '') AS D_982448213,
	REPLACE(JSON_QUERY(row,'$.D_996243083'), '\"', '') AS D_996243083,
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
    

