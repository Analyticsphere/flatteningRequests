-- FYI: This query was automatically generated using R code written by Jake 
-- Peters. The code references a the source table schema and a configuration
-- file specifying various parameters. This style of query was developed by 
-- Warren Lu to flatten the Connect4Cancer datasets in 2022. See GitHub repo
-- for further documentation.
-- 
-- Repository: https://github.com/Analyticsphere/flatteningRequests
-- Relavent functions: generate_flattening_query.R
-- 
-- source_table: nih-nci-dceg-connect-stg-5519.Connect.cancerScreeningHistorySurvey
-- destination table: nih-nci-dceg-connect-stg-5519.FlatConnect.cancerScreeningHistorySurvey_JP -- notes
    
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
    "D_124071431": [
        160709137
    ],
    "D_219076879": [
        309925717,
        535003378,
        875157431
    ],
    "D_229764995": [
        535003378
    ],
    "D_356470898": [
        535003378
    ],
    "D_365148290": [
        203210013,
        219088015,
        283265038,
        535003378
    ],
    "D_465056475": [
        535003378
    ],
    "D_506456253.D_506456253": [
        256196714,
        325506683,
        335563082,
        492902023,
        520432394,
        667901971,
        752953170,
        802859122,
        807835037,
        955881350
    ],
    "D_729984625": [
        118789503,
        209101810,
        215525943,
        220755749,
        413734739,
        465318416,
        532603425,
        533491176,
        630100221,
        633546100,
        654450030,
        692881833,
        715563991,
        733236542,
        797189152,
        866002271,
        961987554
    ],
    "D_890391968": [
        108025529,
        434651539,
        505282171,
        578416151,
        582784267,
        700100953,
        751402477,
        846483618
    ],
    "D_957875649": [
        535003378
    ],
    "D_961267001": [
        419571068,
        535003378,
        577887061,
        726183532,
        898042848
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
  `nih-nci-dceg-connect-stg-5519.FlatConnect.cancerScreeningHistorySurvey_JP` -- destination_table
  OPTIONS (description="Source table: Connect.cancerScreeningHistorySurvey; Scheduled Query: FlatConnect.cancerScreeningHistorySurvey_JP; GitHub: https://github.com/Analyticsphere/flatteningRequests/tree/main/queryGenerators/FlatConnect_queries/cancerScreeningHistorySurvey; Team: Analytics; Maintainer: Jake Peters; Super Users: Kelsey; Notes: This table is a flattened version of Connect.cancerScreeningHistorySurvey.") -- table_description
  AS (
  WITH
    json_data AS (
    SELECT
      [handleRow(TO_JSON_STRING(input_row))] AS body
    FROM
      `nih-nci-dceg-connect-stg-5519.Connect.cancerScreeningHistorySurvey` AS input_row -- source_table
    WHERE Connect_ID IS NOT NULL), -- filter_statement
    flattened_data AS (
    SELECT
      	REPLACE(JSON_QUERY(row,'$.Connect_ID'), '\"', '') AS Connect_ID,
	REPLACE(JSON_QUERY(row,'$.D_111580947'), '\"', '') AS D_111580947,
	REPLACE(JSON_QUERY(row,'$.D_116546181.D_623218391'), '\"', '') AS D_116546181_D_623218391,
	REPLACE(JSON_QUERY(row,'$.D_116546181.D_802622485'), '\"', '') AS D_116546181_D_802622485,
	REPLACE(JSON_QUERY(row,'$.D_124071431.d_160709137'), '\"', '') AS D_124071431_d_160709137,
	REPLACE(JSON_QUERY(row,'$.D_129025755'), '\"', '') AS D_129025755,
	REPLACE(JSON_QUERY(row,'$.D_166524009.D_623218391'), '\"', '') AS D_166524009_D_623218391,
	REPLACE(JSON_QUERY(row,'$.D_166524009.D_802622485'), '\"', '') AS D_166524009_D_802622485,
	REPLACE(JSON_QUERY(row,'$.D_203366781.D_623218391'), '\"', '') AS D_203366781_D_623218391,
	REPLACE(JSON_QUERY(row,'$.D_203366781.D_802622485'), '\"', '') AS D_203366781_D_802622485,
	REPLACE(JSON_QUERY(row,'$.D_219076879.d_309925717'), '\"', '') AS D_219076879_d_309925717,
	REPLACE(JSON_QUERY(row,'$.D_219076879.d_535003378'), '\"', '') AS D_219076879_d_535003378,
	REPLACE(JSON_QUERY(row,'$.D_219076879.d_875157431'), '\"', '') AS D_219076879_d_875157431,
	REPLACE(JSON_QUERY(row,'$.D_221189280'), '\"', '') AS D_221189280,
	REPLACE(JSON_QUERY(row,'$.D_229764995.d_535003378'), '\"', '') AS D_229764995_d_535003378,
	REPLACE(JSON_QUERY(row,'$.D_236655642'), '\"', '') AS D_236655642,
	REPLACE(JSON_QUERY(row,'$.D_250003172.D_623218391'), '\"', '') AS D_250003172_D_623218391,
	REPLACE(JSON_QUERY(row,'$.D_250003172.D_802622485'), '\"', '') AS D_250003172_D_802622485,
	REPLACE(JSON_QUERY(row,'$.D_250353250.D_623218391'), '\"', '') AS D_250353250_D_623218391,
	REPLACE(JSON_QUERY(row,'$.D_250353250.D_802622485'), '\"', '') AS D_250353250_D_802622485,
	REPLACE(JSON_QUERY(row,'$.D_277890744.D_623218391'), '\"', '') AS D_277890744_D_623218391,
	REPLACE(JSON_QUERY(row,'$.D_277890744.D_802622485'), '\"', '') AS D_277890744_D_802622485,
	REPLACE(JSON_QUERY(row,'$.D_292314539'), '\"', '') AS D_292314539,
	REPLACE(JSON_QUERY(row,'$.D_305769587'), '\"', '') AS D_305769587,
	REPLACE(JSON_QUERY(row,'$.D_314345103.D_623218391'), '\"', '') AS D_314345103_D_623218391,
	REPLACE(JSON_QUERY(row,'$.D_314345103.D_802622485'), '\"', '') AS D_314345103_D_802622485,
	REPLACE(JSON_QUERY(row,'$.D_315184598'), '\"', '') AS D_315184598,
	REPLACE(JSON_QUERY(row,'$.D_341080024.D_623218391'), '\"', '') AS D_341080024_D_623218391,
	REPLACE(JSON_QUERY(row,'$.D_341080024.D_802622485'), '\"', '') AS D_341080024_D_802622485,
	REPLACE(JSON_QUERY(row,'$.D_343990595.D_623218391'), '\"', '') AS D_343990595_D_623218391,
	REPLACE(JSON_QUERY(row,'$.D_343990595.D_802622485'), '\"', '') AS D_343990595_D_802622485,
	REPLACE(JSON_QUERY(row,'$.D_350184001.D_623218391'), '\"', '') AS D_350184001_D_623218391,
	REPLACE(JSON_QUERY(row,'$.D_350184001.D_802622485'), '\"', '') AS D_350184001_D_802622485,
	REPLACE(JSON_QUERY(row,'$.d_350996955'), '\"', '') AS d_350996955,
	REPLACE(JSON_QUERY(row,'$.D_356470898.d_535003378'), '\"', '') AS D_356470898_d_535003378,
	REPLACE(JSON_QUERY(row,'$.D_365148290.d_203210013'), '\"', '') AS D_365148290_d_203210013,
	REPLACE(JSON_QUERY(row,'$.D_365148290.d_219088015'), '\"', '') AS D_365148290_d_219088015,
	REPLACE(JSON_QUERY(row,'$.D_365148290.d_283265038'), '\"', '') AS D_365148290_d_283265038,
	REPLACE(JSON_QUERY(row,'$.D_365148290.d_535003378'), '\"', '') AS D_365148290_d_535003378,
	REPLACE(JSON_QUERY(row,'$.D_367942006.D_623218391'), '\"', '') AS D_367942006_D_623218391,
	REPLACE(JSON_QUERY(row,'$.D_367942006.D_802622485'), '\"', '') AS D_367942006_D_802622485,
	REPLACE(JSON_QUERY(row,'$.D_372457715'), '\"', '') AS D_372457715,
	REPLACE(JSON_QUERY(row,'$.D_397748152'), '\"', '') AS D_397748152,
	REPLACE(JSON_QUERY(row,'$.D_413039729'), '\"', '') AS D_413039729,
	REPLACE(JSON_QUERY(row,'$.D_429157727'), '\"', '') AS D_429157727,
	REPLACE(JSON_QUERY(row,'$.D_431721131'), '\"', '') AS D_431721131,
	REPLACE(JSON_QUERY(row,'$.D_434993234'), '\"', '') AS D_434993234,
	REPLACE(JSON_QUERY(row,'$.D_439760665.D_623218391'), '\"', '') AS D_439760665_D_623218391,
	REPLACE(JSON_QUERY(row,'$.D_439760665.D_802622485'), '\"', '') AS D_439760665_D_802622485,
	REPLACE(JSON_QUERY(row,'$.D_459223565'), '\"', '') AS D_459223565,
	REPLACE(JSON_QUERY(row,'$.D_465056475.d_535003378'), '\"', '') AS D_465056475_d_535003378,
	REPLACE(JSON_QUERY(row,'$.D_496928179.D_623218391'), '\"', '') AS D_496928179_D_623218391,
	REPLACE(JSON_QUERY(row,'$.D_496928179.D_802622485'), '\"', '') AS D_496928179_D_802622485,
	REPLACE(JSON_QUERY(row,'$.D_506456253.D_506456253.d_256196714'), '\"', '') AS D_506456253_D_506456253_d_256196714,
	REPLACE(JSON_QUERY(row,'$.D_506456253.D_506456253.d_325506683'), '\"', '') AS D_506456253_D_506456253_d_325506683,
	REPLACE(JSON_QUERY(row,'$.D_506456253.D_506456253.d_335563082'), '\"', '') AS D_506456253_D_506456253_d_335563082,
	REPLACE(JSON_QUERY(row,'$.D_506456253.D_506456253.d_492902023'), '\"', '') AS D_506456253_D_506456253_d_492902023,
	REPLACE(JSON_QUERY(row,'$.D_506456253.D_506456253.d_520432394'), '\"', '') AS D_506456253_D_506456253_d_520432394,
	REPLACE(JSON_QUERY(row,'$.D_506456253.D_506456253.d_667901971'), '\"', '') AS D_506456253_D_506456253_d_667901971,
	REPLACE(JSON_QUERY(row,'$.D_506456253.D_506456253.d_752953170'), '\"', '') AS D_506456253_D_506456253_d_752953170,
	REPLACE(JSON_QUERY(row,'$.D_506456253.D_506456253.d_802859122'), '\"', '') AS D_506456253_D_506456253_d_802859122,
	REPLACE(JSON_QUERY(row,'$.D_506456253.D_506456253.d_807835037'), '\"', '') AS D_506456253_D_506456253_d_807835037,
	REPLACE(JSON_QUERY(row,'$.D_506456253.D_506456253.d_955881350'), '\"', '') AS D_506456253_D_506456253_d_955881350,
	REPLACE(JSON_QUERY(row,'$.D_506456253.D_942347130'), '\"', '') AS D_506456253_D_942347130,
	REPLACE(JSON_QUERY(row,'$.D_522645663.D_623218391'), '\"', '') AS D_522645663_D_623218391,
	REPLACE(JSON_QUERY(row,'$.D_522645663.D_802622485'), '\"', '') AS D_522645663_D_802622485,
	REPLACE(JSON_QUERY(row,'$.D_524918714'), '\"', '') AS D_524918714,
	REPLACE(JSON_QUERY(row,'$.D_541338917'), '\"', '') AS D_541338917,
	REPLACE(JSON_QUERY(row,'$.D_552399966'), '\"', '') AS D_552399966,
	REPLACE(JSON_QUERY(row,'$.D_553034408'), '\"', '') AS D_553034408,
	REPLACE(JSON_QUERY(row,'$.D_560107274'), '\"', '') AS D_560107274,
	REPLACE(JSON_QUERY(row,'$.D_569358384'), '\"', '') AS D_569358384,
	REPLACE(JSON_QUERY(row,'$.D_584375130'), '\"', '') AS D_584375130,
	REPLACE(JSON_QUERY(row,'$.D_588828383'), '\"', '') AS D_588828383,
	REPLACE(JSON_QUERY(row,'$.D_590067228'), '\"', '') AS D_590067228,
	REPLACE(JSON_QUERY(row,'$.D_619830872'), '\"', '') AS D_619830872,
	REPLACE(JSON_QUERY(row,'$.D_645149364.D_623218391'), '\"', '') AS D_645149364_D_623218391,
	REPLACE(JSON_QUERY(row,'$.D_645149364.D_802622485'), '\"', '') AS D_645149364_D_802622485,
	REPLACE(JSON_QUERY(row,'$.D_671670265.D_623218391'), '\"', '') AS D_671670265_D_623218391,
	REPLACE(JSON_QUERY(row,'$.D_671670265.D_802622485'), '\"', '') AS D_671670265_D_802622485,
	REPLACE(JSON_QUERY(row,'$.D_675055475.D_623218391'), '\"', '') AS D_675055475_D_623218391,
	REPLACE(JSON_QUERY(row,'$.D_675055475.D_802622485'), '\"', '') AS D_675055475_D_802622485,
	REPLACE(JSON_QUERY(row,'$.D_695772621'), '\"', '') AS D_695772621,
	REPLACE(JSON_QUERY(row,'$.D_698242759'), '\"', '') AS D_698242759,
	REPLACE(JSON_QUERY(row,'$.D_702922820'), '\"', '') AS D_702922820,
	REPLACE(JSON_QUERY(row,'$.D_709088568'), '\"', '') AS D_709088568,
	REPLACE(JSON_QUERY(row,'$.D_729390410'), '\"', '') AS D_729390410,
	REPLACE(JSON_QUERY(row,'$.D_729984625.d_118789503'), '\"', '') AS D_729984625_d_118789503,
	REPLACE(JSON_QUERY(row,'$.D_729984625.d_209101810'), '\"', '') AS D_729984625_d_209101810,
	REPLACE(JSON_QUERY(row,'$.D_729984625.d_215525943'), '\"', '') AS D_729984625_d_215525943,
	REPLACE(JSON_QUERY(row,'$.D_729984625.d_220755749'), '\"', '') AS D_729984625_d_220755749,
	REPLACE(JSON_QUERY(row,'$.D_729984625.d_413734739'), '\"', '') AS D_729984625_d_413734739,
	REPLACE(JSON_QUERY(row,'$.D_729984625.d_465318416'), '\"', '') AS D_729984625_d_465318416,
	REPLACE(JSON_QUERY(row,'$.D_729984625.d_532603425'), '\"', '') AS D_729984625_d_532603425,
	REPLACE(JSON_QUERY(row,'$.D_729984625.d_533491176'), '\"', '') AS D_729984625_d_533491176,
	REPLACE(JSON_QUERY(row,'$.D_729984625.d_630100221'), '\"', '') AS D_729984625_d_630100221,
	REPLACE(JSON_QUERY(row,'$.D_729984625.d_633546100'), '\"', '') AS D_729984625_d_633546100,
	REPLACE(JSON_QUERY(row,'$.D_729984625.d_654450030'), '\"', '') AS D_729984625_d_654450030,
	REPLACE(JSON_QUERY(row,'$.D_729984625.d_692881833'), '\"', '') AS D_729984625_d_692881833,
	REPLACE(JSON_QUERY(row,'$.D_729984625.d_715563991'), '\"', '') AS D_729984625_d_715563991,
	REPLACE(JSON_QUERY(row,'$.D_729984625.d_733236542'), '\"', '') AS D_729984625_d_733236542,
	REPLACE(JSON_QUERY(row,'$.D_729984625.d_797189152'), '\"', '') AS D_729984625_d_797189152,
	REPLACE(JSON_QUERY(row,'$.D_729984625.d_866002271'), '\"', '') AS D_729984625_d_866002271,
	REPLACE(JSON_QUERY(row,'$.D_729984625.d_961987554'), '\"', '') AS D_729984625_d_961987554,
	REPLACE(JSON_QUERY(row,'$.D_745742553'), '\"', '') AS D_745742553,
	REPLACE(JSON_QUERY(row,'$.D_758881441.D_623218391'), '\"', '') AS D_758881441_D_623218391,
	REPLACE(JSON_QUERY(row,'$.D_758881441.D_802622485'), '\"', '') AS D_758881441_D_802622485,
	REPLACE(JSON_QUERY(row,'$.D_761831751'), '\"', '') AS D_761831751,
	REPLACE(JSON_QUERY(row,'$.D_765772509'), '\"', '') AS D_765772509,
	REPLACE(JSON_QUERY(row,'$.d_784119588'), '\"', '') AS d_784119588,
	REPLACE(JSON_QUERY(row,'$.D_787505819.D_623218391'), '\"', '') AS D_787505819_D_623218391,
	REPLACE(JSON_QUERY(row,'$.D_787505819.D_802622485'), '\"', '') AS D_787505819_D_802622485,
	REPLACE(JSON_QUERY(row,'$.D_802157786'), '\"', '') AS D_802157786,
	REPLACE(JSON_QUERY(row,'$.D_811848389'), '\"', '') AS D_811848389,
	REPLACE(JSON_QUERY(row,'$.D_822790675'), '\"', '') AS D_822790675,
	REPLACE(JSON_QUERY(row,'$.D_839555631.D_623218391'), '\"', '') AS D_839555631_D_623218391,
	REPLACE(JSON_QUERY(row,'$.D_839555631.D_802622485'), '\"', '') AS D_839555631_D_802622485,
	REPLACE(JSON_QUERY(row,'$.D_841573821.D_623218391'), '\"', '') AS D_841573821_D_623218391,
	REPLACE(JSON_QUERY(row,'$.D_841573821.D_802622485'), '\"', '') AS D_841573821_D_802622485,
	REPLACE(JSON_QUERY(row,'$.D_846067697'), '\"', '') AS D_846067697,
	REPLACE(JSON_QUERY(row,'$.D_852697610'), '\"', '') AS D_852697610,
	REPLACE(JSON_QUERY(row,'$.D_860253133'), '\"', '') AS D_860253133,
	REPLACE(JSON_QUERY(row,'$.D_860839849'), '\"', '') AS D_860839849,
	REPLACE(JSON_QUERY(row,'$.D_861488320'), '\"', '') AS D_861488320,
	REPLACE(JSON_QUERY(row,'$.D_869938650'), '\"', '') AS D_869938650,
	REPLACE(JSON_QUERY(row,'$.D_884856351'), '\"', '') AS D_884856351,
	REPLACE(JSON_QUERY(row,'$.D_890391968.d_108025529'), '\"', '') AS D_890391968_d_108025529,
	REPLACE(JSON_QUERY(row,'$.D_890391968.d_434651539'), '\"', '') AS D_890391968_d_434651539,
	REPLACE(JSON_QUERY(row,'$.D_890391968.d_505282171'), '\"', '') AS D_890391968_d_505282171,
	REPLACE(JSON_QUERY(row,'$.D_890391968.d_578416151'), '\"', '') AS D_890391968_d_578416151,
	REPLACE(JSON_QUERY(row,'$.D_890391968.d_582784267'), '\"', '') AS D_890391968_d_582784267,
	REPLACE(JSON_QUERY(row,'$.D_890391968.d_700100953'), '\"', '') AS D_890391968_d_700100953,
	REPLACE(JSON_QUERY(row,'$.D_890391968.d_751402477'), '\"', '') AS D_890391968_d_751402477,
	REPLACE(JSON_QUERY(row,'$.D_890391968.d_846483618'), '\"', '') AS D_890391968_d_846483618,
	REPLACE(JSON_QUERY(row,'$.D_891793989'), '\"', '') AS D_891793989,
	REPLACE(JSON_QUERY(row,'$.D_900561740'), '\"', '') AS D_900561740,
	REPLACE(JSON_QUERY(row,'$.D_907385951.D_623218391'), '\"', '') AS D_907385951_D_623218391,
	REPLACE(JSON_QUERY(row,'$.D_907385951.D_802622485'), '\"', '') AS D_907385951_D_802622485,
	REPLACE(JSON_QUERY(row,'$.D_936444833.D_623218391'), '\"', '') AS D_936444833_D_623218391,
	REPLACE(JSON_QUERY(row,'$.D_936444833.D_802622485'), '\"', '') AS D_936444833_D_802622485,
	REPLACE(JSON_QUERY(row,'$.D_949587133.D_623218391'), '\"', '') AS D_949587133_D_623218391,
	REPLACE(JSON_QUERY(row,'$.D_949587133.D_802622485'), '\"', '') AS D_949587133_D_802622485,
	REPLACE(JSON_QUERY(row,'$.D_950690103.D_623218391'), '\"', '') AS D_950690103_D_623218391,
	REPLACE(JSON_QUERY(row,'$.D_950690103.D_802622485'), '\"', '') AS D_950690103_D_802622485,
	REPLACE(JSON_QUERY(row,'$.D_951847507'), '\"', '') AS D_951847507,
	REPLACE(JSON_QUERY(row,'$.D_957875649.d_535003378'), '\"', '') AS D_957875649_d_535003378,
	REPLACE(JSON_QUERY(row,'$.D_961267001.d_419571068'), '\"', '') AS D_961267001_d_419571068,
	REPLACE(JSON_QUERY(row,'$.D_961267001.d_535003378'), '\"', '') AS D_961267001_d_535003378,
	REPLACE(JSON_QUERY(row,'$.D_961267001.d_577887061'), '\"', '') AS D_961267001_d_577887061,
	REPLACE(JSON_QUERY(row,'$.D_961267001.d_726183532'), '\"', '') AS D_961267001_d_726183532,
	REPLACE(JSON_QUERY(row,'$.D_961267001.d_898042848'), '\"', '') AS D_961267001_d_898042848,
	REPLACE(JSON_QUERY(row,'$.D_975268706.D_623218391'), '\"', '') AS D_975268706_D_623218391,
	REPLACE(JSON_QUERY(row,'$.D_975268706.D_802622485'), '\"', '') AS D_975268706_D_802622485,
	REPLACE(JSON_QUERY(row,'$.D_976626196.D_623218391'), '\"', '') AS D_976626196_D_623218391,
	REPLACE(JSON_QUERY(row,'$.D_976626196.D_802622485'), '\"', '') AS D_976626196_D_802622485,
	REPLACE(JSON_QUERY(row,'$.D_988661374'), '\"', '') AS D_988661374,
	REPLACE(JSON_QUERY(row,'$.D_991871399'), '\"', '') AS D_991871399,
	REPLACE(JSON_QUERY(row,'$.D_998519118.D_623218391'), '\"', '') AS D_998519118_D_623218391,
	REPLACE(JSON_QUERY(row,'$.D_998519118.D_802622485'), '\"', '') AS D_998519118_D_802622485,
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
    

