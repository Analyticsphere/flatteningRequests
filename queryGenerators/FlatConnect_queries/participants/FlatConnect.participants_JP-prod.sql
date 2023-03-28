 
  -- FYI: This query was automatically generated using R code written by Jake 
  -- Peters. The code references a the source table schema and a configuration
  -- file specifying various parameters. This style of query was developed by 
  -- Warren Lu to flatten the Connect4Cancer datasets in 2022. See GitHub repo
  -- for further documentation.
  -- 
  -- Repository: https://github.com/Analyticsphere/flatteningRequests
  -- Relavent functions: generate_flattening_query.R
  -- 
  -- source_table: nih-nci-dceg-connect-prod-6d04.Connect.participants
  -- destination table: FlatConnect.participants_JP
   -- notes
    
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
 -- ARRAYS_TO_BE_FLATTENED
  
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
  FlatConnect.participants_JP -- destination_table
  OPTIONS (description="Source table: Connect.participants; Scheduled Query: FlatConnect.participants_JP; GitHub: https://github.com/Analyticsphere/flatteningRequests/tree/main/queryGenerators/FlatConnect_queries/participants; Team: Analytics; Maintainer: Jake Peters; Super Users: Jing, Kelsey; Notes: This table is a flattened version of Connect.participants. It is used for recruitment reporting purposes.") -- table_description
  AS (
  WITH
    json_data AS (
    SELECT
      Connect_ID,
      [handleRow(TO_JSON_STRING(input_row))] AS body
    FROM
      `nih-nci-dceg-connect-prod-6d04.Connect.participants` AS input_row -- source_table
    ), -- filter_statement
    flattened_data AS (
    SELECT
      	REPLACE(JSON_QUERY(row,'$.Connect_ID'), '\"', '') AS Connect_ID,
	REPLACE(JSON_QUERY(row,'$.d_100767870'), '\"', '') AS d_100767870,
	REPLACE(JSON_QUERY(row,'$.d_104278817'), '\"', '') AS d_104278817,
	REPLACE(JSON_QUERY(row,'$.d_113579866'), '\"', '') AS d_113579866,
	REPLACE(JSON_QUERY(row,'$.d_117249500'), '\"', '') AS d_117249500,
	REPLACE(JSON_QUERY(row,'$.d_119449326'), '\"', '') AS d_119449326,
	REPLACE(JSON_QUERY(row,'$.d_121430614'), '\"', '') AS d_121430614,
	REPLACE(JSON_QUERY(row,'$.d_123456789'), '\"', '') AS d_123456789,
	REPLACE(JSON_QUERY(row,'$.d_123868967'), '\"', '') AS d_123868967,
	REPLACE(JSON_QUERY(row,'$.d_126331570'), '\"', '') AS d_126331570,
	REPLACE(JSON_QUERY(row,'$.d_130371375.d_266600170.d_222373868'), '\"', '') AS d_130371375_d_266600170_d_222373868,
	REPLACE(JSON_QUERY(row,'$.d_130371375.d_266600170.d_297462035'), '\"', '') AS d_130371375_d_266600170_d_297462035,
	REPLACE(JSON_QUERY(row,'$.d_130371375.d_266600170.d_320023644'), '\"', '') AS d_130371375_d_266600170_d_320023644,
	REPLACE(JSON_QUERY(row,'$.d_130371375.d_266600170.d_438636757'), '\"', '') AS d_130371375_d_266600170_d_438636757,
	REPLACE(JSON_QUERY(row,'$.d_130371375.d_266600170.d_648228701'), '\"', '') AS d_130371375_d_266600170_d_648228701,
	REPLACE(JSON_QUERY(row,'$.d_130371375.d_266600170.d_648936790'), '\"', '') AS d_130371375_d_266600170_d_648936790,
	REPLACE(JSON_QUERY(row,'$.d_130371375.d_266600170.d_731498909'), '\"', '') AS d_130371375_d_266600170_d_731498909,
	REPLACE(JSON_QUERY(row,'$.d_130371375.d_266600170.d_787567527'), '\"', '') AS d_130371375_d_266600170_d_787567527,
	REPLACE(JSON_QUERY(row,'$.d_130371375.d_266600170.d_862774033'), '\"', '') AS d_130371375_d_266600170_d_862774033,
	REPLACE(JSON_QUERY(row,'$.d_130371375.d_266600170.d_945795905'), '\"', '') AS d_130371375_d_266600170_d_945795905,
	REPLACE(JSON_QUERY(row,'$.d_130371375.d_303552867.d_222373868'), '\"', '') AS d_130371375_d_303552867_d_222373868,
	REPLACE(JSON_QUERY(row,'$.d_130371375.d_303552867.d_297462035'), '\"', '') AS d_130371375_d_303552867_d_297462035,
	REPLACE(JSON_QUERY(row,'$.d_130371375.d_303552867.d_320023644'), '\"', '') AS d_130371375_d_303552867_d_320023644,
	REPLACE(JSON_QUERY(row,'$.d_130371375.d_303552867.d_438636757'), '\"', '') AS d_130371375_d_303552867_d_438636757,
	REPLACE(JSON_QUERY(row,'$.d_130371375.d_303552867.d_648228701'), '\"', '') AS d_130371375_d_303552867_d_648228701,
	REPLACE(JSON_QUERY(row,'$.d_130371375.d_303552867.d_648936790'), '\"', '') AS d_130371375_d_303552867_d_648936790,
	REPLACE(JSON_QUERY(row,'$.d_130371375.d_303552867.d_731498909'), '\"', '') AS d_130371375_d_303552867_d_731498909,
	REPLACE(JSON_QUERY(row,'$.d_130371375.d_303552867.d_862774033'), '\"', '') AS d_130371375_d_303552867_d_862774033,
	REPLACE(JSON_QUERY(row,'$.d_130371375.d_496823485.d_222373868'), '\"', '') AS d_130371375_d_496823485_d_222373868,
	REPLACE(JSON_QUERY(row,'$.d_130371375.d_496823485.d_297462035'), '\"', '') AS d_130371375_d_496823485_d_297462035,
	REPLACE(JSON_QUERY(row,'$.d_130371375.d_496823485.d_320023644'), '\"', '') AS d_130371375_d_496823485_d_320023644,
	REPLACE(JSON_QUERY(row,'$.d_130371375.d_496823485.d_438636757'), '\"', '') AS d_130371375_d_496823485_d_438636757,
	REPLACE(JSON_QUERY(row,'$.d_130371375.d_496823485.d_648228701'), '\"', '') AS d_130371375_d_496823485_d_648228701,
	REPLACE(JSON_QUERY(row,'$.d_130371375.d_496823485.d_648936790'), '\"', '') AS d_130371375_d_496823485_d_648936790,
	REPLACE(JSON_QUERY(row,'$.d_130371375.d_496823485.d_731498909'), '\"', '') AS d_130371375_d_496823485_d_731498909,
	REPLACE(JSON_QUERY(row,'$.d_130371375.d_496823485.d_862774033'), '\"', '') AS d_130371375_d_496823485_d_862774033,
	REPLACE(JSON_QUERY(row,'$.d_130371375.d_650465111.d_222373868'), '\"', '') AS d_130371375_d_650465111_d_222373868,
	REPLACE(JSON_QUERY(row,'$.d_130371375.d_650465111.d_297462035'), '\"', '') AS d_130371375_d_650465111_d_297462035,
	REPLACE(JSON_QUERY(row,'$.d_130371375.d_650465111.d_320023644'), '\"', '') AS d_130371375_d_650465111_d_320023644,
	REPLACE(JSON_QUERY(row,'$.d_130371375.d_650465111.d_438636757'), '\"', '') AS d_130371375_d_650465111_d_438636757,
	REPLACE(JSON_QUERY(row,'$.d_130371375.d_650465111.d_648228701'), '\"', '') AS d_130371375_d_650465111_d_648228701,
	REPLACE(JSON_QUERY(row,'$.d_130371375.d_650465111.d_648936790'), '\"', '') AS d_130371375_d_650465111_d_648936790,
	REPLACE(JSON_QUERY(row,'$.d_130371375.d_650465111.d_731498909'), '\"', '') AS d_130371375_d_650465111_d_731498909,
	REPLACE(JSON_QUERY(row,'$.d_130371375.d_650465111.d_862774033'), '\"', '') AS d_130371375_d_650465111_d_862774033,
	REPLACE(JSON_QUERY(row,'$.d_131458944'), '\"', '') AS d_131458944,
	REPLACE(JSON_QUERY(row,'$.d_141450621'), '\"', '') AS d_141450621,
	REPLACE(JSON_QUERY(row,'$.d_142654897.d_177402915'), '\"', '') AS d_142654897_d_177402915,
	REPLACE(JSON_QUERY(row,'$.d_142654897.d_196856782'), '\"', '') AS d_142654897_d_196856782,
	REPLACE(JSON_QUERY(row,'$.d_142654897.d_206879104'), '\"', '') AS d_142654897_d_206879104,
	REPLACE(JSON_QUERY(row,'$.d_142654897.d_241590841'), '\"', '') AS d_142654897_d_241590841,
	REPLACE(JSON_QUERY(row,'$.d_142654897.d_285130077'), '\"', '') AS d_142654897_d_285130077,
	REPLACE(JSON_QUERY(row,'$.d_142654897.d_326825649'), '\"', '') AS d_142654897_d_326825649,
	REPLACE(JSON_QUERY(row,'$.d_142654897.d_461488577'), '\"', '') AS d_142654897_d_461488577,
	REPLACE(JSON_QUERY(row,'$.d_142654897.d_462314689'), '\"', '') AS d_142654897_d_462314689,
	REPLACE(JSON_QUERY(row,'$.d_142654897.d_520301146'), '\"', '') AS d_142654897_d_520301146,
	REPLACE(JSON_QUERY(row,'$.d_142654897.d_549687190'), '\"', '') AS d_142654897_d_549687190,
	REPLACE(JSON_QUERY(row,'$.d_142654897.d_607081902'), '\"', '') AS d_142654897_d_607081902,
	REPLACE(JSON_QUERY(row,'$.d_142654897.d_639721694'), '\"', '') AS d_142654897_d_639721694,
	REPLACE(JSON_QUERY(row,'$.d_142654897.d_642287621'), '\"', '') AS d_142654897_d_642287621,
	REPLACE(JSON_QUERY(row,'$.d_142654897.d_684726272'), '\"', '') AS d_142654897_d_684726272,
	REPLACE(JSON_QUERY(row,'$.d_142654897.d_791389099'), '\"', '') AS d_142654897_d_791389099,
	REPLACE(JSON_QUERY(row,'$.d_142654897.d_819377306'), '\"', '') AS d_142654897_d_819377306,
	REPLACE(JSON_QUERY(row,'$.d_142654897.d_829269606'), '\"', '') AS d_142654897_d_829269606,
	REPLACE(JSON_QUERY(row,'$.d_142654897.d_942255248'), '\"', '') AS d_142654897_d_942255248,
	REPLACE(JSON_QUERY(row,'$.d_142654897.d_967372009'), '\"', '') AS d_142654897_d_967372009,
	REPLACE(JSON_QUERY(row,'$.d_150818546'), '\"', '') AS d_150818546,
	REPLACE(JSON_QUERY(row,'$.d_153211406'), '\"', '') AS d_153211406,
	REPLACE(JSON_QUERY(row,'$.d_153713899'), '\"', '') AS d_153713899,
	REPLACE(JSON_QUERY(row,'$.d_158291096'), '\"', '') AS d_158291096,
	REPLACE(JSON_QUERY(row,'$.d_161366008'), '\"', '') AS d_161366008,
	REPLACE(JSON_QUERY(row,'$.d_163284008'), '\"', '') AS d_163284008,
	REPLACE(JSON_QUERY(row,'$.d_167958071'), '\"', '') AS d_167958071,
	REPLACE(JSON_QUERY(row,'$.d_173836415.d_266600170.d_139245758'), '\"', '') AS d_173836415_d_266600170_d_139245758,
	REPLACE(JSON_QUERY(row,'$.d_173836415.d_266600170.d_156605577'), '\"', '') AS d_173836415_d_266600170_d_156605577,
	REPLACE(JSON_QUERY(row,'$.d_173836415.d_266600170.d_184451682'), '\"', '') AS d_173836415_d_266600170_d_184451682,
	REPLACE(JSON_QUERY(row,'$.d_173836415.d_266600170.d_185243482'), '\"', '') AS d_173836415_d_266600170_d_185243482,
	REPLACE(JSON_QUERY(row,'$.d_173836415.d_266600170.d_185243482.integer'), '\"', '') AS d_173836415_d_266600170_d_185243482_integer,
	REPLACE(JSON_QUERY(row,'$.d_173836415.d_266600170.d_185243482.provided'), '\"', '') AS d_173836415_d_266600170_d_185243482_provided,
	REPLACE(JSON_QUERY(row,'$.d_173836415.d_266600170.d_185243482.string'), '\"', '') AS d_173836415_d_266600170_d_185243482_string,
	REPLACE(JSON_QUERY(row,'$.d_173836415.d_266600170.d_198261154'), '\"', '') AS d_173836415_d_266600170_d_198261154,
	REPLACE(JSON_QUERY(row,'$.d_173836415.d_266600170.d_210921343'), '\"', '') AS d_173836415_d_266600170_d_210921343,
	REPLACE(JSON_QUERY(row,'$.d_173836415.d_266600170.d_224596428'), '\"', '') AS d_173836415_d_266600170_d_224596428,
	REPLACE(JSON_QUERY(row,'$.d_173836415.d_266600170.d_316824786'), '\"', '') AS d_173836415_d_266600170_d_316824786,
	REPLACE(JSON_QUERY(row,'$.d_173836415.d_266600170.d_341570479'), '\"', '') AS d_173836415_d_266600170_d_341570479,
	REPLACE(JSON_QUERY(row,'$.d_173836415.d_266600170.d_398645039'), '\"', '') AS d_173836415_d_266600170_d_398645039,
	REPLACE(JSON_QUERY(row,'$.d_173836415.d_266600170.d_448660695'), '\"', '') AS d_173836415_d_266600170_d_448660695,
	REPLACE(JSON_QUERY(row,'$.d_173836415.d_266600170.d_452847912'), '\"', '') AS d_173836415_d_266600170_d_452847912,
	REPLACE(JSON_QUERY(row,'$.d_173836415.d_266600170.d_453452655'), '\"', '') AS d_173836415_d_266600170_d_453452655,
	REPLACE(JSON_QUERY(row,'$.d_173836415.d_266600170.d_530173840'), '\"', '') AS d_173836415_d_266600170_d_530173840,
	REPLACE(JSON_QUERY(row,'$.d_173836415.d_266600170.d_534041351'), '\"', '') AS d_173836415_d_266600170_d_534041351,
	REPLACE(JSON_QUERY(row,'$.d_173836415.d_266600170.d_541311218'), '\"', '') AS d_173836415_d_266600170_d_541311218,
	REPLACE(JSON_QUERY(row,'$.d_173836415.d_266600170.d_561681068'), '\"', '') AS d_173836415_d_266600170_d_561681068,
	REPLACE(JSON_QUERY(row,'$.d_173836415.d_266600170.d_592099155'), '\"', '') AS d_173836415_d_266600170_d_592099155,
	REPLACE(JSON_QUERY(row,'$.d_173836415.d_266600170.d_693370086'), '\"', '') AS d_173836415_d_266600170_d_693370086,
	REPLACE(JSON_QUERY(row,'$.d_173836415.d_266600170.d_718172863'), '\"', '') AS d_173836415_d_266600170_d_718172863,
	REPLACE(JSON_QUERY(row,'$.d_173836415.d_266600170.d_728696253'), '\"', '') AS d_173836415_d_266600170_d_728696253,
	REPLACE(JSON_QUERY(row,'$.d_173836415.d_266600170.d_740582332'), '\"', '') AS d_173836415_d_266600170_d_740582332,
	REPLACE(JSON_QUERY(row,'$.d_173836415.d_266600170.d_769615780'), '\"', '') AS d_173836415_d_266600170_d_769615780,
	REPLACE(JSON_QUERY(row,'$.d_173836415.d_266600170.d_786930107'), '\"', '') AS d_173836415_d_266600170_d_786930107,
	REPLACE(JSON_QUERY(row,'$.d_173836415.d_266600170.d_822274939'), '\"', '') AS d_173836415_d_266600170_d_822274939,
	REPLACE(JSON_QUERY(row,'$.d_173836415.d_266600170.d_847159717'), '\"', '') AS d_173836415_d_266600170_d_847159717,
	REPLACE(JSON_QUERY(row,'$.d_173836415.d_266600170.d_860477844'), '\"', '') AS d_173836415_d_266600170_d_860477844,
	REPLACE(JSON_QUERY(row,'$.d_173836415.d_266600170.d_880794013'), '\"', '') AS d_173836415_d_266600170_d_880794013,
	REPLACE(JSON_QUERY(row,'$.d_173836415.d_266600170.d_915179629'), '\"', '') AS d_173836415_d_266600170_d_915179629,
	REPLACE(JSON_QUERY(row,'$.d_173836415.d_266600170.d_939818935'), '\"', '') AS d_173836415_d_266600170_d_939818935,
	REPLACE(JSON_QUERY(row,'$.d_173836415.d_266600170.d_982213346'), '\"', '') AS d_173836415_d_266600170_d_982213346,
	REPLACE(JSON_QUERY(row,'$.d_175506524.d_175506524'), '\"', '') AS d_175506524_d_175506524,
	REPLACE(JSON_QUERY(row,'$.d_175506524.d_905787778'), '\"', '') AS d_175506524_d_905787778,
	REPLACE(JSON_QUERY(row,'$.d_175732191'), '\"', '') AS d_175732191,
	REPLACE(JSON_QUERY(row,'$.d_187894482'), '\"', '') AS d_187894482,
	REPLACE(JSON_QUERY(row,'$.d_205553981'), '\"', '') AS d_205553981,
	REPLACE(JSON_QUERY(row,'$.d_217640691'), '\"', '') AS d_217640691,
	REPLACE(JSON_QUERY(row,'$.d_222161762'), '\"', '') AS d_222161762,
	REPLACE(JSON_QUERY(row,'$.d_230663853'), '\"', '') AS d_230663853,
	REPLACE(JSON_QUERY(row,'$.d_231676651'), '\"', '') AS d_231676651,
	REPLACE(JSON_QUERY(row,'$.d_253883960'), '\"', '') AS d_253883960,
	REPLACE(JSON_QUERY(row,'$.d_254109640'), '\"', '') AS d_254109640,
	REPLACE(JSON_QUERY(row,'$.d_262357957'), '\"', '') AS d_262357957,
	REPLACE(JSON_QUERY(row,'$.d_262613359'), '\"', '') AS d_262613359,
	REPLACE(JSON_QUERY(row,'$.d_264644252'), '\"', '') AS d_264644252,
	REPLACE(JSON_QUERY(row,'$.d_265193023'), '\"', '') AS d_265193023,
	REPLACE(JSON_QUERY(row,'$.d_266952173'), '\"', '') AS d_266952173,
	REPLACE(JSON_QUERY(row,'$.d_268665918'), '\"', '') AS d_268665918,
	REPLACE(JSON_QUERY(row,'$.d_269050420'), '\"', '') AS d_269050420,
	REPLACE(JSON_QUERY(row,'$.d_271757434'), '\"', '') AS d_271757434,
	REPLACE(JSON_QUERY(row,'$.d_285488731'), '\"', '') AS d_285488731,
	REPLACE(JSON_QUERY(row,'$.d_288972510'), '\"', '') AS d_288972510,
	REPLACE(JSON_QUERY(row,'$.d_289750687'), '\"', '') AS d_289750687,
	REPLACE(JSON_QUERY(row,'$.d_297147359.d_297147359'), '\"', '') AS d_297147359_d_297147359,
	REPLACE(JSON_QUERY(row,'$.d_297147359.d_366295286'), '\"', '') AS d_297147359_d_366295286,
	REPLACE(JSON_QUERY(row,'$.d_299274441.d_299274441'), '\"', '') AS d_299274441_d_299274441,
	REPLACE(JSON_QUERY(row,'$.d_299274441.d_457532784'), '\"', '') AS d_299274441_d_457532784,
	REPLACE(JSON_QUERY(row,'$.d_304438543'), '\"', '') AS d_304438543,
	REPLACE(JSON_QUERY(row,'$.d_311580100'), '\"', '') AS d_311580100,
	REPLACE(JSON_QUERY(row,'$.d_315032037'), '\"', '') AS d_315032037,
	REPLACE(JSON_QUERY(row,'$.d_331584571.d_266600170.d_135591601'), '\"', '') AS d_331584571_d_266600170_d_135591601,
	REPLACE(JSON_QUERY(row,'$.d_331584571.d_266600170.d_343048998'), '\"', '') AS d_331584571_d_266600170_d_343048998,
	REPLACE(JSON_QUERY(row,'$.d_331584571.d_266600170.d_840048338'), '\"', '') AS d_331584571_d_266600170_d_840048338,
	REPLACE(JSON_QUERY(row,'$.d_335767902'), '\"', '') AS d_335767902,
	REPLACE(JSON_QUERY(row,'$.d_348474836'), '\"', '') AS d_348474836,
	REPLACE(JSON_QUERY(row,'$.d_352891568'), '\"', '') AS d_352891568,
	REPLACE(JSON_QUERY(row,'$.d_359404406'), '\"', '') AS d_359404406,
	REPLACE(JSON_QUERY(row,'$.d_371067537'), '\"', '') AS d_371067537,
	REPLACE(JSON_QUERY(row,'$.d_372303208'), '\"', '') AS d_372303208,
	REPLACE(JSON_QUERY(row,'$.d_379080287'), '\"', '') AS d_379080287,
	REPLACE(JSON_QUERY(row,'$.d_386488297'), '\"', '') AS d_386488297,
	REPLACE(JSON_QUERY(row,'$.d_387198193'), '\"', '') AS d_387198193,
	REPLACE(JSON_QUERY(row,'$.d_388711124'), '\"', '') AS d_388711124,
	REPLACE(JSON_QUERY(row,'$.d_390198398'), '\"', '') AS d_390198398,
	REPLACE(JSON_QUERY(row,'$.d_399159511'), '\"', '') AS d_399159511,
	REPLACE(JSON_QUERY(row,'$.d_404289911'), '\"', '') AS d_404289911,
	REPLACE(JSON_QUERY(row,'$.d_407743866'), '\"', '') AS d_407743866,
	REPLACE(JSON_QUERY(row,'$.d_412000022'), '\"', '') AS d_412000022,
	REPLACE(JSON_QUERY(row,'$.d_421823980'), '\"', '') AS d_421823980,
	REPLACE(JSON_QUERY(row,'$.d_430551721'), '\"', '') AS d_430551721,
	REPLACE(JSON_QUERY(row,'$.d_431428747'), '\"', '') AS d_431428747,
	REPLACE(JSON_QUERY(row,'$.d_436680969'), '\"', '') AS d_436680969,
	REPLACE(JSON_QUERY(row,'$.d_438643922'), '\"', '') AS d_438643922,
	REPLACE(JSON_QUERY(row,'$.d_442166669'), '\"', '') AS d_442166669,
	REPLACE(JSON_QUERY(row,'$.d_451953807'), '\"', '') AS d_451953807,
	REPLACE(JSON_QUERY(row,'$.d_452166062'), '\"', '') AS d_452166062,
	REPLACE(JSON_QUERY(row,'$.d_452942800'), '\"', '') AS d_452942800,
	REPLACE(JSON_QUERY(row,'$.d_454067894'), '\"', '') AS d_454067894,
	REPLACE(JSON_QUERY(row,'$.d_454205108'), '\"', '') AS d_454205108,
	REPLACE(JSON_QUERY(row,'$.d_454445267'), '\"', '') AS d_454445267,
	REPLACE(JSON_QUERY(row,'$.d_459098666'), '\"', '') AS d_459098666,
	REPLACE(JSON_QUERY(row,'$.d_471168198'), '\"', '') AS d_471168198,
	REPLACE(JSON_QUERY(row,'$.d_471593703'), '\"', '') AS d_471593703,
	REPLACE(JSON_QUERY(row,'$.d_479278368'), '\"', '') AS d_479278368,
	REPLACE(JSON_QUERY(row,'$.d_480305327'), '\"', '') AS d_480305327,
	REPLACE(JSON_QUERY(row,'$.d_491099823'), '\"', '') AS d_491099823,
	REPLACE(JSON_QUERY(row,'$.d_494982282'), '\"', '') AS d_494982282,
	REPLACE(JSON_QUERY(row,'$.d_506826178'), '\"', '') AS d_506826178,
	REPLACE(JSON_QUERY(row,'$.d_507120821'), '\"', '') AS d_507120821,
	REPLACE(JSON_QUERY(row,'$.d_512820379'), '\"', '') AS d_512820379,
	REPLACE(JSON_QUERY(row,'$.d_517311251'), '\"', '') AS d_517311251,
	REPLACE(JSON_QUERY(row,'$.d_521824358'), '\"', '') AS d_521824358,
	REPLACE(JSON_QUERY(row,'$.d_523768810'), '\"', '') AS d_523768810,
	REPLACE(JSON_QUERY(row,'$.d_524352591.d_524352591'), '\"', '') AS d_524352591_d_524352591,
	REPLACE(JSON_QUERY(row,'$.d_524352591.d_902332801'), '\"', '') AS d_524352591_d_902332801,
	REPLACE(JSON_QUERY(row,'$.d_526455436'), '\"', '') AS d_526455436,
	REPLACE(JSON_QUERY(row,'$.d_534669573'), '\"', '') AS d_534669573,
	REPLACE(JSON_QUERY(row,'$.d_536735468'), '\"', '') AS d_536735468,
	REPLACE(JSON_QUERY(row,'$.d_538619788'), '\"', '') AS d_538619788,
	REPLACE(JSON_QUERY(row,'$.d_541836531'), '\"', '') AS d_541836531,
	REPLACE(JSON_QUERY(row,'$.d_544150384'), '\"', '') AS d_544150384,
	REPLACE(JSON_QUERY(row,'$.d_547363263'), '\"', '') AS d_547363263,
	REPLACE(JSON_QUERY(row,'$.d_558435199'), '\"', '') AS d_558435199,
	REPLACE(JSON_QUERY(row,'$.d_564964481'), '\"', '') AS d_564964481,
	REPLACE(JSON_QUERY(row,'$.d_566565527'), '\"', '') AS d_566565527,
	REPLACE(JSON_QUERY(row,'$.d_576083042'), '\"', '') AS d_576083042,
	REPLACE(JSON_QUERY(row,'$.d_577794331'), '\"', '') AS d_577794331,
	REPLACE(JSON_QUERY(row,'$.d_592227431'), '\"', '') AS d_592227431,
	REPLACE(JSON_QUERY(row,'$.d_596510649'), '\"', '') AS d_596510649,
	REPLACE(JSON_QUERY(row,'$.d_598680838'), '\"', '') AS d_598680838,
	REPLACE(JSON_QUERY(row,'$.d_603278289.d_603278289'), '\"', '') AS d_603278289_d_603278289,
	REPLACE(JSON_QUERY(row,'$.d_603278289.d_821930940'), '\"', '') AS d_603278289_d_821930940,
	REPLACE(JSON_QUERY(row,'$.d_613641698'), '\"', '') AS d_613641698,
	REPLACE(JSON_QUERY(row,'$.d_614264509'), '\"', '') AS d_614264509,
	REPLACE(JSON_QUERY(row,'$.d_620696506'), '\"', '') AS d_620696506,
	REPLACE(JSON_QUERY(row,'$.d_624030581'), '\"', '') AS d_624030581,
	REPLACE(JSON_QUERY(row,'$.d_634434746'), '\"', '') AS d_634434746,
	REPLACE(JSON_QUERY(row,'$.d_635101039'), '\"', '') AS d_635101039,
	REPLACE(JSON_QUERY(row,'$.d_637147033'), '\"', '') AS d_637147033,
	REPLACE(JSON_QUERY(row,'$.d_639172801'), '\"', '') AS d_639172801,
	REPLACE(JSON_QUERY(row,'$.d_646873644'), '\"', '') AS d_646873644,
	REPLACE(JSON_QUERY(row,'$.d_650597106'), '\"', '') AS d_650597106,
	REPLACE(JSON_QUERY(row,'$.d_657475009'), '\"', '') AS d_657475009,
	REPLACE(JSON_QUERY(row,'$.d_659990606'), '\"', '') AS d_659990606,
	REPLACE(JSON_QUERY(row,'$.d_663265240'), '\"', '') AS d_663265240,
	REPLACE(JSON_QUERY(row,'$.d_664453818'), '\"', '') AS d_664453818,
	REPLACE(JSON_QUERY(row,'$.d_672401635'), '\"', '') AS d_672401635,
	REPLACE(JSON_QUERY(row,'$.d_677381583'), '\"', '') AS d_677381583,
	REPLACE(JSON_QUERY(row,'$.d_684635302'), '\"', '') AS d_684635302,
	REPLACE(JSON_QUERY(row,'$.d_685002411.d_194410742'), '\"', '') AS d_685002411_d_194410742,
	REPLACE(JSON_QUERY(row,'$.d_685002411.d_217367618'), '\"', '') AS d_685002411_d_217367618,
	REPLACE(JSON_QUERY(row,'$.d_685002411.d_277479354'), '\"', '') AS d_685002411_d_277479354,
	REPLACE(JSON_QUERY(row,'$.d_685002411.d_352996056'), '\"', '') AS d_685002411_d_352996056,
	REPLACE(JSON_QUERY(row,'$.d_685002411.d_867203506'), '\"', '') AS d_685002411_d_867203506,
	REPLACE(JSON_QUERY(row,'$.d_685002411.d_949501163'), '\"', '') AS d_685002411_d_949501163,
	REPLACE(JSON_QUERY(row,'$.d_685002411.d_994064239'), '\"', '') AS d_685002411_d_994064239,
	REPLACE(JSON_QUERY(row,'$.d_693626233'), '\"', '') AS d_693626233,
	REPLACE(JSON_QUERY(row,'$.d_699625233'), '\"', '') AS d_699625233,
	REPLACE(JSON_QUERY(row,'$.d_703385619'), '\"', '') AS d_703385619,
	REPLACE(JSON_QUERY(row,'$.d_711256590'), '\"', '') AS d_711256590,
	REPLACE(JSON_QUERY(row,'$.d_714419972'), '\"', '') AS d_714419972,
	REPLACE(JSON_QUERY(row,'$.d_715390138'), '\"', '') AS d_715390138,
	REPLACE(JSON_QUERY(row,'$.d_719222792.d_719222792'), '\"', '') AS d_719222792_d_719222792,
	REPLACE(JSON_QUERY(row,'$.d_726389747'), '\"', '') AS d_726389747,
	REPLACE(JSON_QUERY(row,'$.d_734828170'), '\"', '') AS d_734828170,
	REPLACE(JSON_QUERY(row,'$.d_736251808'), '\"', '') AS d_736251808,
	REPLACE(JSON_QUERY(row,'$.d_744604255'), '\"', '') AS d_744604255,
	REPLACE(JSON_QUERY(row,'$.d_747006172'), '\"', '') AS d_747006172,
	REPLACE(JSON_QUERY(row,'$.d_756862764'), '\"', '') AS d_756862764,
	REPLACE(JSON_QUERY(row,'$.d_764403541'), '\"', '') AS d_764403541,
	REPLACE(JSON_QUERY(row,'$.d_764863765'), '\"', '') AS d_764863765,
	REPLACE(JSON_QUERY(row,'$.d_765336427'), '\"', '') AS d_765336427,
	REPLACE(JSON_QUERY(row,'$.d_770257102'), '\"', '') AS d_770257102,
	REPLACE(JSON_QUERY(row,'$.d_771146804'), '\"', '') AS d_771146804,
	REPLACE(JSON_QUERY(row,'$.d_773707518'), '\"', '') AS d_773707518,
	REPLACE(JSON_QUERY(row,'$.d_777719027'), '\"', '') AS d_777719027,
	REPLACE(JSON_QUERY(row,'$.d_793072415'), '\"', '') AS d_793072415,
	REPLACE(JSON_QUERY(row,'$.d_795827569'), '\"', '') AS d_795827569,
	REPLACE(JSON_QUERY(row,'$.d_808663245'), '\"', '') AS d_808663245,
	REPLACE(JSON_QUERY(row,'$.d_821247024'), '\"', '') AS d_821247024,
	REPLACE(JSON_QUERY(row,'$.d_822499427'), '\"', '') AS d_822499427,
	REPLACE(JSON_QUERY(row,'$.d_826240317'), '\"', '') AS d_826240317,
	REPLACE(JSON_QUERY(row,'$.d_827220437'), '\"', '') AS d_827220437,
	REPLACE(JSON_QUERY(row,'$.d_828729648'), '\"', '') AS d_828729648,
	REPLACE(JSON_QUERY(row,'$.d_831041022'), '\"', '') AS d_831041022,
	REPLACE(JSON_QUERY(row,'$.d_832139544'), '\"', '') AS d_832139544,
	REPLACE(JSON_QUERY(row,'$.d_844088537'), '\"', '') AS d_844088537,
	REPLACE(JSON_QUERY(row,'$.d_849786503'), '\"', '') AS d_849786503,
	REPLACE(JSON_QUERY(row,'$.d_851245875'), '\"', '') AS d_851245875,
	REPLACE(JSON_QUERY(row,'$.d_866089092'), '\"', '') AS d_866089092,
	REPLACE(JSON_QUERY(row,'$.d_869588347'), '\"', '') AS d_869588347,
	REPLACE(JSON_QUERY(row,'$.d_875010152'), '\"', '') AS d_875010152,
	REPLACE(JSON_QUERY(row,'$.d_878865966'), '\"', '') AS d_878865966,
	REPLACE(JSON_QUERY(row,'$.d_883668444'), '\"', '') AS d_883668444,
	REPLACE(JSON_QUERY(row,'$.d_892050548'), '\"', '') AS d_892050548,
	REPLACE(JSON_QUERY(row,'$.d_906417725'), '\"', '') AS d_906417725,
	REPLACE(JSON_QUERY(row,'$.d_912301837'), '\"', '') AS d_912301837,
	REPLACE(JSON_QUERY(row,'$.d_914594314'), '\"', '') AS d_914594314,
	REPLACE(JSON_QUERY(row,'$.d_914639140'), '\"', '') AS d_914639140,
	REPLACE(JSON_QUERY(row,'$.d_919254129'), '\"', '') AS d_919254129,
	REPLACE(JSON_QUERY(row,'$.d_919699172'), '\"', '') AS d_919699172,
	REPLACE(JSON_QUERY(row,'$.d_943232079'), '\"', '') AS d_943232079,
	REPLACE(JSON_QUERY(row,'$.d_948195369'), '\"', '') AS d_948195369,
	REPLACE(JSON_QUERY(row,'$.d_949302066'), '\"', '') AS d_949302066,
	REPLACE(JSON_QUERY(row,'$.d_958588520'), '\"', '') AS d_958588520,
	REPLACE(JSON_QUERY(row,'$.d_974100830'), '\"', '') AS d_974100830,
	REPLACE(JSON_QUERY(row,'$.d_976570371'), '\"', '') AS d_976570371,
	REPLACE(JSON_QUERY(row,'$.d_982105411'), '\"', '') AS d_982105411,
	REPLACE(JSON_QUERY(row,'$.d_982402227'), '\"', '') AS d_982402227,
	REPLACE(JSON_QUERY(row,'$.d_983278853'), '\"', '') AS d_983278853,
	REPLACE(JSON_QUERY(row,'$.d_987563196'), '\"', '') AS d_987563196,
	REPLACE(JSON_QUERY(row,'$.d_990579614'), '\"', '') AS d_990579614,
	REPLACE(JSON_QUERY(row,'$.d_995036844'), '\"', '') AS d_995036844,
	REPLACE(JSON_QUERY(row,'$.d_996038075'), '\"', '') AS d_996038075,
	REPLACE(JSON_QUERY(row,'$.firstSurveyCompletedSeen'), '\"', '') AS firstSurveyCompletedSeen,
	REPLACE(JSON_QUERY(row,'$.pin'), '\"', '') AS pin,
	REPLACE(JSON_QUERY(row,'$.query.firstName'), '\"', '') AS query_firstName,
	REPLACE(JSON_QUERY(row,'$.query.lastName'), '\"', '') AS query_lastName,
	REPLACE(JSON_QUERY(row,'$.state.d_119643471'), '\"', '') AS state_d_119643471,
	REPLACE(JSON_QUERY(row,'$.state.d_147176963'), '\"', '') AS state_d_147176963,
	REPLACE(JSON_QUERY(row,'$.state.d_148197146'), '\"', '') AS state_d_148197146,
	REPLACE(JSON_QUERY(row,'$.state.d_158291096'), '\"', '') AS state_d_158291096,
	REPLACE(JSON_QUERY(row,'$.state.d_188797763'), '\"', '') AS state_d_188797763,
	REPLACE(JSON_QUERY(row,'$.state.d_435027713'), '\"', '') AS state_d_435027713,
	REPLACE(JSON_QUERY(row,'$.state.d_444699761'), '\"', '') AS state_d_444699761,
	REPLACE(JSON_QUERY(row,'$.state.d_477091792'), '\"', '') AS state_d_477091792,
	REPLACE(JSON_QUERY(row,'$.state.d_481139103'), '\"', '') AS state_d_481139103,
	REPLACE(JSON_QUERY(row,'$.state.d_521025370'), '\"', '') AS state_d_521025370,
	REPLACE(JSON_QUERY(row,'$.state.d_527823810'), '\"', '') AS state_d_527823810,
	REPLACE(JSON_QUERY(row,'$.state.d_538553381'), '\"', '') AS state_d_538553381,
	REPLACE(JSON_QUERY(row,'$.state.d_547895941'), '\"', '') AS state_d_547895941,
	REPLACE(JSON_QUERY(row,'$.state.d_557461333'), '\"', '') AS state_d_557461333,
	REPLACE(JSON_QUERY(row,'$.state.d_559534463'), '\"', '') AS state_d_559534463,
	REPLACE(JSON_QUERY(row,'$.state.d_570452130'), '\"', '') AS state_d_570452130,
	REPLACE(JSON_QUERY(row,'$.state.d_629484663'), '\"', '') AS state_d_629484663,
	REPLACE(JSON_QUERY(row,'$.state.d_667474224'), '\"', '') AS state_d_667474224,
	REPLACE(JSON_QUERY(row,'$.state.d_679832994'), '\"', '') AS state_d_679832994,
	REPLACE(JSON_QUERY(row,'$.state.d_684926335'), '\"', '') AS state_d_684926335,
	REPLACE(JSON_QUERY(row,'$.state.d_697256759'), '\"', '') AS state_d_697256759,
	REPLACE(JSON_QUERY(row,'$.state.d_706256705'), '\"', '') AS state_d_706256705,
	REPLACE(JSON_QUERY(row,'$.state.d_706283025.d_163284008'), '\"', '') AS state_d_706283025_d_163284008,
	REPLACE(JSON_QUERY(row,'$.state.d_706283025.d_163534562'), '\"', '') AS state_d_706283025_d_163534562,
	REPLACE(JSON_QUERY(row,'$.state.d_706283025.d_196038514'), '\"', '') AS state_d_706283025_d_196038514,
	REPLACE(JSON_QUERY(row,'$.state.d_706283025.d_208102461'), '\"', '') AS state_d_706283025_d_208102461,
	REPLACE(JSON_QUERY(row,'$.state.d_706283025.d_209509101'), '\"', '') AS state_d_706283025_d_209509101,
	REPLACE(JSON_QUERY(row,'$.state.d_706283025.d_211023960'), '\"', '') AS state_d_706283025_d_211023960,
	REPLACE(JSON_QUERY(row,'$.state.d_706283025.d_260703126'), '\"', '') AS state_d_706283025_d_260703126,
	REPLACE(JSON_QUERY(row,'$.state.d_706283025.d_331787113'), '\"', '') AS state_d_706283025_d_331787113,
	REPLACE(JSON_QUERY(row,'$.state.d_706283025.d_347614743'), '\"', '') AS state_d_706283025_d_347614743,
	REPLACE(JSON_QUERY(row,'$.state.d_706283025.d_363026564'), '\"', '') AS state_d_706283025_d_363026564,
	REPLACE(JSON_QUERY(row,'$.state.d_706283025.d_377633816'), '\"', '') AS state_d_706283025_d_377633816,
	REPLACE(JSON_QUERY(row,'$.state.d_706283025.d_381509125'), '\"', '') AS state_d_706283025_d_381509125,
	REPLACE(JSON_QUERY(row,'$.state.d_706283025.d_387198193'), '\"', '') AS state_d_706283025_d_387198193,
	REPLACE(JSON_QUERY(row,'$.state.d_706283025.d_400259098'), '\"', '') AS state_d_706283025_d_400259098,
	REPLACE(JSON_QUERY(row,'$.state.d_706283025.d_405352246'), '\"', '') AS state_d_706283025_d_405352246,
	REPLACE(JSON_QUERY(row,'$.state.d_706283025.d_415693436'), '\"', '') AS state_d_706283025_d_415693436,
	REPLACE(JSON_QUERY(row,'$.state.d_706283025.d_491099823'), '\"', '') AS state_d_706283025_d_491099823,
	REPLACE(JSON_QUERY(row,'$.state.d_706283025.d_496935183'), '\"', '') AS state_d_706283025_d_496935183,
	REPLACE(JSON_QUERY(row,'$.state.d_706283025.d_497530905'), '\"', '') AS state_d_706283025_d_497530905,
	REPLACE(JSON_QUERY(row,'$.state.d_706283025.d_517101990'), '\"', '') AS state_d_706283025_d_517101990,
	REPLACE(JSON_QUERY(row,'$.state.d_706283025.d_535928798'), '\"', '') AS state_d_706283025_d_535928798,
	REPLACE(JSON_QUERY(row,'$.state.d_706283025.d_566047367'), '\"', '') AS state_d_706283025_d_566047367,
	REPLACE(JSON_QUERY(row,'$.state.d_706283025.d_579618065'), '\"', '') AS state_d_706283025_d_579618065,
	REPLACE(JSON_QUERY(row,'$.state.d_706283025.d_627995442'), '\"', '') AS state_d_706283025_d_627995442,
	REPLACE(JSON_QUERY(row,'$.state.d_706283025.d_702433259'), '\"', '') AS state_d_706283025_d_702433259,
	REPLACE(JSON_QUERY(row,'$.state.d_706283025.d_705732561'), '\"', '') AS state_d_706283025_d_705732561,
	REPLACE(JSON_QUERY(row,'$.state.d_706283025.d_719451909'), '\"', '') AS state_d_706283025_d_719451909,
	REPLACE(JSON_QUERY(row,'$.state.d_706283025.d_744197145'), '\"', '') AS state_d_706283025_d_744197145,
	REPLACE(JSON_QUERY(row,'$.state.d_706283025.d_755545718'), '\"', '') AS state_d_706283025_d_755545718,
	REPLACE(JSON_QUERY(row,'$.state.d_706283025.d_771146804'), '\"', '') AS state_d_706283025_d_771146804,
	REPLACE(JSON_QUERY(row,'$.state.d_706283025.d_831137710'), '\"', '') AS state_d_706283025_d_831137710,
	REPLACE(JSON_QUERY(row,'$.state.d_706283025.d_836460125'), '\"', '') AS state_d_706283025_d_836460125,
	REPLACE(JSON_QUERY(row,'$.state.d_706283025.d_873405723'), '\"', '') AS state_d_706283025_d_873405723,
	REPLACE(JSON_QUERY(row,'$.state.d_706283025.d_897366187'), '\"', '') AS state_d_706283025_d_897366187,
	REPLACE(JSON_QUERY(row,'$.state.d_706283025.d_950040334'), '\"', '') AS state_d_706283025_d_950040334,
	REPLACE(JSON_QUERY(row,'$.state.d_711794630'), '\"', '') AS state_d_711794630,
	REPLACE(JSON_QUERY(row,'$.state.d_725929722'), '\"', '') AS state_d_725929722,
	REPLACE(JSON_QUERY(row,'$.state.d_749475364'), '\"', '') AS state_d_749475364,
	REPLACE(JSON_QUERY(row,'$.state.d_793822265'), '\"', '') AS state_d_793822265,
	REPLACE(JSON_QUERY(row,'$.state.d_821247024'), '\"', '') AS state_d_821247024,
	REPLACE(JSON_QUERY(row,'$.state.d_821247024.integer'), '\"', '') AS state_d_821247024_integer,
	REPLACE(JSON_QUERY(row,'$.state.d_821247024.provided'), '\"', '') AS state_d_821247024_provided,
	REPLACE(JSON_QUERY(row,'$.state.d_821247024.string'), '\"', '') AS state_d_821247024_string,
	REPLACE(JSON_QUERY(row,'$.state.d_849518448'), '\"', '') AS state_d_849518448,
	REPLACE(JSON_QUERY(row,'$.state.d_875549268'), '\"', '') AS state_d_875549268,
	REPLACE(JSON_QUERY(row,'$.state.d_934298480'), '\"', '') AS state_d_934298480,
	REPLACE(JSON_QUERY(row,'$.state.d_953614051'), '\"', '') AS state_d_953614051,
	REPLACE(JSON_QUERY(row,'$.state.state.d_706283025.d_517101990'), '\"', '') AS state_state_d_706283025_d_517101990,
	REPLACE(JSON_QUERY(row,'$.state.state.d_793822265'), '\"', '') AS state_state_d_793822265,
	REPLACE(JSON_QUERY(row,'$.state.studyId'), '\"', '') AS state_studyId,
	REPLACE(JSON_QUERY(row,'$.state.studyId.integer'), '\"', '') AS state_studyId_integer,
	REPLACE(JSON_QUERY(row,'$.state.studyId.provided'), '\"', '') AS state_studyId_provided,
	REPLACE(JSON_QUERY(row,'$.state.studyId.string'), '\"', '') AS state_studyId_string,
	REPLACE(JSON_QUERY(row,'$.state.uid'), '\"', '') AS state_uid,
	REPLACE(JSON_QUERY(row,'$.token'), '\"', '') AS token,
	REPLACE(JSON_QUERY(row,'$.uid'), '\"', '') AS uid,
	REPLACE(JSON_QUERY(row,'$.undefined'), '\"', '') AS undefined,
	REPLACE(JSON_QUERY(row,'$.unverifiedSeen'), '\"', '') AS unverifiedSeen,
	REPLACE(JSON_QUERY(row,'$.utm_id'), '\"', '') AS utm_id,
	REPLACE(JSON_QUERY(row,'$.utm_source'), '\"', '') AS utm_source,
	REPLACE(JSON_QUERY(row,'$.verifiedSeen'), '\"', '') AS verifiedSeen -- selects
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

