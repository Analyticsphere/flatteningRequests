-- FYI: This query was automatically generated using R code written by Jake 
-- Peters. The code references a the source table schema and a configuration
-- file specifying various parameters. This style of query was developed by 
-- Warren Lu to flatten the Connect4Cancer datasets in 2022. See GitHub repo
-- for further documentation.
-- 
-- Repository: https://github.com/Analyticsphere/flatteningRequests
-- Relavent functions: generate_flattening_query.R
-- 
-- source_table: nih-nci-dceg-connect-stg-5519.Connect.clinicalBioSurvey_v1
-- destination table: nih-nci-dceg-connect-stg-5519.FlatConnect.clinicalBioSurvey_v1_JP -- notes
    
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
        214414872,
        235386560,
        406943303,
        535003378,
        756774083,
        811126581,
        955154600
    ],
    "D_705336878_1_1.D_705336878_1_1": [
        160627865,
        162167690,
        178318661,
        283776061,
        367747257,
        406943303,
        423283665,
        524258008,
        579409935,
        653630699,
        657566099,
        679368947,
        684149600,
        756774083,
        760243464,
        807835037,
        810340693,
        896163801,
        960642359
    ],
    "D_705336878_2_2.D_705336878_2_2": [
        162167690,
        367747257,
        579409935,
        653630699,
        679368947,
        756774083
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
  `nih-nci-dceg-connect-stg-5519.FlatConnect.clinicalBioSurvey_v1_JP` -- destination_table
  OPTIONS (description="Source table: Connect.clinicalBioSurvey_v1; Scheduled Query: FlatConnect.clinicalBioSurvey_v1_JP; GitHub: https://github.com/Analyticsphere/flatteningRequests/tree/main/queryGenerators/FlatConnect_queries/clinicalBioSurvey_v1; Team: Analytics; Maintainer: Jake Peters; Super Users: Kelsey; Notes: This table is a flattened version of Connect.clinicalBioSurvey_v1.") -- table_description
  AS (
  WITH
    json_data AS (
    SELECT
      [handleRow(TO_JSON_STRING(input_row))] AS body
    FROM
      `nih-nci-dceg-connect-stg-5519.Connect.clinicalBioSurvey_v1` AS input_row -- source_table
    WHERE Connect_ID IS NOT NULL), -- filter_statement
    flattened_data AS (
    SELECT
      	REPLACE(JSON_QUERY(row,'$.Connect_ID'), '\"', '') AS Connect_ID,
	REPLACE(JSON_QUERY(row,'$.D_108733102_1_1'), '\"', '') AS D_108733102_1_1,
	REPLACE(JSON_QUERY(row,'$.D_108733102_2_2'), '\"', '') AS D_108733102_2_2,
	REPLACE(JSON_QUERY(row,'$.D_110872086.D_110872086'), '\"', '') AS D_110872086_D_110872086,
	REPLACE(JSON_QUERY(row,'$.D_110872086.D_637540387'), '\"', '') AS D_110872086_D_637540387,
	REPLACE(JSON_QUERY(row,'$.D_112151599'), '\"', '') AS D_112151599,
	REPLACE(JSON_QUERY(row,'$.D_114280729.D_108389123'), '\"', '') AS D_114280729_D_108389123,
	REPLACE(JSON_QUERY(row,'$.D_114280729.D_336856410'), '\"', '') AS D_114280729_D_336856410,
	REPLACE(JSON_QUERY(row,'$.D_114280729.D_368669706'), '\"', '') AS D_114280729_D_368669706,
	REPLACE(JSON_QUERY(row,'$.D_114280729.D_374567479'), '\"', '') AS D_114280729_D_374567479,
	REPLACE(JSON_QUERY(row,'$.D_114280729.D_481587023'), '\"', '') AS D_114280729_D_481587023,
	REPLACE(JSON_QUERY(row,'$.D_114280729.D_518602598'), '\"', '') AS D_114280729_D_518602598,
	REPLACE(JSON_QUERY(row,'$.D_114280729.D_590361055'), '\"', '') AS D_114280729_D_590361055,
	REPLACE(JSON_QUERY(row,'$.D_114280729.D_605818246'), '\"', '') AS D_114280729_D_605818246,
	REPLACE(JSON_QUERY(row,'$.D_114280729.D_702905707'), '\"', '') AS D_114280729_D_702905707,
	REPLACE(JSON_QUERY(row,'$.D_114280729.D_747085418'), '\"', '') AS D_114280729_D_747085418,
	REPLACE(JSON_QUERY(row,'$.D_114280729.D_770190369'), '\"', '') AS D_114280729_D_770190369,
	REPLACE(JSON_QUERY(row,'$.D_114280729.D_790860504'), '\"', '') AS D_114280729_D_790860504,
	REPLACE(JSON_QUERY(row,'$.D_114280729.D_966214244'), '\"', '') AS D_114280729_D_966214244,
	REPLACE(JSON_QUERY(row,'$.D_114280729.D_986119909'), '\"', '') AS D_114280729_D_986119909,
	REPLACE(JSON_QUERY(row,'$.D_114280729.D_994153376'), '\"', '') AS D_114280729_D_994153376,
	REPLACE(JSON_QUERY(row,'$.D_130311122'), '\"', '') AS D_130311122,
	REPLACE(JSON_QUERY(row,'$.D_136730307.D_126794793'), '\"', '') AS D_136730307_D_126794793,
	REPLACE(JSON_QUERY(row,'$.D_136730307.D_218793117'), '\"', '') AS D_136730307_D_218793117,
	REPLACE(JSON_QUERY(row,'$.D_136730307.D_338613869'), '\"', '') AS D_136730307_D_338613869,
	REPLACE(JSON_QUERY(row,'$.D_136730307.D_962475128'), '\"', '') AS D_136730307_D_962475128,
	REPLACE(JSON_QUERY(row,'$.D_136730307.D_989576239'), '\"', '') AS D_136730307_D_989576239,
	REPLACE(JSON_QUERY(row,'$.D_169509213_1_1'), '\"', '') AS D_169509213_1_1,
	REPLACE(JSON_QUERY(row,'$.D_179406442_1_1'), '\"', '') AS D_179406442_1_1,
	REPLACE(JSON_QUERY(row,'$.D_191057574'), '\"', '') AS D_191057574,
	REPLACE(JSON_QUERY(row,'$.D_191057574_V2'), '\"', '') AS D_191057574_V2,
	REPLACE(JSON_QUERY(row,'$.D_220055064_1_1.D_220055064_1_1'), '\"', '') AS D_220055064_1_1_D_220055064_1_1,
	REPLACE(JSON_QUERY(row,'$.D_220055064_2_2.D_220055064_2_2'), '\"', '') AS D_220055064_2_2_D_220055064_2_2,
	REPLACE(JSON_QUERY(row,'$.D_220055064_3_3.D_220055064_3_3'), '\"', '') AS D_220055064_3_3_D_220055064_3_3,
	REPLACE(JSON_QUERY(row,'$.D_220055064_4_4.D_220055064_4_4'), '\"', '') AS D_220055064_4_4_D_220055064_4_4,
	REPLACE(JSON_QUERY(row,'$.d_220355297'), '\"', '') AS d_220355297,
	REPLACE(JSON_QUERY(row,'$.D_234714655'), '\"', '') AS D_234714655,
	REPLACE(JSON_QUERY(row,'$.D_273371161'), '\"', '') AS D_273371161,
	REPLACE(JSON_QUERY(row,'$.D_299417266'), '\"', '') AS D_299417266,
	REPLACE(JSON_QUERY(row,'$.D_299417266_V2'), '\"', '') AS D_299417266_V2,
	REPLACE(JSON_QUERY(row,'$.D_366980310_1_1'), '\"', '') AS D_366980310_1_1,
	REPLACE(JSON_QUERY(row,'$.D_366980310_2_2'), '\"', '') AS D_366980310_2_2,
	REPLACE(JSON_QUERY(row,'$.D_380603392'), '\"', '') AS D_380603392,
	REPLACE(JSON_QUERY(row,'$.D_389465772_1_1'), '\"', '') AS D_389465772_1_1,
	REPLACE(JSON_QUERY(row,'$.D_389465772_2_2'), '\"', '') AS D_389465772_2_2,
	REPLACE(JSON_QUERY(row,'$.D_424718457.D_424718457'), '\"', '') AS D_424718457_D_424718457,
	REPLACE(JSON_QUERY(row,'$.D_430166879_1_1'), '\"', '') AS D_430166879_1_1,
	REPLACE(JSON_QUERY(row,'$.D_451163824_1_1'), '\"', '') AS D_451163824_1_1,
	REPLACE(JSON_QUERY(row,'$.D_451163824_2_2'), '\"', '') AS D_451163824_2_2,
	REPLACE(JSON_QUERY(row,'$.D_470484596.D_214414872'), '\"', '') AS D_470484596_D_214414872,
	REPLACE(JSON_QUERY(row,'$.D_470484596.D_235386560'), '\"', '') AS D_470484596_D_235386560,
	REPLACE(JSON_QUERY(row,'$.D_470484596.D_406943303'), '\"', '') AS D_470484596_D_406943303,
	REPLACE(JSON_QUERY(row,'$.D_470484596.D_535003378'), '\"', '') AS D_470484596_D_535003378,
	REPLACE(JSON_QUERY(row,'$.D_470484596.D_756774083'), '\"', '') AS D_470484596_D_756774083,
	REPLACE(JSON_QUERY(row,'$.D_470484596.D_811126581'), '\"', '') AS D_470484596_D_811126581,
	REPLACE(JSON_QUERY(row,'$.D_470484596.D_955154600'), '\"', '') AS D_470484596_D_955154600,
	REPLACE(JSON_QUERY(row,'$.D_487532606.D_520755310'), '\"', '') AS D_487532606_D_520755310,
	REPLACE(JSON_QUERY(row,'$.D_487532606.D_619765650'), '\"', '') AS D_487532606_D_619765650,
	REPLACE(JSON_QUERY(row,'$.D_487532606.D_839329467'), '\"', '') AS D_487532606_D_839329467,
	REPLACE(JSON_QUERY(row,'$.D_494226443'), '\"', '') AS D_494226443,
	REPLACE(JSON_QUERY(row,'$.D_498462481_1_1'), '\"', '') AS D_498462481_1_1,
	REPLACE(JSON_QUERY(row,'$.D_498462481_2_2'), '\"', '') AS D_498462481_2_2,
	REPLACE(JSON_QUERY(row,'$.D_518916981'), '\"', '') AS D_518916981,
	REPLACE(JSON_QUERY(row,'$.D_522008539'), '\"', '') AS D_522008539,
	REPLACE(JSON_QUERY(row,'$.D_563539159'), '\"', '') AS D_563539159,
	REPLACE(JSON_QUERY(row,'$.D_591826144'), '\"', '') AS D_591826144,
	REPLACE(JSON_QUERY(row,'$.D_644459734'), '\"', '') AS D_644459734,
	REPLACE(JSON_QUERY(row,'$.D_689861450'), '\"', '') AS D_689861450,
	REPLACE(JSON_QUERY(row,'$.D_689861450_V2'), '\"', '') AS D_689861450_V2,
	REPLACE(JSON_QUERY(row,'$.D_694503437_1_1'), '\"', '') AS D_694503437_1_1,
	REPLACE(JSON_QUERY(row,'$.D_694503437_2_2'), '\"', '') AS D_694503437_2_2,
	REPLACE(JSON_QUERY(row,'$.D_705336878_1_1.D_218852075_1_1'), '\"', '') AS D_705336878_1_1_D_218852075_1_1,
	REPLACE(JSON_QUERY(row,'$.D_705336878_1_1.D_705336878_1_1.D_160627865'), '\"', '') AS D_705336878_1_1_D_705336878_1_1_D_160627865,
	REPLACE(JSON_QUERY(row,'$.D_705336878_1_1.D_705336878_1_1.D_162167690'), '\"', '') AS D_705336878_1_1_D_705336878_1_1_D_162167690,
	REPLACE(JSON_QUERY(row,'$.D_705336878_1_1.D_705336878_1_1.D_178318661'), '\"', '') AS D_705336878_1_1_D_705336878_1_1_D_178318661,
	REPLACE(JSON_QUERY(row,'$.D_705336878_1_1.D_705336878_1_1.D_283776061'), '\"', '') AS D_705336878_1_1_D_705336878_1_1_D_283776061,
	REPLACE(JSON_QUERY(row,'$.D_705336878_1_1.D_705336878_1_1.D_367747257'), '\"', '') AS D_705336878_1_1_D_705336878_1_1_D_367747257,
	REPLACE(JSON_QUERY(row,'$.D_705336878_1_1.D_705336878_1_1.D_406943303'), '\"', '') AS D_705336878_1_1_D_705336878_1_1_D_406943303,
	REPLACE(JSON_QUERY(row,'$.D_705336878_1_1.D_705336878_1_1.D_423283665'), '\"', '') AS D_705336878_1_1_D_705336878_1_1_D_423283665,
	REPLACE(JSON_QUERY(row,'$.D_705336878_1_1.D_705336878_1_1.D_524258008'), '\"', '') AS D_705336878_1_1_D_705336878_1_1_D_524258008,
	REPLACE(JSON_QUERY(row,'$.D_705336878_1_1.D_705336878_1_1.D_579409935'), '\"', '') AS D_705336878_1_1_D_705336878_1_1_D_579409935,
	REPLACE(JSON_QUERY(row,'$.D_705336878_1_1.D_705336878_1_1.D_653630699'), '\"', '') AS D_705336878_1_1_D_705336878_1_1_D_653630699,
	REPLACE(JSON_QUERY(row,'$.D_705336878_1_1.D_705336878_1_1.D_657566099'), '\"', '') AS D_705336878_1_1_D_705336878_1_1_D_657566099,
	REPLACE(JSON_QUERY(row,'$.D_705336878_1_1.D_705336878_1_1.D_679368947'), '\"', '') AS D_705336878_1_1_D_705336878_1_1_D_679368947,
	REPLACE(JSON_QUERY(row,'$.D_705336878_1_1.D_705336878_1_1.D_684149600'), '\"', '') AS D_705336878_1_1_D_705336878_1_1_D_684149600,
	REPLACE(JSON_QUERY(row,'$.D_705336878_1_1.D_705336878_1_1.D_756774083'), '\"', '') AS D_705336878_1_1_D_705336878_1_1_D_756774083,
	REPLACE(JSON_QUERY(row,'$.D_705336878_1_1.D_705336878_1_1.D_760243464'), '\"', '') AS D_705336878_1_1_D_705336878_1_1_D_760243464,
	REPLACE(JSON_QUERY(row,'$.D_705336878_1_1.D_705336878_1_1.D_807835037'), '\"', '') AS D_705336878_1_1_D_705336878_1_1_D_807835037,
	REPLACE(JSON_QUERY(row,'$.D_705336878_1_1.D_705336878_1_1.D_810340693'), '\"', '') AS D_705336878_1_1_D_705336878_1_1_D_810340693,
	REPLACE(JSON_QUERY(row,'$.D_705336878_1_1.D_705336878_1_1.D_896163801'), '\"', '') AS D_705336878_1_1_D_705336878_1_1_D_896163801,
	REPLACE(JSON_QUERY(row,'$.D_705336878_1_1.D_705336878_1_1.D_960642359'), '\"', '') AS D_705336878_1_1_D_705336878_1_1_D_960642359,
	REPLACE(JSON_QUERY(row,'$.D_705336878_2_2.D_705336878_2_2.D_162167690'), '\"', '') AS D_705336878_2_2_D_705336878_2_2_D_162167690,
	REPLACE(JSON_QUERY(row,'$.D_705336878_2_2.D_705336878_2_2.D_367747257'), '\"', '') AS D_705336878_2_2_D_705336878_2_2_D_367747257,
	REPLACE(JSON_QUERY(row,'$.D_705336878_2_2.D_705336878_2_2.D_579409935'), '\"', '') AS D_705336878_2_2_D_705336878_2_2_D_579409935,
	REPLACE(JSON_QUERY(row,'$.D_705336878_2_2.D_705336878_2_2.D_653630699'), '\"', '') AS D_705336878_2_2_D_705336878_2_2_D_653630699,
	REPLACE(JSON_QUERY(row,'$.D_705336878_2_2.D_705336878_2_2.D_679368947'), '\"', '') AS D_705336878_2_2_D_705336878_2_2_D_679368947,
	REPLACE(JSON_QUERY(row,'$.D_705336878_2_2.D_705336878_2_2.D_756774083'), '\"', '') AS D_705336878_2_2_D_705336878_2_2_D_756774083,
	REPLACE(JSON_QUERY(row,'$.D_715581797_1_1'), '\"', '') AS D_715581797_1_1,
	REPLACE(JSON_QUERY(row,'$.D_715581797_V2_1_1'), '\"', '') AS D_715581797_V2_1_1,
	REPLACE(JSON_QUERY(row,'$.D_715581797_V2_2_2'), '\"', '') AS D_715581797_V2_2_2,
	REPLACE(JSON_QUERY(row,'$.D_715581797_V2_3_3'), '\"', '') AS D_715581797_V2_3_3,
	REPLACE(JSON_QUERY(row,'$.D_715581797_V2_4_4'), '\"', '') AS D_715581797_V2_4_4,
	REPLACE(JSON_QUERY(row,'$.D_744230001_1_1'), '\"', '') AS D_744230001_1_1,
	REPLACE(JSON_QUERY(row,'$.D_749956170.D_143206081'), '\"', '') AS D_749956170_D_143206081,
	REPLACE(JSON_QUERY(row,'$.D_749956170.D_223008071'), '\"', '') AS D_749956170_D_223008071,
	REPLACE(JSON_QUERY(row,'$.D_749956170.D_431203595'), '\"', '') AS D_749956170_D_431203595,
	REPLACE(JSON_QUERY(row,'$.D_749956170.D_516899143'), '\"', '') AS D_749956170_D_516899143,
	REPLACE(JSON_QUERY(row,'$.D_749956170.D_527872064'), '\"', '') AS D_749956170_D_527872064,
	REPLACE(JSON_QUERY(row,'$.D_749956170.D_691752394'), '\"', '') AS D_749956170_D_691752394,
	REPLACE(JSON_QUERY(row,'$.D_751358419.D_238135048'), '\"', '') AS D_751358419_D_238135048,
	REPLACE(JSON_QUERY(row,'$.D_751358419.D_524096053'), '\"', '') AS D_751358419_D_524096053,
	REPLACE(JSON_QUERY(row,'$.D_751358419.D_632714520'), '\"', '') AS D_751358419_D_632714520,
	REPLACE(JSON_QUERY(row,'$.D_751358419.D_635026188'), '\"', '') AS D_751358419_D_635026188,
	REPLACE(JSON_QUERY(row,'$.D_751358419.D_814101706'), '\"', '') AS D_751358419_D_814101706,
	REPLACE(JSON_QUERY(row,'$.D_775313030_1_1'), '\"', '') AS D_775313030_1_1,
	REPLACE(JSON_QUERY(row,'$.D_775313030_2_2'), '\"', '') AS D_775313030_2_2,
	REPLACE(JSON_QUERY(row,'$.D_782396371_1_1'), '\"', '') AS D_782396371_1_1,
	REPLACE(JSON_QUERY(row,'$.D_798452445'), '\"', '') AS D_798452445,
	REPLACE(JSON_QUERY(row,'$.D_803339020_1_1'), '\"', '') AS D_803339020_1_1,
	REPLACE(JSON_QUERY(row,'$.D_813989715.D_151327643'), '\"', '') AS D_813989715_D_151327643,
	REPLACE(JSON_QUERY(row,'$.D_813989715.D_283112988'), '\"', '') AS D_813989715_D_283112988,
	REPLACE(JSON_QUERY(row,'$.D_813989715.D_440872808'), '\"', '') AS D_813989715_D_440872808,
	REPLACE(JSON_QUERY(row,'$.D_813989715.D_580629349'), '\"', '') AS D_813989715_D_580629349,
	REPLACE(JSON_QUERY(row,'$.D_813989715.D_874168085'), '\"', '') AS D_813989715_D_874168085,
	REPLACE(JSON_QUERY(row,'$.D_813989715.D_874223830'), '\"', '') AS D_813989715_D_874223830,
	REPLACE(JSON_QUERY(row,'$.D_847578001.D_167695804'), '\"', '') AS D_847578001_D_167695804,
	REPLACE(JSON_QUERY(row,'$.D_847578001.D_215996690'), '\"', '') AS D_847578001_D_215996690,
	REPLACE(JSON_QUERY(row,'$.D_847578001.D_462737492'), '\"', '') AS D_847578001_D_462737492,
	REPLACE(JSON_QUERY(row,'$.D_847578001.D_469675296'), '\"', '') AS D_847578001_D_469675296,
	REPLACE(JSON_QUERY(row,'$.D_847578001.D_488415137'), '\"', '') AS D_847578001_D_488415137,
	REPLACE(JSON_QUERY(row,'$.D_847578001.D_730334054'), '\"', '') AS D_847578001_D_730334054,
	REPLACE(JSON_QUERY(row,'$.D_857165713.D_187399900'), '\"', '') AS D_857165713_D_187399900,
	REPLACE(JSON_QUERY(row,'$.D_857165713.D_219358831'), '\"', '') AS D_857165713_D_219358831,
	REPLACE(JSON_QUERY(row,'$.D_857165713.D_243443780'), '\"', '') AS D_857165713_D_243443780,
	REPLACE(JSON_QUERY(row,'$.D_857165713.D_357462273'), '\"', '') AS D_857165713_D_357462273,
	REPLACE(JSON_QUERY(row,'$.D_857165713.D_636367178'), '\"', '') AS D_857165713_D_636367178,
	REPLACE(JSON_QUERY(row,'$.D_857165713.D_638380747'), '\"', '') AS D_857165713_D_638380747,
	REPLACE(JSON_QUERY(row,'$.D_857165713.D_847529903'), '\"', '') AS D_857165713_D_847529903,
	REPLACE(JSON_QUERY(row,'$.D_860011428'), '\"', '') AS D_860011428,
	REPLACE(JSON_QUERY(row,'$.D_867307558'), '\"', '') AS D_867307558,
	REPLACE(JSON_QUERY(row,'$.D_875535246'), '\"', '') AS D_875535246,
	REPLACE(JSON_QUERY(row,'$.D_877074400'), '\"', '') AS D_877074400,
	REPLACE(JSON_QUERY(row,'$.D_877074400_V2'), '\"', '') AS D_877074400_V2,
	REPLACE(JSON_QUERY(row,'$.D_890156588'), '\"', '') AS D_890156588,
	REPLACE(JSON_QUERY(row,'$.D_890156588_V2'), '\"', '') AS D_890156588_V2,
	REPLACE(JSON_QUERY(row,'$.D_893966847_1_1'), '\"', '') AS D_893966847_1_1,
	REPLACE(JSON_QUERY(row,'$.D_928530823_1_1'), '\"', '') AS D_928530823_1_1,
	REPLACE(JSON_QUERY(row,'$.D_928530823_2_2'), '\"', '') AS D_928530823_2_2,
	REPLACE(JSON_QUERY(row,'$.D_930944000_1_1'), '\"', '') AS D_930944000_1_1,
	REPLACE(JSON_QUERY(row,'$.D_934384452'), '\"', '') AS D_934384452,
	REPLACE(JSON_QUERY(row,'$.D_959877599.D_700620868'), '\"', '') AS D_959877599_D_700620868,
	REPLACE(JSON_QUERY(row,'$.D_980800222_1_1.D_173502329_1_1'), '\"', '') AS D_980800222_1_1_D_173502329_1_1,
	REPLACE(JSON_QUERY(row,'$.D_980800222_1_1.D_366972678_1_1'), '\"', '') AS D_980800222_1_1_D_366972678_1_1,
	REPLACE(JSON_QUERY(row,'$.D_980800222_2_2.D_173502329_2_2'), '\"', '') AS D_980800222_2_2_D_173502329_2_2,
	REPLACE(JSON_QUERY(row,'$.D_980800222_2_2.D_366972678_2_2'), '\"', '') AS D_980800222_2_2_D_366972678_2_2,
	REPLACE(JSON_QUERY(row,'$.D_984121390_1_1'), '\"', '') AS D_984121390_1_1,
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
    

