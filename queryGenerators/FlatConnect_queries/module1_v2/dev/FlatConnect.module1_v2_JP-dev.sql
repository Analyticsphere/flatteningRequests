-- FYI: This query was automatically generated using R code written by Jake 
-- Peters. The code references a the source table schema and a configuration
-- file specifying various parameters. This style of query was developed by 
-- Warren Lu to flatten the Connect4Cancer datasets in 2022. See GitHub repo
-- for further documentation.
-- 
-- Repository: https://github.com/Analyticsphere/flatteningRequests
-- Relavent functions: generate_flattening_query.R
-- 
-- source_table: nih-nci-dceg-connect-dev.Connect.module1_v2
-- destination table: nih-nci-dceg-connect-dev.FlatConnect.module1_v2_JP -- notes
    
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
    "D_167101091": [
        535003378
    ],
    "D_180961306": [
        535003378
    ],
    "D_384191091.D_384191091": [
        412790539,
        636411467
    ],
    "D_525535977.D_525535977": [
        773844957
    ],
    "D_543728565": [
        535003378
    ],
    "D_588212264.D_588212264": [
        163149180
    ],
    "D_624179836": [
        535003378
    ],
    "D_700374192.D_700374192": [
        766533183
    ],
    "D_725626004": [
        535003378
    ],
    "D_797626610.D_797626610": [
        163149180
    ],
    "D_874046190": [
        535003378
    ],
    "D_874709643": [
        535003378
    ],
    "D_894259747": [
        407340134
    ],
    "D_900541533": [
        535003378
    ],
    "D_936904328": [
        245893583,
        506790433,
        601057066
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
  `nih-nci-dceg-connect-dev.FlatConnect.module1_v2_JP` -- destination_table
  OPTIONS (description="Source table: Connect.module1_v2; Scheduled Query: FlatConnect.module1_v2_JP; GitHub: https://github.com/Analyticsphere/flatteningRequests/tree/main/queryGenerators/FlatConnect_queries/module1_v2; Team: Analytics; Maintainer: Jake Peters; Super Users: Kelsey; Notes: This table is a flattened version of Connect.module1_v2.") -- table_description
  AS (
  WITH
    json_data AS (
    SELECT
      [handleRow(TO_JSON_STRING(input_row))] AS body
    FROM
      `nih-nci-dceg-connect-dev.Connect.module1_v2` AS input_row -- source_table
    ), -- filter_statement
    flattened_data AS (
    SELECT
      	REPLACE(JSON_QUERY(row,'$.Connect_ID'), '\"', '') AS Connect_ID,
	REPLACE(JSON_QUERY(row,'$.D_114314839.D_340854069'), '\"', '') AS D_114314839_D_340854069,
	REPLACE(JSON_QUERY(row,'$.D_114314839.D_600462977'), '\"', '') AS D_114314839_D_600462977,
	REPLACE(JSON_QUERY(row,'$.D_167101091.D_535003378'), '\"', '') AS D_167101091_D_535003378,
	REPLACE(JSON_QUERY(row,'$.D_170440011'), '\"', '') AS D_170440011,
	REPLACE(JSON_QUERY(row,'$.D_178774803.D_334956961'), '\"', '') AS D_178774803_D_334956961,
	REPLACE(JSON_QUERY(row,'$.D_180961306.D_535003378'), '\"', '') AS D_180961306_D_535003378,
	REPLACE(JSON_QUERY(row,'$.D_201449164.D_206625031'), '\"', '') AS D_201449164_D_206625031,
	REPLACE(JSON_QUERY(row,'$.D_276353712'), '\"', '') AS D_276353712,
	REPLACE(JSON_QUERY(row,'$.D_283634623'), '\"', '') AS D_283634623,
	REPLACE(JSON_QUERY(row,'$.D_285718391.D_286149234'), '\"', '') AS D_285718391_D_286149234,
	REPLACE(JSON_QUERY(row,'$.D_289664241.D_289664241'), '\"', '') AS D_289664241_D_289664241,
	REPLACE(JSON_QUERY(row,'$.D_301414575.D_206625031'), '\"', '') AS D_301414575_D_206625031,
	REPLACE(JSON_QUERY(row,'$.D_323512813'), '\"', '') AS D_323512813,
	REPLACE(JSON_QUERY(row,'$.D_338924834'), '\"', '') AS D_338924834,
	REPLACE(JSON_QUERY(row,'$.D_347860896'), '\"', '') AS D_347860896,
	REPLACE(JSON_QUERY(row,'$.D_354326265.D_378988419'), '\"', '') AS D_354326265_D_378988419,
	REPLACE(JSON_QUERY(row,'$.D_367374606'), '\"', '') AS D_367374606,
	REPLACE(JSON_QUERY(row,'$.D_367803647.D_367803647'), '\"', '') AS D_367803647_D_367803647,
	REPLACE(JSON_QUERY(row,'$.D_384191091.D_384191091.D_412790539'), '\"', '') AS D_384191091_D_384191091_D_412790539,
	REPLACE(JSON_QUERY(row,'$.D_384191091.D_384191091.D_636411467'), '\"', '') AS D_384191091_D_384191091_D_636411467,
	REPLACE(JSON_QUERY(row,'$.D_406098499_1_1'), '\"', '') AS D_406098499_1_1,
	REPLACE(JSON_QUERY(row,'$.D_407056417'), '\"', '') AS D_407056417,
	REPLACE(JSON_QUERY(row,'$.D_431721131'), '\"', '') AS D_431721131,
	REPLACE(JSON_QUERY(row,'$.D_434316600'), '\"', '') AS D_434316600,
	REPLACE(JSON_QUERY(row,'$.D_446999144'), '\"', '') AS D_446999144,
	REPLACE(JSON_QUERY(row,'$.D_453252072'), '\"', '') AS D_453252072,
	REPLACE(JSON_QUERY(row,'$.D_479353866'), '\"', '') AS D_479353866,
	REPLACE(JSON_QUERY(row,'$.D_525535977.D_525535977.D_773844957'), '\"', '') AS D_525535977_D_525535977_D_773844957,
	REPLACE(JSON_QUERY(row,'$.D_535621923'), '\"', '') AS D_535621923,
	REPLACE(JSON_QUERY(row,'$.D_537153788'), '\"', '') AS D_537153788,
	REPLACE(JSON_QUERY(row,'$.D_543728565.D_535003378'), '\"', '') AS D_543728565_D_535003378,
	REPLACE(JSON_QUERY(row,'$.D_555481393.D_555481393'), '\"', '') AS D_555481393_D_555481393,
	REPLACE(JSON_QUERY(row,'$.D_579563781_1_1'), '\"', '') AS D_579563781_1_1,
	REPLACE(JSON_QUERY(row,'$.D_588212264.D_588212264.D_163149180'), '\"', '') AS D_588212264_D_588212264_D_163149180,
	REPLACE(JSON_QUERY(row,'$.D_589588769_1_1'), '\"', '') AS D_589588769_1_1,
	REPLACE(JSON_QUERY(row,'$.D_607773106_1_1'), '\"', '') AS D_607773106_1_1,
	REPLACE(JSON_QUERY(row,'$.D_613744428'), '\"', '') AS D_613744428,
	REPLACE(JSON_QUERY(row,'$.D_624179836.D_535003378'), '\"', '') AS D_624179836_D_535003378,
	REPLACE(JSON_QUERY(row,'$.D_627122657'), '\"', '') AS D_627122657,
	REPLACE(JSON_QUERY(row,'$.D_641651634.D_173508724'), '\"', '') AS D_641651634_D_173508724,
	REPLACE(JSON_QUERY(row,'$.D_646504105'), '\"', '') AS D_646504105,
	REPLACE(JSON_QUERY(row,'$.D_650399623'), '\"', '') AS D_650399623,
	REPLACE(JSON_QUERY(row,'$.D_659784914'), '\"', '') AS D_659784914,
	REPLACE(JSON_QUERY(row,'$.D_677262300'), '\"', '') AS D_677262300,
	REPLACE(JSON_QUERY(row,'$.D_694265648'), '\"', '') AS D_694265648,
	REPLACE(JSON_QUERY(row,'$.D_700374192.D_700374192.D_766533183'), '\"', '') AS D_700374192_D_700374192_D_766533183,
	REPLACE(JSON_QUERY(row,'$.D_724181652'), '\"', '') AS D_724181652,
	REPLACE(JSON_QUERY(row,'$.D_725626004.D_535003378'), '\"', '') AS D_725626004_D_535003378,
	REPLACE(JSON_QUERY(row,'$.D_739294356'), '\"', '') AS D_739294356,
	REPLACE(JSON_QUERY(row,'$.D_746012894'), '\"', '') AS D_746012894,
	REPLACE(JSON_QUERY(row,'$.D_759004335'), '\"', '') AS D_759004335,
	REPLACE(JSON_QUERY(row,'$.D_761310265'), '\"', '') AS D_761310265,
	REPLACE(JSON_QUERY(row,'$.D_769668224'), '\"', '') AS D_769668224,
	REPLACE(JSON_QUERY(row,'$.D_783167257'), '\"', '') AS D_783167257,
	REPLACE(JSON_QUERY(row,'$.D_784967158'), '\"', '') AS D_784967158,
	REPLACE(JSON_QUERY(row,'$.d_794613585'), '\"', '') AS d_794613585,
	REPLACE(JSON_QUERY(row,'$.D_796828094'), '\"', '') AS D_796828094,
	REPLACE(JSON_QUERY(row,'$.D_797626610.D_797626610.D_163149180'), '\"', '') AS D_797626610_D_797626610_D_163149180,
	REPLACE(JSON_QUERY(row,'$.D_820324171'), '\"', '') AS D_820324171,
	REPLACE(JSON_QUERY(row,'$.D_847533056'), '\"', '') AS D_847533056,
	REPLACE(JSON_QUERY(row,'$.D_848580651'), '\"', '') AS D_848580651,
	REPLACE(JSON_QUERY(row,'$.D_868232409'), '\"', '') AS D_868232409,
	REPLACE(JSON_QUERY(row,'$.D_869387390_1_1.D_478706011_1_1'), '\"', '') AS D_869387390_1_1_D_478706011_1_1,
	REPLACE(JSON_QUERY(row,'$.D_874046190.D_535003378'), '\"', '') AS D_874046190_D_535003378,
	REPLACE(JSON_QUERY(row,'$.D_874709643.D_535003378'), '\"', '') AS D_874709643_D_535003378,
	REPLACE(JSON_QUERY(row,'$.D_875208305'), '\"', '') AS D_875208305,
	REPLACE(JSON_QUERY(row,'$.D_890637099_1_1'), '\"', '') AS D_890637099_1_1,
	REPLACE(JSON_QUERY(row,'$.D_894259747.D_407340134'), '\"', '') AS D_894259747_D_407340134,
	REPLACE(JSON_QUERY(row,'$.D_900541533.D_535003378'), '\"', '') AS D_900541533_D_535003378,
	REPLACE(JSON_QUERY(row,'$.D_912857732.D_121646540'), '\"', '') AS D_912857732_D_121646540,
	REPLACE(JSON_QUERY(row,'$.D_912857732.D_821387277'), '\"', '') AS D_912857732_D_821387277,
	REPLACE(JSON_QUERY(row,'$.D_936904328.D_245893583'), '\"', '') AS D_936904328_D_245893583,
	REPLACE(JSON_QUERY(row,'$.D_936904328.D_506790433'), '\"', '') AS D_936904328_D_506790433,
	REPLACE(JSON_QUERY(row,'$.D_936904328.D_601057066'), '\"', '') AS D_936904328_D_601057066,
	REPLACE(JSON_QUERY(row,'$.D_952682839'), '\"', '') AS D_952682839,
	REPLACE(JSON_QUERY(row,'$.D_986275155.D_661719912'), '\"', '') AS D_986275155_D_661719912,
	REPLACE(JSON_QUERY(row,'$.D_986275155.D_801653230'), '\"', '') AS D_986275155_D_801653230,
	REPLACE(JSON_QUERY(row,'$.D_987107433'), '\"', '') AS D_987107433,
	REPLACE(JSON_QUERY(row,'$.D_992987417'), '\"', '') AS D_992987417,
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
    

