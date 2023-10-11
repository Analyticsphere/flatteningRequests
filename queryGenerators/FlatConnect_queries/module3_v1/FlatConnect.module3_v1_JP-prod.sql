CREATE TEMP FUNCTION
  handleM1(input_row STRING)
  RETURNS STRING
  LANGUAGE js AS r"""

  function getNestedObjectValue(obj, pathString) {
    function getNestedObjectValueFromPathArray(obj, pathArray) {
      let currKey = pathArray[0];
      if (
        typeof obj[currKey] === 'undefined' || obj[currKey] === null ||
        pathArray.length === 1
      )
        return obj[currKey];

      return getNestedObjectValueFromPathArray(obj[currKey], pathArray.slice(1));
    }

    return getNestedObjectValueFromPathArray(obj, pathString.split('.'));
  }

  function setNestedObjectValue(obj, pathString, value) {
    function setNestedObjectValueFromPathArray(obj, pathArray, value) {
      let currKey = pathArray[0];

      if (pathArray.length === 1) {
        obj[currKey] = value;
        return;
      }

      if (
        typeof obj[currKey] === 'undefined' ||
        typeof obj[currKey] !== 'object'
      ) {
        obj[currKey] = {};
      }

      return setNestedObjectValueFromPathArray(
        obj[currKey],
        pathArray.slice(1),
        value
      );
    }
    return setNestedObjectValueFromPathArray(obj, pathString.split('.'), value);
  }

  const arraysToBeFlattened={
 "D_103566006.D_103566006": [
  123926260,
  741643840,
  807835037,
  959535900
 ],
 "D_116080663.D_116080663": [
  648960871
 ],
 "D_154603497.D_154603497": [
  648960871
 ],
 "D_257300783.D_257300783": [
  648960871
 ],
 "D_378254382.D_378254382": [
  648960871
 ],
 "D_447720598": [
  181769837,
  549079588,
  889023234,
  896953195
 ],
 "D_470862706.D_178609656": [
  104676242,
  137733407,
  663253668,
  789689151
 ],
 "D_470862706.D_261957180": [
  104676242,
  137733407,
  663253668,
  789689151
 ],
 "D_470862706.D_356133766": [
  104676242,
  137733407,
  663253668,
  789689151
 ],
 "D_470862706.D_643512687": [
  104676242,
  137733407,
  663253668,
  789689151
 ],
 "D_470862706.D_688123102": [
  104676242,
  137733407,
  663253668,
  789689151
 ],
 "D_470862706.D_690918725": [
  104676242,
  137733407,
  663253668,
  789689151
 ],
 "D_470862706.D_912659087": [
  104676242,
  137733407,
  663253668,
  789689151
 ],
 "D_546410473.D_546410473": [
  648960871
 ],
 "D_550244580.D_550244580": [
  648960871
 ],
 "D_556837046.D_556837046": [
  648960871
 ],
 "D_653198444.D_653198444": [
  648960871
 ],
 "D_742254559.D_742254559": [
  648960871
 ],
 "D_789159346.D_789159346": [
  572260608,
  732465277,
  807835037,
  895826652
 ],
 "D_790436165.D_790436165": [
  648960871
 ],
 "D_814115676.D_814115676": [
  648960871
 ],
 "D_830608495": [
  158841370,
  349034127,
  535003378,
  675129470,
  713594719,
  760521319
 ],
 "D_942945860.D_942945860": [
  648960871
 ],
 "D_947205597": [
  198133418,
  535003378,
  539648641,
  686310465,
  706254326,
  712653855,
  817381897
 ]
}

  function handleM1JS(row) {
    for (let arrPath of Object.keys(arraysToBeFlattened)) {
      let currObj = {};
      let inputConceptIdList = getNestedObjectValue(row, arrPath);
      if (!inputConceptIdList || inputConceptIdList.length === 0) continue;
      inputConceptIdList = inputConceptIdList.map(v => +v);

      for (let cid of arraysToBeFlattened[arrPath]) {
        if (inputConceptIdList.indexOf(cid) >= 0) {
          currObj['D_' + cid] = 1;
        } else currObj['D_' + cid] = 0;
      }

      setNestedObjectValue(row, arrPath, currObj);
    }
    return JSON.stringify(row);
  }

  const row = JSON.parse(input_row);
  return handleM1JS(row);

""";

  CREATE OR REPLACE TABLE FlatConnect.module3_v1_JP 
OPTIONS (description="Source table: Connect.module4_v1; Scheduled Query: FlatConnect.module3_v1_JP; GitHub: https://github.com/Analyticsphere/flatteningRequests/tree/main/queryGenerators/FlatConnect_queries/module3_v1; Team: Analytics; Maintainer: Jake Peters; Super Users: Kelsey; Notes: This table is a flattened version of Connect.module3_v1.") 
AS (
  WITH
  json_data AS (
    SELECT
      Connect_ID,
      uid,
      [handleM1(TO_JSON_STRING(input_row))] AS body
    FROM
      `nih-nci-dceg-connect-prod-6d04.Connect.module3_v1` AS input_row where Connect_ID is not null
  ),
  flattened_data AS (
    SELECT
      REPLACE(JSON_QUERY(row,'$.Connect_ID'), '\"', '') AS Connect_ID,
REPLACE(JSON_QUERY(row,'$.D_101170268'), '\"', '') AS D_101170268,
REPLACE(JSON_QUERY(row,'$.D_101710639'), '\"', '') AS D_101710639,
REPLACE(JSON_QUERY(row,'$.D_103045461'), '\"', '') AS D_103045461,
REPLACE(JSON_QUERY(row,'$.D_103566006.D_103566006.D_123926260'), '\"', '') AS D_103566006_D_103566006_D_123926260,
REPLACE(JSON_QUERY(row,'$.D_103566006.D_103566006.D_741643840'), '\"', '') AS D_103566006_D_103566006_D_741643840,
REPLACE(JSON_QUERY(row,'$.D_103566006.D_103566006.D_807835037'), '\"', '') AS D_103566006_D_103566006_D_807835037,
REPLACE(JSON_QUERY(row,'$.D_103566006.D_103566006.D_959535900'), '\"', '') AS D_103566006_D_103566006_D_959535900,
REPLACE(JSON_QUERY(row,'$.D_103566006.D_325879966'), '\"', '') AS D_103566006_D_325879966,
REPLACE(JSON_QUERY(row,'$.D_115909186'), '\"', '') AS D_115909186,
REPLACE(JSON_QUERY(row,'$.D_116014097'), '\"', '') AS D_116014097,
REPLACE(JSON_QUERY(row,'$.D_116080663.D_116080663.D_648960871'), '\"', '') AS D_116080663_D_116080663_D_648960871,
REPLACE(JSON_QUERY(row,'$.D_116080663.D_889949861'), '\"', '') AS D_116080663_D_889949861,
REPLACE(JSON_QUERY(row,'$.D_116510981'), '\"', '') AS D_116510981,
REPLACE(JSON_QUERY(row,'$.D_119257304'), '\"', '') AS D_119257304,
REPLACE(JSON_QUERY(row,'$.D_119378795'), '\"', '') AS D_119378795,
REPLACE(JSON_QUERY(row,'$.D_127871793'), '\"', '') AS D_127871793,
REPLACE(JSON_QUERY(row,'$.D_129677435'), '\"', '') AS D_129677435,
REPLACE(JSON_QUERY(row,'$.D_133396976'), '\"', '') AS D_133396976,
REPLACE(JSON_QUERY(row,'$.D_140848765'), '\"', '') AS D_140848765,
REPLACE(JSON_QUERY(row,'$.D_142164107'), '\"', '') AS D_142164107,
REPLACE(JSON_QUERY(row,'$.D_145078867'), '\"', '') AS D_145078867,
REPLACE(JSON_QUERY(row,'$.D_146319128'), '\"', '') AS D_146319128,
REPLACE(JSON_QUERY(row,'$.D_146877593'), '\"', '') AS D_146877593,
REPLACE(JSON_QUERY(row,'$.D_147028925'), '\"', '') AS D_147028925,
REPLACE(JSON_QUERY(row,'$.D_154603497.D_154603497.D_648960871'), '\"', '') AS D_154603497_D_154603497_D_648960871,
REPLACE(JSON_QUERY(row,'$.D_154603497.D_234430294'), '\"', '') AS D_154603497_D_234430294,
REPLACE(JSON_QUERY(row,'$.D_171050898'), '\"', '') AS D_171050898,
REPLACE(JSON_QUERY(row,'$.D_175568583'), '\"', '') AS D_175568583,
REPLACE(JSON_QUERY(row,'$.D_175995366'), '\"', '') AS D_175995366,
REPLACE(JSON_QUERY(row,'$.D_177252178'), '\"', '') AS D_177252178,
REPLACE(JSON_QUERY(row,'$.D_178242940'), '\"', '') AS D_178242940,
REPLACE(JSON_QUERY(row,'$.D_178665310'), '\"', '') AS D_178665310,
REPLACE(JSON_QUERY(row,'$.D_179510070'), '\"', '') AS D_179510070,
REPLACE(JSON_QUERY(row,'$.D_180308733'), '\"', '') AS D_180308733,
REPLACE(JSON_QUERY(row,'$.D_182431332'), '\"', '') AS D_182431332,
REPLACE(JSON_QUERY(row,'$.D_183546626'), '\"', '') AS D_183546626,
REPLACE(JSON_QUERY(row,'$.D_194557808'), '\"', '') AS D_194557808,
REPLACE(JSON_QUERY(row,'$.D_197309242'), '\"', '') AS D_197309242,
REPLACE(JSON_QUERY(row,'$.D_199612020'), '\"', '') AS D_199612020,
REPLACE(JSON_QUERY(row,'$.D_204884007'), '\"', '') AS D_204884007,
REPLACE(JSON_QUERY(row,'$.D_208578552'), '\"', '') AS D_208578552,
REPLACE(JSON_QUERY(row,'$.D_210016366'), '\"', '') AS D_210016366,
REPLACE(JSON_QUERY(row,'$.D_213679054'), '\"', '') AS D_213679054,
REPLACE(JSON_QUERY(row,'$.D_227439141'), '\"', '') AS D_227439141,
REPLACE(JSON_QUERY(row,'$.D_228752045'), '\"', '') AS D_228752045,
REPLACE(JSON_QUERY(row,'$.D_237415094'), '\"', '') AS D_237415094,
REPLACE(JSON_QUERY(row,'$.D_238422161'), '\"', '') AS D_238422161,
REPLACE(JSON_QUERY(row,'$.D_238953261'), '\"', '') AS D_238953261,
REPLACE(JSON_QUERY(row,'$.D_241771440'), '\"', '') AS D_241771440,
REPLACE(JSON_QUERY(row,'$.D_242276545'), '\"', '') AS D_242276545,
REPLACE(JSON_QUERY(row,'$.D_242987943'), '\"', '') AS D_242987943,
REPLACE(JSON_QUERY(row,'$.D_243519559'), '\"', '') AS D_243519559,
REPLACE(JSON_QUERY(row,'$.D_250456537'), '\"', '') AS D_250456537,
REPLACE(JSON_QUERY(row,'$.D_252017075.D_106272739'), '\"', '') AS D_252017075_D_106272739,
REPLACE(JSON_QUERY(row,'$.D_252017075.D_257564920'), '\"', '') AS D_252017075_D_257564920,
REPLACE(JSON_QUERY(row,'$.D_252017075.D_359443374'), '\"', '') AS D_252017075_D_359443374,
REPLACE(JSON_QUERY(row,'$.D_252017075.D_670036093'), '\"', '') AS D_252017075_D_670036093,
REPLACE(JSON_QUERY(row,'$.D_254689566'), '\"', '') AS D_254689566,
REPLACE(JSON_QUERY(row,'$.D_254739443'), '\"', '') AS D_254739443,
REPLACE(JSON_QUERY(row,'$.D_257300783.D_257300783.D_648960871'), '\"', '') AS D_257300783_D_257300783_D_648960871,
REPLACE(JSON_QUERY(row,'$.D_257300783.D_424527685'), '\"', '') AS D_257300783_D_424527685,
REPLACE(JSON_QUERY(row,'$.D_258291510'), '\"', '') AS D_258291510,
REPLACE(JSON_QUERY(row,'$.D_258703325'), '\"', '') AS D_258703325,
REPLACE(JSON_QUERY(row,'$.D_259481608'), '\"', '') AS D_259481608,
REPLACE(JSON_QUERY(row,'$.D_265469878.D_265469878'), '\"', '') AS D_265469878_D_265469878,
REPLACE(JSON_QUERY(row,'$.D_265469878.D_781650542'), '\"', '') AS D_265469878_D_781650542,
REPLACE(JSON_QUERY(row,'$.D_268240612.D_268240612'), '\"', '') AS D_268240612_D_268240612,
REPLACE(JSON_QUERY(row,'$.D_268240612.D_531654334'), '\"', '') AS D_268240612_D_531654334,
REPLACE(JSON_QUERY(row,'$.D_268407776'), '\"', '') AS D_268407776,
REPLACE(JSON_QUERY(row,'$.D_276575533.D_276575533'), '\"', '') AS D_276575533_D_276575533,
REPLACE(JSON_QUERY(row,'$.D_276575533.D_810608313'), '\"', '') AS D_276575533_D_810608313,
REPLACE(JSON_QUERY(row,'$.D_286598574'), '\"', '') AS D_286598574,
REPLACE(JSON_QUERY(row,'$.D_287715042'), '\"', '') AS D_287715042,
REPLACE(JSON_QUERY(row,'$.D_304657762'), '\"', '') AS D_304657762,
REPLACE(JSON_QUERY(row,'$.D_306320432'), '\"', '') AS D_306320432,
REPLACE(JSON_QUERY(row,'$.D_311569789'), '\"', '') AS D_311569789,
REPLACE(JSON_QUERY(row,'$.D_318569032'), '\"', '') AS D_318569032,
REPLACE(JSON_QUERY(row,'$.D_319397722'), '\"', '') AS D_319397722,
REPLACE(JSON_QUERY(row,'$.D_325229459'), '\"', '') AS D_325229459,
REPLACE(JSON_QUERY(row,'$.D_327050687'), '\"', '') AS D_327050687,
REPLACE(JSON_QUERY(row,'$.D_333072838'), '\"', '') AS D_333072838,
REPLACE(JSON_QUERY(row,'$.D_334292580'), '\"', '') AS D_334292580,
REPLACE(JSON_QUERY(row,'$.D_336810811'), '\"', '') AS D_336810811,
REPLACE(JSON_QUERY(row,'$.D_337810964'), '\"', '') AS D_337810964,
REPLACE(JSON_QUERY(row,'$.D_338467033'), '\"', '') AS D_338467033,
REPLACE(JSON_QUERY(row,'$.D_344583659'), '\"', '') AS D_344583659,
REPLACE(JSON_QUERY(row,'$.D_344631681'), '\"', '') AS D_344631681,
REPLACE(JSON_QUERY(row,'$.D_345457702'), '\"', '') AS D_345457702,
REPLACE(JSON_QUERY(row,'$.D_352986465'), '\"', '') AS D_352986465,
REPLACE(JSON_QUERY(row,'$.D_355689185'), '\"', '') AS D_355689185,
REPLACE(JSON_QUERY(row,'$.D_356470028'), '\"', '') AS D_356470028,
REPLACE(JSON_QUERY(row,'$.D_360277949'), '\"', '') AS D_360277949,
REPLACE(JSON_QUERY(row,'$.D_363377579'), '\"', '') AS D_363377579,
REPLACE(JSON_QUERY(row,'$.D_365370279'), '\"', '') AS D_365370279,
REPLACE(JSON_QUERY(row,'$.D_370253256'), '\"', '') AS D_370253256,
REPLACE(JSON_QUERY(row,'$.D_370352501'), '\"', '') AS D_370352501,
REPLACE(JSON_QUERY(row,'$.D_370901901'), '\"', '') AS D_370901901,
REPLACE(JSON_QUERY(row,'$.D_372398512'), '\"', '') AS D_372398512,
REPLACE(JSON_QUERY(row,'$.D_375227733'), '\"', '') AS D_375227733,
REPLACE(JSON_QUERY(row,'$.D_377064836.D_377064836'), '\"', '') AS D_377064836_D_377064836,
REPLACE(JSON_QUERY(row,'$.D_377064836.D_779642303'), '\"', '') AS D_377064836_D_779642303,
REPLACE(JSON_QUERY(row,'$.D_378254382.D_378254382.D_648960871'), '\"', '') AS D_378254382_D_378254382_D_648960871,
REPLACE(JSON_QUERY(row,'$.D_378254382.D_628818510'), '\"', '') AS D_378254382_D_628818510,
REPLACE(JSON_QUERY(row,'$.D_381268397'), '\"', '') AS D_381268397,
REPLACE(JSON_QUERY(row,'$.D_383748300'), '\"', '') AS D_383748300,
REPLACE(JSON_QUERY(row,'$.D_384562783'), '\"', '') AS D_384562783,
REPLACE(JSON_QUERY(row,'$.D_385490512'), '\"', '') AS D_385490512,
REPLACE(JSON_QUERY(row,'$.D_386950237'), '\"', '') AS D_386950237,
REPLACE(JSON_QUERY(row,'$.D_387489930'), '\"', '') AS D_387489930,
REPLACE(JSON_QUERY(row,'$.D_388614168'), '\"', '') AS D_388614168,
REPLACE(JSON_QUERY(row,'$.D_394049992'), '\"', '') AS D_394049992,
REPLACE(JSON_QUERY(row,'$.D_394410256'), '\"', '') AS D_394410256,
REPLACE(JSON_QUERY(row,'$.D_395480458'), '\"', '') AS D_395480458,
REPLACE(JSON_QUERY(row,'$.D_399659169'), '\"', '') AS D_399659169,
REPLACE(JSON_QUERY(row,'$.D_400969127'), '\"', '') AS D_400969127,
REPLACE(JSON_QUERY(row,'$.D_402300898'), '\"', '') AS D_402300898,
REPLACE(JSON_QUERY(row,'$.D_405178123'), '\"', '') AS D_405178123,
REPLACE(JSON_QUERY(row,'$.D_411088265'), '\"', '') AS D_411088265,
REPLACE(JSON_QUERY(row,'$.D_411960217'), '\"', '') AS D_411960217,
REPLACE(JSON_QUERY(row,'$.D_417552256'), '\"', '') AS D_417552256,
REPLACE(JSON_QUERY(row,'$.D_420155119'), '\"', '') AS D_420155119,
REPLACE(JSON_QUERY(row,'$.D_429228540'), '\"', '') AS D_429228540,
REPLACE(JSON_QUERY(row,'$.D_434034366'), '\"', '') AS D_434034366,
REPLACE(JSON_QUERY(row,'$.D_442002787'), '\"', '') AS D_442002787,
REPLACE(JSON_QUERY(row,'$.D_444759994'), '\"', '') AS D_444759994,
REPLACE(JSON_QUERY(row,'$.D_445187084'), '\"', '') AS D_445187084,
REPLACE(JSON_QUERY(row,'$.D_445537380'), '\"', '') AS D_445537380,
REPLACE(JSON_QUERY(row,'$.D_445712879'), '\"', '') AS D_445712879,
REPLACE(JSON_QUERY(row,'$.D_447720598.D_181769837'), '\"', '') AS D_447720598_D_181769837,
REPLACE(JSON_QUERY(row,'$.D_447720598.D_549079588'), '\"', '') AS D_447720598_D_549079588,
REPLACE(JSON_QUERY(row,'$.D_447720598.D_889023234'), '\"', '') AS D_447720598_D_889023234,
REPLACE(JSON_QUERY(row,'$.D_447720598.D_896953195'), '\"', '') AS D_447720598_D_896953195,
REPLACE(JSON_QUERY(row,'$.D_448968121'), '\"', '') AS D_448968121,
REPLACE(JSON_QUERY(row,'$.D_450151555'), '\"', '') AS D_450151555,
REPLACE(JSON_QUERY(row,'$.D_457290610'), '\"', '') AS D_457290610,
REPLACE(JSON_QUERY(row,'$.D_458395129'), '\"', '') AS D_458395129,
REPLACE(JSON_QUERY(row,'$.D_459096272'), '\"', '') AS D_459096272,
REPLACE(JSON_QUERY(row,'$.D_463908980'), '\"', '') AS D_463908980,
REPLACE(JSON_QUERY(row,'$.D_466473224'), '\"', '') AS D_466473224,
REPLACE(JSON_QUERY(row,'$.D_467751479'), '\"', '') AS D_467751479,
REPLACE(JSON_QUERY(row,'$.D_470862706.D_178609656.D_104676242'), '\"', '') AS D_470862706_D_178609656_D_104676242,
REPLACE(JSON_QUERY(row,'$.D_470862706.D_178609656.D_137733407'), '\"', '') AS D_470862706_D_178609656_D_137733407,
REPLACE(JSON_QUERY(row,'$.D_470862706.D_178609656.D_663253668'), '\"', '') AS D_470862706_D_178609656_D_663253668,
REPLACE(JSON_QUERY(row,'$.D_470862706.D_178609656.D_789689151'), '\"', '') AS D_470862706_D_178609656_D_789689151,
REPLACE(JSON_QUERY(row,'$.D_470862706.D_261957180.D_104676242'), '\"', '') AS D_470862706_D_261957180_D_104676242,
REPLACE(JSON_QUERY(row,'$.D_470862706.D_261957180.D_137733407'), '\"', '') AS D_470862706_D_261957180_D_137733407,
REPLACE(JSON_QUERY(row,'$.D_470862706.D_261957180.D_663253668'), '\"', '') AS D_470862706_D_261957180_D_663253668,
REPLACE(JSON_QUERY(row,'$.D_470862706.D_261957180.D_789689151'), '\"', '') AS D_470862706_D_261957180_D_789689151,
REPLACE(JSON_QUERY(row,'$.D_470862706.D_356133766.D_104676242'), '\"', '') AS D_470862706_D_356133766_D_104676242,
REPLACE(JSON_QUERY(row,'$.D_470862706.D_356133766.D_137733407'), '\"', '') AS D_470862706_D_356133766_D_137733407,
REPLACE(JSON_QUERY(row,'$.D_470862706.D_356133766.D_663253668'), '\"', '') AS D_470862706_D_356133766_D_663253668,
REPLACE(JSON_QUERY(row,'$.D_470862706.D_356133766.D_789689151'), '\"', '') AS D_470862706_D_356133766_D_789689151,
REPLACE(JSON_QUERY(row,'$.D_470862706.D_643512687.D_104676242'), '\"', '') AS D_470862706_D_643512687_D_104676242,
REPLACE(JSON_QUERY(row,'$.D_470862706.D_643512687.D_137733407'), '\"', '') AS D_470862706_D_643512687_D_137733407,
REPLACE(JSON_QUERY(row,'$.D_470862706.D_643512687.D_663253668'), '\"', '') AS D_470862706_D_643512687_D_663253668,
REPLACE(JSON_QUERY(row,'$.D_470862706.D_643512687.D_789689151'), '\"', '') AS D_470862706_D_643512687_D_789689151,
REPLACE(JSON_QUERY(row,'$.D_470862706.D_688123102.D_104676242'), '\"', '') AS D_470862706_D_688123102_D_104676242,
REPLACE(JSON_QUERY(row,'$.D_470862706.D_688123102.D_137733407'), '\"', '') AS D_470862706_D_688123102_D_137733407,
REPLACE(JSON_QUERY(row,'$.D_470862706.D_688123102.D_663253668'), '\"', '') AS D_470862706_D_688123102_D_663253668,
REPLACE(JSON_QUERY(row,'$.D_470862706.D_688123102.D_789689151'), '\"', '') AS D_470862706_D_688123102_D_789689151,
REPLACE(JSON_QUERY(row,'$.D_470862706.D_690918725.D_104676242'), '\"', '') AS D_470862706_D_690918725_D_104676242,
REPLACE(JSON_QUERY(row,'$.D_470862706.D_690918725.D_137733407'), '\"', '') AS D_470862706_D_690918725_D_137733407,
REPLACE(JSON_QUERY(row,'$.D_470862706.D_690918725.D_663253668'), '\"', '') AS D_470862706_D_690918725_D_663253668,
REPLACE(JSON_QUERY(row,'$.D_470862706.D_690918725.D_789689151'), '\"', '') AS D_470862706_D_690918725_D_789689151,
REPLACE(JSON_QUERY(row,'$.D_470862706.D_912659087.D_104676242'), '\"', '') AS D_470862706_D_912659087_D_104676242,
REPLACE(JSON_QUERY(row,'$.D_470862706.D_912659087.D_137733407'), '\"', '') AS D_470862706_D_912659087_D_137733407,
REPLACE(JSON_QUERY(row,'$.D_470862706.D_912659087.D_663253668'), '\"', '') AS D_470862706_D_912659087_D_663253668,
REPLACE(JSON_QUERY(row,'$.D_470862706.D_912659087.D_789689151'), '\"', '') AS D_470862706_D_912659087_D_789689151,
REPLACE(JSON_QUERY(row,'$.D_471435289'), '\"', '') AS D_471435289,
REPLACE(JSON_QUERY(row,'$.D_472007171'), '\"', '') AS D_472007171,
REPLACE(JSON_QUERY(row,'$.D_480155628'), '\"', '') AS D_480155628,
REPLACE(JSON_QUERY(row,'$.D_480217113'), '\"', '') AS D_480217113,
REPLACE(JSON_QUERY(row,'$.D_480426504'), '\"', '') AS D_480426504,
REPLACE(JSON_QUERY(row,'$.D_481182460'), '\"', '') AS D_481182460,
REPLACE(JSON_QUERY(row,'$.D_481662464'), '\"', '') AS D_481662464,
REPLACE(JSON_QUERY(row,'$.D_492742957'), '\"', '') AS D_492742957,
REPLACE(JSON_QUERY(row,'$.D_493642768'), '\"', '') AS D_493642768,
REPLACE(JSON_QUERY(row,'$.D_495592561'), '\"', '') AS D_495592561,
REPLACE(JSON_QUERY(row,'$.D_501545129'), '\"', '') AS D_501545129,
REPLACE(JSON_QUERY(row,'$.D_503058092'), '\"', '') AS D_503058092,
REPLACE(JSON_QUERY(row,'$.D_510183971'), '\"', '') AS D_510183971,
REPLACE(JSON_QUERY(row,'$.D_511145980'), '\"', '') AS D_511145980,
REPLACE(JSON_QUERY(row,'$.D_515023660'), '\"', '') AS D_515023660,
REPLACE(JSON_QUERY(row,'$.D_517100968.D_517100968'), '\"', '') AS D_517100968_D_517100968,
REPLACE(JSON_QUERY(row,'$.D_517100968.D_810608313'), '\"', '') AS D_517100968_D_810608313,
REPLACE(JSON_QUERY(row,'$.D_521073718'), '\"', '') AS D_521073718,
REPLACE(JSON_QUERY(row,'$.D_524914900'), '\"', '') AS D_524914900,
REPLACE(JSON_QUERY(row,'$.D_524971659'), '\"', '') AS D_524971659,
REPLACE(JSON_QUERY(row,'$.D_525152373'), '\"', '') AS D_525152373,
REPLACE(JSON_QUERY(row,'$.D_532755074'), '\"', '') AS D_532755074,
REPLACE(JSON_QUERY(row,'$.D_534351312'), '\"', '') AS D_534351312,
REPLACE(JSON_QUERY(row,'$.D_536166591'), '\"', '') AS D_536166591,
REPLACE(JSON_QUERY(row,'$.D_537807075'), '\"', '') AS D_537807075,
REPLACE(JSON_QUERY(row,'$.D_546410473.D_546410473.D_648960871'), '\"', '') AS D_546410473_D_546410473_D_648960871,
REPLACE(JSON_QUERY(row,'$.D_546410473.D_663551714'), '\"', '') AS D_546410473_D_663551714,
REPLACE(JSON_QUERY(row,'$.D_549400618'), '\"', '') AS D_549400618,
REPLACE(JSON_QUERY(row,'$.D_550244580.D_517801904'), '\"', '') AS D_550244580_D_517801904,
REPLACE(JSON_QUERY(row,'$.D_550244580.D_550244580.D_648960871'), '\"', '') AS D_550244580_D_550244580_D_648960871,
REPLACE(JSON_QUERY(row,'$.D_550850568'), '\"', '') AS D_550850568,
REPLACE(JSON_QUERY(row,'$.D_556837046.D_141874857'), '\"', '') AS D_556837046_D_141874857,
REPLACE(JSON_QUERY(row,'$.D_556837046.D_556837046.D_648960871'), '\"', '') AS D_556837046_D_556837046_D_648960871,
REPLACE(JSON_QUERY(row,'$.D_556963634'), '\"', '') AS D_556963634,
REPLACE(JSON_QUERY(row,'$.D_558707243'), '\"', '') AS D_558707243,
REPLACE(JSON_QUERY(row,'$.D_559388168'), '\"', '') AS D_559388168,
REPLACE(JSON_QUERY(row,'$.D_560293868'), '\"', '') AS D_560293868,
REPLACE(JSON_QUERY(row,'$.D_562595294'), '\"', '') AS D_562595294,
REPLACE(JSON_QUERY(row,'$.D_564438246'), '\"', '') AS D_564438246,
REPLACE(JSON_QUERY(row,'$.D_568113091'), '\"', '') AS D_568113091,
REPLACE(JSON_QUERY(row,'$.D_584608368'), '\"', '') AS D_584608368,
REPLACE(JSON_QUERY(row,'$.D_585702271'), '\"', '') AS D_585702271,
REPLACE(JSON_QUERY(row,'$.D_585819411.D_585819411'), '\"', '') AS D_585819411_D_585819411,
REPLACE(JSON_QUERY(row,'$.D_585819411.D_810608313'), '\"', '') AS D_585819411_D_810608313,
REPLACE(JSON_QUERY(row,'$.D_589885595'), '\"', '') AS D_589885595,
REPLACE(JSON_QUERY(row,'$.D_591438156'), '\"', '') AS D_591438156,
REPLACE(JSON_QUERY(row,'$.D_592957464'), '\"', '') AS D_592957464,
REPLACE(JSON_QUERY(row,'$.D_594280517'), '\"', '') AS D_594280517,
REPLACE(JSON_QUERY(row,'$.D_594608533'), '\"', '') AS D_594608533,
REPLACE(JSON_QUERY(row,'$.D_596004621'), '\"', '') AS D_596004621,
REPLACE(JSON_QUERY(row,'$.D_598954337'), '\"', '') AS D_598954337,
REPLACE(JSON_QUERY(row,'$.D_605063358'), '\"', '') AS D_605063358,
REPLACE(JSON_QUERY(row,'$.D_606675249'), '\"', '') AS D_606675249,
REPLACE(JSON_QUERY(row,'$.D_618062863'), '\"', '') AS D_618062863,
REPLACE(JSON_QUERY(row,'$.D_620425840'), '\"', '') AS D_620425840,
REPLACE(JSON_QUERY(row,'$.D_622210148'), '\"', '') AS D_622210148,
REPLACE(JSON_QUERY(row,'$.D_624111331'), '\"', '') AS D_624111331,
REPLACE(JSON_QUERY(row,'$.D_628770824'), '\"', '') AS D_628770824,
REPLACE(JSON_QUERY(row,'$.D_633553324.D_164707243'), '\"', '') AS D_633553324_D_164707243,
REPLACE(JSON_QUERY(row,'$.D_633553324.D_175385712'), '\"', '') AS D_633553324_D_175385712,
REPLACE(JSON_QUERY(row,'$.D_633553324.D_294629316'), '\"', '') AS D_633553324_D_294629316,
REPLACE(JSON_QUERY(row,'$.D_633553324.D_771426895'), '\"', '') AS D_633553324_D_771426895,
REPLACE(JSON_QUERY(row,'$.D_633553324.D_772143730'), '\"', '') AS D_633553324_D_772143730,
REPLACE(JSON_QUERY(row,'$.D_633553324.D_818310825'), '\"', '') AS D_633553324_D_818310825,
REPLACE(JSON_QUERY(row,'$.D_633553324.D_843593800'), '\"', '') AS D_633553324_D_843593800,
REPLACE(JSON_QUERY(row,'$.D_639684251'), '\"', '') AS D_639684251,
REPLACE(JSON_QUERY(row,'$.D_640944113'), '\"', '') AS D_640944113,
REPLACE(JSON_QUERY(row,'$.D_643099118'), '\"', '') AS D_643099118,
REPLACE(JSON_QUERY(row,'$.D_645361463'), '\"', '') AS D_645361463,
REPLACE(JSON_QUERY(row,'$.D_645681293'), '\"', '') AS D_645681293,
REPLACE(JSON_QUERY(row,'$.D_648216681'), '\"', '') AS D_648216681,
REPLACE(JSON_QUERY(row,'$.D_652477674'), '\"', '') AS D_652477674,
REPLACE(JSON_QUERY(row,'$.D_653198444.D_622843252'), '\"', '') AS D_653198444_D_622843252,
REPLACE(JSON_QUERY(row,'$.D_653198444.D_653198444.D_648960871'), '\"', '') AS D_653198444_D_653198444_D_648960871,
REPLACE(JSON_QUERY(row,'$.D_656911826'), '\"', '') AS D_656911826,
REPLACE(JSON_QUERY(row,'$.D_659140794'), '\"', '') AS D_659140794,
REPLACE(JSON_QUERY(row,'$.D_662762705'), '\"', '') AS D_662762705,
REPLACE(JSON_QUERY(row,'$.D_667002790'), '\"', '') AS D_667002790,
REPLACE(JSON_QUERY(row,'$.D_671380198'), '\"', '') AS D_671380198,
REPLACE(JSON_QUERY(row,'$.D_673842048'), '\"', '') AS D_673842048,
REPLACE(JSON_QUERY(row,'$.D_674378360'), '\"', '') AS D_674378360,
REPLACE(JSON_QUERY(row,'$.D_677733128'), '\"', '') AS D_677733128,
REPLACE(JSON_QUERY(row,'$.D_679300202'), '\"', '') AS D_679300202,
REPLACE(JSON_QUERY(row,'$.D_681586194'), '\"', '') AS D_681586194,
REPLACE(JSON_QUERY(row,'$.D_685659661'), '\"', '') AS D_685659661,
REPLACE(JSON_QUERY(row,'$.D_694261722'), '\"', '') AS D_694261722,
REPLACE(JSON_QUERY(row,'$.D_695423703'), '\"', '') AS D_695423703,
REPLACE(JSON_QUERY(row,'$.D_698314386'), '\"', '') AS D_698314386,
REPLACE(JSON_QUERY(row,'$.D_700173707'), '\"', '') AS D_700173707,
REPLACE(JSON_QUERY(row,'$.D_702729897'), '\"', '') AS D_702729897,
REPLACE(JSON_QUERY(row,'$.D_705282587'), '\"', '') AS D_705282587,
REPLACE(JSON_QUERY(row,'$.D_711858159'), '\"', '') AS D_711858159,
REPLACE(JSON_QUERY(row,'$.D_716554850'), '\"', '') AS D_716554850,
REPLACE(JSON_QUERY(row,'$.D_719293094'), '\"', '') AS D_719293094,
REPLACE(JSON_QUERY(row,'$.D_720305356'), '\"', '') AS D_720305356,
REPLACE(JSON_QUERY(row,'$.D_722122662'), '\"', '') AS D_722122662,
REPLACE(JSON_QUERY(row,'$.D_723763329'), '\"', '') AS D_723763329,
REPLACE(JSON_QUERY(row,'$.D_724079256'), '\"', '') AS D_724079256,
REPLACE(JSON_QUERY(row,'$.D_724239872'), '\"', '') AS D_724239872,
REPLACE(JSON_QUERY(row,'$.D_724990284'), '\"', '') AS D_724990284,
REPLACE(JSON_QUERY(row,'$.D_726847824'), '\"', '') AS D_726847824,
REPLACE(JSON_QUERY(row,'$.D_738219521.D_392301875'), '\"', '') AS D_738219521_D_392301875,
REPLACE(JSON_QUERY(row,'$.D_738219521.D_509170471'), '\"', '') AS D_738219521_D_509170471,
REPLACE(JSON_QUERY(row,'$.D_738219521.D_575237218'), '\"', '') AS D_738219521_D_575237218,
REPLACE(JSON_QUERY(row,'$.D_738219521.D_576358766'), '\"', '') AS D_738219521_D_576358766,
REPLACE(JSON_QUERY(row,'$.D_738219521.D_666840938'), '\"', '') AS D_738219521_D_666840938,
REPLACE(JSON_QUERY(row,'$.D_738219521.D_767711152'), '\"', '') AS D_738219521_D_767711152,
REPLACE(JSON_QUERY(row,'$.D_738219521.D_888898065'), '\"', '') AS D_738219521_D_888898065,
REPLACE(JSON_QUERY(row,'$.D_738219521.D_976863738'), '\"', '') AS D_738219521_D_976863738,
REPLACE(JSON_QUERY(row,'$.D_742254559.D_185168695'), '\"', '') AS D_742254559_D_185168695,
REPLACE(JSON_QUERY(row,'$.D_742254559.D_742254559.D_648960871'), '\"', '') AS D_742254559_D_742254559_D_648960871,
REPLACE(JSON_QUERY(row,'$.D_742544044'), '\"', '') AS D_742544044,
REPLACE(JSON_QUERY(row,'$.D_746817928'), '\"', '') AS D_746817928,
REPLACE(JSON_QUERY(row,'$.D_751046675'), '\"', '') AS D_751046675,
REPLACE(JSON_QUERY(row,'$.D_752795304'), '\"', '') AS D_752795304,
REPLACE(JSON_QUERY(row,'$.D_755298453'), '\"', '') AS D_755298453,
REPLACE(JSON_QUERY(row,'$.D_755923078.D_459020144'), '\"', '') AS D_755923078_D_459020144,
REPLACE(JSON_QUERY(row,'$.D_755923078.D_755923078'), '\"', '') AS D_755923078_D_755923078,
REPLACE(JSON_QUERY(row,'$.D_761037053'), '\"', '') AS D_761037053,
REPLACE(JSON_QUERY(row,'$.D_763164658'), '\"', '') AS D_763164658,
REPLACE(JSON_QUERY(row,'$.D_766791333'), '\"', '') AS D_766791333,
REPLACE(JSON_QUERY(row,'$.D_777877159'), '\"', '') AS D_777877159,
REPLACE(JSON_QUERY(row,'$.D_780685299'), '\"', '') AS D_780685299,
REPLACE(JSON_QUERY(row,'$.D_780721084'), '\"', '') AS D_780721084,
REPLACE(JSON_QUERY(row,'$.D_780767323'), '\"', '') AS D_780767323,
REPLACE(JSON_QUERY(row,'$.D_785139115'), '\"', '') AS D_785139115,
REPLACE(JSON_QUERY(row,'$.D_786517174'), '\"', '') AS D_786517174,
REPLACE(JSON_QUERY(row,'$.D_787047261'), '\"', '') AS D_787047261,
REPLACE(JSON_QUERY(row,'$.D_789159346.D_214983579'), '\"', '') AS D_789159346_D_214983579,
REPLACE(JSON_QUERY(row,'$.D_789159346.D_789159346.D_572260608'), '\"', '') AS D_789159346_D_789159346_D_572260608,
REPLACE(JSON_QUERY(row,'$.D_789159346.D_789159346.D_732465277'), '\"', '') AS D_789159346_D_789159346_D_732465277,
REPLACE(JSON_QUERY(row,'$.D_789159346.D_789159346.D_807835037'), '\"', '') AS D_789159346_D_789159346_D_807835037,
REPLACE(JSON_QUERY(row,'$.D_789159346.D_789159346.D_895826652'), '\"', '') AS D_789159346_D_789159346_D_895826652,
REPLACE(JSON_QUERY(row,'$.D_789271762'), '\"', '') AS D_789271762,
REPLACE(JSON_QUERY(row,'$.D_790436165.D_408696162'), '\"', '') AS D_790436165_D_408696162,
REPLACE(JSON_QUERY(row,'$.D_790436165.D_790436165.D_648960871'), '\"', '') AS D_790436165_D_790436165_D_648960871,
REPLACE(JSON_QUERY(row,'$.D_795265404'), '\"', '') AS D_795265404,
REPLACE(JSON_QUERY(row,'$.D_795761911'), '\"', '') AS D_795761911,
REPLACE(JSON_QUERY(row,'$.D_798549704'), '\"', '') AS D_798549704,
REPLACE(JSON_QUERY(row,'$.D_798627028'), '\"', '') AS D_798627028,
REPLACE(JSON_QUERY(row,'$.D_810324917.D_479400169'), '\"', '') AS D_810324917_D_479400169,
REPLACE(JSON_QUERY(row,'$.D_810324917.D_810324917'), '\"', '') AS D_810324917_D_810324917,
REPLACE(JSON_QUERY(row,'$.D_812430894'), '\"', '') AS D_812430894,
REPLACE(JSON_QUERY(row,'$.D_814115676.D_814115676.D_648960871'), '\"', '') AS D_814115676_D_814115676_D_648960871,
REPLACE(JSON_QUERY(row,'$.D_814115676.D_851145096'), '\"', '') AS D_814115676_D_851145096,
REPLACE(JSON_QUERY(row,'$.D_815776236'), '\"', '') AS D_815776236,
REPLACE(JSON_QUERY(row,'$.D_816891201'), '\"', '') AS D_816891201,
REPLACE(JSON_QUERY(row,'$.D_818380721'), '\"', '') AS D_818380721,
REPLACE(JSON_QUERY(row,'$.D_820220019'), '\"', '') AS D_820220019,
REPLACE(JSON_QUERY(row,'$.D_821629868'), '\"', '') AS D_821629868,
REPLACE(JSON_QUERY(row,'$.D_827661413'), '\"', '') AS D_827661413,
REPLACE(JSON_QUERY(row,'$.D_830608495.D_158841370'), '\"', '') AS D_830608495_D_158841370,
REPLACE(JSON_QUERY(row,'$.D_830608495.D_349034127'), '\"', '') AS D_830608495_D_349034127,
REPLACE(JSON_QUERY(row,'$.D_830608495.D_535003378'), '\"', '') AS D_830608495_D_535003378,
REPLACE(JSON_QUERY(row,'$.D_830608495.D_675129470'), '\"', '') AS D_830608495_D_675129470,
REPLACE(JSON_QUERY(row,'$.D_830608495.D_713594719'), '\"', '') AS D_830608495_D_713594719,
REPLACE(JSON_QUERY(row,'$.D_830608495.D_760521319'), '\"', '') AS D_830608495_D_760521319,
REPLACE(JSON_QUERY(row,'$.D_831873859'), '\"', '') AS D_831873859,
REPLACE(JSON_QUERY(row,'$.D_845122623'), '\"', '') AS D_845122623,
REPLACE(JSON_QUERY(row,'$.D_847277590'), '\"', '') AS D_847277590,
REPLACE(JSON_QUERY(row,'$.D_857219333'), '\"', '') AS D_857219333,
REPLACE(JSON_QUERY(row,'$.D_858858333'), '\"', '') AS D_858858333,
REPLACE(JSON_QUERY(row,'$.D_859228803'), '\"', '') AS D_859228803,
REPLACE(JSON_QUERY(row,'$.D_863593410'), '\"', '') AS D_863593410,
REPLACE(JSON_QUERY(row,'$.D_863664828'), '\"', '') AS D_863664828,
REPLACE(JSON_QUERY(row,'$.D_864290328'), '\"', '') AS D_864290328,
REPLACE(JSON_QUERY(row,'$.D_868868325'), '\"', '') AS D_868868325,
REPLACE(JSON_QUERY(row,'$.D_872821562'), '\"', '') AS D_872821562,
REPLACE(JSON_QUERY(row,'$.D_878281875'), '\"', '') AS D_878281875,
REPLACE(JSON_QUERY(row,'$.D_880067330'), '\"', '') AS D_880067330,
REPLACE(JSON_QUERY(row,'$.D_891825638'), '\"', '') AS D_891825638,
REPLACE(JSON_QUERY(row,'$.D_891996278'), '\"', '') AS D_891996278,
REPLACE(JSON_QUERY(row,'$.D_908399883'), '\"', '') AS D_908399883,
REPLACE(JSON_QUERY(row,'$.D_910501158'), '\"', '') AS D_910501158,
REPLACE(JSON_QUERY(row,'$.D_915293401'), '\"', '') AS D_915293401,
REPLACE(JSON_QUERY(row,'$.D_921623945'), '\"', '') AS D_921623945,
REPLACE(JSON_QUERY(row,'$.D_921663542'), '\"', '') AS D_921663542,
REPLACE(JSON_QUERY(row,'$.D_922708844'), '\"', '') AS D_922708844,
REPLACE(JSON_QUERY(row,'$.D_923371868'), '\"', '') AS D_923371868,
REPLACE(JSON_QUERY(row,'$.D_925215135'), '\"', '') AS D_925215135,
REPLACE(JSON_QUERY(row,'$.D_928803137'), '\"', '') AS D_928803137,
REPLACE(JSON_QUERY(row,'$.D_929034795'), '\"', '') AS D_929034795,
REPLACE(JSON_QUERY(row,'$.D_930208825'), '\"', '') AS D_930208825,
REPLACE(JSON_QUERY(row,'$.D_933417196.D_810608313'), '\"', '') AS D_933417196_D_810608313,
REPLACE(JSON_QUERY(row,'$.D_933417196.D_933417196'), '\"', '') AS D_933417196_D_933417196,
REPLACE(JSON_QUERY(row,'$.D_937550939'), '\"', '') AS D_937550939,
REPLACE(JSON_QUERY(row,'$.D_938333920'), '\"', '') AS D_938333920,
REPLACE(JSON_QUERY(row,'$.D_942945860.D_742716427'), '\"', '') AS D_942945860_D_742716427,
REPLACE(JSON_QUERY(row,'$.D_942945860.D_942945860.D_648960871'), '\"', '') AS D_942945860_D_942945860_D_648960871,
REPLACE(JSON_QUERY(row,'$.D_947205597.D_198133418'), '\"', '') AS D_947205597_D_198133418,
REPLACE(JSON_QUERY(row,'$.D_947205597.D_535003378'), '\"', '') AS D_947205597_D_535003378,
REPLACE(JSON_QUERY(row,'$.D_947205597.D_539648641'), '\"', '') AS D_947205597_D_539648641,
REPLACE(JSON_QUERY(row,'$.D_947205597.D_686310465'), '\"', '') AS D_947205597_D_686310465,
REPLACE(JSON_QUERY(row,'$.D_947205597.D_706254326'), '\"', '') AS D_947205597_D_706254326,
REPLACE(JSON_QUERY(row,'$.D_947205597.D_712653855'), '\"', '') AS D_947205597_D_712653855,
REPLACE(JSON_QUERY(row,'$.D_947205597.D_817381897'), '\"', '') AS D_947205597_D_817381897,
REPLACE(JSON_QUERY(row,'$.D_951151270'), '\"', '') AS D_951151270,
REPLACE(JSON_QUERY(row,'$.D_951901114'), '\"', '') AS D_951901114,
REPLACE(JSON_QUERY(row,'$.D_960332453'), '\"', '') AS D_960332453,
REPLACE(JSON_QUERY(row,'$.D_963040791'), '\"', '') AS D_963040791,
REPLACE(JSON_QUERY(row,'$.D_964154290'), '\"', '') AS D_964154290,
REPLACE(JSON_QUERY(row,'$.D_970325871'), '\"', '') AS D_970325871,
REPLACE(JSON_QUERY(row,'$.D_981755099'), '\"', '') AS D_981755099,
REPLACE(JSON_QUERY(row,'$.D_982470672'), '\"', '') AS D_982470672,
REPLACE(JSON_QUERY(row,'$.D_989594002'), '\"', '') AS D_989594002,
REPLACE(JSON_QUERY(row,'$.D_992757226'), '\"', '') AS D_992757226,
REPLACE(JSON_QUERY(row,'$.D_995220236'), '\"', '') AS D_995220236,
REPLACE(JSON_QUERY(row,'$.D_997412869'), '\"', '') AS D_997412869,
REPLACE(JSON_QUERY(row,'$.d_951528844'), '\"', '') AS d_951528844,
REPLACE(JSON_QUERY(row,'$.sha'), '\"', '') AS sha,
REPLACE(JSON_QUERY(row,'$.uid'), '\"', '') AS uid
    from json_data, UNNEST(body) as row
  )

SELECT *, FORMAT_TIMESTAMP("%Y%m%d", DATETIME(CURRENT_TIMESTAMP(), "America/New_York")) AS date
FROM flattened_data
ORDER BY Connect_ID
)