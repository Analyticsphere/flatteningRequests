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
 "__error__": []
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

  CREATE OR REPLACE TABLE FlatConnect.menstrualSurvey_v1_JP AS (
  WITH
  json_data AS (
    SELECT
      Connect_ID,
      uid,
      [handleM1(TO_JSON_STRING(input_row))] AS body
    FROM
      `nih-nci-dceg-connect-stg-5519.Connect.menstrualSurvey_v1` AS input_row where Connect_ID is not null
  ),
  flattened_data AS (
    SELECT
      Connect_ID,
      uid,
      REPLACE(JSON_QUERY(row,'$.Connect_ID'), '\"', '') AS Connect_ID,
REPLACE(JSON_QUERY(row,'$.D_593467240'), '\"', '') AS D_593467240,
REPLACE(JSON_QUERY(row,'$.D_951357171'), '\"', '') AS D_951357171,
REPLACE(JSON_QUERY(row,'$.__has_error__'), '\"', '') AS __has_error__,
REPLACE(JSON_QUERY(row,'$.treeJSON'), '\"', '') AS treeJSON,
REPLACE(JSON_QUERY(row,'$.uid'), '\"', '') AS uid
    from json_data, UNNEST(body) as row
  )

SELECT *, FORMAT_TIMESTAMP("%Y%m%d", DATETIME(CURRENT_TIMESTAMP(), "America/New_York")) AS date
FROM flattened_data
ORDER BY Connect_ID
)