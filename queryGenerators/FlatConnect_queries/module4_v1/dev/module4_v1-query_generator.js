const fs = require('fs');

const config = require('./'+'query-config.json');

const pathToConceptIdList = require('./'+config.lists_json);
let variables = fs
  .readFileSync('./'+config.variables_csv, 'utf8')
  .split(/\r?\n/)
  .map((v) => v.trim())
  .filter((v) => v.length > 0);

variables = new Set([
  ...variables,
  ...generatePathFromConceptIdList(pathToConceptIdList),
]); // remove duplicates

const selects = generateSelects([...variables], 'row');

for (let tier of Object.keys(config.data_source)) {
  const content = `CREATE TEMP FUNCTION
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

  const arraysToBeFlattened=${JSON.stringify(pathToConceptIdList, null, ' ')}

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

  CREATE OR REPLACE TABLE ${config.output_table} 
OPTIONS (description="${config.table_description}") 
AS (
  WITH
  json_data AS (
    SELECT
      Connect_ID,
      uid,
      [handleM1(TO_JSON_STRING(input_row))] AS body
    FROM
      \`${config.data_source[tier]}\` AS input_row where Connect_ID is not null
  ),
  flattened_data AS (
    SELECT
      ${selects}
    from json_data, UNNEST(body) as row
  )

SELECT *, FORMAT_TIMESTAMP("%Y%m%d", DATETIME(CURRENT_TIMESTAMP(), "America/New_York")) AS date
FROM flattened_data
ORDER BY Connect_ID
)`;

  fs.writeFileSync(`./${config.query_name}-${tier}.sql`, content);
}

function generateSelects(variables = [], rowName = 'row') {
  if (variables.length === 0) return '* except(Connect_ID)';

  let selectList = [];
  for (let variable of variables) {
    const columnName = variable.replaceAll('.', '_');
    selectList.push(
      `REPLACE(JSON_QUERY(${rowName},'$.${variable}'), '\\"', '') AS ${columnName}`
    );
  }

  selectList.sort((a, b) => {
    let aStr = a.split('AS')[1];
    let bStr = b.split('AS')[1];
    return aStr <= bStr ? -1 : 1;
  });
  return selectList.join(',\n');
}

function generatePathFromConceptIdList(obj) {
  let result = new Set();
  for (let [path, conceptIdList] of Object.entries(obj)) {
    for (let cid of conceptIdList) {
      result.add(path + '.D_' + cid);
    }
  }
  return result;
}
