const fs = require('fs');

const dataSource = {
  dev: 'nih-nci-dceg-connect-dev.Connect.participants',
  stg: 'nih-nci-dceg-connect-stg-5519.Connect.participants',
  prod: 'nih-nci-dceg-connect-prod-6d04.Connect.participants',
};
const pathToConceptIdList = require('./M3-lists');
let variables = fs
  .readFileSync('./M3-variables.csv', 'utf8')
  .split(/\r?\n/)
  .map((v) => v.trim()).filter(v => v.length > 0);

variables = new Set([
  ...variables,
  ...generatePathFromConceptIdList(pathToConceptIdList),
]); // remove duplicates

const selects = generateSelects([...variables], 'row');

for (let tier of Object.keys(dataSource)) {
  const content = `CREATE TEMP FUNCTION
  handleRow(input_row STRING)
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
  
  function handleRowJS(row) {
    for (let arrPath of Object.keys(arraysToBeFlattened)) {
      let currObj = {};
      let inputConceptIdList = getNestedObjectValue(row, arrPath);
      if (!inputConceptIdList || !Array.isArray(inputConceptIdList) ||  inputConceptIdList.length === 0) continue;
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
  return handleRowJS(row);

""";
  
  
  
  CREATE OR REPLACE TABLE M3.flatModule3_WL AS (
    WITH
    json_data AS (
      SELECT
        Connect_ID,
        [handleRow(TO_JSON_STRING(input_row))] AS body
      FROM
        \`${dataSource[tier]}\` AS input_row where Connect_ID is not null
    ),
    flattened_data AS (
      SELECT
        Connect_ID,
        ${selects}
      from json_data, UNNEST(body) as row
    )

  SELECT *, FORMAT_TIMESTAMP("%Y%m%d", DATETIME(CURRENT_TIMESTAMP(), "America/New_York")) AS date
  FROM flattened_data
  ORDER BY Connect_ID
  )`;

  fs.writeFileSync(`./flatModule3_WL-${tier}.txt`, content);
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
      path = path.trim(); // remove spaces at ends
      result.add(path + '.D_' + cid);
    }
  }
  return result;
}
