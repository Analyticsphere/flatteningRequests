// Load required packages
const fs = require('fs')

// Load *-lists.js file as object
const pathToConceptIdList = require('./module4_v1-lists-from-schema');

// pretty-print JSON object to string
const jsonData = JSON.stringify(pathToConceptIdList, null, 4);

// write JSON string to a file
fs.writeFile('module4_v1-lists-from-schema.json', jsonData, err => {
  if (err) {
    throw err
  }
  console.log('JSON data is saved.')
});
