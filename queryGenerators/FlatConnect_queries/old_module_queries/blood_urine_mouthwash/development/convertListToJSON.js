// Load required packages
const fs = require('fs')

// Load *-lists.js file as object
const pathToConceptIdList = require('./BUM-lists-stg');

// pretty-print JSON object to string
const jsonData = JSON.stringify(pathToConceptIdList, null, 4);

// write JSON string to a file
fs.writeFile('BUM-lists-stg.json', jsonData, err => {
  if (err) {
    throw err
  }
  console.log('JSON data is saved.')
});
