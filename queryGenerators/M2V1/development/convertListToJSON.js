// Load required packages
const fs = require('fs')

// Load *-lists.js file as object
const pathToConceptIdList = require('./M2V1-lists');

// pretty-print JSON object to string
const jsonData = JSON.stringify(pathToConceptIdList, null, 4);

// write JSON string to a file
fs.writeFile('M2V1-lists.json', jsonData, err => {
  if (err) {
    throw err
  }
  console.log('JSON data is saved.')
});
