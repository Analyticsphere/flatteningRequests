// Load required packages
const fs = require('fs')

// Load *-lists.js file as object
const pathToConceptIdList = require('./M1V2-lists');

// pretty-print JSON object to string
const jsonData = JSON.stringify(pathToConceptIdList, null, 4);

// write JSON string to a file
fs.writeFile('M1V2-lists.json', jsonData, err => {
  if (err) {
    throw err
  }
  console.log('JSON data is saved.')
});
