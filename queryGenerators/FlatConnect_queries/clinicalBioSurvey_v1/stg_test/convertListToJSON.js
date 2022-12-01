// Load required packages
const fs = require('fs')

// Load *-lists.js file as object
const pathToConceptIdList = require('./clinicalBioSurvey_v1-schema');

// pretty-print JSON object to string
const jsonData = JSON.stringify(pathToConceptIdList, null, 4);

// write JSON string to a file
fs.writeFile('clinicalBioSurvey_v1-schema.json', jsonData, err => {
  if (err) {
    throw err
  }
  console.log('JSON data is saved.')
});
