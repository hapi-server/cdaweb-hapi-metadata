const {util}  = require('./util.js');

function buildMasterTable(datasets, outFile) {

  // attributeHashes will have keys of VarDescription and VarAttributes
  let attributeHashes = restructureObject(datasets);

  let tableCSV = [];
  let tableJSON = [];

  let hline = "datasetID,ParameterName," 
              + Object.keys(attributeHashes["VarDescription"]).join(",") 
              + ","
              + Object.keys(attributeHashes["VarAttributes"]).join(",");

  tableCSV.push(hline);
  tableJSON.push(hline.split(","));

  for (let dsidx in datasets) {

    if (!datasets[dsidx]['_masters']) {
      continue;
    }

    for (let [variableName, variable] of Object.entries(datasets[dsidx]['_masters']['json']["_CDFVariables"])) {

      let attributesCSV = [];
      let attributesJSON = [];
      for (let key of Object.keys(attributeHashes)) {
        for (let attributeName of Object.keys(attributeHashes[key])) {

          if (!variable[key]) {
            console.error(`No ${key} for ${variableName}`);
            attributesCSV.push("?");
            attributesJSON.push("?");
            continue;
          }

          let attributeValue = variable[key][attributeName];

          if (!attributeValue) {
            attributeValue = "";
          }

          if (attributeValue === " ") {
            // A single blank space has signficance. Use a special character to
            // highlight it. See
            // https://cdaweb.gsfc.nasa.gov/pub/software/cdawlib/0MASTERS/00readme.txt
            attributeValue = "␣";
          }

          attributesJSON.push(attributeValue);

          if (typeof attributeValue === 'string') {
            // Escape double quotes (not sure if needed).
            attributeValue = attributeValue.replace(/"/g,'""');
          }

          if (/,/.test(attributeValue)) {
            // If commas, wrap in quotes.
            if (Array.isArray(attributeValue)) {
              attributeValue = `"[${attributeValue.join(",")}]"`;
            } else {
              attributeValue = `"${attributeValue}"`;
            }
          }

          attributesCSV.push(attributeValue);

        }
      }

      let line = datasets[dsidx]["id"] + "," + variableName + "," + attributesCSV.join(",");
      tableCSV.push(line);
      tableJSON.push([datasets[dsidx]["id"], variableName, ...attributesJSON])
    }
  }
  util.writeSync(outFile + ".csv", tableCSV.join("\n"));
  util.writeSync(outFile + ".json", JSON.stringify(tableJSON));
}
module.exports.buildMasterTable = buildMasterTable;

function restructureObject(datasets) {

  /*
  Convert JSON with arrays of objects to objects with objects. For example
  { "Epoch": [ 
      {"VarDescription": [{"DataType": "CDF_TIME_TT2000"}, ...] },
      {"VarAttributes": [{"CATDESC": "Default time"}, ...] }
    ]
  }
  // is converted to
    {
      "Epoch": {
        "VarDescription": {
          "DataType": "CDF_TIME_TT2000",
          ...
        },
        "VarAttributes": {
          "CATDESC": "Default time",
          ...
        }
      }
   }
  */

  let varAttributesHash = {}; // Store variable attribute attributes encountered.
  let varDescriptionHash = {}; // Store variable description attributes encountered.

  for (let dsidx in datasets) {

    if (! datasets[dsidx]['_masters']) {
      continue;
    }
    let variables = datasets[dsidx]['_masters']['json']['CDFVariables'];
    let variablesNew = {};
    for (let variable of variables) {

      // variable should have the form
      // { Epoch: [ { VarDescription: [Array] }, { VarAttributes: [Array] } ] }
      let variableKeys = Object.keys(variable);

      if (variableKeys.length > 1) {
        console.error("Expected only one variable key in variable object.")
        process.exit(0);
      }
      variableName = variableKeys[0];

      // variableArray will have the form
      // [ { VarDescription: [Array] }, { VarAttributes: [Array] } ]
      let variableArray = variable[variableName];

      // variableObject will have the form
      // {{ VarDescription: [Array] }, { VarAttributes: [Array] }}
      let variableObject = array2Object(variableArray);

      for (let [key, value] of Object.entries(variableObject)) {
        variableObject[key] = sortKeys(array2Object(value));
        if (key === 'VarAttributes') {
          for (let akey of Object.keys(variableObject[key])) {
            varAttributesHash[akey] = null;
          }
        }
        if (key === 'VarDescription') {
          for (let akey of Object.keys(variableObject[key])) {
            varDescriptionHash[akey] = null;
          }
        }
      }
      variablesNew[variableName] = variableObject;
    }
    datasets[dsidx]['_masters']['json']["_CDFVariables"] = variablesNew;
  }

  return {"VarDescription": varDescriptionHash, "VarAttributes": varAttributesHash};

  function sortKeys(object) {
    return Object.keys(object).sort().reduce(
      (objEntries, key) => {
        objEntries[key] = object[key]; 
        return objEntries;
      }, {});
  }

  function array2Object(array) {
    let object = {};
    for (let element of array) {
      let key = Object.keys(element)[0];
      object[key] = element[key];
    }
    return object;
  }
}
