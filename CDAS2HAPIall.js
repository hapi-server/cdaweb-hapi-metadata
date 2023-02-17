// Create a HAPI all.json catalog based on
//   https://spdf.gsfc.nasa.gov/pub/catalogs/all.xml
// and queries to
//   https://cdaweb.gsfc.nasa.gov/WebServices/REST/

const fs      = require('fs');
const request = require("request");
const moment  = require('moment');
const xml2js  = require('xml2js').parseString;
const argv    = require('yargs')
                  .default
                    ({
                      'idregex': '^AC_'
                    })
                  .argv;

let HAPI_VERSION = "3.2";
let DATSET_ID_RE = new RegExp(argv.idregex);

let fnameAll = 'all/all-bw.json';
let fnameAllFull = 'all/all-bw-full.json'

// pool should be set outside of loop. See
// https://www.npmjs.com/package/request#requestoptions-callback
// Set max sockets to a single host.
let pool = {maxSockets: 3};  

let baseURL = "https://cdaweb.gsfc.nasa.gov/WS/cdasr/1/dataviews/sp_phys/datasets/";
let allURL  = "https://spdf.gsfc.nasa.gov/pub/catalogs/all.xml";

let outDir  = "cache/bw";
if (!fs.existsSync(outDir)) {fs.mkdirSync(outDir, {recursive: true})}

function obj2json(obj) {return JSON.stringify(obj, null, 2)};
function writecb(err) { console.error(err) };

catalog();

function catalog() {

  // Request all.xml to get dataset names.
  // Then call variables() to get list of variables for each dataset.

  let fnameJSON = outDir + "/all.json";

  if (fs.existsSync(fnameJSON)) {
    console.log("Reading: " + fnameJSON);
    let body = fs.readFileSync(fnameJSON);
    finished(JSON.parse(body), true);
    return;
  }

  let reqOpts = {uri: allURL};
  console.log("Requesting: " + allURL);
  request(reqOpts, function (err,res,body) {
    if (err) console.log(err);
    console.log("Received: " + allURL);
    xml2js(body, function (err, jsonObj) {
      finished(jsonObj, false);
    });
  });

  function finished(body, fromCache) {

    if (fromCache == false) {
      console.log("Writing: " + fnameJSON);
      fs.writeFile(fnameJSON, obj2json(body), 'utf-8', writecb);
    }    

    let CATALOG = extractDatasetInfo(body);

    variables(CATALOG);

    let allIds = [];
    for (dataset of CATALOG) {
      // Could create allIds array in extractDatasetInfo()
      // to avoid this loop.
      allIds.push(dataset['id']);
    }
    let allIdsFile = outDir + "/ids.txt";
    console.log("Writing: " + allIdsFile);
    fs.writeFile(allIdsFile, allIds.join("\n"), writecb);

  }
}

function variables(CATALOG) {

  // Call /variables endpoint to get list of variables for each dataset.
  // Then call variableDetails() to get additional metadata for variables.

  let ididx = 0;

  for (ididx = 0; ididx < CATALOG.length; ididx++) {
    let url = baseURL + CATALOG[ididx]['id'] + "/variables";
    let fname = outDir + "/" + CATALOG[ididx]['id'] + "-variables.json";
    requestVariables(url, fname, ididx);
  }

  function requestVariables(url, fname, ididx) {

    if (fs.existsSync(fname)) {
      console.log("Reading: " + fname);
      let body = fs.readFileSync(fname, 'utf-8');
      finished(ididx, fname, body, true)
      return;
    }

    let reqOpts = {uri: url, pool: pool, headers: {'Accept':'application/json'}};
    console.log("Requesting: " + url.replace(baseURL,""));
    request(reqOpts, function (err, res, body) {
      if (err) console.log(err);
      console.log("Received: " + url.replace(baseURL, ""));
      finished(ididx, fname, body, false);
    });
  }

  function finished(ididx, fname, variablesResponse, fromCache) {

    if (!finished.N) {finished.N = 0;}
    finished.N = finished.N + 1;
    variablesResponse = JSON.parse(variablesResponse);

    if (fromCache == false) {
      console.log("Writing: " + fname);
      fs.writeFile(fname, obj2json(variablesResponse), writecb);
    }

    extractParameterNames(variablesResponse, CATALOG, ididx);

    if (finished.N == CATALOG.length) {
      variableDetails(CATALOG);
    }
  }
}

function variableDetails(CATALOG) {

  // Call /variables endpoint to get CDFML with data for all variables in dataset
  // Then call finalizeCatalog() to put CATALOG in final HAPI all.json form.

  for (ididx = 0; ididx < CATALOG.length; ididx++) {
    parameters = null;
    parameters = [];
    for (name of Object.keys(CATALOG[ididx]['info']['x_parameters'])) {
      parameters.push(name);
    }
    parameters = parameters.join(",")

    let stop = moment(CATALOG[ididx]['info']['startDate']).add(1,'day').toISOString().replace(".000Z","Z");

    let url = baseURL + CATALOG[ididx]['id'] + "/variables";
    url = baseURL
            + CATALOG[ididx]['id']
            + "/data/"
            + CATALOG[ididx]['info']['startDate'].replace(/-|:/g,"")
            + "," 
            + stop.replace(/-|:/g,"")
            + "/"
            + parameters
            + "?format=json";

    let fname = outDir + "/" + CATALOG[ididx]['id'] + '-cdfml.json';
    requestVariableDetails(url, fname, ididx);
  }

  function requestVariableDetails(url, fname, ididx) {

    if (fs.existsSync(fname)) {
      console.log("Reading: " + fname);
      let body = fs.readFileSync(fname, 'utf-8');
      finished(ididx, fname, body, true)
      return;
    }

    let reqOpts = {uri: url, pool: pool, headers: {'Accept':'application/json'}};
    console.log("Requesting: " + url);
    request(reqOpts, function (err,res,body) {
      if (err) console.log(err);
      console.log("Received: " + url);
      finished(ididx, fname, body, false);
    });
  }

  function finished(ididx, fname, body, fromCache) {

    if (!finished.N) {finished.N = 0;}
    finished.N = finished.N + 1;

    if (!body) {
      console.error("Problem with: " + fname);
      return;
    }

    if (body.match("Internal Server Error") || body.match("Bad Request") || body.match("No data available") || body.match("Not Found")) {
      console.error("Problem with: " + fname);
      return;
    }

    body = JSON.parse(body);

    if (!body['CDF']) {
      console.error("Problem with: " + fname);
      return;
    }
  
    if (body && body['Error'] && body['Error'].length > 0) {
      console.error("Request for "
          + CATALOG[ididx]['id']
          + " gave\nError: "
          + body['Error'][0]
          + "\nMessage: "
          + body['Message'][0]
          + "\nStatus: "
          + body['Message'][0]);
      return;
    }

    let cdfVariables = body['CDF'][0]['cdfVariables'];
    if (cdfVariables.length > 1) {        
      console.error("Case of more than one cdfVariable not implemented.");
      console.error(cdfVariables)
      process.exit(1);
    }

    let orphanAttributes = body['CDF'][0]['orphanAttributes'];
    if (orphanAttributes && orphanAttributes['attribute'].length > 0) {
      if (fromCache == false) {
        let fnameOrphan = fname.replace(".json", ".orphan.json");
        console.log("Writing: " + fnameOrphan);
        fs.writeFile(fnameOrphan, obj2json(orphanAttributes['attribute']), writecb);
      }      
    }

    // Keep only first two data records.
    for ([idx, variable] of Object.entries(cdfVariables['variable'])) {
      let cdfVarRecords = variable['cdfVarData']['record'];
      if (cdfVarRecords.length > 2) {
        body['CDF'][0]['cdfVariables']["variable"][idx]['cdfVarData']['record'] = cdfVarRecords.slice(0, 2);
      }
    }

    if (fromCache == false) {
      console.log("Writing: " + fname);
      fs.writeFile(fname, obj2json(body), writecb);
      if (body['Warning'].length > 0) {
        let fnameWarn = fname.replace(".json", ".warning.json");
        console.log("Writing: " + fnameWarn);
        fs.writeFile(fnameWarn, obj2json(body['Warning']), writecb);
      }
    }

    extractDatasetAttributes(body['CDF'][0]['cdfGAttributes'], CATALOG, ididx);
    extractParameterAttributes(body['CDF'][0]['cdfVariables'], CATALOG, ididx);

    if (finished.N == CATALOG.length) {
      finalizeCatalog(CATALOG);
    }
  }
}

function extractDatasetInfo(allJSONResponse) {

  let CATALOG = [];
  let datasets = allJSONResponse['sites']['datasite'][0]['dataset'];
  for (dataset of datasets) {
    let id = dataset['$']['serviceprovider_ID'];
    if (DATSET_ID_RE.test(id) == false) {
      //console.log("Skipping " + id);
      continue;
    }
    //console.log("Keeping " + id);
    let startDate = dataset['$']['timerange_start'].replace(" ","T") + "Z";
    let stopDate = dataset['$']['timerange_stop'].replace(" ","T") + "Z";
    let contact = dataset['data_producer'][0]['$']['name'].trim() + ' @ ' + dataset['data_producer'][0]['$']['affiliation'].trim();
    CATALOG.push({
      "id": id,
      "info": {
        "startDate": startDate,
        "stopDate": stopDate,
        "contact": contact,
        "resourceURL": "https://cdaweb.gsfc.nasa.gov/misc/Notes.html#" + id
      }
    });
  }
  return CATALOG;
}

function extractParameterNames(variablesResponse, CATALOG, ididx) {

  CATALOG[ididx]['info']['HAPI'] = HAPI_VERSION;
  CATALOG[ididx]['info']['x_parameters'] = {};
  let VariableDescription = variablesResponse['VariableDescription'];
  for (variable of VariableDescription) {
    parameter = {
                  'name': variable['Name'],          
                  'description': variable['LongDescription'] || variable['ShortDescription']
                };
    CATALOG[ididx]['info']['x_parameters'][variable['Name']] = parameter;
  }
  return CATALOG;
}

function extractDatasetAttributes(cdfGAttributes, CATALOG, ididx) {
  
  CATALOG[ididx]['x_gAttributes'] = cdfGAttributes;

  for (attribute of cdfGAttributes['attribute']) {
    if (attribute['name'] === 'TIME_RESOLUTION') {
      CATALOG[ididx]['info']['cadence'] = attribute['entry'][0]['value'];
    }
    if (attribute['name'] === 'SPASE_DATASETRESOURCEID') {
      CATALOG[ididx]['info']['resourceID'] = attribute['entry'][0]['value'];
    }
    if (attribute['name'] === 'GENERATION_DATE') {
      CATALOG[ididx]['info']['creationDate'] = attribute['entry'][0]['value'];
    }
    // MODS => modificationDate
    // ACKNOWLEDGEMENT => citation or datasetCitation
    // RULES_OF_USE => newly proposed datasetTermsOfUse https://github.com/hapi-server/data-specification/issues/155
  }
}

function extractParameterAttributes(cdfVariables, CATALOG, ididx) {

  for (variable of cdfVariables['variable']) {
    let vAttributesKept = extractKeepers(variable['cdfVAttributes']['attribute']);

    if (!CATALOG[ididx]['info']['x_parameters'][variable['name']]) {
      CATALOG[ididx]['info']['x_parameters'][variable['name']] = {};
      // CATALOG[ididx]['x_parameters'] was initialized with
      // all of the variables returned by /variables endpoint.
      // This list does not include support variables. So we add them
      // here. 
      CATALOG[ididx]['info']['x_parameters'][variable['name']]['name'] = variable['name'];
    }
    CATALOG[ididx]['info']['x_parameters'][variable['name']]['vAttributesKept'] = vAttributesKept;
    CATALOG[ididx]['info']['x_parameters'][variable['name']]['variable'] = variable;
  }

  function extractKeepers(attributes) {
    let keptAttributes = {}
    for (attribute of attributes) {
      //console.log(attribute['name'])
      if (attribute['name'] === 'LABLAXIS') {
        keptAttributes['label'] = attribute['entry'][0]['value'];
      }
      if (attribute['name'] === 'FILLVAL') {
        keptAttributes['fill'] = attribute['entry'][0]['value'];
      }
      if (attribute['name'] === 'UNITS') {
        keptAttributes['units'] = attribute['entry'][0]['value'];
      }
      if (attribute['name'] === 'DIM_SIZES') {
        let size = attribute['entry'][0]['value'];
        if (size !== "0")
          keptAttributes['size'] = [parseInt(size)];
      }
      if (attribute['name'] === 'DEPEND_1') {
        keptAttributes['DEPEND_1'] = attribute['entry'][0]['value'];
      }
      if (attribute['name'] === 'LABL_PTR_1') {
        keptAttributes['LABL_PTR_1'] = attribute['entry'][0]['value'];
      }
      if (attribute['name'] === 'VAR_TYPE') {
        keptAttributes['VAR_TYPE'] = attribute['entry'][0]['value'];
      }      
    }
    return keptAttributes;
  }
}

function extractLabel(labelVariable) {

  let delimiter = labelVariable['cdfVarData']['record'][0]['elementDelimiter'];
  let re = new RegExp(delimiter,'g');
  let label = labelVariable['cdfVarData']['record'][0]['value'][0]
                .replace(re,"")
                .replace(/[^\S\r\n]/g,"")
                .trim()
                .split("\n")
    return label;
}

function extractDepend1(depend1Variable) {

  if (depend1Variable['cdfVarInfo']['cdfDatatype'] !== "CDF_CHAR") {
    console.error("DEPEND_1 variable '" + depend1Variable['name'] + "' is not of type CDF_CHAR");
    console.error('This case is not implemented.');
    process.exit(0);
  }

  let delimiter = depend1Variable['cdfVarData']['record'][0]['elementDelimiter'];
  let re = new RegExp(delimiter,'g');
  let depend1 = depend1Variable['cdfVarData']['record'][0]['value'][0]
                .replace(re,"")
                .replace(/[^\S\r\n]/g,"")
                .trim()
                .split("\n")
    return depend1;
}

function extractVectorComponents(depend1) {
  let vectorComponents = false;

  if (depend1.length == 3) {
    // TODO: Get coordinateSystemName
    if (depend1[0] === 'x_component' && depend1[1] === 'y_component' && depend1[2] === 'z_component') {
      vectorComponents = ['x', 'y', 'z'];
    }
    if (depend1[0] === 'x' && depend1[1] === 'y' && depend1[2] === 'z') {
      vectorComponents = ['x', 'y', 'z'];
    }
  }
  return vectorComponents;
}

function extractCoordinateSystemName(dataset) {

  let coordinateSystemName = false;
  let knownNames = ["GSM", "GCI", "GSE", "RTN", "GEO", "MAG"]
  for (knownName of knownNames) {
    if (dataset['name'].includes(knownName)) {
      coordinateSystemName = knownName;
      return coordinateSystemName;
    }
  }
}

function str2ISODuration(cadenceStr) {

  let cadence;
  if (cadenceStr.match(/day/)) {
    cadence = "P" + cadenceStr.replace(/\s.*days?/,'D');
  } else if (cadenceStr.match(/hour/)) {
    cadence = "PT" + cadenceStr.replace(/\s.*hours?/,'H');
  } else if (cadenceStr.match(/minute/)) {
    cadence = "PT" + cadenceStr.replace(/\s.*minute?/,'M');
  } else if (cadenceStr.match(/second/)) {
    cadence = "PT" + cadenceStr.replace(/\s.*second?/,'S');
  } else {
    console.log("Could not parse cadence: " + cadenceStr);
  }
  return cadence;
}

function finalizeCatalog(CATALOG) {

  // Move HAPI-related parameter metadata from info['x_parameters']
  // to info['parameters']. Then delete info['x_parameters']

  for (dataset of CATALOG) {

    if (dataset['info']['cadence']) {
      dataset['info']['cadence'] = str2ISODuration(dataset['info']['cadence']);
    }

    let x_parameters = dataset['info']['x_parameters'];

    let pidx = 0;
    let parameters = [];
    for (parameter of Object.keys(x_parameters)) {

      // Don't put metadata parameters into parameters array.
      if (x_parameters[parameter]['vAttributesKept']['VAR_TYPE'] === "metadata") {
        continue;
      }

      let copy = JSON.parse(obj2json(x_parameters[parameter]));
      parameters.push(copy);

      // Move kept vAttributes up
      for (key of Object.keys(x_parameters[parameter]['vAttributesKept'])) {
        parameters[pidx][key] = x_parameters[parameter]['vAttributesKept'][key];
      }

      // Remove non-HAPI content
      delete parameters[pidx]['vAttributesKept'];
      delete parameters[pidx]['variable']
      delete parameters[pidx]['VAR_TYPE'];

      // Extract DEPEND_1
      let vectorComponents = false;
      if (x_parameters[parameter]['vAttributesKept']['DEPEND_1']) {
        let DEPEND_1 = x_parameters[parameter]['vAttributesKept']['DEPEND_1'];
        let depend1 = extractDepend1(x_parameters[DEPEND_1]['variable'])
        vectorComponents = extractVectorComponents(depend1)
        if (vectorComponents) {
          dataset['info']['parameters'][pidx]['vectorComponents'] = ['x', 'y', 'z'];
          delete parameters[pidx]['DEPEND_1'];
        } else {
          parameters[pidx]['_x_DEPEND_1'] = depend1;
        }
      }

      // Extract labels
      if (x_parameters[parameter]['vAttributesKept']['LABL_PTR_1']) {
        let LABL_PTR_1 = x_parameters[parameter]['vAttributesKept']['LABL_PTR_1'];
        let label = extractLabel(x_parameters[LABL_PTR_1]['variable'])
        parameters[pidx]['label'] = label;
        delete parameters[pidx]['LABL_PTR_1'];
      }

      if (vectorComponents) {
        let coordinateSystemName = extractCoordinateSystemName(dataset['info']['parameters'][pidx]);
        if (coordinateSystemName) {
          parameters[pidx]['coordinateSystemName'] = coordinateSystemName;
        }
      }

      pidx = pidx + 1;
    }

    dataset['info']['parameters'] = parameters;
  }

  // Write HAPI all file with extra 
  console.log("Writing: " + fnameAllFull);
  fs.writeFile(fnameAllFull, obj2json(CATALOG), writecb);
  for (dataset of CATALOG) {
    delete dataset['info']['x_parameters'];
    delete dataset['x_gAttributes'];
  }

  for (dataset of CATALOG) {
    let parameters = dataset['info']['parameters'];
    let Np = dataset['info']['parameters'].length;
    parameters.unshift(
                {
                  "name": "Time",
                  "type": "isotime",
                  "units": "UTC",
                  "length": 24,
                  "fill": null
                });
    if (parameters[Np-1]['name'] !== 'Epoch') {
      // Epoch was a variable in CDFML that is not in the /variables list.
      console.error('Expected last parameter to be Epoch');
      process.exit(1);
    }
    // Remove integer Epoch parameter.
    parameters = parameters.slice(0, -1);
  }

  // Write one info file per dataset
  for (dataset of CATALOG) {
    let fnameDataset = outDir + '/' + dataset['id'] + '.json';
    console.log("Writing: " + fnameDataset);
    fs.writeFile(fnameDataset, obj2json(dataset), writecb);
  }

  // Write HAPI all.json containing all content from all info files.
  console.log("Writing: " + fnameAll);
  fs.writeFile(fnameAll, obj2json(CATALOG), writecb);
}
