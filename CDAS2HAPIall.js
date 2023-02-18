// Create a HAPI all.json catalog based on
//   https://spdf.gsfc.nasa.gov/pub/catalogs/all.xml
// and queries to
//   https://cdaweb.gsfc.nasa.gov/WS/cdasr
// CDASR documentation:
//   https://cdaweb.gsfc.nasa.gov/WebServices/REST/

const HAPI_VERSION = "3.2";

// Command line options
const argv = require('yargs')
              .default
                ({
                  'idregex': '^AC_',
                  'maxsockets': 3,
                  'cachedir': "cache/bw",
                  'all': 'all/all-bw.json',
                  'allfull': 'all/all-bw-full.json',
                  'cdasr': 'https://cdaweb.gsfc.nasa.gov/WS/cdasr/1/dataviews/sp_phys/datasets/',
                  'allxml': 'https://spdf.gsfc.nasa.gov/pub/catalogs/all.xml'
                })
              .argv;

// Async xml2js function (used once).
const xml2js = require('xml2js').parseString;

/////////////////////////////////////////////////////////////////////////////
// Start wrapped Node.js functions
const request = require("request");

// pool should be set in outer most scope. See
// https://www.npmjs.com/package/request#requestoptions-callback
let pool = {maxSockets: argv.maxsockets};
function get(opts, cb) {
  opts['pool'] = pool;
  request(opts, cb);
}

const fs = require('fs');
function existsSync(fname) {
  return fs.existsSync(fname);
}
function writeAsync(fname, data) {
  fs.writeFile(fname, data, 'utf-8', (err) => {if (err) console.error(err)});
}
function writeSync(fname, data) {
  fs.writeFileSync(fname, data, 'utf-8');
}
function readSync(fname) {
  return fs.readFileSync(fname, 'utf-8');
}
function mkdirSync(dir, opts) {
  fs.mkdirSync(dir, opts);
}
function appendSync(fname, data) {
  fs.appendFileSync(fname, data.trim());
}
// End wrapped Node.js functions
/////////////////////////////////////////////////////////////////////////////

/////////////////////////////////////////////////////////////////////////////
// Start time-related functions
const moment  = require('moment');
function incrementTime(timestr, incr, unit) {
  return moment(timestr).add(incr,unit).toISOString().replace(".000Z","Z");
}
function sameDateTime(a, b) {
  return moment(a).isSame(b);
}
function str2ISODateTime(key, str) {

  let stro = str;  

  // moment.js won't handle YYYY-MM-DD +0000.
  str = str.replace(/([0-9]{4})([0-9]{2})([0-9]{2})/,"$1-$2-$3Z");
  if (str !== stro) {
    log(`\tNote: ${key} = ${stro} => ${str}`);
    return str;
  }
  // moment.js won't handle YYYYMMDD +0000.
  str = str.replace(/([0-9]{4})-([0-9]{2})-([0-9]{2})/,"$1-$2-$3Z");
  if (str !== stro) {
    log(`\tNote: ${key} = ${stro} => ${str}`);
    return str;
  }

  // See if moment.js can parse string. If so, cast to ISO.
  moment.suppressDeprecationWarnings = true
  let dateObj = moment(str + " +0000");
  if (dateObj.isValid()) {
    log(`\tNote: ${key} = ${stro} => ${dateObj.toISOString()}`);
    return dateObj.toISOString();
  } else {
    log(`\tWarning: Unparsed ${key} = ${stro}`);
  }  
}
function str2ISODuration(cadenceStr, fnameCDFML) {
  let cadence;
  if (cadenceStr.match(/day/)) {
    cadence = "P" + cadenceStr.replace(/\s.*days?/,'D');
  } else if (cadenceStr.match(/hour/)) {
    cadence = "PT" + cadenceStr.replace(/\s.*hours?/,'H');
  } else if (cadenceStr.match(/minute/)) {
    cadence = "PT" + cadenceStr.replace(/\s.*minutes?/,'M');
  } else if (cadenceStr.match(/second/)) {
    cadence = "PT" + cadenceStr.replace(/\s.*seconds?/,'S');
  } else {
    warning(fnameCDFML, "\tWarning: Could not parse cadence: " + cadenceStr);
  }
  return cadence;
}
// End time-related functions
/////////////////////////////////////////////////////////////////////////////

// Begin convenience functions
function obj2json(obj) {return JSON.stringify(obj, null, 2)}

function log(msg) {
  console.log(msg.replace(/^\t/,'  '));  
}
function warning(fname, msg) {
  fname = fname.replace(".json", ".warning.txt");
  if (!warning.fname) {
    warning.fname = fname;
    writeSync(fname, msg.replace(/^\t/,''));
  } else {
    appendSync(fname, msg.replace(/^\t/,''));
  }
  log(msg);
}
// End convenience functions
/////////////////////////////////////////////////////////////////////////////

if (!existsSync(argv.cachedir)) {
  mkdirSync(argv.cachedir, {recursive: true});  
}

catalog();

function catalog() {

  // Request all.xml to get dataset names.
  // Then call variables() to get list of variables for each dataset.

  let fnameJSON = argv.cachedir + "/all.json";

  if (existsSync(fnameJSON)) {
    log("Reading: " + fnameJSON);
    let body = readSync(fnameJSON);
    finished(JSON.parse(body), true);
    return;
  }

  let reqOpts = {uri: argv.allxml};
  log("Requesting: " + argv.allxml);
  get(reqOpts, function (err,res,body) {
    if (err) log(err);
    log("Received: " + argv.allxml);
    xml2js(body, function (err, jsonObj) {
      finished(jsonObj, false);
    });
  });

  function finished(body, fromCache) {

    if (fromCache == false) {
      log("Writing: " + fnameJSON);
      writeAsync(fnameJSON, obj2json(body), 'utf-8');
    }    

    let CATALOG = extractDatasetInfo(body);

    let allIds = [];
    for (let dataset of CATALOG) {
      // Could create allIds array in extractDatasetInfo()
      // to avoid this loop.
      allIds.push(dataset['id']);
    }
    let allIdsFile = argv.cachedir + "/ids.txt";
    log("Writing: " + allIdsFile);
    writeSync(allIdsFile, allIds.join("\n"));

    variables(CATALOG);

  }
}

function variables(CATALOG) {

  // Call /variables endpoint to get list of variables for each dataset.
  // Then call variableDetails() to get additional metadata for variables.

  let ididx = 0;
  for (let ididx = 0; ididx < CATALOG.length; ididx++) {
    let url = argv.cdasr + CATALOG[ididx]['id'] + "/variables";
    let fnameVariables = argv.cachedir + "/" + CATALOG[ididx]['id'] + "-variables.json";
    requestVariables(url, fnameVariables, ididx);
  }

  function requestVariables(url, fnameVariables, ididx) {

    log(CATALOG[ididx]['id']);
    if (existsSync(fnameVariables)) {
      log("\tReading: " + fnameVariables);
      let body = readSync(fnameVariables);
      finished(ididx, fnameVariables, body, true)
      return;
    }

    let reqOpts = {uri: url, headers: {'Accept':'application/json'}};
    log("\tRequesting: " + url.replace(argv.cdasr,""));
    get(reqOpts, function (err, res, body) {
      if (err) log(err);
      log("\tReceived: " + url.replace(argv.cdasr, ""));
      finished(ididx, fnameVariables, body, false);
    });
  }

  function finished(ididx, fnameVariables, variablesResponse, fromCache) {

    if (!finished.N) {finished.N = 0;}
    finished.N = finished.N + 1;
    variablesResponse = JSON.parse(variablesResponse);

    if (fromCache == false) {
      log("\tWriting: " + fnameVariables);
      writeAsync(fnameVariables, obj2json(variablesResponse));
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

  for (let ididx = 0; ididx < CATALOG.length; ididx++) {
    parameters = null;
    parameters = [];
    for (let name of Object.keys(CATALOG[ididx]['info']['x_parameters'])) {
      parameters.push(name);
    }
    parameters = parameters.join(",")
    //parameters = parameters[0];

    let stop = incrementTime(CATALOG[ididx]['info']['startDate'], 1, 'hour');
    let url = argv.cdasr + CATALOG[ididx]['id'] + "/variables";
    url = argv.cdasr
            + CATALOG[ididx]['id']
            + "/data/"
            + CATALOG[ididx]['info']['startDate'].replace(/-|:/g,"")
            + "," 
            + stop.replace(/-|:/g,"")
            + "/"
            + parameters
            + "?format=json";

    let fnameCDFML = argv.cachedir + "/" + CATALOG[ididx]['id'] + '-cdfml.json';
    requestVariableDetails(url, fnameCDFML, ididx);
  }

  function requestVariableDetails(url, fnameCDFML, ididx) {
    
    log(CATALOG[ididx]['id']);

    if (existsSync(fnameCDFML)) {
      log("\tReading: " + fnameCDFML);
      let body = readSync(fnameCDFML);
      finished(ididx, fnameCDFML, body, true)
      return;
    }

    let reqOpts = {uri: url, headers: {'Accept':'application/json'}};
    log("\tRequesting: " + url);
    get(reqOpts, function (err,res,body) {
      if (err) log(err);
      log("\tReceived: " + url);
      finished(ididx, fnameCDFML, body, false);
    });
  }

  function finished(ididx, fnameCDFML, body, fromCache) {

    if (!finished.N) {finished.N = 0;}
    finished.N = finished.N + 1;

    if (!body) {
      console.error("Problem with: " + fnameCDFML);
      return;
    }

    if (body.match("Internal Server Error") || body.match("Bad Request") || body.match("No data available") || body.match("Not Found")) {
      console.error("Problem with: " + fnameCDFML);
      return;
    }

    body = JSON.parse(body);

    if (!body['CDF']) {
      console.error("Problem with: " + fnameCDFML);
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
        let fnameOrphan = fnameCDFML.replace(".json", ".orphan.json");
        log("Writing: " + fnameOrphan);
        writeAsync(fnameOrphan, obj2json(orphanAttributes['attribute']));
      }      
    }
    
    for (let [idx, variable] of Object.entries(cdfVariables['variable'])) {
      let cdfVarRecords = variable['cdfVarData']['record'];

      if (variable['name'] === 'Epoch') {
        let startDate = CATALOG[ididx]['info']['startDate'];
        let sampleStartDate = cdfVarRecords[0]['value'][0];
        if (!sameDateTime(startDate, sampleStartDate)) {
          warning(fnameCDFML.replace("-cdfml",""), "\tWarning: Given start of  " + startDate + " differs from first record at " + sampleStartDate);
        }
        sampleStopDate = cdfVarRecords.slice(-1)[0]['value'][0];

        let Nr = cdfVarRecords.length;
        if (Nr >= 50) {
          CATALOG[ididx]['info']['sampleStartDate'] = sampleStartDate;
          CATALOG[ididx]['info']['sampleStopDate'] = cdfVarRecords[49]['value'][0];
        } else {
          warning(fnameCDFML.replace("-cdfml",""), `\tWarning: Received ${Nr} records; want >= 50 to compute sampleStopDate`);
        }
      }

      // Keep only first two data records.
      // Can't do b/c we need it cached for sample start/stop above.
      if (cdfVarRecords.length > 2) {
        body['CDF'][0]['cdfVariables']["variable"][idx]['cdfVarData']['record'] = cdfVarRecords.slice(0, 2);
      }
    }

    if (fromCache == false) {
      log("\tWriting: " + fnameCDFML);
      writeSync(fnameCDFML, obj2json(body));
      if (body['Warning'].length > 0) {
        let fnameWarn = fnameCDFML.replace(".json", ".warning.json");
        log("\tWriting: " + fnameWarn);
        writeAsync(fnameWarn, obj2json(body['Warning']));
      }
    }

    extractDatasetAttributes(body['CDF'][0]['cdfGAttributes'], CATALOG, ididx);
    extractParameterAttributes(body['CDF'][0]['cdfVariables'], CATALOG, ididx);

    if (finished.N == CATALOG.length) {
      finalizeCatalog(CATALOG, fnameCDFML);
    }
  }
}

function finalizeCatalog(CATALOG, fnameCDFML) {

  // Move HAPI-related parameter metadata from info['x_parameters']
  // to info['parameters']. Then delete info['x_parameters']

  for (let dataset of CATALOG) {
    
    if (dataset['info']['cadence']) {
      dataset['info']['cadence'] = str2ISODuration(dataset['info']['cadence'], fnameCDFML);
    }

    let x_parameters = dataset['info']['x_parameters'];

    let pidx = 0;
    let parameters = [];
    for (parameter of Object.keys(x_parameters)) {
      
      // Don't put metadata parameters into parameters array.
      if (x_parameters[parameter]['vAttributesKept']['VAR_TYPE'] !== "data") {
        if (x_parameters[parameter]['name'] !== 'Epoch') {
          //warning(fnameCDFML, "\tNote: Skipping non-data parameter " + parameter);
          continue;
        }
      }

      let copy = JSON.parse(obj2json(x_parameters[parameter]));
      parameters.push(copy);

      // Move kept vAttributes up
      for (let key of Object.keys(x_parameters[parameter]['vAttributesKept'])) {
        parameters[pidx][key] = x_parameters[parameter]['vAttributesKept'][key];
      }

      // Extract DEPEND_1
      let vectorComponents = false;
      if (x_parameters[parameter]['vAttributesKept']['DEPEND_1']) {
        let DEPEND_1 = x_parameters[parameter]['vAttributesKept']['DEPEND_1'];
        let depend1 = extractDepend1(x_parameters[DEPEND_1]['variable'])
        vectorComponents = extractVectorComponents(depend1)
        if (vectorComponents) {
          parameters[pidx]['vectorComponents'] = ['x', 'y', 'z'];
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
        let coordinateSystemName = extractCoordinateSystemName(parameters[pidx]);
        if (coordinateSystemName) {
          parameters[pidx]['coordinateSystemName'] = coordinateSystemName;
        }
      }

      // Remove non-HAPI content
      delete parameters[pidx]['vAttributesKept'];
      delete parameters[pidx]['variable']
      delete parameters[pidx]['VAR_TYPE'];

      pidx = pidx + 1;
    }

    let Np = parameters.length;
    if (parameters[Np-1]['name'] !== 'Epoch') {
      // Epoch is a non-data variable in CDFML that is not in 
      // list returned by the /variables call.
      console.error('Expected last parameter to be Epoch');
      log(CATALOG[ididx])
      log(parameters)
      process.exit(1);
    }

    let firstTimeValue = x_parameters['Epoch']['variable']['cdfVarData']['record'][0]['value'][0];
    let timePadValue = x_parameters['Epoch']['variable']['cdfVarInfo']['padValue'];

    // Remove integer Epoch parameter.
    parameters = parameters.slice(0, -1);

    // Prepend Time parameter
    parameters.unshift(
                {
                  "name": "Time",
                  "type": "isotime",
                  "units": "UTC",
                  "length": firstTimeValue.length,
                  "fill": timePadValue
                });


    dataset['info']['parameters'] = parameters;
  }

  // Write one info file per dataset
  for (let dataset of CATALOG) {
    delete dataset['info']['x_parameters'];
    delete dataset['x_gAttributes'];

    let fnameInfo = argv.cachedir + '/' + dataset['id'] + '.json';
    //log("\tWriting: " + fnameInfo);
    writeAsync(fnameInfo, obj2json(dataset));
  }
  log(`Wrote ${CATALOG.length} info files to ${argv.cachedir}`);

  // Write HAPI all.json containing all content from all info files.
  log("Writing: " + argv.all);
  writeAsync(argv.all, obj2json(CATALOG));
}

function extractDatasetInfo(allJSONResponse) {

  let CATALOG = [];
  let datasets = allJSONResponse['sites']['datasite'][0]['dataset'];
  for (let dataset of datasets) {
    let id = dataset['$']['serviceprovider_ID'];
    let re = new RegExp(argv.idregex);
    if (re.test(id) == false) {
      //log("Skipping " + id);
      continue;
    }
    //log("Keeping " + id);
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
  for (let variable of VariableDescription) {
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

  for (let attribute of cdfGAttributes['attribute']) {
    if (attribute['name'] === 'TIME_RESOLUTION') {
      CATALOG[ididx]['info']['cadence'] = attribute['entry'][0]['value'];
    }
    if (attribute['name'] === 'SPASE_DATASETRESOURCEID') {
      CATALOG[ididx]['info']['resourceID'] = attribute['entry'][0]['value'];
    }
    if (attribute['name'] === 'GENERATION_DATE') {
      let creationDate = attribute['entry'][0]['value'];
      CATALOG[ididx]['info']['creationDate'] = str2ISODateTime('GENERATION_DATE',creationDate);

    }
    if (attribute['name'] === 'ACKNOWLEDGEMENT') {
      CATALOG[ididx]['info']['datasetCitation'] = catCharEntries(attribute['entry']);
    }
    if (attribute['name'] === 'RULES_OF_USE') {
      CATALOG[ididx]['info']['datasetTermsOfUse'] = catCharEntries(attribute['entry']);
    }
  }

  function catCharEntries(entries) {
    let cat = "";
    for (let entry of entries) {
      cat = cat.trim() + " " + entry['value'];
    }
    return cat;
  }
}

function extractParameterAttributes(cdfVariables, CATALOG, ididx) {

  let x_parameters = CATALOG[ididx]['info']['x_parameters'];
  for (let variable of cdfVariables['variable']) {

    let vAttributesKept = extractKeepers(variable['cdfVAttributes']['attribute']);

    if (!x_parameters[variable['name']]) {
      x_parameters[variable['name']] = {};
      // CATALOG[ididx]['x_parameters'] was initialized with all of the 
      // variables returned by /variables endpoint. This list does not 
      // include support variables. So we add them here.
      x_parameters[variable['name']]['name'] = variable['name'];
    }

    x_parameters[variable['name']]['vAttributesKept'] = vAttributesKept;
    x_parameters[variable['name']]['variable'] = variable;
  }

  function extractKeepers(attributes) {
    let keptAttributes = {}
    for (let attribute of attributes) {
      if (attribute['name'] === 'LABLAXIS') {
        keptAttributes['label'] = attribute['entry'][0]['value'];
      }
      if (attribute['name'] === 'FILLVAL') {
        keptAttributes['fill'] = attribute['entry'][0]['value'];
      }
      if (attribute['name'] === 'MISSING_VALUE') {
        keptAttributes['_fillMissing'] = attribute['entry'][0]['value'];
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
      if (attribute['name'] === 'DEPEND_2') {
        console.error(variable + " has a DEPEND_2");
        process.exit(0);
      }
    }
    return keptAttributes;
  }
}

function extractLabel(labelVariable) {

  let delimiter = labelVariable['cdfVarData']['record'][0]['elementDelimiter'];
  let re = new RegExp(delimiter,'g');
  let label = labelVariable['cdfVarData']['record'][0]['value'][0];
  label
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
  for (let knownName of knownNames) {
    if (dataset['name'].includes(knownName)) {
      coordinateSystemName = knownName;
      return coordinateSystemName;
    }
  }
}
