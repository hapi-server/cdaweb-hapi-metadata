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
                  'skip': '^ALOUETTE2,AIM_CIPS_SCI_3A',
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
  log("Requested: " + opts['uri']);
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
  if (!fs.existsSync(dir))
    fs.mkdirSync(dir, opts, {recursive: true});
}
function appendSync(fname, data) {
  fs.appendFileSync(fname, data, {flags: "a+"});
}
// End wrapped Node.js functions
/////////////////////////////////////////////////////////////////////////////

/////////////////////////////////////////////////////////////////////////////
// Start time-related functions
const moment  = require('moment');
function incrementTime(timestr, incr, unit) {
  return moment(timestr).add(incr,unit).toISOString().slice(0,19) + "Z";
}
function sameDateTime(a, b) {
  return moment(a).isSame(b);
}
function str2ISODateTime(key, stro) {

  str = stro.trim();

  // e.g., 201707006
  str = str.replace(/^([0-9]{4})([0-9]{3})([0-9]{2})$/,"$1$2");
  if (str.length === 7) {    
    let y_doy = str.replace(/^([0-9]{4})([0-9]{3})$/,"$1-$2").split("-");
    str = new Date(new Date(y_doy[0]+'-01-01Z').getTime() + parseInt(y_doy[1])*86400000).toISOString().slice(0,10);
  } else {
    str = str.replace(/([0-9]{4})([0-9]{2})([0-9]{2})/,"$1-$2-$3");
    str = str.replace(/([0-9]{4})-([0-9]{2})-([0-9]{2})/,"$1-$2-$3Z");
  }

  // See if moment.js can parse string. If so, cast to ISO 8601.
  moment.suppressDeprecationWarnings = true
  let offset = ""
  if (str.length === 8) {
    let offset = " +0000";
  }
  let dateObj = moment(str + offset);
  if (dateObj.isValid()) {
    str = dateObj.toISOString().slice(0, 10);
    if (str.split("-")[0] === "8888") return undefined;
    log(`  Note: ${key} = ${stro} => ${str}`);
    return str;
  } else {
    return undefined;
  }  
}
function str2ISODuration(cadenceStr) {
  let cadence;
  if (cadenceStr.match(/day/)) {
    cadence = "P" + cadenceStr.replace(/\s.*days?/,'D');
  } else if (cadenceStr.match(/hour/)) {
    cadence = "PT" + cadenceStr.replace(/\s.*hours?/,'H');
  } else if (cadenceStr.match(/hr/)) {
    cadence = "PT" + cadenceStr.replace(/\s.*hrs?/,'H');
  } else if (cadenceStr.match(/minute/)) {
    cadence = "PT" + cadenceStr.replace(/\s.*minutes?/,'M');
  } else if (cadenceStr.match(/min/)) {
    cadence = "PT" + cadenceStr.replace(/\s.*mins?/,'M');
  } else if (cadenceStr.match(/second/)) {
    cadence = "PT" + cadenceStr.replace(/\s.*seconds?/,'S');
  } else if (cadenceStr.match(/sec/)) {
    cadence = "PT" + cadenceStr.replace(/\s.*secs?/,'S');
  } else if (cadenceStr.match(/[0-9]s/)) {
    cadence = "PT" + cadenceStr.replace(/([0-9].*)s/,'$1S');
  } else if (cadenceStr.match(/millisecond/)) {
    cadence = "PT" + cadenceStr.replace(/\s.*milliseconds?/,'S');
  } else if (cadenceStr.match(/ms/)) {
    cadence = "PT" + cadenceStr.replace(/\s.*ms/,'S');
  } else {
    return undefined;
  }
  return cadence.trim();
}
// End time-related functions
/////////////////////////////////////////////////////////////////////////////

// Begin convenience functions
function obj2json(obj) {return JSON.stringify(obj, null, 2)}

function log(msg, msgf) {
  mkdirSync(argv.cachedir);
  let fname = argv.cachedir + "/log.txt";
  if (!log.fname) {
    fs.rmSync(fname, { recursive: true, force: true });
    log.fname = fname;
  }
  if (msgf) {
    appendSync(fname, msgf + "\n");
  } else {
    appendSync(fname, msg + "\n");    
  }
  console.log(msg);  
}
function warning(dsid, msg) {
  let warnDir = argv.cachedir + "/" + dsid.split("_")[0];
  mkdirSync(warnDir);
  let fname = warnDir + "/" + dsid + ".warning.txt";
  msgf = "  Warning: " + msg;
  if (!warning.fname) {
    fs.rmSync(fname, { recursive: true, force: true });
    warning.fname = fname;
  }
  appendSync(fname, msgf);
  const chalk = require("chalk");
  msg = "  " + chalk.yellow.bold("Warning: ") + msg;
  log(msg, msgf);
}
function error(dsid, msg, exit) {
  let errorDir = argv.cachedir;
  if (dsid) {
    errorDir = argv.cachedir + "/" + dsid.split("_")[0];
  }
  mkdirSync(errorDir);
  let fname = errorDir + "/" + dsid + ".error.txt";
  if (!error.fname) {
    fs.rmSync(fname, { recursive: true, force: true });
    error.fname = fname;
  }
  if (Array.isArray(msg)) {
    msg = msg.join('\n').replace(/\n/g,'\n    ');
  }
  appendSync(fname, "  " + msg);
  const chalk = require("chalk");
  let msgf = msg.split("\n");
  msg = "  " + chalk.red.bold("Error: ") + msg;
  if (dsid) {
    msg = dsid + "\n" + msg;    
  }
  log(msg, msgf);
  if (exit === undefined || exit == true) {
    process.exit(1);
  }
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

  let fnameAllJSON = argv.cachedir + "/all.xml.json";

  if (existsSync(fnameAllJSON)) {
    log("Reading: " + fnameAllJSON);
    let body = readSync(fnameAllJSON);
    finished(JSON.parse(body), true);
    return;
  }

  get({uri: argv.allxml}, function (err,res,body) {

    if (err) log(err);
    log("Received: " + argv.allxml);

    let fnameAllXML = argv.cachedir + "/all.xml";
    log("Writing: " + fnameAllXML);
    writeSync(fnameAllXML, obj2json(body), 'utf-8');

    xml2js(body, function (err, jsonObj) {
      finished(jsonObj, false);
    });
  });

  function finished(body, fromCache) {

    if (fromCache == false) {
      log("Writing: " + fnameAllJSON);
      writeSync(fnameAllJSON, obj2json(body), 'utf-8');
    }

    let CATALOG = extractDatasetInfo(body);
    variables(CATALOG);

  }
}

function variables(CATALOG) {

  // Call /variables endpoint to get list of variables for each dataset.
  // Then call variableDetails() to get additional metadata for variables.

  let ididx = 0;
  for (let ididx = 0; ididx < CATALOG.length; ididx++) {
    let url = argv.cdasr + CATALOG[ididx]['id'] + "/variables";
    let dirId = argv.cachedir + "/" + CATALOG[ididx]['id'].split("_")[0];
    mkdirSync(dirId)    
    let fnameVariables = dirId + "/" + CATALOG[ididx]['id'] + "-variables.json";
    requestVariables(url, fnameVariables, ididx);
  }

  function requestVariables(url, fnameVariables, ididx) {

    log(CATALOG[ididx]['id']);

    if (existsSync(fnameVariables)) {
      log("  Reading: " + fnameVariables);
      let body = readSync(fnameVariables);
      finished(ididx, fnameVariables, body, true)
      return;
    }

    let reqOpts = {uri: url, headers: {'Accept':'application/json'}};
    get(reqOpts, function (err, res, body) {
      if (err) log(err);
      log("  Received: " + url.replace(argv.cdasr, ""));
      finished(ididx, fnameVariables, body, false);
    });
  }

  function finished(ididx, fnameVariables, variablesResponse, fromCache) {

    if (!finished.N) {finished.N = 0;}
    finished.N = finished.N + 1;
    variablesResponse = JSON.parse(variablesResponse);

    if (fromCache == false) {
      log("Writing: " + fnameVariables);
      writeSync(fnameVariables, obj2json(variablesResponse));
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
    requestVariableDetails(ididx, parameters);
  }

  function requestVariableDetails(ididx, parameters, timeRangeScalePower) {

    if (timeRangeScalePower === undefined) {
      timeRangeScalePower = 10;
    }

    log(CATALOG[ididx]['id']);

    let fnameCDFML = argv.cachedir + "/" + CATALOG[ididx]['id'].split("_")[0] + "/" + CATALOG[ididx]['id'] + '-cdfml.json';
    if (existsSync(fnameCDFML)) {
      log("  Reading: " + fnameCDFML);
      let body = readSync(fnameCDFML);
      finished(ididx, fnameCDFML, body, true);
      return;
    }

    if (!requestVariableDetails.tries) {
      requestVariableDetails.tries = {};
    }
    if (!requestVariableDetails.tries[ididx]) {
      requestVariableDetails.tries[ididx] = 0;
    }
    requestVariableDetails.tries[ididx] += 1;

    let seconds = 100*Math.pow(timeRangeScalePower, requestVariableDetails.tries[ididx] - 1);

    let stop = incrementTime(CATALOG[ididx]['info']['startDate'], seconds, 'seconds');
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

    let reqOpts = {uri: url, headers: {'Accept':'application/json'}};
    get(reqOpts, function (err,res,body) {
      log("Received: " + url.replace(argv.cdasr, ""));
      if (err) {
        error(CATALOG[ididx]['id'], err, true);
      }
      let timeRangeScalePower = 10;
      if (!body) {
        err = true;
        reason = "Empty body";
      }
      if (body.match("Internal Server Error") || body.match("Bad Request") || body.match("No data available") || body.match("Not Found")) {
        err = true;
        reason = body;
      }
      if (body.match("Requested amount of data is too large")) {
        err = true;
        timeRangeScalePower = 1/10;
        reason = "Requested amount of data is too large";
      }      
      if (err && requestVariableDetails.tries[ididx] < 4) {
        log("Retrying (#" + requestVariableDetails.tries[ididx] + ") " + CATALOG[ididx]['id'] + " due to '" + reason + "'.");
        requestVariableDetails(ididx, parameters, timeRangeScalePower)
      } else {
        finished(ididx, fnameCDFML, body, false);
      }
    });
  }

  function finished(ididx, fnameCDFML, body, fromCache) {

    if (!finished.N) {finished.N = 0;}
    finished.N = finished.N + 1;

    body = JSON.parse(body);

    if (!body['CDF']) {
      error(CATALOG[ididx]['id'], "Problem with " + CATALOG[ididx]['id'] + ": JSON has no CDF element. Omitting. Returned content: \n" + obj2json(body), false);
      CATALOG[ididx]['x_variables'] = null;
      if (body['Error'] && body['Error'].length > 0) {
        error(CATALOG[ididx]['id'],
            [
              "Request for "+ CATALOG[ididx]['id'] + " gave",
              "Error: " + body['Error'][0],
              "Message: " + body['Message'][0],
              "Status: " + body['Message'][0],
              "Omitting."
            ]
            , false);
      }
    } else if (!body['CDF'][0]) {
      error(CATALOG[ididx]['id'], "Problem with " + CATALOG[ididx]['id'] + ": JSON has no CDF[0] element. Omitting. Returned content: \n" + obj2json(body), false);
      CATALOG[ididx]['x_variables'] = null;      
    } else {
      let cdfVariables = body['CDF'][0]['cdfVariables'];
      if (cdfVariables.length > 1) {        
        error(CATALOG[ididx]['id'], ["Case of more than one cdfVariable not implemented. Omitting.", cdfVariables], true);
        CATALOG[ididx]['x_variables'] = null;
      }

      let orphanAttributes = body['CDF'][0]['orphanAttributes'];
      if (orphanAttributes && orphanAttributes['attribute'].length > 0) {
        if (fromCache == false) {
          let fnameOrphan = fnameCDFML.replace(".json", ".orphan.json");
          log("Writing: " + fnameOrphan);
          writeAsync(fnameOrphan, obj2json(orphanAttributes['attribute']));
        }      
      }

      // Keep only first two data records.
      for (let [idx, variable] of Object.entries(cdfVariables['variable'])) {
        if (variable['cdfVarData']['record'].length > 2) {
          body['CDF'][0]['cdfVariables']["variable"][idx]['cdfVarData']['record'] 
            = body['CDF'][0]['cdfVariables']["variable"][idx]['cdfVarData']['record'].slice(0,2)
        }
      }

      if (fromCache == false) {
        log("Writing: " + fnameCDFML);
        writeSync(fnameCDFML, obj2json(body));
        if (body['Warning'].length > 0) {
          let fnameCDFMLWarn = fnameCDFML.replace(".json", ".warning.json");
          log("Writing: " + fnameCDFMLWarn);
          writeSync(fnameCDFML, obj2json(body['Warning']));
        }
      }

      CATALOG[ididx]['x_additionalMetadata'] = {};
      CATALOG[ididx]['x_additionalMetadata']['CDF'] = body['CDF'][0];
      CATALOG[ididx]['x_variables'] = body['CDF'][0]['cdfVariables'];
    }
  
    if (finished.N == CATALOG.length) {
      finalizeCatalog(CATALOG);
    }
  }
}

function subsetDataset(dataset) {
  // Look for parameters that have more than one DEPEND_0.
  let x_parameters = dataset['info']['x_parameters'];
  let DEPEND_0s = {};
  for (parameter of Object.keys(x_parameters)) {
    let DEPEND_0 = x_parameters[parameter]['x_vAttributesKept']['x_DEPEND_0'];
    if (DEPEND_0 !== undefined) {
      DEPEND_0s[DEPEND_0] = DEPEND_0;
    }
  }
  DEPEND_0s = Object.keys(DEPEND_0s);
  if (DEPEND_0s.length == 1) {
    return undefined;
  }

  log("  Note: " + DEPEND_0s.length + " DEPEND_0s");
  let datasets = [];
  for ([sdsidx, DEPEND_0] of Object.entries(DEPEND_0s)) {
    newdataset = JSON.parse(JSON.stringify(dataset));
    newdataset['id'] = newdataset['id'] + "@" + sdsidx;
    for (parameter of Object.keys(newdataset['info']['x_parameters'])) {
      let depend_0 = x_parameters[parameter]['x_vAttributesKept']['x_DEPEND_0'];
      if (depend_0 !== DEPEND_0) {
        delete parameter;
      }
    }
    datasets.push(newdataset)
  }
  return datasets;
}

function finalizeCatalog(CATALOG) {

  // Move HAPI-related parameter metadata from info['x_parameters'] to
  // info['parameters']. Then delete x_ keys.

  // Subset datasets that have mutliple DEPEND_0s
  let CATALOGexpanded = JSON.parse(JSON.stringify(CATALOG));
  for (let [dsidx, dataset] of Object.entries(CATALOG)) {

    if (!dataset['x_variables']) {
      error(dataset['id'], "  Omitting " + dataset['id'] + " from HAPI all.json because no variable attributes.", false);
      continue;
    }

    extractParameterAttributes(dataset);
    extractDatasetAttributes(dataset);

    let subdatasets = subsetDataset(dataset);
    if (subdatasets !== undefined) {
      log("  Note: " + subdatasets.length + " sub-datasets");
      CATALOGexpanded.splice(dsidx, 1, ...subdatasets);
    }
  }

  CATALOG = null
  CATALOG = CATALOGexpanded;

  // Remove nulled elements.
  CATALOG = CATALOG.filter(function (el) {return el != null;});

  for (let dataset of CATALOG) {

    if (!dataset['x_variables']) {
      error(dataset['id'], "  Omitting " + dataset['id'] + " from HAPI all.json because not variable attributes.", false);
      continue;
    }

    log(dataset['id']);

    extractParameterAttributes(dataset);
    extractDatasetAttributes(dataset);

    if (dataset['info']['cadence']) {
      let cadence = str2ISODuration(dataset['info']['cadence']);
      if (cadence !== undefined) {
        dataset['info']['cadence'] = cadence;
      } else {
        warning(dataset['id'], "Could not parse cadence: " + dataset['info']['cadence']);
      }
    }

    let x_parameters = dataset['info']['x_parameters'];
    console.log(x_parameters)
    let pidx = 0;
    let parameters = [];
    for (parameter of Object.keys(x_parameters)) {


      if (x_parameters[parameter]['x_vAttributesKept']['x_VAR_TYPE'] !== "data") {
        // Don't put metadata parameters into parameters array.
        if (!x_parameters[parameter]['name'].toLowerCase().startsWith('epoch')) {
          console.log('Omitting' + x_parameters[parameter]['name'])
          continue;
        }
      }

      let copy = JSON.parse(obj2json(x_parameters[parameter]));
      parameters.push(copy);

      // Move kept vAttributes up
      for (let key of Object.keys(x_parameters[parameter]['x_vAttributesKept'])) {
        parameters[pidx][key] = x_parameters[parameter]['x_vAttributesKept'][key];
      }

      if (!parameters[pidx]['units']) {
        warning(dataset['id'], "No units for " + parameters[pidx]['name']);
        parameters[pidx]['units'] = null;
      }
      if (parameters[pidx]['bins'] && !parameters[pidx]['bins']) {
        warning(dataset['id'], "No bin units for " + parameters[pidx]['name']);
        parameters[pidx]['bins']['units'] = null;
      }

      let DEPEND_0 = x_parameters[parameter]['x_vAttributesKept']['x_DEPEND_0'];
      if (DEPEND_0 && !DEPEND_0.toLowerCase().startsWith('epoch')) {
        warning(dataset['id'], `${parameter} has DEPEND_0 name of '${DEPEND_0}'; expected 'Epoch'`);
      }

      // Extract DEPEND_1
      let vectorComponents = false;
      if (x_parameters[parameter]['x_vAttributesKept']['x_DEPEND_1']) {
        let DEPEND_1 = x_parameters[parameter]['x_vAttributesKept']['x_DEPEND_1'];
        let depend1 = extractDepend1(dataset['id'], x_parameters[DEPEND_1]['x_variable']);
        if (Array.isArray(depend1)) {
          vectorComponents = extractVectorComponents(depend1)
          if (vectorComponents) {
            parameters[pidx]['x_vectorComponents'] = ['x', 'y', 'z'];
          } else {
            warning(dataset['id'], "Un-handled DEPEND_1 for " + parameter);
          }
        } else {
          parameters[pidx]['bins'] = depend1;
        }
        delete parameters[pidx]['x_DEPEND_1'];
      }

      // Extract labels
      if (x_parameters[parameter]['x_vAttributesKept']['x_LABL_PTR_1']) {
        let LABL_PTR_1 = x_parameters[parameter]['x_vAttributesKept']['x_LABL_PTR_1'];
        let label = extractLabel(x_parameters[LABL_PTR_1]['x_variable'])
        parameters[pidx]['label'] = label;
        delete parameters[pidx]['x_LABL_PTR_1'];
      }

      if (vectorComponents) {
        let coordinateSystemName = extractCoordinateSystemName(parameters[pidx]);
        if (coordinateSystemName) {
          parameters[pidx]['x_coordinateSystemName'] = coordinateSystemName;
        }
      }

      // Remove non-HAPI content
      delete parameters[pidx]['x_vAttributesKept'];
      delete parameters[pidx]['x_variables']
      delete parameters[pidx]['x_VAR_TYPE'];
      delete parameters[pidx]['x_DEPEND_0'];

      pidx = pidx + 1;
    }

    let Np = parameters.length;
    if (!parameters[Np-1]['name'].toLowerCase().startsWith('epoch')) {
      // Epoch is a non-data variable in CDFML that is not in 
      // list returned by the /variables call.
      error(dataset['id'], "Expected last parameter name converted to lower case to start with 'epoch' not " + parameters[Np-1]['name'] + ". Omitting.", false);
      dataset = null;
      continue;
    }

    let EpochName = parameters[Np-1]['name'];
    let firstTimeValue = x_parameters[EpochName]['x_variable']['cdfVarData']['record'][0]['value'][0];
    let timePadValue = x_parameters[EpochName]['x_variable']['cdfVarInfo']['padValue'];

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

  // Remove nulled elements.
  CATALOG = CATALOG.filter(function (el) {return el != null;});

  // Write one info file per dataset
  let allIds = [];
  for (let dataset of CATALOG) {
    allIds.push(dataset['id']);

    delete dataset['x_variables'];
    delete dataset['info']['x_parameters'];

    let fnameInfo = argv.cachedir + '/' + dataset['id'].split("_")[0] + '/' + dataset['id'] + '.json';
    writeAsync(fnameInfo, obj2json(dataset));
    if (CATALOG.length == 1) {
      log(`Wrote: ${fnameInfo}`);
    }
  }

  if (CATALOG.length > 1) {
    log(`Wrote ${CATALOG.length} info files to ${argv.cachedir}`);
  }

  // Write HAPI all.json containing all content from all info files.
  let allIdsFile = argv.cachedir + "/ids-hapi.txt";
  log("Writing: " + allIdsFile);
  writeSync(allIdsFile, allIds.join("\n"));

  // Write HAPI all.json containing all content from all info files.
  log("Writing: " + argv.all);
  writeAsync(argv.all, obj2json(CATALOG));
}

function extractDatasetInfo(allJSONResponse) {

  let CATALOG = [];
  let allIds = [];
  let datasets = allJSONResponse['sites']['datasite'][0]['dataset'];
  for (let dataset of datasets) {

    let id = dataset['$']['serviceprovider_ID'];
    allIds.push(id);

    let re = new RegExp(argv.idregex);
    if (re.test(id) == false) {
      continue;
    }

    let skips = argv.skip.split(",");
    let omit = false;
    for (skip of skips) {
      let re = new RegExp(skip);
      if (re.test(id) == true) {
        log("Note: Skipping " + id + " b/c matches regex '" + skip + "' in skips.");
        omit = true;
        break;
      }
    }
    if (omit) {continue;}

    let startDate = dataset['$']['timerange_start'].replace(" ","T") + "Z";
    let stopDate = dataset['$']['timerange_stop'].replace(" ","T") + "Z";
    let contact = dataset['data_producer'][0]['$']['name'].trim() + ' @ ' + dataset['data_producer'][0]['$']['affiliation'].trim();
    CATALOG.push({
      "id": id,
      "info": {
        "HAPI": HAPI_VERSION,
        "startDate": startDate,
        "stopDate": stopDate,
        "contact": contact,
        "resourceURL": "https://cdaweb.gsfc.nasa.gov/misc/Notes.html#" + id
      }
    });
  }

  let allIdsFile = argv.cachedir + "/ids-cdasr.txt";
  log("Writing: " + allIdsFile);
  writeSync(allIdsFile, allIds.join("\n"));

  if (CATALOG.length == 0) {
    error(null, `Regex '${argv.idregex}' did not match and dataset ids.`, true);
  }
  return CATALOG;
}

function extractParameterNames(variablesResponse, CATALOG, ididx) {

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

function extractDatasetAttributes(dataset) {
  
  cdfGAttributes = dataset['x_additionalMetadata']['CDF']['cdfGAttributes']

  for (let attribute of cdfGAttributes['attribute']) {
    if (attribute['name'] === 'TIME_RESOLUTION') {
      dataset['info']['cadence'] = attribute['entry'][0]['value'];
    }
    if (attribute['name'] === 'SPASE_DATASETRESOURCEID') {
      dataset['info']['resourceID'] = attribute['entry'][0]['value'];
    }
    if (attribute['name'] === 'GENERATION_DATE') {
      let creationDateo = attribute['entry'][0]['value'];
      creationDate = str2ISODateTime('GENERATION_DATE', creationDateo);
      if (creationDate) {
        dataset['info']['x_creationDate'] = creationDate;
      } else {
        warning(dataset['id'], "Could not parse GENERATION_DATE = " + creationDateo);
      }
    }
    if (attribute['name'] === 'ACKNOWLEDGEMENT') {
      dataset['info']['x_datasetCitation'] = catCharEntries(attribute['entry']);
    }
    if (attribute['name'] === 'RULES_OF_USE') {
      dataset['info']['x_datasetTermsOfUse'] = catCharEntries(attribute['entry']);
    }
  }

  function catCharEntries(entries) {
    let cat = "";
    for (let entry of entries) {
      cat = cat.trim() + " " + entry['value'];
    }
    return cat.slice(1);
  }
}

function extractParameterAttributes(dataset) {

  cdfVariables = dataset['x_variables'];
  let x_parameters = dataset['info']['x_parameters'];

  for (let [idx, variable] of Object.entries(cdfVariables['variable'])) {
    let vAttributesKept = extractKeepers(dataset['id'], variable['cdfVAttributes']['attribute']);

    if (!x_parameters[variable['name']]) {
      x_parameters[variable['name']] = {};
      // CATALOG[ididx]['x_parameters'] was initialized with all of the 
      // variables returned by /variables endpoint. This list does not 
      // include support variables. So we add them here.
      x_parameters[variable['name']]['name'] = variable['name'];
    }
    if (vAttributesKept['x_VAR_TYPE'] === "data") {
      x_parameters[variable['name']]['type'] = cdftype2hapitype(variable['cdfVarInfo']['cdfDatatype']);
    }
    x_parameters[variable['name']]['x_vAttributesKept'] = vAttributesKept;
    x_parameters[variable['name']]['x_variable'] = variable;
  }

  function cdftype2hapitype(cdftype) {
    if (['CDF_FLOAT', 'CDF_DOUBLE', 'CDF_REAL4', 'CDF_REAL8'].includes(cdftype)) {
      return "double";
    } else if (cdftype.startsWith('CDF_INT') || cdftype.startsWith('CDF_UINT') || cdftype.startsWith('CDF_BYTE')) {
      return "integer";
    } else if (cdftype .startsWith('CDF_EPOCH')) {
      return "integer";
    } else {
      error(dataset['id'], "Unhandled CDF datatype " + cdftype);
    }
  }
}

function extractKeepers(dsid, attributes) {

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
    if (attribute['name'] === 'CATDESC') {
      keptAttributes['description'] = attribute['entry'][0]['value'];
    }
    if (attribute['name'] === 'FIELDNAM') {
      keptAttributes['x_labelTitle'] = attribute['entry'][0]['value'];
    }
    if (attribute['name'] === 'DIM_SIZES') {
      let size = attribute['entry'][0]['value'];
      if (size !== "0")
        keptAttributes['size'] = [parseInt(size)];
    }
    if (attribute['name'] === 'DEPEND_0') {
      keptAttributes['x_DEPEND_0'] = attribute['entry'][0]['value'];
    }
    if (attribute['name'] === 'DEPEND_1') {
      keptAttributes['x_DEPEND_1'] = attribute['entry'][0]['value'];
    }
    if (attribute['name'] === 'DEPEND_2') {
      error(dsid, variable + " has a DEPEND_2", true);
    }
    if (attribute['name'] === 'LABL_PTR_1') {
      keptAttributes['x_LABL_PTR_1'] = attribute['entry'][0]['value'];
    }
    if (attribute['name'] === 'VAR_TYPE') {
      keptAttributes['x_VAR_TYPE'] = attribute['entry'][0]['value'];
    }
  }
  return keptAttributes;
}

function extractLabel(labelVariable) {

  let delimiter = labelVariable['cdfVarData']['record'][0]['elementDelimiter'];
  let re = new RegExp(delimiter,'g');
  let label = labelVariable['cdfVarData']['record'][0]['value'][0];
  return label
          .replace(re, "")
          .replace(/[^\S\r\n]/g,"")
          .trim()
          .split("\n");
}

function extractDepend1(dsid, depend1Variable) {

  let DEPEND_1_TYPE = depend1Variable['cdfVarInfo']['cdfDatatype'];
  if (!['CDF_CHAR', 'CDF_UCHAR'].includes(DEPEND_1_TYPE)) {
    // Return a bins object;
    let bins = {};
    let keptAttributes = extractKeepers(dsid, depend1Variable['cdfVAttributes']['attribute']);
    bins['centers'] = depend1Variable['cdfVarData']['record'][0]['value'][0].split(" ");
    if (['CDF_FLOAT', 'CDF_DOUBLE', 'CDF_REAL4', 'CDF_REAL8'].includes(DEPEND_1_TYPE)) {
      for (cidx in bins['centers']) {
        bins['centers'][cidx] = parseFloat(bins['centers'][cidx]);
      }
    } else if (DEPEND_1_TYPE.startsWith('CDF_INT') || DEPEND_1_TYPE.startsWith('CDF_UINT')) {
      for (cidx in bins['centers']) {
        bins['centers'][cidx] = parseInt(bins['centers'][cidx]);
      }
    } else {
      error(null, "Un-handled DEPEND_1 type: " + DEPEND_1_TYPE, true);
    }
    bins['name'] = keptAttributes['name'];
    bins['units'] = keptAttributes['units'];
    bins['description'] = keptAttributes['description'];
    return bins;
  }

  // Return an array of strings.
  let delimiter = depend1Variable['cdfVarData']['record'][0]['elementDelimiter'];
  let depend1 = depend1Variable['cdfVarData']['record'][0]['value'][0]
                  .replace( new RegExp(delimiter,'g'), "")
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
