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
                  'skip': '',
                  'cachedir': "cache/bwm",
                  'maxsockets': 3,
                  'all': 'all/all-bwm.json',
                  'allfull': 'all/all-bwm-full.json',
                  'allxml': 'https://spdf.gsfc.nasa.gov/pub/catalogs/all.xml',
                  'debug': false
                })
              .option('debug',{'type': 'boolean'})
              .argv;


// Async xml2js function (used once).
const xml2js = require('xml2js').parseString;
const spawnSync = require('child_process').spawnSync;
const execSync = require('child_process').execSync;
const exec = require('child_process').exec;

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
  if (!fs.existsSync(dir))
    fs.mkdirSync(dir, opts, {recursive: true});
}
function appendSync(fname, data) {
  fs.appendFileSync(fname, data, {flags: "a+"});
}
// End wrapped Node.js functions
/////////////////////////////////////////////////////////////////////////////

/////////////////////////////////////////////////////////////////////////////
// Begin time-related functions
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

if (!existsSync(argv.cachedir + "/0MASTERS")) {
  mkdirSync(argv.cachedir + "/0MASTERS", {recursive: true});  
}


main();


function main() {

  // Request all.xml to get dataset ids, starts, stops, and
  // master CDF URL. Put each as element of CATALOG array
  // and then call masters(CATALOG).

  let fnameAllJSON = argv.cachedir + "/all.xml.json";
  let fnameAllXML = argv.cachedir + "/all.xml";

  if (existsSync(fnameAllJSON)) {
    log("Reading: " + fnameAllJSON);
    let body = readSync(fnameAllJSON);
    finished(JSON.parse(body), true);
    return;
  }

  log("Requested: " + argv.allxml);
  get({uri: argv.allxml}, function (err,res,body) {

    if (err) {
      error(["Could not download " + argv.allxml + ". Error info: ", err], true);
    }
    log("Received: " + argv.allxml);

    log("Writing: " + fnameAllXML);
    writeSync(fnameAllXML, obj2json(body));

    xml2js(body, function (err, jsonObj) {
      if (err) {
        error(["Could not convert all.xml to JSON. Error info: ", err], true);
      }
      finished(jsonObj, false);
    });
  });

  function finished(body, fromCache) {

    if (fromCache == false) {
      log("Writing: " + fnameAllJSON);
      writeSync(fnameAllJSON, obj2json(body));
    }

    let CATALOG = extractAllXMLDatasetInfo(body);
    masters(CATALOG);
  }
}

function masters(CATALOG) {

  // For each element in CATALOG, get master CDF, convert to XML
  // and then convert XML to JSON. Place JSON in element CATALOG.
  //
  // When all master CDFs converted, HAPIInfo() to process
  // JSON for each CATALOG element and create new elements of CATALOG
  // if a dataset in CATALOG has more than one DEPEND_0. Final result
  // is CATALOG with elements of HAPI info responses for datasets
  // with only one DEPEND_0.

  for (let ididx = 0; ididx < CATALOG.length; ididx++) {
    requestAndParseMaster(ididx);
  }

  function requestAndParseMaster(ididx) {

    let id = CATALOG[ididx]['id'];
    //log(CATALOG[ididx]['id']);

    let mode = 'cdfxdf'; 

    let fname = CATALOG[ididx]['_mastercdf'].split("/").slice(-1);
    fname = argv.cachedir + "/0MASTERS/" + fname[0].slice(0,-4);

    let fnameMasterCDF  = fname + ".cdf";
    let fnameMasterJSON = fname + "-xml2js.json";
    let fnameMasterXML  = fname + "-" + mode + ".xml";

    if (existsSync(fnameMasterJSON)) {
      log(id + " | Reading: " + fnameMasterJSON);
      let jsonObj = readSync(fnameMasterJSON, 'utf-8');
      jsonObj = JSON.parse(jsonObj);
      finished(ididx, fnameMasterJSON, jsonObj, true);
    } else if (existsSync(fnameMasterCDF)) {
      cdf2json(finished);
    } else {
      let url = CATALOG[ididx]['_mastercdf'];
      let reqOpts = {uri: url, encoding: null};
      // TODO: Use .pipe to stream directly to file.
      log(id + " | Requested: " + url);
      get(reqOpts, function (err,res,body) {
        if (err) {
          error(CATALOG[ididx]['id'], url + " failed.", false);
          finished(ididx, fnameMasterJSON, jsonObj, false);
          return;
        }
        log(id + " | Received: " + url);
        writeSync(fnameMasterCDF, body);
        cdf2json(finished);
      });
    }

    function cdf2json(finished) {

      let cdfxml;
      let cmd = "java CDF2CDFML -withZ -mode:" + mode + " -output:STDOUT " + fnameMasterCDF;

      try {
        log(id + " | Executing: " + cmd);
        cdfxml = execSync(cmd, {maxBuffer: 8*1024*1024});
        log(id + " | Executed:  " + cmd);
        log(id + " | Writing: " + fnameMasterXML)
        writeSync(fnameMasterXML, cdfxml.toString());
      } catch (err) {
        error(CATALOG[ididx]['id'], cmd + " failed.", false);
        finished(ididx, fnameMasterJSON, null, false);
      }

      log(id + " | Converting " + fnameMasterXML + " to JSON.");
      xml2js(cdfxml, function (err, jsonObj) {
        if (err) {
          error(CATALOG[ididx]['id'],
                [id + "Conversion of XML to JSON failed. Error: ", err], false);
        } else {
          finished(ididx, fnameMasterJSON, jsonObj, false);
        }
      });
    }

  }

  function finished(ididx, fnameMasterJSON, jsonObj, fromCache) {

    if (finished.N == undefined) {
      finished.N = 0;
    }
    finished.N = finished.N + 1;

    if (fromCache == false) {
      log(CATALOG[ididx]['id'] + " | Writing: " + fnameMasterJSON);
      writeSync(fnameMasterJSON, obj2json(jsonObj));
    }

    if (jsonObj !== null) {
      CATALOG[ididx]['x_additionalMetadata'] = {};
      CATALOG[ididx]['x_additionalMetadata']['CDF'] = jsonObj['CDF'];
    }

    if (finished.N == CATALOG.length) {
      // Remove nulled elements.
      CATALOG = CATALOG.filter(function (el) {return el != null;});
      log("-".repeat(60));
      log(CATALOG.length + " master CDF files ready for creation of HAPI info responses");
      log("-".repeat(60));
      //HAPIInfo(CATALOG);
    }

  }
}

function HAPIInfo(CATALOG) {

  // Subset datasets that have mutliple DEPEND_0s
  let CATALOGexpanded = JSON.parse(JSON.stringify(CATALOG));
  for (let [dsidx, dataset] of Object.entries(CATALOGexpanded)) {

    if (argv.debug) {
      console.log(CATALOG[dsidx]['id'] + " before extracting parameter attributes.");
      console.log(dataset);
    }

    extractParameterAttributes(dataset);
    if (argv.debug) {
      console.log(CATALOG[dsidx]['id'] + " after extracting parameter attributes.");
      console.log(dataset);
    }

    extractDatasetAttributes(dataset);
    if (argv.debug) {
      console.log(CATALOG[dsidx]['id'] + " after extracting dataset attributes.");
      console.log(dataset);
    }

    // Subset if needed.
    let subdatasets = subsetDataset(dataset);
    if (subdatasets !== undefined) {
      log("  Note: " + subdatasets.length + " sub-datasets");
      CATALOGexpanded.splice(dsidx, 1, ...subdatasets);
    }
  }

  CATALOG = null
  CATALOG = CATALOGexpanded;


  // Remove nulled elements due to processing error;
  CATALOG = CATALOG.filter(function (el) {return el != null;});

  for (let dataset of CATALOG) {

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

    // Array of parameter objects
    let parameters = dataset['info']['parameters'];

    // Temporary object with keys of parameter names
    let parametersObj = {};
    for (let parameter of parameters) {
      let pname = parameter['name'];
      parametersObj[pname] = parameter;
    }

    pidx = 0; // Index of a kept data parameter
    parameters = [];
    for (pname of Object.keys(parametersObj)) {

      parameter = parametersObj[pname];

      let x_attributes = parameter['x_attributes'];

      if (x_attributes['DEPEND_2'] === null) {
        error(dataset['id'], pname + " has an un-handled DEPEND_2. Omitting dataset.", false);
        dataset = null;
        break;
      }

      if (dataset === null) {continue;}

      if (!parameter['units']) {
        warning(dataset['id'], "No units for " + pname);
        parameter['units'] = null;
      }

      if (parameter['bins'] && !parameter['bins']) {
        warning(dataset['id'], "No bin units for " + pname);
        parameter['bins']['units'] = null;
      }

      let DEPEND_0 = x_attributes['DEPEND_0'];
      if (DEPEND_0 && !DEPEND_0.toLowerCase().startsWith('epoch')) {
        warning(dataset['id'], `${pname} has DEPEND_0 name of '${DEPEND_0}'; expected 'Epoch'`);
      }

      // Extract DEPEND_1
      let vectorComponents = false;
      if (x_attributes['DEPEND_1']) {
        let DEPEND_1 = x_attributes['DEPEND_1'];
        //console.log(parametersObj[DEPEND_1]['x_variable'])
        let depend1 = extractDepend1(dataset['id'], parametersObj[DEPEND_1]['x_variable']);
        if (Array.isArray(depend1)) {
          vectorComponents = extractVectorComponents(depend1)
          if (vectorComponents) {
            parameter['x_vectorComponents'] = ['x', 'y', 'z'];
          } else {
            warning(dataset['id'], "Un-handled DEPEND_1 vector components for " + pname + ": " + depend1.join(", "));
          }
        } else {
          parameter['bins'] = depend1;
        }
      }

      // Extract labels
      if (x_attributes['LABL_PTR_1']) {
        let LABL_PTR_1 = x_attributes['LABL_PTR_1'];
        let label = extractLabel(parametersObj[LABL_PTR_1]['x_variable']);
        parameter['label'] = label;
      }

      if (vectorComponents) {
        let coordinateSystemName = extractCoordinateSystemName(parameter);
        if (coordinateSystemName) {
          parameter['x_coordinateSystemName'] = coordinateSystemName;
        }
      }

      if (parameter['x_attributes']['VAR_TYPE'] === 'data') {
        // Keep data parameter
        parameters[pidx] = parametersObj[pname];
        delete parameters[pidx]['x_attributes']
        delete parameters[pidx]['x_variable']
        pidx = pidx + 1;
      }
    }

    delete dataset['x_additionalMetadata']

    // Prepend Time parameter
    parameters.unshift(
                {
                  "name": "Time",
                  "type": "isotime",
                  "units": "UTC",
                  "length": "",
                  "fill": ""
                });


    dataset['info'] = parameters;

  }

  //console.log(CATALOG)
  // Remove nulled elements.
  CATALOG = CATALOG.filter(function (el) {return el != null;});

  // Write one info file per dataset
  let allIds = [];
  for (let dataset of CATALOG) {
    allIds.push(dataset['id']);
    //console.log(dataset)
    let fnameInfo = argv.cachedir + '/' + dataset['id'].split("_")[0] + '/' + dataset['id'] + '.json';

    writeAsync(fnameInfo, obj2json(dataset));
    if (CATALOG.length == 1) {
      log(`  Wrote: ${fnameInfo}`);
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

function extractAllXMLDatasetInfo(allJSONResponse) {

  let CATALOG = [];
  let allIds = [];
  let datasets = allJSONResponse['sites']['datasite'][0]['dataset'];

  for (let dataset of datasets) {

    let id = dataset['$']['serviceprovider_ID'];
    let mastercdf = dataset['mastercdf'][0]['$']['serviceprovider_ID'];

    let re = new RegExp(argv.idregex);
    if (re.test(id) == false) {
      continue;
    }

    if (argv.skip !== '') {
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
    }

    let startDate = dataset['$']['timerange_start'].replace(" ","T") + "Z";
    let stopDate = dataset['$']['timerange_stop'].replace(" ","T") + "Z";
    let contact = dataset['data_producer'][0]['$']['name'].trim() + ' @ ' + dataset['data_producer'][0]['$']['affiliation'].trim();
    CATALOG.push({
      "id": id,
      "_mastercdf": mastercdf,
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

function subsetDataset(dataset) {

  // Look for parameters that have differing DEPEND_0s.
  let parameters = dataset['info']['parameters'];
  let DEPEND_0s = {};
  for (parameter of Object.keys(parameters)) {
    let DEPEND_0 = parameters[parameter]['x_attributes']['DEPEND_0'];
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
    for (parameter of Object.keys(newdataset['info']['parameters'])) {
      let depend_0 = parameters[parameter]['x_attributes']['DEPEND_0'];
      if (depend_0 !== DEPEND_0) {
        delete parameter;
      }
    }
    datasets.push(newdataset)
  }
  return datasets;
}

function extractDatasetAttributes(dataset) {
  
  cdfGAttributes = dataset['x_additionalMetadata']['CDF']['cdfGAttributes'][0]['parameter'];
  if (argv.debug) {
    console.log('cdfGAttributes:');
  }

  for (let attribute of cdfGAttributes) {
    if (argv.debug) {
      console.log(attribute);
    }
    let value = attribute['value'][0]['_'];
    if (attribute['$']['name'] === 'Time_resolution') {
      dataset['info']['cadence'] = value;
    }
    if (attribute['name'] === 'spase_DatasetResourceID') {
      dataset['info']['resourceID'] = value;
    }
    if (attribute['name'] === 'Generation_date') {
      let creationDateo = value;
      creationDate = str2ISODateTime('GENERATION_DATE', creationDateo);
      if (creationDate) {
        dataset['info']['x_creationDate'] = creationDate;
      } else {
        warning(dataset['id'], "Could not parse Generation_date = " + creationDateo);
      }
    }
    if (attribute['name'] === 'Acknowledgement') {
      dataset['info']['x_datasetCitation'] = catCharEntries(value);
    }
    if (attribute['name'] === 'Rules_of_use') {
      dataset['info']['x_datasetTermsOfUse'] = catCharEntries(value);
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

  cdfVariables = dataset['x_additionalMetadata']['CDF']['cdfVariables'][0]['variable'];

  let parameters = [];
  for (let [idx, variable] of Object.entries(cdfVariables)) {
    let name = variable['$']['name'];
    parameter = {"name": name, 'x_variable': variable};
    extractVariableAttributes(dataset['id'], variable, parameter);
    parameters.push(parameter);
  }
  dataset['info']['parameters'] = parameters;
}

function extractVariableAttributes(dsid, variable, parameter) {

  //console.log(variable)
  let dimSizes = variable['cdfVarInfo'][0]['$']['dimSizes'];
  let padValue = variable['cdfVarInfo'][0]['$']['padValue'];
  let cdfDatatype = variable['cdfVarInfo'][0]['$']['cdfDatatype'];

  parameter['_pad'] = padValue;
  if (dimSizes && dimSizes !== "0") {
    parameter['size'] = [parseInt(dimSizes)];
  }

  let vattributes = variable['cdfVAttributes'][0]['parameter'];

  if (argv.debug) {
    console.log("variable");
    console.log(variable)
    console.log("variable attributes");
    console.log(vattributes)
  }

  // Map CDF variable attributes to HAPI parameter attributes
  attributeMap = {
    'LABLAXIS': '_label',
    'FILLVAL': 'fill',
    'MISSING_VALUE': 'pad',
    'UNITS': 'units',  
    'CATDESC': 'description',
    'FIELDNAM': '_labelTitle',
    'DIM_SIZES': 'size'
  }

  let x_attributes = {};
  for (let vattribute of vattributes) {
    let vname = vattribute['$']['name'];
    let vvalue = vattribute['value'][0]['_'];
    let pname = attributeMap[vname];

    x_attributes[vname] = vvalue;

    if (pname) { parameter[pname] = vvalue; }

    if (x_attributes['VAR_TYPE'] === 'data') {
      parameter['type'] = cdftype2hapitype(cdfDatatype);
    }

    parameter['x_attributes'] = x_attributes;
  }

  function cdftype2hapitype(cdftype) {
    if (['CDF_FLOAT', 'CDF_DOUBLE', 'CDF_REAL4', 'CDF_REAL8'].includes(cdftype)) {
      return "double";
    } else if (cdftype.startsWith('CDF_INT') || cdftype.startsWith('CDF_UINT') || cdftype.startsWith('CDF_BYTE')) {
      return "integer";
    } else if (cdftype .startsWith('CDF_EPOCH')) {
      return "integer";
    } else {
      error(dataset['id'], "Un-handled CDF datatype " + cdftype);
    }
  }
}

function extractLabel(labelVariable) {

  //let delimiter = labelVariable['cdfVarData']['record'][0]['elementDelimiter'];
  //let re = new RegExp(delimiter,'g');
  //        .replace(re, "")
  let label = labelVariable['cdfVarData'][0]['array'][0]['data'][0]
  return label
          .replace(/[^\S\r\n]/g,"")
          .trim()
          .split("\n");
}

function extractDepend1(dsid, depend1Variable) {

  //console.log(depend1Variable['cdfVarInfo'][0]['$'])
  let DEPEND_1_TYPE = depend1Variable['cdfVarInfo'][0]['$']['cdfDatatype'];
  //console.log(DEPEND_1_TYPE)
  if (!['CDF_CHAR', 'CDF_UCHAR'].includes(DEPEND_1_TYPE)) {
    // Return a bins object;
    let bins = {};
    //let keptAttributes = extractVariableAttributes(dsid, depend1Variable);
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
      error(null, "Un-handled DEPEND_1 type for bins variable " + depend1Variable['name'] + DEPEND_1_TYPE, true);
    }
    bins['name'] = keptAttributes['name'];
    bins['units'] = keptAttributes['units'];
    bins['description'] = keptAttributes['description'];
    return bins;
  }

  // Return an array of strings.
  //let delimiter = depend1Variable['cdfVarData']['record'][0]['elementDelimiter'];
  //                .replace( new RegExp(delimiter,'g'), "")
  let depend1 = depend1Variable['cdfVarData'][0]['array'][0]['data'][0]
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
