// Create a HAPI all.json catalog and one info file per datasets based on
//   https://spdf.gsfc.nasa.gov/pub/catalogs/all.xml
// and queries to
//   https://cdaweb.gsfc.nasa.gov/WS/cdasr
// CDASR documentation:
//   https://cdaweb.gsfc.nasa.gov/WebServices/REST/

const argv    = require('./lib/cli.js').argv();
const types   = require('./lib/types.js');
const cadence = require('./lib/cadence.js');
const spase   = require('./lib/spase.js');
const {util}  = require('./lib/util.js');
const {meta}  = require('./lib/meta.js');
const { buildMasterTable } = require('./lib/table.js');

// meta.run() gets all needed metadata files.
// buildHAPI() is called when complete.

//meta.run();
//meta.run(buildHAPI);
buildHAPI();

// To only get metadata files:
//meta.run();

// To only build HAPI metadata based on cached metadata files:
//buildHAPI();

//////////////////////////////////////////////////////////////////////////////

function buildHAPI(CATALOG) {
  // If called using buildHAPI(), get datasets by reading *-combined.json files
  // in cachedir. Otherwise, use datasets in CATALOG when called using
  // buildHAPI(CATALOG).
  let datasets = CATALOG ? CATALOG['datasets'] : readDatasets();
  buildHAPIFiles(datasets);
  buildHAPIInfos(datasets);
}

function buildHAPIInfos(datasets) {

  let msg = `\n*Processing ${datasets.length} dataset${util.plural(datasets)}*\n`;
  util.log(null, msg);

  for (let dsidx in datasets) {
    createParameters(dsidx, datasets);
  }
  datasets = datasets.filter(item => item !== null);

  if (datasets.length == 0) {
    util.log(null, '\n\nNo datasets to process.');
    dropDataset(); // Write file
    console.log("Exiting.");
    process.exit(0);
  }

  util.debug(null, 'Looking for datasets with more than one DEPEND_0.');

  datasets = subsetDatasets(datasets);

  //buildMasterTable(datasets, argv['cachedir'] + '/masters');
  //process.exit(0);

  let catalog = [];
  for (let dsidx in datasets) {

    let dataset = datasets[dsidx];
    let dsid = dataset['id'];
    util.log(dsid, dsid, "", 'blue');

    if (dataset['_data'] === null) {
      let emsg = 'No data sample obtained';
      dropDataset(dsidx, dataset, emsg, "CDAWeb");
      continue;
    }

    if (!dataset['_variables']) {
      dropDataset(dsidx, dataset, 'No _variables.', "CDAWeb");
      continue;
    }

    let _allxml = dataset["_allxml"];
    dataset['title'] = _allxml['description'][0]['$']['short'];
    let producer = _allxml['data_producer'][0]['$']
    let contact = producer['name'].trim() + ' @ ' + producer['affiliation'].trim();
    dataset['info']['contact'] = contact;
    let rURL = 'https://cdaweb.gsfc.nasa.gov/misc/Notes.html#' + dataset["id"];
    dataset['info']['resourceURL'] = rURL;

    if (!dataset['_spase'] && !dataset['_spaseError']) {
      util.error(dsid, "No SPASE", false, 'spase');
    }

    if (dataset['_spaseError'] && dataset['_spaseError'].length > 0) {
      let base = dsid.split("@");
      if (base.length == 1 || base[1] == "0") {
        util.error(dsid, dataset['_spaseError'], false, 'spase');
      }
    }

    if (/\s\s.*|\( | \)/.test(dataset['title'])) {
      // TODO: Get from SPASE, if available.
      let msg = 'Check title formatting for extra spaces: '
      msg += `'${dataset['title']}'`
      util.warning(dsid, msg);
    }

    let _data = JSON.parse(util.readSync(dataset['_data']))['CDF'][0];
    let cdfVariables = _data['cdfVariables']['variable'];

    extractDatasetAttributes(dataset, _data);
    cadence.extractCadence(dataset, _data);

    if (dataset['_spase']) {
      if (dataset['_spase']['NumericalData']) {
        if (!dataset['_spase']['NumericalData'][0]['Parameter']) {
          util.warning(dataset['id'], `(SPASE) No Parameter node`, false);
        }
      } else {
        util.warning(dataset['id'], `(SPASE) No NumericalData node`, false);
      }
    }

    let DEPEND_0s = [];
    let parameterArray = [];
    let parameters = dataset['info']['parameters'];
    for (let name of Object.keys(parameters)) {

      let parameter = parameters[name];
      let varType = parameter['_vAttributesKept']['_VAR_TYPE'];

      let err = checkDeltaVar(dsidx, datasets, parameter, parameters);
      if (err !== null) {
        dropDataset(dsidx, datasets, err.message, err.cause);
        break;
      }


      if (parameter['_vAttributesKept']['_DEPEND_2'] === null) {
        let msg = `Parameter '${name}' has an un-handled DEPEND_2.`;
        //dropDataset(dsidx, datasets, msg, "NotImplemented");
        //break;
      }

      // Move kept vAttributes up
      for (let key of Object.keys(parameter['_vAttributesKept'])) {
        if (!key.startsWith('_'))
          parameter[key] = parameter['_vAttributesKept'][key];
      }

      if (!parameter['fill'] && varType == 'data') {
        let msg = 'No fill for data parameter ' + parameter['name'];
        util.warning(dataset['id'], msg);
      }

      if (parameter['fill'] && varType == 'data' && parameter['fill'].toLowerCase() == "nan") {
        // Found in THA_L2_MOM
        //util.warning(dataset['id'], 'FILLVAL cast to lower case is nan 
        //for ' + parameter['name'] + '. Setting to -1e31');
        //parameter['fill'] = "-1e31";
      }
      
      if (!parameter['units'] && varType == 'data') {
        util.warning(dataset['id'], '(CDAWeb) No units in for ' + parameter['name']);

        let spaseUnits = spase.extractSPASEUnits(dataset['_spase'], parameter['name']);
        if (!spaseUnits) {
          util.warning(dataset['id'], '(SPASE) No units for ' + parameter['name']);
        }
        parameter['units'] = null;
      }

      // Extract DEPEND_1
      if (parameter['_vAttributesKept']['_DEPEND_1']) {

        let DEPEND_1 = parameter['_vAttributesKept']['_DEPEND_1'];
        let vidx = parameters[DEPEND_1]['_variableIndex']

        if (cdfVariables[vidx]['cdfVarInfo']['recVariance'] === 'VARY') {
          let msg = `Parameter '${name}' has an un-handled DEPEND_1 with `
          msg += `recVariance 'VARY'.`;
          dropDataset(dsidx, datasets, msg, "NotImplemented");
          break;
        }
        let DEPEND_1_TYPE = cdfVariables[vidx]['cdfVarInfo']['cdfDatatype'];
        if (!types.handledType(DEPEND_1_TYPE)) {
          let msg = `Parameter '${name}' has an DEPEND_1 with cdfDatatype `;
          msg += `'${DEPEND_1_TYPE}', which is not handled.`;
          dropDataset(dsidx, datasets, msg, "NotImplemented");
          break;
        }

        let depend1 = extractDepend1(dataset['id'], cdfVariables[vidx]);
        if (Array.isArray(depend1)) {
          extractCoordSysNameAndVecComps(dataset['id'], parameter, depend1);
        } else {
          parameter['bins'] = [depend1];
        }
      }

      if (parameter['bins'] && !parameter['bins'][0]['units']) {
        let msg = `No bin units for bin parameter `
        msg += `'${parameter['bins'][0]["name"]}' of '${parameter['name']}'`
        util.warning(dataset['id'], msg);
        parameter['bins']['units'] = null;
      }

      // Extract labels
      if (parameter['_vAttributesKept']['_LABL_PTR_1']) {
        let LABL_PTR_1 = parameter['_vAttributesKept']['_LABL_PTR_1'];
        let vidx = parameters[LABL_PTR_1]['_variableIndex']        
        let label = extractLabel(cdfVariables[vidx]);
        //let label = extractLabel(parameters[LABL_PTR_1]['_variable']);
        parameter['label'] = label;
      }

      if (varType === 'data') {    
        let DEPEND_0 = parameter['_vAttributesKept']['_DEPEND_0'];
        if (name !== DEPEND_0 && !parameter['_vAttributesKept']['_DEPEND_0']) {
          let msg = `No DEPEND_0 for variable '${name}'`;
          dropDataset(dsidx, datasets, msg, "CDAWeb");
          break;
        }
    
        let vidx = parameters[DEPEND_0]['_variableIndex']
        let cdfDatatype = cdfVariables[vidx]['cdfVarInfo']['cdfDatatype'];
        if (!types.timeType(cdfDatatype)) {
          let msg = `DEPEND_0 variable '${name}' has type '${cdfDatatype}', `
          msg += `which is a CDF_EPOCH or CDF_TIME type.`;
          dropDataset(dsidx, datasets, msg, "CDAWeb");
          break;
        }

        DEPEND_0s.push(DEPEND_0);  
        parameterArray.push(util.copy(parameter));
      }
    }

    if (datasets[dsidx] === null) continue;

    let EpochName = DEPEND_0s[0]; // Should be all same here, so take first.
    let vidx = parameters[EpochName]['_variableIndex'];
    let firstTimeValue = extractRecords(cdfVariables[vidx]['cdfVarData']['record'])[0];

    parameterArray
          .unshift({
                      name: 'Time',
                      type: 'isotime',
                      units: 'UTC',
                      length: firstTimeValue.length,
                      fill: null,
                  });

    dataset['info']['parameters'] = parameterArray;

    dataset['_infoFile'] = argv.infodir + '/CDAWeb/info/' + dsid + '.json';

    writeInfoFile(dataset);
    deleteUnderscoreKeys(dataset);
    catalog.push({'id': dataset['id'], 'title': dataset['title']});
  }

  console.log("");

  dropDataset(); // Write file

  // Write HAPI catalog.json containing /catalog response JSON
  let fnameCatalog = argv.infodir + '/CDAWeb/catalog.json';
  util.writeSync(fnameCatalog, util.obj2json(catalog));
  util.note(null, 'Wrote ' + fnameCatalog);

  // Write HAPI all.json containing all content from all info files.
  let fnameAll = argv.infodir + '/CDAWeb/all.json';
  util.writeSync(fnameAll, util.obj2json(datasets));
  util.note(null, 'Wrote ' + fnameAll);

  util.note(null, "Console messages written to " + util.log.logFileName);
}

function buildHAPIFiles(datasets) {

  let all = [];
  let catalog = [];

  inventory();
  files1();

  // Sort by part of ID before slash
  all = all.sort(
        function (a, b) {
            a = a.id.split("/")[0];
            b = b.id.split("/")[0];
            if (a > b) {return 1;} 
            if (a < b) {return -1;}
            return 0;
        });

  let fnameAll = argv.infodir + '/CDAWeb-files/all.json';
  util.writeSync(fnameAll, util.obj2json(all));
  let fnameCat = argv.infodir + '/CDAWeb-files/catalog.json';
  util.writeSync(fnameCat, util.obj2json(catalog));

  function inventory() {

    if (argv['omit'].includes('inventory')) {
      return;
    }

    let msg = '\n*Creating HAPI catalog and info responses for CDAWeb inventory datasets.*\n';
    util.log(null, msg);

    for (let dataset of datasets) {
      let id = dataset['id'];

      util.log(id, id, "", 'blue');

      let fileList = util.baseDir(id) + '/' + id + '-inventory.json';
      let inventory = JSON.parse(util.readSync(fileList))["InventoryDescription"];
      let intervals = inventory[0]["TimeInterval"];
      if (intervals === undefined) {
        util.warning(id, 'No intervals');
        continue;
      }

      let data = [];
      let startLen = 0;
      let endLen = 0;
      for (let interval of intervals) {
        data.push([interval['Start'],interval['End']]);
        startLen = Math.max(startLen,interval['Start'].length);
        endLen = Math.max(endLen,interval['End'].length);
      }
      let startDate = data[0][0];
      let stopDate = data[data.length-1][1];

      if (data.length == 0) {
        util.warning(id, 'No intervals');
        continue;
      } else {
        util.note(id, `${data.length} interval${util.plural(data)}`);
      }

      let info = {
                    "startDate": startDate,
                    "stopDate" : stopDate,
                    "timeStampLocation": "begin",
                    "parameters":
                      [
                        { 
                          "name": "Time",
                          "type": "isotime",
                          "units": "UTC",
                          "fill": null,
                          "length": startLen
                        },
                        { 
                          "name": "End",
                          "type": "isotime",
                          "units": "UTC",
                          "fill": null,
                          "length": endLen
                        }
                      ]
      }

      let fnameData = argv.infodir + '/CDAWeb-files/data/' + id + '/inventory.csv';
      util.writeSync(fnameData, data.join("\n"));
      util.note(id,'Wrote ' + fnameData);

      let fnameInfo = argv.infodir + '/CDAWeb-files/info/' + id + '/inventory.json';
      util.writeSync(fnameInfo, util.obj2json(info));
      util.note(id,'Wrote ' + fnameInfo);

      let title = "Time intervals of data availability from /inventory endpoint";
      title += " at https://cdaweb.gsfc.nasa.gov/WebServices/REST/";
      let ida = id + "/inventory";
      all.push({"id": ida, "title": title, "info": info});
      catalog.push({"id": ida, "title": title});
    }
  }

  function files1() {

    if (argv['omit'].includes('files1')) {
      return;
    }

    let msg = '\n*Creating HAPI catalog and info responses for CDAWeb ';
    msg += 'files datasets (version 1 method).*\n';
    util.log(null, msg);

    let sum = 0;
    for (let dataset of datasets) {
      let id = dataset['id'];
      util.log(id, id, "", 'blue');

      let fileList = dataset['_files1'];
      if (!fileList) {
        util.warning(id, 'No _files1');
        continue;
      }
      let files = JSON.parse(util.readSync(fileList))["FileDescription"];
      if (!files) {
        util.warning(id, 'No FileDescription in ' + fileList);
        continue;
      }

      let data = [];
      let startLen = 0;
      let endLen = 0;
      let urlLen = 0;
      let lastLen = 0;

      dataset['_files1Size'] = 0;
      for (let file of files) {
        dataset['_files1Size'] += file['Length'];
        data.push([
            file['StartTime'],
            file['EndTime'],
            file['Name'],
            file['LastModified'],
            file['Length']
        ]);
        startLen = Math.max(startLen,file['StartTime'].length);
        endLen = Math.max(endLen,file['EndTime'].length);
        urlLen = Math.max(urlLen,file['Name'].length);
        lastLen = Math.max(urlLen,file['LastModified'].length);
      }
      sum += dataset['_files1Size'];

      let startDate = data[0][0];
      let stopDate = data[data.length-1][1];
    
      if (data.length == 0) {
        util.warning(id, 'No files');
        continue;
      } else {
        let msg = `${files.length} file${util.plural(data)}; `
        msg += `${util.sizeOf(dataset['_files1Size'])}`;
        util.note(id, msg);
      }

      let info = {
                    "startDate": startDate,
                    "stopDate" : stopDate,
                    "timeStampLocation": "begin",
                    "parameters":
                      [
                        { 
                          "name": "Time",
                          "type": "isotime",
                          "units": "UTC",
                          "fill": null,
                          "length": startLen
                        },
                        { 
                          "name": "EndTime",
                          "type": "isotime",
                          "units": "UTC",
                          "fill": null,
                          "length": endLen
                        },
                        { 
                          "name": "URL",
                          "type": "string",
                          "x_stringType": {
                            "uri": {
                              "scheme": "https",
                              "mediaType": "application/x-cdf"
                            }
                          },
                          "units": null,
                          "fill": null,
                          "length": urlLen
                        },
                        { 
                          "name": "LastModified",
                          "type": "isotime",
                          "units": "UTC",
                          "fill": null,
                          "length": lastLen
                        },
                        { 
                          "name": "Length",
                          "type": "integer",
                          "fill": null,
                          "units": "bytes"
                        }
                      ]
                  }

      let fnameData = argv.infodir + '/CDAWeb-files/data/' + id + '/files.csv';
      util.writeSync(fnameData, data.join("\n"));
      util.note(id,'Wrote ' + fnameData);

      let fnameInfo = argv.infodir + '/CDAWeb-files/info/' + id + '/files.json';
      util.writeSync(fnameInfo, util.obj2json(info));
      util.note(id,'Wrote ' + fnameInfo);

      let title = "List of files obtained from /orig_data of ";
      title += "https://cdaweb.gsfc.nasa.gov/WebServices/REST/";
      let ida = id + "/files";
      all.push({"id": ida, "title": title, "info": info});
      catalog.push({"id": ida, "title": title});

    }
    console.log(`\n\nTotal size of files for all datasets: ${util.sizeOf(sum)}`);
  }
}

function writeInfoFile(dataset) {

  let id = dataset['id'];

  // Re-order keys
  let order = {"id": null, "title": null, "info": null};
  dataset = Object.assign(order, dataset);

  order = {
            "startDate": null,
            "stopDate": null,
            "sampleStartDate": null,
            "sampleStopDate": null,
            "x_numberOfSampleRecords": null,
            "cadence": null,
            "contact": null,
            "resourceURL": null,
            "resourceID": null,
            "x_datasetCitation": null,
            "x_datasetTermsOfUse": null,
            "parameters": null
          }
  dataset['info'] = Object.assign(order, dataset['info']);

  for (let key of Object.keys(dataset['info'])) {
    if (dataset['info'][key] === null) {
      delete dataset['info'][key];
    } 
  }

  let fnameInfoFull = util.baseDir(id) + '/' + id + '-info-full.json';
  util.writeSync(fnameInfoFull, util.obj2json(dataset));
  util.note(id, 'Wrote ' + fnameInfoFull);

  let fnameInfo = dataset['_infoFile'];  
  deleteUnderscoreKeys(dataset);
  util.writeSync(fnameInfo, util.obj2json(dataset['info']));
  util.note(id, 'Wrote ' + fnameInfo);

  let fnameInfo2 = util.baseDir(id) + '/' + id + '-info.json';
  util.writeSync(fnameInfo2, util.obj2json(dataset['info']));
}

function readDatasets() {

  let datasets = [];
  const globSync = require('glob').globSync;
  const globStr = argv['cachedir'] + '/**/*-combined.json';
  util.log(null, `\n*Reading cached metadata in -combined.json files.*\n`);
  util.log(null, `  Finding files that match ${globStr}`);
  const combinedFiles = globSync(globStr).sort();
  util.log(null, `  ${combinedFiles.length} cached files match ${globStr}`);
  for (let file of combinedFiles) {
    let id = file.split('/').slice(-1)[0].split("-combined")[0];
    if (!argv['debug']) {
      process.stdout.write("\r" + " ".repeat(80) + "\r");
      process.stdout.write(id);
    }
    if (util.idFilter(id, argv['keepids'], argv['omitids'])) {
      datasets.push(JSON.parse(util.readSync(file)));
    }
  }
  if (!argv['debug']) {
    process.stdout.write("\r" + " ".repeat(80) + "\r");
  }
  let plural = util.plural(datasets);
  let msg = `  ${datasets.length} file${plural} after keepids and omitids filter`;
  util.log(null, msg);

  return datasets;
}

function deleteUnderscoreKeys(dataset) {

    for (let parameter of dataset['info']['parameters']) {
      let keys = Object.keys(parameter);
      for (let key of keys) {
        if (key.startsWith("_")) {
          delete parameter[key];
        }
      }
    }

    let keys = Object.keys(dataset);
    for (let key of keys) {
      if (key.startsWith("_")) {
        delete dataset[key];
      }
    }
}

function subsetDatasets(datasets) {

  let datasetsExpanded = [];

  for (let dataset of datasets) {

    let subdatasets = subsetDataset(dataset);
    if (subdatasets.length > 1) {
      util.log(dataset['id'], dataset['id'], "", 'blue');
      util.note(dataset['id'], subdatasets.length + ' sub-datasets');
    }

    for (let d of subdatasets) {
      datasetsExpanded.push(d);
    }
  }
  datasets = null;

  return datasetsExpanded;

  function subsetDataset(dataset) {

    if (dataset['_data'] === null) {
      return [dataset];
    }

    // Find datasets that have more than one DEPEND_0. Split associated
    // dataset into datasets that has parameters with the same DEPEND_0.
    let parameters = dataset['info']['parameters'];
    let DEPEND_0s = {};
    for (let parameter of Object.keys(parameters)) {
      if (parameters[parameter]['_vAttributesKept']['_VAR_TYPE'] !== 'data') {
        continue;
      }
      let DEPEND_0 = parameters[parameter]['_vAttributesKept']['_DEPEND_0'];
      if (DEPEND_0 !== undefined) {
        DEPEND_0s[DEPEND_0] = DEPEND_0;
      }
    }
    DEPEND_0s = Object.keys(DEPEND_0s);
    if (DEPEND_0s.length == 1) {
      dataset['info']['x_DEPEND_0'] = DEPEND_0s[0];
      return [dataset];
    }

    let datasets = [];
    for (let [sdsidx, DEPEND_0] of Object.entries(DEPEND_0s)) {
      //console.log(DEPEND_0)
      let newdataset = util.copy(dataset);
      newdataset['id'] = newdataset['id'] + '@' + sdsidx;
      newdataset['info']['x_DEPEND_0'] = DEPEND_0;
      for (let parameter of Object.keys(newdataset['info']['parameters'])) {
        let _VAR_TYPE = parameters[parameter]['_vAttributesKept']['_VAR_TYPE'];
        // Non-data parameters are kept even if they are not referenced by
        // variables in the new dataset. This is acceptable because
        // all non-data parameters are deleted after needed information
        // from them is extracted.
        let depend_0 = parameters[parameter]['_vAttributesKept']['_DEPEND_0'];
        //console.log(" " + parameter + " " + depend_0)
        if (depend_0 !== DEPEND_0 && _VAR_TYPE === "data") {
          //console.log(" Deleting " + parameter)
          delete newdataset['info']['parameters'][parameter];
        }
      }
      datasets.push(newdataset);
    }
    return datasets;
  }
}

function checkDeltaVar(dsidx, datasets, parameter, parameters, direction) {

  if (direction === undefined) {
    let msgPlus = checkDeltaVar(dsidx, datasets, parameter, parameters, "PLUS");
    if (msgPlus) {
      return msgPlus;
    }
    let msgMinus = checkDeltaVar(dsidx, datasets, parameter, parameters, "MINUS");
    if (msgMinus) {
      return msgMinus;
    }
    return null;
  }

  let name = parameter['name'];
  let deltaVarName = `DELTA_${direction}_VAR`
  if (parameter['_vAttributesKept'][deltaVarName]) {
    let deltaVar = parameter['_vAttributesKept'][deltaVarName];
    if (!parameters[deltaVar]) {
      let msg = `Parameter '${name}' has ${deltaVarName} = `;
      msg += `'${deltaVar}' that was not found in list of variables.`;
      return {'message': msg, 'cause': 'CDAWeb'};
    }
    let msg = `Parameter '${name}' has an un-handled ${deltaVarName}.`;
    return {'message': msg, 'cause': 'NotImplemented'};
  }
  return null;
}

function dropDataset(dsidx, datasets, msg, category, showid) {

  if (!dsidx) {
    if (dropDataset.log !== undefined) {
      for (let origin of Object.keys(dropDataset.log)) {
        let fnameDropped = argv.cachedir 
                         + `/ids-cdas-processed-dropped-${origin}.txt`;
        let log = "";
        for (let key of Object.keys(dropDataset.log[origin])) {
          let msg = dropDataset.log[origin][key];
          if (Array.isArray(msg)) {
            msg = msg.join("\n  ");
          }
          log += key + "\n  " + msg + "\n\n";
        }
        util.writeSync(fnameDropped, log);
        let Nd = Object.keys(dropDataset.log[origin]).length;
        util.note(null, `Wrote ${Nd} IDs to ` + fnameDropped);
      }
    }
    return;
  }

  if (dropDataset.log === undefined) {
    dropDataset.log = {"NotImplemented": {}, "CDAWeb": {}};
  }
  if (category === "NotImplemented") {
    dropDataset.log[category][datasets[dsidx]['id']] = msg;
    msg = "(Not implemented) " + msg;
  }
  if (category === "CDAWeb") {
    dropDataset.log[category][datasets[dsidx]['id']] = msg;
    msg = "(CDAWeb) " + msg;
  }
  msg = msg + " Dropping dataset.";
  if (showid) {
    console.log(datasets[dsidx]['id']);
    util.error(datasets[dsidx]['id'], msg, false);
    //process.exit(0);
  } else {
    util.error(datasets[dsidx]['id'], msg, false);
  }
  datasets[dsidx] = null;
}

function createParameters(dsidx, datasets) {

  // Create dataset['info']['parameters'] array of objects. Give each object
  // a name and description taken CDAS /variables request.
  if (datasets[dsidx]['_variablesError']) {
    let emsg = datasets[dsidx]['_variablesError'];
    dropDataset(dsidx, datasets, emsg, "CDAWeb", true);
    return;
  }

  extractParameterNames(datasets[dsidx]);

  if (datasets[dsidx]['_dataError']) {
    let emsg = datasets[dsidx]['_dataError'];
    dropDataset(dsidx, datasets, emsg, "CDAWeb", true);
    return;
  }

  let msg = `Reading ${datasets[dsidx]['id']} file: ${datasets[dsidx]['_data']}`;
  util.debug(null, msg);

  let _data = JSON.parse(util.readSync(datasets[dsidx]['_data']))['CDF'][0];

  let err = extractParameterAttributes(datasets[dsidx], _data);
  if (err !== null) {
    dropDataset(dsidx, datasets, err, "NotImplemented", true);
    return;
  }
}

function extractParameterNames(dataset) {

  let variableDescriptions = dataset['_variables']['VariableDescription'];
  let parameters = {};
  for (let variableDescription of variableDescriptions) {
    let descr = variableDescription['LongDescription'] || variableDescription['ShortDescription'];
    let name = variableDescription['Name'];
    parameters[name] = {
      name: name,
      description: descr,
    };
  }
  dataset['info']['parameters'] = parameters;
}

function extractRecords(recordArray) {
  let records = [];
  if (records['value']) {
    for (let r of recordArray) {
      records.append(r['value']);
    }
  } else {
    return recordArray;
  }
}

function extractDatasetAttributes(dataset, _data) {

  let epochVariableName = dataset['info']['x_DEPEND_0'];
  let epochVariable;
  for (let variable of _data['cdfVariables']['variable']) {
    if (variable['name'] === epochVariableName) {
      epochVariable = variable;
      break;
    }
  }

  let epochRecords = extractRecords(epochVariable['cdfVarData']['record']);
  let Nr = epochVariable['cdfVarData']['record'].length;
  dataset['info']['sampleStartDate'] = epochRecords[0];

  if (Nr > argv['minrecords']) Nr = argv['minrecords']

  dataset['info']['sampleStopDate'] = epochRecords[Nr-1];
  dataset['info']['x_numberOfSampleRecords'] = Nr;

  for (let attribute of _data['cdfGAttributes']['attribute']) {
    if (attribute['name'] === 'TIME_RESOLUTION') {
      dataset['info']['cadence'] = attribute['entry'][0]['value'];
    }
    if (attribute['name'] === 'SPASE_DATASETRESOURCEID') {
      dataset['info']['resourceID'] = attribute['entry'][0]['value'];
    }
    if (attribute['name'] === 'ACKNOWLEDGEMENT') {
      dataset['info']['x_datasetCitation'] = catCharEntries(attribute['entry']);
    }
    if (attribute['name'] === 'RULES_OF_USE') {
      dataset['info']['x_datasetTermsOfUse'] = catCharEntries(attribute['entry']);
    }
  }

  function catCharEntries(entries) {
    let cat = '';
    for (let entry of entries) {
      cat = cat.trim() + ' ' + entry['value'];
    }
    return cat.trim();
  }
}

function extractParameterAttributes(dataset, _data) {

  let cdfVariables = _data['cdfVariables']['variable'];

  let cdfVariableNames = [];
  for (let variable of cdfVariables) {
    cdfVariableNames.push(variable['name']);
  }

  // The /variables step was previously needed b/c we were
  // making a request for CDF in JSON and the ALL-VARIABLES option
  // was not available.
  // TODO: Add command line option to skip /variables steps.
  for (let parameterName of Object.keys(dataset['info']['parameters'])) {
    if (!cdfVariableNames.includes(parameterName)) {
      let msg = `(CDAWeb) /variables has '${parameterName}', `;
      msg += `which was not found in CDF.`;
      dataset["_dataError"] = msg;
      delete dataset['info']['parameters'][parameterName];
    }
  }

  // dataset['info']['parameters'] was initialized with all of the variables returned by
  // the /variables endpoint. This list does not include all variables that
  // may be needed, such as record-varying bins. All variables from sample
  // CDF are added in the following loop.
  let parameters = dataset['info']['parameters'];
  for (let [idx, variable] of Object.entries(cdfVariables)) {
    let name = variable['name'];
    let vAttributesKept = extractVariableAttributes(dataset['id'], variable);
    let cdfVarInfo = variable['cdfVarInfo'];

    if (!parameters[name]) {
      parameters[name] = {};
    }
    parameters[name]['_cdfVariable'] = variable;

    if (vAttributesKept['_VAR_TYPE'] === 'data') {
      let cdfType = types.cdfType2hapiType(cdfVarInfo['cdfDatatype']);
      if (cdfType === null) {
        return `Un-handled CDF datatype '${cdfVarInfo['cdfDatatype']}'`;
      }
      parameters[name]['type'] = cdfType;
      if (cdfType === 'string') {
        parameters[name]['length'] = cdfVarInfo['padValue'].length;
        parameters[name]['fill'] = cdfVarInfo['padValue'];
      }
    }
    parameters[name]['_vAttributesKept'] = vAttributesKept;
    parameters[name]['_variableIndex'] = idx;

  }

  return null;
}

function extractVariableAttributes(dsid, variable) {

  let attributes = variable['cdfVAttributes']['attribute'];

  let keptAttributes = {};
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
      if (size !== '0') keptAttributes['size'] = [parseInt(size)];
    }
    if (attribute['name'] === 'DEPEND_0') {
      keptAttributes['_DEPEND_0'] = attribute['entry'][0]['value'];
    }
    if (attribute['name'] === 'DEPEND_1') {
      keptAttributes['_DEPEND_1'] = attribute['entry'][0]['value'];
    }
    if (attribute['name'] === 'DEPEND_2') {
      keptAttributes['_DEPEND_2'] = null;
    }
    if (attribute['name'] === 'LABL_PTR_1') {
      keptAttributes['_LABL_PTR_1'] = attribute['entry'][0]['value'];
    }
    if (attribute['name'] === 'VAR_TYPE') {
      keptAttributes['_VAR_TYPE'] = attribute['entry'][0]['value'];
    }
    if (attribute['name'].startsWith('DELTA_')) {
      keptAttributes["_" + attribute['name']] = attribute['entry'][0]['value'];
    }
    if (attribute['name'] === 'VAR_NOTES') {
      let desc = "";
      if (keptAttributes['description']) {
        desc = keptAttributes['description'].trim();        
      }
      let VAR_NOTES = attribute['entry'][0]['value'];
      let sep = ". ";
      if (desc.endsWith(".")) sep = " ";
      if (desc == "") sep = "";
      keptAttributes['description'] = desc + sep + VAR_NOTES;
    }
  }
  return keptAttributes;
}

function extractLabel(labelVariable) {

  if (!labelVariable['cdfVarData']['record'][0]['elementDelimiter']) {
    return labelVariable['cdfVarData']['record'][0]['value'];
  }

  let delimiter = labelVariable['cdfVarData']['record'][0]['elementDelimiter'];
  let re = new RegExp(delimiter, 'g');
  let label = labelVariable['cdfVarData']['record'][0]['value'][0];
  return label
    .replace(re, '')
    .replace(/[^\S\r\n]/g, '')
    .trim()
    .split('\n');
}

function extractDepend1(dsid, depend1Variable) {

  let DEPEND_1_TYPE = depend1Variable['cdfVarInfo']['cdfDatatype'];

  if (!types.charType(DEPEND_1_TYPE)) {
    // Return a bins object.
    let bins = {};
    let keptAttributes = extractVariableAttributes(dsid, depend1Variable);
    bins['name'] = depend1Variable['name'];
    bins['description'] = keptAttributes['description'];
    bins['label'] = keptAttributes['label'];
    bins['units'] = keptAttributes['units'] || null;

    bins['centers'] = extractRecords(depend1Variable['cdfVarData']['record'])
    if (types.floatType(DEPEND_1_TYPE)) {
      for (let cidx in bins['centers']) {
        bins['centers'][cidx] = parseFloat(bins['centers'][cidx]);
      }
    } else if (types.intType(DEPEND_1_TYPE)) {
      for (let cidx in bins['centers']) {
        bins['centers'][cidx] = parseInt(bins['centers'][cidx]);
      }
    }
    return bins;
  }

  // Return an array of strings.
  let delimiter = depend1Variable['cdfVarData']['record'][0]['elementDelimiter'];
  let depend1 = depend1Variable['cdfVarData']['record'][0]['value'][0]
                  .replace(new RegExp(delimiter, 'g'), '')
                  .replace(/[^\S\r\n]/g, '')
                  .trim()
                  .split('\n');
  return depend1;
}

function extractCoordSysNameAndVecComps(dsid, parameter, depend1) {
  let foundComponents = false;

  if (depend1.length == 3) {
    if (
      depend1[0] === 'x_component' &&
      depend1[1] === 'y_component' &&
      depend1[2] === 'z_component'
    ) {
      foundComponents = true;
    }
    if (depend1[0] === 'x' && depend1[1] === 'y' && depend1[2] === 'z') {
      foundComponents = true;
    }
  }
  if (foundComponents) {
    util.warning(dsid,
                  parameter['name'] 
                + ': Assumed DEPEND_1 = [' 
                + depend1.join(', ')
                + '] => vectorComponents = [x, y, z]');
    parameter['vectorComponents'] = ['x', 'y', 'z'];
  }

  let foundName = false;
  if (foundComponents) {
    let knownNames = ['GSM', 'GCI', 'GSE', 'RTN', 'GEO', 'MAG'];
    for (let knownName of knownNames) {
      if (parameter['name'].includes(knownName)) {
        foundName = true;
        util.warning(dsid,
                      parameter['name']
                    + ': Assumed parameter name = '
                    + parameter['name']
                    + ' => coordinateSystemName = '
                    + knownName);
        parameter['coordinateSystemName'] = knownName;
      }
    }
    if (foundName == false) {
      util.warning(dsid,
                    'Could not infer coordinateSystemName '
                  + ' from parameter name = '
                  + parameter['name']);
    }
  }
}
