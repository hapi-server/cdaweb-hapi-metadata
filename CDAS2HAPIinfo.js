// Create a HAPI all.json catalog based on
//   https://spdf.gsfc.nasa.gov/pub/catalogs/all.xml
// and queries to
//   https://cdaweb.gsfc.nasa.gov/WS/cdasr
// CDASR documentation:
//   https://cdaweb.gsfc.nasa.gov/WebServices/REST/

// Command line options

const argv = require('./CDAS2HAPIinfo.cli.js').argv();
const {util} = require('./CDAS2HAPIinfo.util.js');
const {meta} = require('./CDAS2HAPIinfo.meta.js');

// meta.run() gets all needed metadata files.
// buildHAPI() is called when complete.
meta.run(buildHAPI);

// To only get metadata files:
//meta.run();

// To only build HAPI metadata based on cached metadata files:
//buildHAPI()

//////////////////////////////////////////////////////////////////////////////

function buildHAPI(CATALOG) {

  let datasets = [];
  if (CATALOG !== undefined) {
    datasets = CATALOG['datasets'];
  } else {
    const globSync = require('glob').globSync;
    const globStr = argv['cachedir'] + '/**/*-combined.json';
    util.log(null, `\n*Reading cached metadata in -combined.json files.*\n`, "");
    util.log(null, `  Finding files that match ${globStr}`, "");
    const combinedFiles = globSync(globStr).sort();
    util.log(null, `  ${combinedFiles.length} cached files match ${globStr}`, "");
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
    util.log(null, msg, "");

  }

  buildHAPIFiles(datasets);

  //console.log(datasets)
  //process.exit()
  util.log(null, `\n*Processing ${datasets.length} datasets*\n`, "");

  let dataset;
  for (let dsidx in datasets) {
    // Create dataset['info']['parameters'] array of objects. Give each object
    // a name and description taken CDAS /variables request.
    dataset = datasets[dsidx];
    extractParameterNames(dataset);
    if (dataset['_data'] === null) {
      continue;
    }
    let _data = JSON.parse(util.readSync(dataset['_data']))['CDF'][0];
    process.stdout.write("\r" + " ".repeat(80) + "\r");
    process.stdout.write(dataset['id']);
    extractParameterAttributes(dataset, _data);
  }
  process.stdout.write("\r" + " ".repeat(80) + "\r");

  datasets = datasets.filter(item => item !== null);

  util.debug(null, 'Looking for datasets with more than one DEPEND_0.');
  datasets = subsetDatasets(datasets);

  let omsg = ' from HAPI all.json because no variable attributes.';

  let catalog = [];
  for (let dataset of datasets) {

    let dsid = dataset['id'];
    util.log(dsid, dsid, "", 'blue');

    if (dataset['_data'] === null) {
      let msg = 'Omitting ' + dsid + ' because no data sample obtained';
      util.error(dsid, msg, false);
      continue;
    }

    if (!dataset['_variables']) {
      util.error(dsid, 'Omitting ' + dsid + omsg, false);
      continue;
    }

    let _allxml = dataset["_allxml"];
    dataset['title'] = _allxml['description'][0]['$']['short'];
    let producer = _allxml['data_producer'][0]['$']
    let contact = producer['name'].trim() + ' @ ' + producer['affiliation'].trim();
    dataset['info']['contact'] = contact;
    let rURL = 'https://cdaweb.gsfc.nasa.gov/misc/Notes.html#' + dataset["id"];
    dataset['info']['resourceURL'] = rURL;

    if (dataset['_spaseError']) {
      util.warning(dsid, dataset['_spaseError']);
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
    extractCadence(dataset, _data);

    let DEPEND_0s = [];
    let parameterArray = [];
    let parameters = dataset['info']['parameters'];
    for (let name of Object.keys(parameters)) {

      let parameter = parameters[name];
      let varType = parameter['_vAttributesKept']['_VAR_TYPE'];

      let omsg = `parameter ${name} has an un-handled DEPEND_2. Omitting dataset.`;
      if (parameter['_vAttributesKept']['_DEPEND_2'] === null) {
        util.error(dsid, omsg, false);
        dataset = null;
        break;
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
        util.warning(dataset['id'], 'No units for ' + parameter['name']);
        parameter['units'] = null;
      }

      let DEPEND_0 = parameter['_vAttributesKept']['_DEPEND_0'];
      if (DEPEND_0 && !DEPEND_0.toLowerCase().startsWith('epoch')) {
        let msg = `${parameter['name']} has DEPEND_0 name of `
        msg += `'${DEPEND_0}'; expected 'Epoch'`;
        util.warning( dataset['id'], msg);
      }
      DEPEND_0s.push(DEPEND_0);

      // Extract DEPEND_1
      //console.log(name)
      let vectorComponents = false;
      if (parameter['_vAttributesKept']['_DEPEND_1']) {
        let DEPEND_1 = parameter['_vAttributesKept']['_DEPEND_1'];
        let vidx = parameters[DEPEND_1]['_variableIndex']
        //let depend1 = extractDepend1(dataset['id'], parameters[DEPEND_1]['_variable']);
        let depend1 = extractDepend1(dataset['id'], cdfVariables[vidx]);
        //console.log(depend1);
        //process.exit();
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
        parameterArray.push(util.copy(parameter));
      }
    }

    if (dataset === null) continue;

    let EpochName = DEPEND_0s[0];
    let vidx = parameters[EpochName]['_variableIndex'];
    let firstTimeValue = extractRecords(cdfVariables[vidx]['cdfVarData']['record'])[0];
    //let firstTimeValue = parameters[EpochName]['_variable']['cdfVarData']['record'][0]['value'][0];
    //let timePadValue = parameters[EpochName]['_variable']['cdfVarInfo']['padValue'];
    parameterArray
          .unshift(
                    {
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
  //process.exit(0);

  console.log("");

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
  all = all.sort(function(a, b) {
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

    util.log(null, '\n*Creating HAPI catalog and info responses for CDAWeb inventory datasets.*\n', "");

    for (let dataset of datasets) {
      let id = dataset['id'];

      util.log(id, id, "", 'blue');

      let fileList = util.baseDir(id) + '/' + id + '-inventory.json';
      let intervals = JSON.parse(util.readSync(fileList))["InventoryDescription"][0]["TimeInterval"];
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

      let info = 
            {
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

      let title = "Time intervals of data availability from /inventory endpoint at https://cdaweb.gsfc.nasa.gov/WebServices/REST/";
      let ida = id + "/inventory";
      all.push({"id": ida, "title": title, "info": info});
      catalog.push({"id": ida, "title": title});
    }

    //util.log(`\nWrote: ${datasets.length} inventory.json files to ` + argv.infodir + '/CDAWeb-files/info/');
    //util.log(`Wrote: ${datasets.length} inventory.csv files to ` + argv.infodir + '/CDAWeb-files/data/');
  }

  function files1() {

    if (argv['omit'].includes('files1')) {
      return;
    }

    let msg = '\n*Creating HAPI catalog and info responses for CDAWeb ';
    msg += 'files datasets (version 1 method).*\n';
    util.log(null, msg, "");

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

      let info = 
            {
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
                    "x_stringType": {"uri": {"scheme": "https", "mediaType": "application/x-cdf"}},
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
    console.log(`Total size of all files: ${util.sizeOf(sum)}`);
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
    if (dataset._dataError !== undefined) {
      util.error(dataset['id'], dataset._dataError, false);
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
  for (let parameterName of Object.keys(dataset['info']['parameters'])) {
    if (!cdfVariableNames.includes(parameterName)) {
      let msg = `/variables has '${parameterName}', `;
      msg += `which was not found in CDF. Omitting.`;
      dataset._dataError = msg;
      delete dataset['info']['parameters'][parameterName];
    }
  }

  let parameters = dataset['info']['parameters'];
  for (let [idx, variable] of Object.entries(cdfVariables)) {

    let vAttributesKept = extractVariableAttributes(dataset['id'], variable);
    let cdfVarInfo = variable['cdfVarInfo'];
    let name = variable['name'];
    if (!parameters[name]) {
      parameters[name] = {};
      // parameters was initialized with all of the variables returned by
      // the /variables endpoint. This list does not include variables that
      // may be needed
      parameters[name] = variable;
    }

    if (vAttributesKept['_VAR_TYPE'] === 'data') {
      let cdftype = cdftype2hapitype(cdfVarInfo['cdfDatatype']);
      parameters[name]['type'] = cdftype;
      if (cdftype === 'string') {
        parameters[name]['length'] = cdfVarInfo['padValue'].length;
        parameters[name]['fill'] = cdfVarInfo['padValue'];
      }
    }

    parameters[name]['_vAttributesKept'] = vAttributesKept;
    parameters[name]['_variableIndex'] = idx;
  }

  function cdftype2hapitype(cdftype) {
    if (floatType(cdftype)) {
      return 'double';
    } else if (intType(cdftype)) {
      return 'integer';
    } else if (cdftype.startsWith('CDF_EPOCH')) {
      return 'integer';
    } else if (cdftype.startsWith('CDF_CHAR')) {
      return 'string';
    } else {
      util.error(dataset['id'], `Un-handled CDF datatype '${cdftype}'`, true);      
    }
  }
}

function intType(cdfType) {
  return cdfType.startsWith('CDF_INT') || cdfType.startsWith('CDF_UINT');
}
function floatType(cdfType) {
  return ['CDF_FLOAT', 'CDF_DOUBLE', 'CDF_REAL4', 'CDF_REAL8'].includes(cdfType);
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

  if (!['CDF_CHAR', 'CDF_UCHAR'].includes(DEPEND_1_TYPE)) {
    // Return a bins object.
    let bins = {};
    let keptAttributes = extractVariableAttributes(dsid, depend1Variable);
    bins['name'] = depend1Variable['name'];
    bins['description'] = keptAttributes['description'];
    bins['label'] = keptAttributes['label'];
    bins['units'] = keptAttributes['units'] || null;
    if (depend1Variable['cdfVarInfo']['recVariance'] === 'VARY') {
      util.error(dsid, "Un-handled DEPEND1 with recVariance of 'VARY'.",true);
    }

    bins['centers'] =
      depend1Variable['cdfVarData']['record'][0]['value'][0].split(' ');
    if (floatType(DEPEND_1_TYPE)) {
      for (let cidx in bins['centers']) {
        bins['centers'][cidx] = parseFloat(bins['centers'][cidx]);
      }
    } else if (intType(DEPEND_1_TYPE)) {
      for (let cidx in bins['centers']) {
        bins['centers'][cidx] = parseInt(bins['centers'][cidx]);
      }
    } else {
      util.error(dsid,
                'Un-handled DEPEND_1 type for bins variable '
              + depend1Variable['name']
              + DEPEND_1_TYPE,
              true);
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

function extractFromSPASE(dsid, _spase, key) {

  if (!_spase) {return undefined}

  if (key === "Cadence") {
    try {
      return _spase['NumericalData'][0]['TemporalDescription'][0]['Cadence'][0];
    } catch (e) {
      return undefined;
    }
  }
  return undefined;
}

function extractCadence(dataset, _data) {

  let dsid = dataset['id'];

  // Get cadence from data sample
  let cadenceData = undefined;
  let timeRecords = undefined;
  for (let variable of _data['cdfVariables']['variable']) {
    if (variable['name'] === dataset['info']['x_DEPEND_0']) {
      timeRecords = extractRecords(variable['cdfVarData']['record']);
      break;
    }
  }
  if (timeRecords === undefined) {
    util.warning(dsid, "No DEPEND_0 records.");
  } else {
    cadenceData = inferCadence(timeRecords, dsid);
  }

  // Get cadence from CDF
  let cadenceCDF = undefined;
  if (dataset['info']['cadence']) {
    cadenceCDF = util.str2ISODuration(dataset['info']['cadence']);
    if (cadenceCDF !== undefined) {
      let msg = `Inferred cadence of ${cadenceCDF} from CDF attribute `
      msg += `TIME_RESOLUTION = '${dataset['info']['cadence']}'`;
      util.note(dsid, msg);
    } else {
      let msg = 'Could not parse CDF attribute TIME_RESOLUTION: '
      msg += dataset['info']['cadence'] + ' to use for cadence.';
      util.warning(dsid, msg);
    }
  }

  // Get cadence from SPASE
  let cadenceSPASE = extractFromSPASE(dsid, dataset['_spase'], "Cadence");
  if (cadenceSPASE !== undefined) {
    util.note(dsid, 'Cadence from SPASE: ' + cadenceSPASE);
  }

  // Summarize
  if (cadenceSPASE !== undefined && cadenceData !== undefined ) {
    if (!util.sameDuration(cadenceSPASE, cadenceData)) {
      util.warning(dsid, "Cadence mis-match between SPASE and data sample.");
    }
  }
  if (cadenceSPASE !== undefined && cadenceCDF !== undefined ) {
    if (!util.sameDuration(cadenceSPASE, cadenceCDF)) {
      let msg = "Cadence mis-match between SPASE and parsed "
      msg += "TIME_RESOLUTION from CDF.";
      util.warning(dsid, msg);
    }
  }
  if (cadenceCDF !== undefined && cadenceData !== undefined ) {
    if (!util.sameDuration(cadenceCDF, cadenceData)) {
      let msg = "Cadence mis-match between data sample and parsed "
      msg += "TIME_RESOLUTION from CDF.";
      util.warning(dsid, msg);
    }
  }

  // Select
  if (cadenceSPASE !== undefined) {
    util.note(dsid, `Using cadence ${cadenceSPASE} from SPASE`);
    dataset['info']['cadence'] = cadenceSPASE;
  } else if (cadenceCDF !== undefined) {
    let msg = `Using cadence ${cadenceCDF} from parsed TIME_RESOLUTION from CDF`;
    util.note(dsid, msg);
    dataset['info']['cadence'] = cadenceCDF;
  } else if (cadenceData !== undefined) {
    util.note(dsid, `Using cadence ${cadenceData} from sample data`);
    dataset['info']['cadence'] = cadenceData;
  } else {
    let msg = 'No SPASE cadence or CDF attribute of TIME_RESOLUTION '
    msg += 'found to use for cadence.';
    util.warning(dsid, msg);
  }
}

function inferCadence(timeRecords, dsid) {

  let dts = [];
  for (let r = 0; r < timeRecords.length - 1; r++) {
    let dt = new Date(timeRecords[r+1]).getTime() 
           - new Date(timeRecords[r]).getTime();
    dts.push(dt);
  }

  let dthist = histogram(dts);

  let cadenceData = undefined;

  let Nu = Object.keys(dthist).length;
  let Nr = timeRecords.length;
  if (Nu == 1) {
    let S = dthist[0][0];
    cadenceData = "PT" + S + "S";
    util.note(dsid, `Inferred cadence of ${cadenceData} based on ${Nr} records.`);
  } else {
    if (Nr > 1) {
      let msg = `Could not infer cadence from ${Nr} time record(s) because `;
      msg += `more than one (${Nu}) unique Δt values.`;
      util.note(dsid, msg);
    } else {
      util.note(dsid, `Could not infer cadence from ${Nr} time record.`);      
    }
    let dtmsg = "";
    let n = 1;
    for (let i in dthist) {
      let p = (100*dthist[i][1]/Nr);
      if (n == 5 || p < 2) break;
      dtmsg += `${p.toFixed(1)}% @ ${dthist[i][0]}s, `;
      n++;
    }
    if (dthist.length > n) dtmsg += "...";
    if (Nr > 1 && dtmsg !== "") util.note(dsid, "Δt histogram: " + dtmsg);
  }

  if (dthist.length > 0) {
    dthist.sort(function(a, b) {return a[0] - b[0]});
    if (dthist[0][0] <= 0) {
      util.error(dsid, "Time records not monotonic.", false, false);
    }
  }
  return cadenceData;

  function histogram(dts) {
    let counts = {};
    for (let i = 0; i < dts.length; i++) {
      counts[dts[i]] = 1 + (counts[dts[i]] || 0);
    }
    let sorted = [];
    for (let entry in counts) {
      sorted.push([parseFloat(entry)/1000, counts[entry]]);
    }
    sorted.sort(function(a, b) {return b[1] - a[1]});
    return sorted;
  }
}
