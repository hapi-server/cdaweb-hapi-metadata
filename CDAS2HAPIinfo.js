// Create a HAPI all.json catalog based on
//   https://spdf.gsfc.nasa.gov/pub/catalogs/all.xml
// and queries to
//   https://cdaweb.gsfc.nasa.gov/WS/cdasr
// CDASR documentation:
//   https://cdaweb.gsfc.nasa.gov/WebServices/REST/

// Command line options
let argv = require('yargs')
            .help()
            .option('keepids', {
              describe: 'Comma-separated list of regex patterns to include',
              default: '^AC_',
              type: 'string'
            })
            .option('omitids', {
              describe: 'Comma-separated list of regex patterns to exclude (ignored for ids that match keepids pattern)',
              default: '^ALOUETTE2,AIM_CIPS_SCI_3A,^MMS,PO_.*_UVI',
              default: '',
              type: 'string'
            })
            .option('minrecords', {
              describe: 'Minimum # of records for CDAS data request to be successful',
              minrecords: 1440,
              type: 'number'
            })
            .option('debug', {
              describe: "Show additional logging information",
              default: false
            })
            .option("omit", {
              describe: "Comma-separated list of steps to omit from: {inventory, files0, files1, masters, spase}",
              default: "files0",
              type: "string"
            })
            .option("maxsockets", {
              describe: "Maximum open sockets per server",
              default: 3,
              type: "number"
            })
            .option('maxheadage', {
              describe: 'Skip HEAD request and use cached file if header age < maxheadage',
              default: 100*3600*24,
              type: "number"
            })
            .option('maxfileage', {
              describe: 'Request file if age < maxage (in seconds) and HEAD indicates expired',
              default: 100*3600*24,
              type: "number"
            })
            .option('maxtries', {
              describe: 'Maximum # of tries for CDAS data requests',
              default: 4,
              type: "number"
            })
            .option('infodir', {
              describe: '',
              default: "hapi/bw",
              type: "string"
            })
            .option('cachedir', {
              describe: '',
              default: "cache/bw",
              type: "string"
            })
            .option('cdasr', {
              describe: "",
              default: "https://cdaweb.gsfc.nasa.gov/WS/cdasr/1/dataviews/sp_phys/datasets/",
              type: "string"
            })
            .option('allxml', {
              describe: "",
              default: "https://spdf.gsfc.nasa.gov/pub/catalogs/all.xml",
              type: "string"
            })
            .option('allxml', {
              describe: "",
              default: "https://spdf.gsfc.nasa.gov/pub/catalogs/all.xml",
              type: "string"
            })
            .option('hapiversion', {
              describe: "",
              default: "3.1",
              type: "number"
            })
            .option('cdf2json-method', {
              describe: "cdf2json.py or CDF2CDFML+xml2json",
              default: "cdf2json.py",
              type: "string"
            })
            .option('cdf2cdfml', {
              describe: "",
              default: "CDF_BASE=/Applications/cdf/cdf38_1-dist; CLASSPATH=.:$CDF_BASE/cdfjava/classes/cdfjava.jar:$CDF_BASE/cdfjava/classes/cdfjson.jar:$CDF_BASE/cdfjava/classes/gson-2.8.6.jar:$CDF_BASE/cdfjava/classes/javax.json-1.0.4.jar:$CDF_BASE/cdfjava/cdftools/CDFToolsDriver.jar:$CDF_BASE/cdfjava/cdfml/cdfml.jar java CDF2CDFML",
              type: "string"
            })
            .option('cdf2json', {
              describe: "",
              default: "python3 " + __dirname + "/bin/cdf2json.py --maxrecs=1440",
              type: "string"
            })
            .argv;

argv['keepids'] = argv['keepids'].split(',');
argv['omitids'] = argv['omitids'].split(',');
argv['omit'] = argv['omit'].split(',');

const {util} = require('./CDAS2HAPIinfo.util.js');
util.argv = argv;

const {meta} = require('./CDAS2HAPIinfo.meta.js');
meta.argv = argv;

// meta.run() gets all needed metadata files.
// buildHAPI() is called when complete.
meta.run(buildHAPI);

// To only get metadata files:
//meta.run();

// To only build HAPI metadata based on cached metadata files:
//buildHAPI()

//////////////////////////////////////////////////////////////////////////////

function heapUsed() {
  const used = process.memoryUsage().heapUsed / 1024 / 1024;
  console.log(`heapUsed ${Math.round(used * 100) / 100} MB`);
}

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
      if (util.idFilter(id, meta["argv"]['keepids'], meta["argv"]['omitids'])) {
        datasets.push(JSON.parse(util.readSync(file)));
      }
    }
    if (!argv['debug']) {
      process.stdout.write("\r" + " ".repeat(80) + "\r");
    }
    let plural = datasets.length == 1 ? "" : "s";
    util.log(null, `  ${datasets.length} file${plural} after keepids and omitids filter`, "");

  }

  buildHAPIFiles(datasets);

  util.log(null, `\n*Processing ${datasets.length} datasets*\n`, "");

  let dataset;
  for (dsidx in datasets) {
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
    //heapUsed();
  }
  process.stdout.write("\r" + " ".repeat(80) + "\r");

  datasets = datasets.filter(item => item !== null);
  if (datasets.length == 0) {
  }

  util.debug(null, 'Looking for datasets with more than one DEPEND_0.');
  datasets = subsetDatasets(datasets);

  let omsg = ' from HAPI all.json because no variable attributes.';

  let catalog = [];
  for (let dataset of datasets) {

    let dsid = dataset['id'];
    util.log(dsid, dsid, "", 'blue');

    if (dataset['_data'] === null) {
      util.error(dsid, 'Omitting ' + dsid + ' because no data sample obtained', false);
      continue;
    }

    if (!dataset['_variables']) {
      util.error(dsid, 'Omitting ' + dsid + omsg, false);
      continue;
    }

    let _allxml = dataset["_allxml"];
    dataset['title'] = _allxml['description'][0]['$']['short'];
    let producer = _allxml['data_producer'][0]['$']
    dataset['info']['contact'] = producer['name'].trim() + ' @ ' + producer['affiliation'].trim();
    dataset['info']['resourceURL'] = 'https://cdaweb.gsfc.nasa.gov/misc/Notes.html#' + dataset["id"];

    if (dataset['_spaseError']) {
      util.warning(dsid, dataset['_spaseError']);
    }

    if (/\s\s.*|\( | \)/.test(dataset['title'])) {
      // TODO: Get from SPASE, if available.
      util.warning(dsid, "Check title formatting for extra spaces: '" + dataset['title'] + "'");
    }

    let _data = JSON.parse(util.readSync(dataset['_data']))['CDF'][0];
    let cdfVariables = _data['cdfVariables']['variable'];

    extractDatasetAttributes(dataset, _data);
    extractCadence(dataset, _data);

    let DEPEND_0s = [];
    let pidx = 0;
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
        util.warning(dataset['id'], 'No fill for data parameter ' + parameter['name']);
      }

      if (parameter['fill'] && varType == 'data' && parameter['fill'].toLowerCase() == "nan") {
        // Found in THA_L2_MOM
        //util.warning(dataset['id'], 'FILLVAL cast to lower case is nan for ' + parameter['name'] + '. Setting to -1e31');
        //parameter['fill'] = "-1e31";
      }

      if (!parameter['units'] && varType == 'data') {
        util.warning(dataset['id'], 'No units for ' + parameter['name']);
        parameter['units'] = null;
      }

      let DEPEND_0 = parameter['_vAttributesKept']['_DEPEND_0'];
      if (DEPEND_0 && !DEPEND_0.toLowerCase().startsWith('epoch')) {
        util.warning( dataset['id'],
          `${parameter['name']} has DEPEND_0 name of '${DEPEND_0}'; expected 'Epoch'`);
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
        util.warning(dataset['id'], `No bin units for bin parameter '${parameter['bins'][0]["name"]}' of '${parameter['name']}'`);
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

    if (dataset === null) {
      continue;
    }

    let EpochName = DEPEND_0s[0];
    let vidx = parameters[EpochName]['_variableIndex'];
    let firstTimeValue = cdfVariables[vidx]['cdfVarData']['record'][0]['value'][0];
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
  //process.exit(0)
}

function buildHAPIFiles(datasets) {

  let all = [];
  let catalog = [];

  inventory();
  files0();
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

    if (meta.argv['omit'].includes('inventory')) {
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
      for (interval of intervals) {
        data.push([interval['Start'],interval['End']]);
        startLen = Math.max(startLen,interval['Start'].length);
        endLen = Math.max(endLen,interval['End'].length);
      }
      startDate = data[0][0];
      let stopDate = data[data.length-1][1];

      if (data.length == 0) {
        util.warning(id, 'No intervals');
        continue;
      } else {
        let plural = data.length == 1 ? "" : "s";
        util.note(id, `${data.length} interval${plural}`);
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

  function files0() {

    if (meta.argv['omit'].includes('files0')) {
      return;
    }

    util.log(null, '\n*Creating HAPI catalog and info responses for CDAWeb files datasets (version 0 method).*\n');

    for (let dataset of datasets) {

      let id = dataset['id'];
      util.log(id, id, "", 'blue');

      let fileList = dataset['_files0'];
      if (!fileList) {
        util.warning(id, 'No _files1');
        continue;
      }
      let files = JSON.parse(util.readSync(fileList));
      if (files.length === 0) {
        util.warning(id, 'No files in ' + fileList);
        continue;
      }

      let startLen = 0; let endLen = 0; let urlLen = 0;
      for (file of files) {
        timeLen = Math.max(startLen,files[0][0].length);
        urlLen = Math.max(urlLen,files[0][1].length);
      }
      let fileData1 = fileList.replace(/\.json$/,".csv");
      let dirData2 = argv.infodir + '/CDAWeb-files/data/';
      let fileData2 =  dirData2 + id + '/files0.csv';
      util.cp(fileData1, fileData2);

      if (files.length == 0) {
        util.warning(id, 'No files');
        continue;
      } else {
        let plural = files.length == 1 ? "" : "s";
        util.note(id, `${files.length} file${plural}; ${util.sizeOf(dataset['_files0Size'])}`);
      }
      let startDate = files[0][0];
      let stopDate = files[files.length-1][0];
    
      let cadence = "";
      if (files.length > 1) {
        let dt = new Date(files[1][0].replace(/Z$/,"")).getTime() 
               - new Date(startDate.replace(/Z$/,"")).getTime();
        cadence = "PT" + (dt/(3600*1000)) + "H";
      }
      if (/0101_v/.test(files[files.length-1][0]) && /0101_v/.test(files[files.length-2][0])) {
        // One file per year.
        cadence = "P1Y";
      }
      let info = 
            {
              "startDate": startDate,
              "stopDate" : stopDate,
              "cadence": cadence,
              "parameters":
                [
                  { 
                    "name": "Time",
                    "type": "isotime",
                    "units": "UTC",
                    "fill": null,
                    "length": timeLen
                  },
                  { 
                    "name": "URL",
                    "type": "string",
                    "units": null,
                    "fill": null,
                    "length": urlLen
                  },
                  { 
                    "name": "LastModified",
                    "type": "isotime",
                    "units": "UTC",
                    "fill": null,
                    "length": 17
                  },
                  { 
                    "name": "Length",
                    "type": "integer",
                    "fill": null,
                    "units": "bytes"
                  },
                  { 
                    "name": "FileVersion",
                    "type": "string",
                    "fill": null,
                    "units": null
                  }
                ]
            }
      if (cadence === "") {delete info['cadence'];}
      let fnameInfo = argv.infodir + '/CDAWeb-files/info/' + id + '-files0.json';
      util.writeSync(fnameInfo, util.obj2json(info));
      util.note(id, 'Wrote ' + fnameInfo);
      let title = "List of files obtained from index.html files at access URL in all.xml";
      let ida = id + "/files0";
      all.push({"id": ida, "title": title, "info": info});
      catalog.push({"id": ida, "title": title});
    }

    //util.log(`\nWrote: ${datasets.length} files0.json files to ` + argv.infodir + '/CDAWeb-files/info/');
  }

  function files1() {

    if (meta.argv['omit'].includes('files1')) {
      return;
    }

    util.log(null, '\n*Creating HAPI catalog and info responses for CDAWeb files datasets (version 1 method).*\n', "");

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
      let _files1Size = 0;

      dataset['_files1Size'] = 0;
      for (file of files) {
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
      let startDate = data[0][0];
      let stopDate = data[data.length-1][1];
    
      if (data.length == 0) {
        util.warning(id, 'No files');
        continue;
      } else {
        let plural = data.length == 1 ? "" : "s";
        util.note(id, `${files.length} file${plural}; ${util.sizeOf(dataset['_files1Size'])}`);
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

      let title = "List of files obtained from /orig_data of https://cdaweb.gsfc.nasa.gov/WebServices/REST/";
      let ida = id + "/files";
      all.push({"id": ida, "title": title, "info": info});
      catalog.push({"id": ida, "title": title});

    }
    //util.log(`\nWrote: ${datasets.length} files.csv files to ` + argv.infodir + '/CDAWeb-files/data/');
    //util.log(`Wrote: ${datasets.length} files.json files to ` + argv.infodir + '/CDAWeb-files/info/');
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

  for (key of Object.keys(dataset['info'])) {
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

  for (let [dsidx, dataset] of Object.entries(datasets)) {
    let subdatasets = subsetDataset(dataset);
    if (subdatasets.length > 1) {
      util.log(dataset['id'], dataset['id'], "", 'blue');
      util.note(dataset['id'], subdatasets.length + ' sub-datasets');
    }
    if (dataset._dataError !== undefined) {
      util.error(dataset['id'], dataset._dataError, false);
    }
    for (d of subdatasets) {
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
    for (parameter of Object.keys(parameters)) {
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
    for ([sdsidx, DEPEND_0] of Object.entries(DEPEND_0s)) {
      //console.log(DEPEND_0)
      newdataset = util.copy(dataset);
      newdataset['id'] = newdataset['id'] + '@' + sdsidx;
      newdataset['info']['x_DEPEND_0'] = DEPEND_0;
      for (parameter of Object.keys(newdataset['info']['parameters'])) {
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

function extractDatasetAttributes(dataset, _data) {

  let epochVariableName = dataset['info']['x_DEPEND_0'];
  for (let variable of _data['cdfVariables']['variable']) {
    if (variable['name'] === epochVariableName) {
      epochVariable = variable;
      break;
    }
  }

  let Nr = epochVariable['cdfVarData']['record'].length;
  dataset['info']['sampleStartDate'] = epochVariable['cdfVarData']['record'][0]['value'][0];
  if (Nr > argv['minrecords']) {
    Nr = argv['minrecords'];
  }
  dataset['info']['sampleStopDate'] = epochVariable['cdfVarData']['record'][Nr-1]['value'][0];
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

  //console.log(_data);
  //process.exit();
  let cdfVariables = _data['cdfVariables']['variable'];

  let cdfVariableNames = [];
  for (let [idx, variable] of Object.entries(cdfVariables)) {
    cdfVariableNames.push(variable['name']);
  }
  for (let parameterName of Object.keys(dataset['info']['parameters'])) {
    if (!cdfVariableNames.includes(parameterName)) {
      dataset._dataError = `/variables has '${parameterName}', which was not found in CDF. Omitting.`;
      delete dataset['info']['parameters'][parameterName];
    }
  }

  let parameters = dataset['info']['parameters'];
  for (let [idx, variable] of Object.entries(cdfVariables)) {
    let vAttributesKept = extractVariableAttributes(dataset['id'], variable);

    if (!parameters[variable['name']]) {
      parameters[variable['name']] = {};
      // CATALOG[ididx]['parameters'] was initialized with all of the
      // variables returned by /variables endpoint. This list does not
      // include support variables. So we add them here.
      parameters[variable['name']]['name'] = variable['name'];
    }

    if (vAttributesKept['_VAR_TYPE'] === 'data') {
      let cdftype = cdftype2hapitype(variable['cdfVarInfo']['cdfDatatype']);
      parameters[variable['name']]['type'] = cdftype;
      if (cdftype === 'string') {
        parameters[variable['name']]['length'] = variable['cdfVarInfo']['padValue'].length;
        parameters[variable['name']]['fill'] = variable['cdfVarInfo']['padValue'];
      }
    }

    parameters[variable['name']]['_vAttributesKept'] = vAttributesKept;
    //parameters[variable['name']]['_variable'] = variable;
    parameters[variable['name']]['_variableIndex'] = idx;
  }

  function cdftype2hapitype(cdftype) {
    if (
      ['CDF_FLOAT', 'CDF_DOUBLE', 'CDF_REAL4', 'CDF_REAL8'].includes(cdftype)
    ) {
      return 'double';
    } else if (
      cdftype.startsWith('CDF_INT') ||
      cdftype.startsWith('CDF_UINT') ||
      cdftype.startsWith('CDF_BYTE')
    ) {
      return 'integer';
    } else if (cdftype.startsWith('CDF_EPOCH')) {
      return 'integer';
    } else if (cdftype.startsWith('CDF_CHAR')) {
      return 'string';
    } else {
      util.error(dataset['id'], 'Un-handled CDF datatype ' + cdftype, true);      
    }
  }
}

function extractVariableAttributes(dsid, variable) {

  let attributes = variable['cdfVAttributes']['attribute'];

  let keptAttributes = {};
  for (let attribute of attributes) {
    if (0) {
      console.log(
        dsid + '/' + variable['name'] + ' has attribute ' + attribute['name']
      );
    }
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
      if (desc.endsWith(".")) {
        sep = " ";
      }
      if (desc == "") {
        sep = "";
      }
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

    }

    bins['centers'] =
      depend1Variable['cdfVarData']['record'][0]['value'][0].split(' ');
    if (
      ['CDF_FLOAT', 'CDF_DOUBLE', 'CDF_REAL4', 'CDF_REAL8'].includes(
        DEPEND_1_TYPE
      )
    ) {
      for (cidx in bins['centers']) {
        bins['centers'][cidx] = parseFloat(bins['centers'][cidx]);
      }
    } else if (
      DEPEND_1_TYPE.startsWith('CDF_INT') ||
      DEPEND_1_TYPE.startsWith('CDF_UINT')
    ) {
      for (cidx in bins['centers']) {
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
  for (variable of _data['cdfVariables']['variable']) {
    if (variable['name'] === dataset['info']['x_DEPEND_0']) {
      timeRecords = variable['cdfVarData']['record'];
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
      util.note(dsid, `Inferred cadence of ${cadenceCDF} from CDF attribute TIME_RESOLUTION = '${dataset['info']['cadence']}'`);
    } else {
      util.warning(dsid, 'Could not parse CDF attribute TIME_RESOLUTION: ' + dataset['info']['cadence'] + ' to use for cadence.');
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
      util.warning(dsid, "Cadence mis-match between SPASE and parsed TIME_RESOLUTION from CDF.");
    }
  }
  if (cadenceCDF !== undefined && cadenceData !== undefined ) {
    if (!util.sameDuration(cadenceCDF, cadenceData)) {
      util.warning(dsid, "Cadence mis-match between data sample and parsed TIME_RESOLUTION from CDF.");
    }
  }

  // Select
  if (cadenceSPASE !== undefined) {
    util.note(dsid, `Using cadence ${cadenceSPASE} from SPASE`);
    dataset['info']['cadence'] = cadenceSPASE;
  } else if (cadenceCDF !== undefined) {
    util.note(dsid, `Using cadence ${cadenceCDF} from parsed TIME_RESOLUTION from CDF`);
    dataset['info']['cadence'] = cadenceCDF;
  } else if (cadenceData !== undefined) {
    util.note(dsid, `Using cadence ${cadenceData} from sample data`);
    dataset['info']['cadence'] = cadenceData;
  } else {
    util.warning(dsid, 'No SPASE cadence or CDF attribute of TIME_RESOLUTION found to use for cadence.');
  }
}

function inferCadence(timeRecords, dsid) {

  let dts = [];
  for (r = 0; r < timeRecords.length - 1; r++) {
    // slice(0,24) keeps only to ms precision.
    //dt = new Date(timeRecords[r+1]['value'][0].slice(0,24)).getTime() 
    //   - new Date(timeRecords[r]['value'][0].slice(0,24)).getTime();
    dt = new Date(timeRecords[r+1]['value'][0]).getTime() 
       - new Date(timeRecords[r]['value'][0]).getTime();
    dts.push(dt);
  }

  dthist = histogram(dts);

  let cadenceData = undefined;

  let Nu = Object.keys(dthist).length;
  let Nr = timeRecords.length;
  if (Nu == 1) {
    let S = dthist[0][0];
    cadenceData = "PT" + S + "S";
    util.note(dsid, `Inferred cadence of ${cadenceData} based on ${Nr} records.`);
  } else {
    if (Nr > 1) {
      util.note(dsid, `Could not infer cadence from ${Nr} time record(s) because more than one (${Nu}) unique Δt values.`);
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
    if (dthist.length > n) {
      dtmsg += "...";
    }
    if (Nr > 1 && dtmsg !== "") {
      util.note(dsid, "Δt histogram: " + dtmsg);
    }
  }

  if (dthist.length > 0) {
    dthist.sort(function(a, b) {return a[0] - b[0];});
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

    // Sort on counts
    sorted.sort(function(a, b) {
      return b[1] - a[1];
    });
    return sorted;
  }
}
