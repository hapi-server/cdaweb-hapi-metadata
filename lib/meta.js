module.exports.meta = {
  "run": run
}
meta = module.exports.meta;

const argv = require('./cli.js').argv();
const {util} = require('./util.js');

function run(cb) {

  util.log(null, '\n*Preparing input files.*\n');

  run.finished = function (CATALOG) {
    for (let dataset of CATALOG["datasets"]) {
      let id = dataset['id'];
      let fnameCombined = util.baseDir(id) + "/" + id + "-combined.json";
      util.writeSync(fnameCombined, util.obj2json(dataset));
    }
    if (cb) {cb()}
  }

  getAllXML();
}

let logExt = "request";

function getAllXML() {

  // Request all.xml to get dataset names.
  // Could instead use
  //   https://cdaweb.gsfc.nasa.gov/WS/cdasr/1/dataviews/sp_phys/datasets
  // but this URL request takes > 60 seconds (all.xml takes < 0.1 s) and 
  // does not provide SPASE IDs (could still get SPASE IDs from data
  // request as it is in its metadata).

  let fnameAllXML  = argv.cachedir + '/all.xml';
  let CATALOG = {all: {url: argv.allxml}};
  util.get({"uri": argv.allxml, "id": null, "outFile": fnameAllXML},
    function (err, allObj) {
      CATALOG['datasets'] = createDatasets(allObj['json']);
      //util.debug(null, "CATALOG:", logExt);
      //util.debug(null, CATALOG, logExt);
      getFileLists1(CATALOG);
  });

  function createDatasets(json) {

    let allIds = [];
    let keptIds = [];
    let datasets_allxml = json['sites']['datasite'][0]['dataset'];
    let datasets = [];
    for (let dataset_allxml of datasets_allxml) {

      let id = dataset_allxml['$']['serviceprovider_ID'];

      allIds.push(id);

      let keep = util.idFilter(id, argv['keepids'],argv['omitids']);

      if (keep === false) continue;

      keptIds.push(id);

      let fnameAllXML = util.baseDir(id) + "/" + id + "-allxml.json";
      util.writeSync(fnameAllXML, util.obj2json(dataset_allxml));

      let startDate = dataset_allxml['$']['timerange_start'];
      startDate = startDate.replace(' ', 'T') + 'Z';

      let stopDate = dataset_allxml['$']['timerange_stop'];
      stopDate = stopDate.replace(' ', 'T') + 'Z';

      let dataset = {
                      "id": id,
                      "info": {"startDate": startDate, "stopDate": stopDate},
                      "_allxml": dataset_allxml,
                    }

      if (dataset_allxml['mastercdf']) {
        let _masters = {"cdf": {}, "skt": {}, "json": {}};
        let masterCDF = dataset_allxml['mastercdf'][0]['$']['ID'];
        _masters['cdf']['url'] = masterCDF;
        _masters['skt']['url'] = masterCDF
                                  .replace('0MASTERS', '0SKELTABLES')
                                  .replace('.cdf', '.skt');
        _masters['json']['url'] = masterCDF
                                  .replace('0MASTERS', '0JSONS')
                                  .replace('.cdf', '.json');
        dataset['_masters'] = _masters;
      } else {
        let emsg = "(CDAWeb) No mastercdf node in all.xml";
        util.error(id, id + emsg, false, logExt);
        dataset['_mastersError'] = emsg;
      }

      datasets.push(dataset);
    }

    let msgo  = `keepids = '${argv["keepids"]}' and `;
        msgo += `omitids = '${argv["omitids"]}' filter `;
    let msg   = msgo + `left ${datasets.length}/${datasets_allxml.length} datasets.`;

    util.debug(null, msg, logExt);

    let allIdsFile = argv["cachedir"] + '/ids-cdas.txt';
    util.writeSync(allIdsFile, allIds.join('\n'), 'utf8');

    let keptIdsFile = argv["cachedir"] + '/ids-cdas-processed.txt';
    util.writeSync(keptIdsFile, keptIds.join('\n'), 'utf8');

    if (datasets.length == 0) {
      let msg = msgo + `left 0/${datasets_allxml.length} datasets.`;
      util.error(null, msg, true, logExt);
    }

    return datasets;
  }
}

function getFileLists1(CATALOG) {

  if (argv['omit'].includes('files1')) {
    getInventories(CATALOG);
    return;
  }

  for (let dataset of CATALOG['datasets']) {

    let id = dataset['id'];
    let stop = dataset['info']['stopDate'];
    let start = dataset['info']['startDate'];

    let url = argv.cdasr + id + '/orig_data/' 
            + start.replace(/-|:/g, '')
            + ',' 
            + stop.replace(/-|:/g, '') 
            + '/';

    let fnameFiles1 = util.baseDir(id) + "/" + id + "-files1.json";
    let reqObj = {
                    "uri": url,
                    "id": id,
                    "outFile": fnameFiles1,
                    "headers": {"Accept": "application/json"},
                    "parse": true
                  };
    util.get(reqObj, (err, json) => {
      if (err) {
        util.error(id, [url, 'Error message', err.message], false, logExt);
      }

      if (!json['FileDescription']) {
        let emsg = url + '\nNo FileDescription in returned JSON:' 
                 + JSON.stringify(json,null,2);
        dataset['_files1Error'] = emsg;
        util.error(id, emsg, false, logExt);
        finished();
        return;
      }

      let sum = 0;
      for (let description of json['FileDescription']) {
        sum += description['Length']; 
      }

      dataset['_files1Last'] = json['FileDescription'].slice(-1)[0];
      dataset['_files1First'] = json['FileDescription'][0];
      dataset['_files1Sum'] = sum;
      dataset['_files1'] = fnameFiles1;
      finished();
    });
  }

  function finished() {
    if (finished.N == undefined) {finished.N = 0;}
    finished.N = finished.N + 1;
    if (finished.N == CATALOG['datasets'].length) {
      getInventories(CATALOG);
    }
  }  
}

function getInventories(CATALOG) {

  if (argv['omit'].includes('inventory')) {
    getMasters(CATALOG);
    return;
  }

  for (let dataset of CATALOG['datasets']) {

    let id = dataset['id'];
    let url = argv.cdasr + dataset['id'] + '/inventory/';
    let fnameInventory = util.baseDir(id) + "/" + id + "-inventory.json";
    let headers = {"Accept": 'application/json'};
    util.get({"uri": url,
              "id": id,
              "outFile": fnameInventory,
              "headers": headers,
              "parse": true
            },
      (err, obj) => {
        if (err) {
          let emsg = [url, 'Error message', err.message];
          util.error(id, emsg, false, logExt);
          dataset['_inventoryError'] = emsg;
          finished();
        }
        dataset['_inventory'] = fnameInventory;
        finished();
    });
  }

  function finished() {
    if (finished.N === undefined) {finished.N = 0;}
    finished.N = finished.N + 1;
    if (finished.N == CATALOG['datasets'].length) {
      getMasters(CATALOG);
    }
  }  
}

function getMasters(CATALOG) {

  // This function is no longer needed given that we are obtaining
  // metadata from a request for all parameters in each dataset
  // through a web service call. In this case, the web service
  // has already applied the supplementary metadata in the masters
  // to the metadata stored in the raw cdf files.

  if (argv['omit'].includes('masters')) {
    getVariables(CATALOG);
    return;
  }

  let datasets = CATALOG['datasets'];
  for (let dataset of datasets) {

    let id = dataset['id']; 

    if (dataset['_mastersError'] !== undefined) {
      finished();finished();finished();
      continue;
    }

    let urlCDF = dataset['_masters']['cdf']['url'];
    let fnameMasterCDF = util.baseDir(id) + "/" + urlCDF.split('/').slice(-1);
    util.get({"uri": urlCDF, "id": id, "outFile": fnameMasterCDF, "parse": false},
      (err) => {
        if (err) {
          let emsg = [urlCDF, 'Error message', err.message];
          util.error(id, emsg, false, logExt);
          dataset['_masters']['cdfError'] = emsg;
          finished();
          return;
        }
        dataset['_masters']['cdf'] = fnameMasterCDF;
        finished();
    });

    let urlSKT = dataset['_masters']['skt']['url'];
    let fnameMasterSKT = util.baseDir(id) + "/" + urlSKT.split('/').slice(-1);
    util.get({"uri": urlSKT, id: id, "outFile": fnameMasterSKT, "parse": false},
      (err) => {
        if (err) {
          util.error(id, [urlSKT, 'Error message', err], false, logExt);
          dataset['_masters']['sktError'] = [urlSKT, 'Error message', JSON.stringify(err, null, 2)];
          finished();
        }
        dataset['_masters']['skt'] = fnameMasterSKT;
        finished();
    });

    let urlJSON = dataset['_masters']['json']['url'];
    let fnameMasterJSON = util.baseDir(id) + "/" + urlJSON.split('/').slice(-1);
    util.get({uri: urlJSON, id: id, outFile: fnameMasterJSON},
      (err, json) => {
        if (err) {
          util.error(id, [urlSKT, err], false, logExt);
          dataset['_masters']['sktError'] = [urlSKT, 'Error message', JSON.stringify(err, null, 2)];
          return;
        }
        json = json[Object.keys(json)[0]];
        dataset['_masters']['json'] = json;
        finished();
    });
  }

  function finished() {
    if (finished.N == undefined) {finished.N = 0;}
    finished.N = finished.N + 1;
    // 3*datsets.length b/c SKT, JSON, and CDF all downloaded
    if (finished.N == 3*CATALOG['datasets'].length) {
      //datasets = datasets.filter(function (el) {return el != null;});
      //util.debug(null, 'catalog after getMasters():', logExt);
      //util.debug(null, CATALOG, logExt);
      //getVariables(CATALOG);
      //getSPASERecords(CATALOG);
      console.log(CATALOG['datasets'][0])
    }
  }
}

function getVariables(CATALOG) {

  // Call /variables endpoint to get list of variables for each dataset.
  // Then call variableDetails() to get additional metadata for variables
  // by making a request to /data.

  for (let dataset of CATALOG['datasets']) {
    let id = dataset['id'];
    let url = argv.cdasr + dataset['id'] + '/variables';
    let fnameVariables = util.baseDir(id) + '/' + id + '-variables.json';
    requestVariables(url, fnameVariables, dataset);
  }

  function requestVariables(url, fnameVariables, dataset) {
    let id = dataset['id'];
    let headers = {"Accept": 'application/json'};
    let opts = {uri: url, "headers": headers, id: id, outFile: fnameVariables};
    util.get(opts, function (err, variables) {
      if (err) {
        util.error(id, [url, err], false, logExt);
        dataset['_variablesError'] = "Could not get " + url;
        finished();
        return;
      }
      dataset['_variables'] = variables;
      finished();
    });
  }

  function finished() {
    if (finished.N === undefined) finished.N = 0;
    finished.N = finished.N + 1;
    if (finished.N == CATALOG['datasets'].length) {
      getVariableData(CATALOG);
    }
  }
}

function getVariableData(CATALOG) {

  // Call /data endpoint to get CDFML with data for all variables in each dataset.

 for (let dataset of CATALOG['datasets']) {
    requestData(dataset);
  }

  function requestData(dataset) {

    if (dataset['_variables']['_variablesError']) {
      dataset['_dataError'] = "Cannot get data b/c /variables Error.";
      finished();
      return;
    }

    if (dataset['_files1Error']) {
      dataset['_dataError'] = "Cannot get data b/c files1 Error.";
      finished();
      return;
    }

    let names = [];
    let variableDescriptions = dataset['_variables']['VariableDescription'];
    for (let variableDescription of variableDescriptions) {
      names.push(variableDescription['Name']);
    }
    names = names.join(',');

    let id = dataset['id'];
    let fnameFileDescription = util.baseDir(id) + "/" + id + '-filedescription.json';

    //let seconds = 86400;
    let start  = dataset['_files1Last']['StartTime'];
    start = util.incrementTime(start, 0, 'seconds'); // To format
    let stop  = dataset['_files1Last']['EndTime'];
    stop = util.incrementTime(stop, 0, 'seconds'); // To format

    let dt = new Date(stop).getTime() - new Date(start).getTime();
    //log(start, stop, dt)
    if (dt > 86400*1000*10) { // > 10 days
      stop = util.incrementTime(start, 86400*10, 'seconds');
    }
    util.debug(null, ["Request (or cached file) time range derived from", dataset['_files1Last']]);

    let knownMimes = ['application/x-netcdf', 'application/x-cdf'];
    if (!knownMimes.includes(dataset['_files1Last']['MimeType'])) {
      let msg = "Dataset has files that do not have MimeType = 'application/x-cdf' ";
      msg +=  `(found ${dataset['_files1Last']['MimeType']}")`;
      dataset['_dataError'] = msg;
      util.error(id, dataset['_dataError'], false, logExt);
      finished();
      return;
    }

    start = start.replace(/-|:|\.[0-9].*/g,'')
    stop = stop.replace(/-|:|\.[0-9].*/g,'')

    //let url = argv.cdasr + id + '/data/' + start + ',' + stop + '/' + names + '?format=cdf';
    let url1 = argv.cdasr + id + '/data/' + start + ',' + stop 
            + '/ALL-VARIABLES?format=cdf'
    let reqOpts = {
                    uri: url1,
                    headers: {Accept: 'application/json'},
                    id: id,
                    outFile: fnameFileDescription,
                    parse: true,
                    maxFileAge: 0,
                    maxHeadAge: 0
                  };
    util.get(reqOpts, function (err, obj) {

      let epre  = ["Problem with url ", url1];
      let epost =  ["Request time range derived from", JSON.stringify(dataset['_files1Last'],null,2)]
      if (err) {
        let emsg = [...epre, err.message || err, ...epost];
        dataset['_dataError'] = emsg;
        util.error(id, dataset['_dataError'], false, logExt);
        finished();
        return;
      }
      if (!obj['FileDescription']) {
        let emsg = [...epre, "No FileDescription node in response", ...epost];
        dataset['_dataError'] = emsg;
        util.error(id, dataset['_dataError'], false, logExt);
        finished();
        return;
      }
      let url2 = obj['FileDescription'][0]['Name'];
      let base = url2.split("/").slice(-1)[0].replace(".cdf","");
      let fnameCDF = util.baseDir(id) + "/" + base + '-cdas.cdf';
      let reqOpts = {
                      uri: url2,
                      id: id,
                      encoding: null,
                      outFile: fnameCDF,
                      parse: true,
                      maxAge: 3600*12
                    };

      // TODO: If these files get large, will need to switch to piping to file.
      util.get(reqOpts, function (err, obj) {
        if (err) {
          if (!Array.isArray(err)) {err = [err];}
          dataset['_dataError'] = [
                    "Problem with",
                    url2, 
                    "which was referenced in response to",
                    url1,
                    err.message || err, 
                    "Request time range derived from", 
                    JSON.stringify(dataset['_files1Last'],null,2)
                  ];
          console.log(dataset['_dataError']);
          util.error(id, dataset['_dataError'], false, logExt);
          finished();
          return;
        }
        dataset['_data'] = obj['file'];
        finished();
      });
    });
  }

  function finished() {
    if (finished.N === undefined) finished.N = 0;
    finished.N = finished.N + 1;
    if (finished.N == CATALOG['datasets'].length) {
      getSPASERecords(CATALOG);
    }
  }
}

function getSPASERecords(CATALOG) {

  if (argv['omit'].includes('spase')) {
    run.finished(CATALOG);
    return;
  }

  let datasets = CATALOG['datasets'];
  for (let dataset of datasets) {

    let resourceID1 = undefined;
    dataset['_spaseError'] = [];

    if (dataset['_masters']) {
      if (dataset['_masters']['json']['CDFglobalAttributes']) {
        for (let obj of dataset['_masters']['json']['CDFglobalAttributes']) {
          if (obj['spase_DatasetResourceID']) {
            resourceID1 = obj['spase_DatasetResourceID'][0]['0'];
            break;
          }
        }
        if (resourceID1 && !resourceID1.startsWith('spase://')) {
          let msg = `spase_DatasetResourceID in Master JSON '${resourceID1}' does not start with 'spase://'.`;
          dataset['_spaseError'].push(msg);
          resourceID1 = undefined;
        }
      }
    }

    let resourceID2 = undefined;
    if (dataset['_data']) {
      let cdfml = util.readSync(dataset['_data']);
      if (cdfml) {
        let _data = JSON.parse(cdfml)['CDF'][0];
        resourceID2 = getGAttribute(_data['cdfGAttributes'], 'SPASE_DATASETRESOURCEID');
        if (resourceID2 && !resourceID2.startsWith('spase://')) {
          let msg = `SPASE_DATASETRESOURCEID in CDF '${resourceID1}' does not start with 'spase://'.`;
          dataset['_spaseError'].push(msg);
          resourceID2 = undefined;
        }
      }
    }

    if (resourceID1 && resourceID2) {
      if (resourceID1 !== resourceID2) {
        let msg = "Mismatch between spase_DatasetResourceID in Master JSON "
        msg += "'${resourceID1}' and SPASE_DATASETRESOURCEID in CDF '${resourceID2}'";
        dataset['_spaseError'].push(msg)
      }
    }

    if (!resourceID1 && !resourceID2) {
      finished(dataset, null);
      continue;  
    }

    let resourceID = resourceID1 || resourceID2
    let spaseURL = resourceID.replace('spase://', 'https://hpde.io/') + '.xml';
    let spaseFile = argv.cachedir + '/' +
                    dataset['id'].split('_')[0] + '/' +
                    dataset['id'] + '/' +
                    dataset['id'] + '-spase.xml';
    getSPASERecord(dataset, spaseURL, spaseFile);
  }

  function getSPASERecord(dataset, url, outFile) {

    let reqObj = {
      "uri": url,
      "id": dataset['id'],
      "outFile": outFile
    }

    util.get(reqObj, (err, obj) => {
      if (err) {
        obj = null;
        let emsg = [url, err.message || err];
        util.error(dataset['id'], emsg, false, logExt);
        dataset['_spaseError'].push(emsg)
      }
      if (obj !== null && !obj['json']) {
        obj = null;
        let emsg = `Problem with ${url}. HTML returned?`;
        dataset['_spaseError'].push(emsg);
        util.rmSync(outFile);
      }
      if (obj !== null) {
        dataset['_spase'] = obj['json']["Spase"];
      }
      finished();
    });
  }

  function finished() {
    if (finished.N === undefined) finished.N = 0;
    finished.N = finished.N + 1;
    if (finished.N == CATALOG['datasets'].length) {
      run.finished(CATALOG);
    }
  }

}

////////////////////////////////////

function getGAttribute(cdfGAttributes, attributeName) {
  for (let attribute of cdfGAttributes['attribute']) {
    if (attribute['name'] === attributeName) {
      return attribute['entry'][0]['value'];
    }
  }
  return null;
}
