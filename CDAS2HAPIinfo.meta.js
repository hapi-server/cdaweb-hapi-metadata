module.exports.meta = {
  "run": run
}
meta = module.exports.meta;

const argv = require('./CDAS2HAPIinfo.cli.js').argv();
const {util} = require('./CDAS2HAPIinfo.util.js');

function run(cb) {

  util.log(null, '\n*Preparing input files.*\n', "", null);

  run.finished = function (CATALOG) {
    for (dataset of CATALOG["datasets"]) {
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
  //  https://cdaweb.gsfc.nasa.gov/WS/cdasr/1/dataviews/sp_phys/datasets
  // but this URL request takes > 60 seconds (all.xml takes < 0.1 s) and 
  // does not provide SPASE IDs (could still get SPASE IDs from data
  // request as it is in its metadata).

  let fnameAllXML  = argv.cachedir + '/all.xml';
  let fnameAllJSON = fnameAllXML + '.json';
  let CATALOG = {all: {url: argv.allxml}};
  util.get({uri: argv.allxml, id: null, outFile: fnameAllXML},
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
      let mastercdf, masterskt, masterjson;
      if (dataset_allxml['mastercdf']) {
        mastercdf = dataset_allxml['mastercdf'][0]['$']['ID'];
        masterskt = mastercdf
                          .replace('0MASTERS', '0SKELTABLES')
                          .replace('.cdf', '.skt');
        masterjson = mastercdf
                          .replace('0MASTERS', '0JSONS')
                          .replace('.cdf', '.json');
      } else {
        util.error(id, id + ": No mastercdf given in all.xml", false, logExt);
      }

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

      datasets.push(
        {
          "id": id,
          "info": {"startDate": startDate, "stopDate": stopDate},
          "_allxml": dataset_allxml,
          "_masters": {
            "cdf": {"url": mastercdf},
            "skt": {"url": masterskt},
            "json": {"url": masterjson}
        }
      });
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
        util.error(id, [url, 'Error message', err], false, logExt);
      }

      if (!json['FileDescription']) {
        let emsg = url + '\nNo FileDescription in returned JSON:' + JSON.stringify(json,null,2);
        dataset['_files1Error'] = emsg;
        util.error(id, emsg, false, logExt);
        finished(err);
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
      finished(err);
    });
  }

  function finished(err) {
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
    util.get({"uri": url, id: id, "outFile": fnameInventory, "headers": headers, "parse": true},
      (err, obj) => {
        if (err) {
          util.error(id, [url, 'Error message', err], false, logExt);
        }
        dataset['_inventory'] = fnameInventory;
        finished(err);
    });
  }

  function finished(err) {
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

    let urlCDF = dataset['_masters']['cdf']['url'];
    let fnameMasterCDF = util.baseDir(id) + "/" + urlCDF.split('/').slice(-1);
    util.get({"uri": urlCDF, id: id, "outFile": fnameMasterCDF, "parse": false},
      (err, obj) => {
        if (err) {
          util.error(id, [urlCDF, 'Error message', err], false, logExt);
        }
        dataset['_masters']['cdf'] = fnameMasterCDF;
        finished(err);
    });

    let urlSKT = dataset['_masters']['skt']['url'];
    let fnameMasterSKT = util.baseDir(id) + "/" + urlSKT.split('/').slice(-1);
    util.get({"uri": urlSKT, id: id, "outFile": fnameMasterSKT, "parse": false},
      (err, text) => {
        if (err) {
          util.error(id, [urlSKT, err], false, logExt);
        }
        dataset['_masters']['skt'] = fnameMasterSKT;
        finished(err);
    });

    let urlJSON = dataset['_masters']['json']['url'];
    let fnameMasterJSON = util.baseDir(id) + "/" + urlJSON.split('/').slice(-1);
    util.get({uri: urlJSON, id: id, outFile: fnameMasterJSON},
      (err, json) => {
        if (err) {
          util.error(id, [urlJSON, err], false, logExt);
          return;
        }
        json = json[Object.keys(json)[0]];
        dataset['_masters']['json'] = json;
        finished(err);
    });
  }

  function finished(err) {
    if (finished.N == undefined) {finished.N = 0;}
    finished.N = finished.N + 1;
    // 3*datsets.length b/c SKT, JSON, and CDF all downloaded
    if (finished.N == 3*CATALOG['datasets'].length) {
      //datasets = datasets.filter(function (el) {return el != null;});
      //util.debug(null, 'catalog after getMasters():', logExt);
      //util.debug(null, CATALOG, logExt);
      getVariables(CATALOG);
    }
  }
}

function getVariables(CATALOG) {

  // Call /variables endpoint to get list of variables for each dataset.
  // Then call variableDetails() to get additional metadata for variables
  // by making a request to /data.

  let datasets = CATALOG['datasets'];
  for (let dataset of datasets) {
    let id = dataset['id'];
    let url = argv.cdasr + dataset['id'] + '/variables';
    let fnameVariables = util.baseDir(id) + '/' + id + '-variables.json';
    requestVariables(url, fnameVariables, dataset);
  }

  function requestVariables(url, fnameVariables, dataset) {
    let id = dataset['id'];
    let headers = {"Accept": 'application/json'};
    let opts = { uri: url, "headers": headers, id: id, outFile: fnameVariables };
    util.get(opts, function (err, variables) {
      if (err) {
        util.error(id, [url, err], false, logExt);
        finished(err);
        return;
      }
      dataset['_variables'] = variables;
      finished(err);
    });
  }

  function finished(err) {
    if (finished.N === undefined) finished.N = 0;
    finished.N = finished.N + 1;
    if (finished.N == datasets.length) {
      getVariableDataNew(CATALOG);
    }
  }
}

function getVariableDataNew(CATALOG) {

  // Call /data endpoint to get CDFML with data for all variables in each dataset.

  let datasets = CATALOG['datasets'];

  for (let ididx = 0; ididx < datasets.length; ididx++) {
    let names = [];
    let variableDescriptions = datasets[ididx]['_variables']['VariableDescription'];
    for (let variableDescription of variableDescriptions) {
      names.push(variableDescription['Name']);
    }
    names = names.join(',');
    requestData(ididx, names);
  }

  function requestData(ididx, names) {

    let id = datasets[ididx]['id'];
    let fnameFileDescription = util.baseDir(id) + "/" + id + '-filedescription.json';

    let seconds = 86400;
    let start  = datasets[ididx]['_files1Last']['StartTime'];
    start = util.incrementTime(start, 0, 'seconds'); // Formats
    let stop = util.incrementTime(start, seconds, 'seconds')

    start = start.replace(/-|:|\.[0-9].*/g,'')
    stop = stop.replace(/-|:|\.[0-9].*/g,'')
    util.log(id, `If needed, requesting ${id} over ${seconds} second time range starting at ${start}.`, "", null, logExt);

    let url = argv.cdasr + id + '/data/' + start + ',' + stop + '/' + names + '?format=cdf';
    let reqOpts = {
                    uri: url,
                    headers: {Accept: 'application/json'},
                    id: id,
                    outFile: fnameFileDescription,
                    parse: true,
                    maxFileAge: 0
                  };
    util.get(reqOpts, function (err, obj) {
      let url = obj['FileDescription'][0]['Name'];
      let fnameCDF = util.baseDir(id) + "/" + url.split("/").slice(-1)[0].replace(".cdf","") + '-cdas.cdf';
      let reqOpts = {
                      uri: url,
                      id: id,
                      encoding: null,
                      outFile: fnameCDF,
                      parse: true,
                      maxAge: 3600*12
                    };
      // TODO: If these files get large, will need to switch to piping to file.
      util.get(reqOpts, function (err, obj) {
        if (err) {
          util.error(id, err, true, logExt);
          finished(err);
          return;
        }
        datasets[ididx]['_data'] = obj['file'];
        finished(err);
      });
    });
  }
  function finished(err) {
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

    let resourceID = "";
    let emsg = "";
    if (!dataset['_masters']['json']['CDFglobalAttributes']) {
      emsg = "No SPASE in CDFglobalAttributes in Master JSON. ";
    } else {
      for (let obj of dataset['_masters']['json']['CDFglobalAttributes']) {
        if (obj['spase_DatasetResourceID']) {
          resourceID = obj['spase_DatasetResourceID'][0]['0'];
          break;
        }
      }
    }

    //console.log(`${dataset['id']}: Reading ${dataset['_data']}`)
    if (dataset['_data']) {
      let cdfml = util.readSync(dataset['_data']);
      if (resourceID === "" && cdfml) {
        _data = JSON.parse(cdfml)['CDF'][0];
        resourceID = getGAttribute(_data['cdfGAttributes'], 'SPASE_DATASETRESOURCEID');
        if (!resourceID) {
          emsg += "No SPASE_DATASETRESOURCEID in sample CDF file. ";
          finished(dataset, null, emsg);
          continue;
        }
      }
    } else {
      emsg += "No sample CDF file available to search for SPASE_DATASETRESOURCEID. ";
    }

    if (resourceID !== "" && !resourceID.startsWith('spase://')) {
      emsg += `SPASE id of '${resourceID}' does not start with 'spase://'.`;
      finished(dataset, null, emsg);
      return;
    }

    let spaseURL = resourceID.replace('spase://', 'https://hpde.io/') + '.xml';
    let spaseFile = argv.cachedir + '/' +
                    dataset['id'].split('_')[0] + '/' +
                    dataset['id'] + '/' +
                    dataset['id'] + '-spase.xml';
    getSPASERecord(dataset, spaseURL, spaseFile);
  }

  function finished(dataset, obj, emsg) {
    if (!finished.N) finished.N = 0;
    finished.N = finished.N + 1;
    if (emsg !== '') {
      dataset['_spaseError'] = emsg;
    }
    if (obj !== null) {
        dataset['_spase'] = obj['json']["Spase"];
    }
    if (finished.N == CATALOG['datasets'].length) {
      run.finished(CATALOG);
    }
  }

  function getSPASERecord(dataset, url, outFile) {
    util.get({uri: url, id: dataset['id'], outFile: outFile}, (err, obj) => {
      let emsg = "";
      if (err) {
        util.error(dataset['id'], [url, err], false, logExt);
        emsg = "Failed to fetch " + url;
        obj = null;
      }
      if (!obj['json']) {
        emesg = `Problem with ${url}. HTML returned?`;
        obj = null;
        util.rmSync(outFile);
      }
      finished(dataset, obj, emsg);
    });
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
