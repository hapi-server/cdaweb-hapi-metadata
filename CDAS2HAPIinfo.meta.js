module.exports.meta = {
  "run": run
}
meta = module.exports.meta;

function run(cb) {

  util.log('\n*Reading input files.*\n');

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

function getAllXML() {

  // Request all.xml to get dataset names.
  // Could instead use
  //  https://cdaweb.gsfc.nasa.gov/WS/cdasr/1/dataviews/sp_phys/datasets
  // but this URL request takes > 60 seconds (all.xml takes < 0.1 s) and 
  // does not provide SPASE IDs (could still get SPASE IDs from data
  // request as it is in its metadata).

  let fnameAllXML  = meta.argv.cachedir + '/all.xml';
  let fnameAllJSON = fnameAllXML + '.json';

  let CATALOG = {all: {url: meta.argv.allxml}};
  util.get({uri: meta.argv.allxml, outFile: fnameAllXML},
    function (err, allObj) {
      datasets = createDatasets(allObj['json']);
      CATALOG['datasets'] = datasets;
      util.log.debug("CATALOG['all']:");
      util.log.debug(CATALOG);
      util.log.debug("CATALOG['datasets']:");
      util.log.debug(CATALOG['datasets']);
      getFileLists0(CATALOG);
  });  
}

function getFileLists0(CATALOG) {

  // This function is not needed b/c getFileLists1() does
  // the same thing. Could use results from this function
  // as a check, however. This function was written before
  // it was realized that there was a CDAS query that
  // provided similar information.

  if (meta.argv['omit'].includes('files0')) {
    getFileLists1(CATALOG);
    return;
  }

  for (let dataset of CATALOG['datasets']) {
    let rootURL = dataset['_allxml']['access'][0]['URL'][0];
    if (!rootURL.endsWith('/')) {
      rootURL = rootURL + '/';
    }
    getFileList(dataset, rootURL);
  }

  function getFileList(dataset, rootURL) {

    const cheerio = require('cheerio');

    let id = dataset['id'];
    let inventory = {};
    inventory[id] = {};
    let inventory_flat = [];

    if (getFileList.finished === undefined) {getFileList.finished = 0;}
    util.log.debug(`Started a dir walk for ${id}; # of walks left = ${getFileList.finished}`);

    let outFile = meta.argv.cachedir + '/' + id.split('_')[0] + '/' + id + '/files0/index.html';
    getDirIndex(id, rootURL, outFile, inventory[id]);

    function finished(id) {

      getDirIndex[id].started = getDirIndex[id].started - 1;
      util.log.debug(`Finished a dir listing for ${id}; # left = ${getDirIndex[id].started}`);

      if (getDirIndex[id].started == 0) {

        util.log.debug('Finished walking dirs for ' + id);
        getFileList.finished = getFileList.finished + 1;

        let inventoryJSON0 = util.baseDir(id) + '/' + id + '-files0-tree.json';
        util.writeSync(inventoryJSON0, util.obj2json(inventory));

        let inventoryJSON1 = util.baseDir(id) + '/' + id + '-files0.json';

        inventory_flat_split = [];
        let _files0Size = 0;
        for (line of inventory_flat) {
          inventory_flat_split.push(line.split(","));
          _files0Size += parseFloat(inventory_flat_split.slice(-1)[0][3]);
        }
        util.writeSync(inventoryJSON1, util.obj2json(inventory_flat_split));

        let inventoryCSV = util.baseDir(id) + '/' + id + '-files0.csv';
        util.writeSync(inventoryCSV, inventory_flat.join('\n'));

        dataset['_files0Size'] = _files0Size;
        dataset['_files0'] = inventoryJSON1;
        if (getFileList.finished == CATALOG['datasets'].length) {
          util.log.debug('Finished dir walk for all datasets');
          getFileLists1(CATALOG);
        }
      }
    }

    function getDirIndex(id, url, outFile, parent) {

      if (getDirIndex[id] === undefined) getDirIndex[id] = {};
      if (getDirIndex[id].started === undefined) getDirIndex[id].started = 0;
      getDirIndex[id].started = getDirIndex[id].started + 1;

      let reqOpts = {uri: url, outFile: outFile};
      util.get(reqOpts, (err, html) => {
        parent['files'] = [];
        parent['dirs'] = {};
        if (err) {
          util.error(null, [url, err], false);
          finished(id);
          return;
        }
        const $ = cheerio.load(html);
        $('tr').each((i, elem) => {
          // .each is sync so no callback needed.
          // May break if Apache dir listing HTML changes.
          // First three links are not relevant.

          let cols = $(elem).find('td');
          let href = $($(cols[0]).find('a')[0]).attr('href');
          // i < 2 b/c first two rows are header and "Parent Directory"
          if (!href || i < 2) return;
          let mtime = $(cols[1]).text().replace(/([0-9]) ([0-9])/, '$1T$2').trim() + 'Z';
          let size = $(cols[2]).text().trim();
          size = size
                  .replace("E","e18")
                  .replace("P","e15")
                  .replace("T","e12")
                  .replace("G","e9")
                  .replace("M","e6")
                  .replace("K","e3")
                  .replace("B","");

          if (href.endsWith('.cdf')) {
            let fileDate = href.replace(/.*([0-9]{4})([0-9]{2})([0-9]{2}).*/,'$1-$2-$3Z');
            let fileVersion = href.replace(/.*_v(.*)\.cdf/, '$1');
            parent['files'].push([fileDate, url + href, mtime, size, fileVersion]);
            inventory_flat.push(`${fileDate},${url + href},${mtime},${size},${fileVersion}`);
          }
          if (href.endsWith('/')) {
            let subdir = href.replace('/', '');
            let newOutDir = outFile.split('/').slice(0, -1).join('/') + '/' + href;
            let newOutFile = newOutDir + 'index.html';
            parent['dirs'][subdir] = {};
            getDirIndex(id, url + href, newOutFile, parent['dirs'][subdir]);
          }
        });

        let inventoryFile = outFile.replace('.html', '.json');
        util.writeSync(inventoryFile, util.obj2json(parent));
        finished(id);
      });
    }
  }
}

function getFileLists1(CATALOG) {

  if (meta.argv['omit'].includes('files1')) {
    getInventories(CATALOG);
  }

  for (dataset of CATALOG['datasets']) {

    let id = dataset['id'];
    let stop = dataset['info']['stopDate'];
    let start = dataset['info']['startDate'];

    let url = meta.argv.cdasr + id + '/orig_data/' 
            + start.replace(/-|:/g, '')
            + ',' 
            + stop.replace(/-|:/g, '') 
            + '/';

    let fnameCoverage = util.baseDir(id) + "/" + id + "-files1.json";
    let headers = {"Accept": "application/json"};
    util.get({"uri": url, "outFile": fnameCoverage, "headers": headers, "parse": true},
      (err, json) => {
        if (err) {
          util.error(id, [url, 'Error message', err], false);
        }
        dataset['_files1'] = fnameCoverage;
        finished(err);
    });
  }

  function finished(err) {
    if (finished.N == undefined) {finished.N = 0;}
    finished.N = finished.N + 1;
    if (finished.N == datasets.length) {
      //datasets = datasets.filter(function (el) {return el != null;});
      util.log.debug('catalog after masters:');
      util.log.debug(CATALOG);
      getInventories(CATALOG);
    }
  }  
}

function getInventories(CATALOG) {

  if (meta.argv['omit'].includes('inventory')) {
    getMasters(CATALOG);
  }

  for (dataset of CATALOG['datasets']) {

    let id = dataset['id'];
    let url = meta.argv.cdasr + dataset['id'] + '/inventory/';
    let fnameInventory = util.baseDir(id) + "/" + id + "-inventory.json";
    let headers = {"Accept": 'application/json'};
    util.get({"uri": url, "outFile": fnameInventory, "headers": headers, "parse": true},
      (err, obj) => {
        if (err) {
          util.error(id, [url, 'Error message', err], false);
        }
        dataset['_inventory'] = obj["json"];
        finished(err);
    });
  }

  function finished(err) {
    if (finished.N == undefined) {finished.N = 0;}
    finished.N = finished.N + 1;
    if (finished.N == datasets.length) {
      //datasets = datasets.filter(function (el) {return el != null;});
      util.log.debug('catalog after masters:');
      util.log.debug(CATALOG);
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

  if (meta.argv['omit'].includes('masters')) {
    getVariables(CATALOG);
  }

  let datasets = CATALOG['datasets'];

  for (dataset of datasets) {

    let id = dataset['id']; 

    let urlCDF = dataset['_masters']['cdf']['url'];
    let fnameMasterCDF = util.baseDir(id) + "/" + urlCDF.split('/').slice(-1);
    util.get({"uri": urlCDF, "outFile": fnameMasterCDF, "parse": false},
      (err, obj) => {
        if (err) {
          util.error(id, [urlCDF, 'Error message', err], false);
        }
        dataset['_masters']['cdf'] = fnameMasterCDF;
        finished(err);
    });

    let urlSKT = dataset['_masters']['skt']['url'];
    let fnameMasterSKT = util.baseDir(id) + "/" + urlSKT.split('/').slice(-1);
    util.get({"uri": urlSKT, "outFile": fnameMasterSKT},
      (err, text) => {
        if (err) {
          util.error(id, [urlSKT, err], false);
        }
        dataset['_masters']['skt'] = fnameMasterSKT;
        finished(err);
    });

    let urlJSON = dataset['_masters']['json']['url'];
    let fnameMasterJSON = util.baseDir(id) + "/" + urlJSON.split('/').slice(-1);
    util.get({uri: urlJSON, outFile: fnameMasterJSON},
      (err, json) => {
        if (err) {
          util.error(id, [urlJSON, err], false);
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
    if (finished.N == 3*datasets.length) {
      //datasets = datasets.filter(function (el) {return el != null;});
      util.log.debug('catalog after masters:');
      util.log.debug(CATALOG);
      getVariables(CATALOG);
    }
  }
}

function getVariables(CATALOG) {

  // Call /variables endpoint to get list of variables for each dataset.
  // Then call variableDetails() to get additional metadata for variables
  // by making a request to /data.

  let datasets = CATALOG['datasets'];
  for (dataset of datasets) {
    let id = dataset['id'];
    let url = meta.argv.cdasr + dataset['id'] + '/variables';
    let fnameVariables = util.baseDir(id) + '/' + id + '-variables.json';
    requestVariables(url, fnameVariables, dataset);
  }

  function requestVariables(url, fnameVariables, dataset) {
    let id = dataset['id'];
    let headers = {"Accept": 'application/json'};
    let opts = { uri: url, "headers": headers, outFile: fnameVariables };
    util.get(opts, function (err, variables) {
      if (err) {
        util.error(id, [url, err], false);
        finished(err);
        return;
      }
      dataset['_variables'] = variables;
      finished(err);
    });
  }

  function finished(err) {
    if (!finished.N) {
      finished.N = 0;
    }
    finished.N = finished.N + 1;

    if (finished.N == datasets.length) {
      //datasets = datasets.filter(function (el) {return el != null;});
      util.log.debug('datasets after requestVariables:');
      util.log.debug(datasets);
      getVariableDetails(CATALOG);
    }
  }
}

function getVariableDetails(CATALOG) {

  let datasets = CATALOG['datasets'];
  // Call /variables endpoint to get CDFML with data for all variables
  // in each dataset.

  for (let ididx = 0; ididx < datasets.length; ididx++) {
    let names = [];
    let variableDescriptions = datasets[ididx]['_variables']['VariableDescription'];
    for (let variableDescription of variableDescriptions) {
      names.push(variableDescription['Name']);
    }
    names = names.join(',');
    requestVariableDetails(ididx, names);
  }

  function requestVariableDetails(ididx, names, seconds, reason) {

    // TODO: Reconsider using command line call to 'HAPIdata.js --lib
    // cdflib ...' (which calls cdf2csv.py) to get data from first 
    // file in _files0 or _files1 in order to find time range for
    // sample{Start,Stop}. 

    let id = datasets[ididx]['id'];

    if (seconds === undefined) {
      seconds = 100;
    }

    if (!requestVariableDetails.tries) {
      requestVariableDetails.tries = {};
    }
    if (!requestVariableDetails.tries[ididx]) {
      requestVariableDetails.tries[ididx] = 0;
    }
    requestVariableDetails.tries[ididx] += 1;
    let tries = requestVariableDetails.tries[ididx];

    let fnameCDFML = util.baseDir(id) + "/" + id + '-cdas.json';

    if (util.existsSync(fnameCDFML) && tries == 1) {
      util.log(`Reading: ${fnameCDFML}`);
      let body = util.readSync(fnameCDFML);
      finished(ididx, names, fnameCDFML, JSON.parse(body), null);
      return;
    }

    if (tries > 1) {util.rmSync(fnameCDFML);}

    //let start = datasets[ididx]['info']['startDate'];
    //let stop = util.incrementTime(start, seconds, 'seconds');
    let stop = datasets[ididx]['info']['stopDate'];
    let start = util.decrementTime(stop, seconds, 'seconds');

    if (reason === undefined) {
      util.log(`- Trying ${id} (attempt # ${tries}/${meta.argv['maxtries']})`);
      util.log(`- Requesting ${id} over ${seconds} second time range starting at ${start}.`);
    } else {
      util.log(`- Retrying ${id} (attempt # ${tries}/${meta.argv['maxtries']}) because: ${reason}`);
      util.log(`- Requesting ${id} over ${seconds} second time range starting at ${start}.`);      
    }

    let url = meta.argv.cdasr + id + '/variables';
    url = meta.argv.cdasr + id + '/data/' + start.replace(/-|:/g, '') + ',' +
          stop.replace(/-|:/g, '') + '/' + names + '?format=json';

    let headers = {Accept: 'application/json'};
    let reqOpts = {uri: url, headers: headers, outFile: fnameCDFML};
    util.get(reqOpts, function (err, body) {
      if (err !== null) {
        err = true;
        reason = err.message;
      } else {
        if (body === null) {
          err = true;
          reason = 'Empty body (usually non-200 HTTP status)';
        }
        if (typeof body === 'string') {
          if (body.match('Internal Server Error') || body.match('Bad Request') ||
              body.match('No data available') || body.match('Not Found')) {
            err = true;
            reason = body;
            seconds = 10*seconds;
          } else if (body.match('Requested amount of data is too large')) {
            err = true;
            // TODO: Don't send scaled down request if last request was a scale up request.
            seconds = seconds/10;
            reason = 'Requested amount of data is too large';
          } else {
            util.error(id, "Un-handled error condition in get", false);
            err = true;
            reason = body;
            console.log(body)
            // TODO: Handle this case.
          }
        }
      }

      if (err == true && tries < meta.argv['maxtries']) {
        requestVariableDetails(ididx, names, seconds, reason);
      } else {
        if (err == true) body = null;
        finished(ididx, names, fnameCDFML, body, seconds);
      }
    });
  }

  function finished(ididx, names, fnameCDFML, body, seconds) {

    if (!finished.N) {finished.N = 0;}
    finished.N = finished.N + 1;

    let id = datasets[ididx]['id']
    if (body === null) {
      util.rmSync(fnameCDFML);
    } else {
      if (!body['CDF']) {
        util.error(id, 'CDFML JSON has no CDF element. Returned content:\n' +
                        util.obj2json(body),false);

        if (body['Error'] && body['Error'].length > 0) {
          util.error(id,
              [
                'Request for ' + datasets[ididx]['id'] + ' gave',
                'Error: ' + body['Error'][0],
                'Message: ' + body['Message'][0],
                'Status: ' + body['Message'][0],
                'Omitting.',
              ],
              false);
        }
        util.rmSync(fnameCDFML);
        body = null;
      } else if (body['CDF'] && !body['CDF'][0]) {

        util.error(id,'CDFML JSON has no CDF[0] element. Returned content:\n' +
                  util.obj2json(body),false);
        util.rmSync(fnameCDFML);
        body = null;
      } else if (seconds !== null) {

        let cdfVariables = body['CDF'][0]['cdfVariables'];
        if (cdfVariables.length > 1) {
          util.error(id, ['Case of more than one cdfVariables elements not implemented.',
                          cdfVariables],true);
        }

        util.cp(fnameCDFML, fnameCDFML + ".lastok");

        let rstats = getRecordStats(cdfVariables);
        let Nr = rstats['N'];
        let tries = requestVariableDetails.tries[ididx];
        if (Nr < meta.argv['minrecords'] && tries < meta.argv['maxtries']) {
          let secondsLast = seconds;
          seconds = 10*seconds;
          let reason = `# of records returned (${Nr}) < ${meta.argv['minrecords']}`;
          if (rstats['Rps'] !== null) {
            let secondsNew = (meta.argv['minrecords']+1)/rstats['Rps'];
            if (secondsLast != secondsNew) {
              seconds = secondsNew;
            }
          }
          requestVariableDetails(ididx, names, seconds, reason);
          finished.N = finished.N - 1;
          return;
        }
      }
    }

    datasets[ididx]['_data'] = null;
    if (body !== null || util.existsSync(fnameCDFML + ".lastok")) {

      if (body === null) {
        body = JSON.parse(util.readSync(fnameCDFML + ".lastok"));
        util.rmSync(fnameCDFML + ".lastok");
      }

      let rstats = getRecordStats(body['CDF'][0]['cdfVariables']);
      //util.log(`${id}: ${rstats['N']} records.`);

      let orphanAttributes = body['CDF'][0]['orphanAttributes'];
      if (orphanAttributes && orphanAttributes['attribute'].length > 0) {
          let fnameOrphan = fnameCDFML.replace('.json', '.orphan.json');
          util.writeSync(fnameOrphan, util.obj2json(orphanAttributes['attribute']));
      }

      util.writeSync(fnameCDFML, util.obj2json(body), 'utf8');
      if (body['Warning'].length > 0) {
        let fnameCDFMLWarn = fnameCDFML.replace('.json', '.warning.json');
        util.writeSync(fnameCDFMLWarn,util.obj2json(body['Warning']),'utf8');
      }

      //datasets[ididx]['_data'] = body['CDF'][0];
      datasets[ididx]['_data'] = fnameCDFML;

    }

    if (finished.N == datasets.length) {
      util.log.debug('datasets after variableDetails:');
      util.log.debug(datasets);
      getSPASERecords(CATALOG);
    }
  }
}

function getSPASERecords(CATALOG) {

  if (meta.argv['omit'].includes('spase')) {
    run.finished(CATALOG);
  }

  let datasets = CATALOG['datasets'];
  for (let dataset of datasets) {

    if (false && !dataset) {
      finished(dataset, "No dataset");
      return;      
    }

    let resourceID = "";
    let emsg1 = "";
    if (!dataset['_masters']['json']['CDFglobalAttributes']) {
      emsg1 = "No CDFglobalAttributes in master JSON";
      util.error(dataset['id'], emsg1, false);
    } else {
      for (let obj of dataset['_masters']['json']['CDFglobalAttributes']) {
        if (obj['spase_DatasetResourceID']) {
          resourceID = obj['spase_DatasetResourceID'][0]['0'];
          break;
        }
      }
    }
    //console.log(`${dataset['id']}: Reading ${dataset['_data']}`)
    //_data = JSON.parse(util.readSync(dataset['_data']))['CDF'][0];
    //let resourceID = getGAttribute(_data['cdfGAttributes'], 'SPASE_DATASETRESOURCEID');
    //let emsg2 = 'No SPASE_DATASETRESOURCEID in cdfGAttributes in ' + dataset['_data'];

    if (!resourceID || !resourceID.startsWith('spase://')) {
      finished(dataset, emsg1);
      continue;
    }
    let spaseURL = resourceID.replace('spase://', 'https://hpde.io/') + '.xml';
    let spaseFile = meta.argv.cachedir + '/' +
                    dataset['id'].split('_')[0] + '/' +
                    dataset['id'] + '/' +
                    dataset['id'] + '-spase.xml';
    getSPASERecord(dataset, spaseURL, spaseFile);
  }

  function finished(dataset, obj) {
    if (!finished.N) finished.N = 0;
    finished.N = finished.N + 1;
    if (typeof obj === 'string') {
      dataset['_spaseError'] = obj;
    } else {
      dataset['_spase'] = obj['json']["Spase"];      
    }
    if (finished.N == datasets.length) {
      run.finished(CATALOG);
    }
  }

  function getSPASERecord(dataset, url, outFile) {
    util.get({uri: url, outFile: outFile}, (err, obj) => {
      if (err) {
        util.error(dataset['id'], [url, err], false);
        obj = "Failed to fetch " + url;
      }
      finished(dataset, obj);
    });
  }
}

function createDatasets(json) {

  let allIds = [];
  let keptIds = [];
  let datasets_allxml = json['sites']['datasite'][0]['dataset'];

  let datasets = [];
  for (let dataset_allxml of datasets_allxml) {

    let id = dataset_allxml['$']['serviceprovider_ID'];
    let mastercdf = dataset_allxml['mastercdf'][0]['$']['ID'];
    let masterskt = mastercdf
                      .replace('0MASTERS', '0SKELTABLES')
                      .replace('.cdf', '.skt');
    let masterjson = mastercdf
                      .replace('0MASTERS', '0JSONS')
                      .replace('.cdf', '.json');

    allIds.push(id);

    let re = new RegExp(meta["argv"]["idregex"]);
    if (re.test(id) == false) {continue;}

    let omit = false;
    for (skip of meta["argv"]['skipids']) {
      let re = new RegExp(skip);
      if (re.test(id) == true) {
        util.log(`Note: Skipping ${id} b/c it matches regex ${id} in skips.`);
        omit = true;
        break;
      }
    }
    if (omit) {continue;}

    keptIds.push(id);

    let fnameAllXML = util.baseDir(id) + "/" + id + "-allxml.json";
    util.writeSync(fnameAllXML, util.obj2json(dataset_allxml));

    let startDate = dataset_allxml['$']['timerange_start'];
    startDate = startDate.replace(' ', 'T') + 'Z';

    let stopDate = dataset_allxml['$']['timerange_stop'];
    stopDate = stopDate.replace(' ', 'T') + 'Z';

    datasets.push(
      {
        id: id,
        info: {
          startDate: startDate,
          stopDate: stopDate,
        },
      _allxml: dataset_allxml,
      _masters: {
        cdf: {url: mastercdf},
        skt: {url: masterskt},
        json: {url: masterjson}
      }
    });
  }

  util.log.debug(`idregex = ${meta["argv"]["idregex"]} kept ${datasets.length}/${datasets_allxml.length} datasets.`);

  let allIdsFile = meta["argv"]["cachedir"] + '/ids-cdas.txt';
  util.writeSync(allIdsFile, allIds.join('\n'), 'utf8');

  let keptIdsFile = meta["argv"]["cachedir"] + '/ids-cdas-processed.txt';
  util.writeSync(keptIdsFile, keptIds.join('\n'), 'utf8');

  if (datasets.length == 0) {
    util.error(null,`Regex '${meta["argv"]["idregex"]}' did not match and dataset ids.`,true);
  }

  return datasets;
}

function getRecordStats(cdfVariables) {

  let nameIndex = {};
  let DEPEND_0 = null;
  for (let [idx, variable] of Object.entries(cdfVariables['variable'])) {
    if (!DEPEND_0) 
      DEPEND_0 = getVAttribute(variable['cdfVAttributes'],"DEPEND_0");
    nameIndex[variable['name']] = parseInt(idx);
  }

  let timeRecords = cdfVariables['variable'][nameIndex[DEPEND_0]]['cdfVarData']['record'];
  let N = timeRecords.length;
  if (N < 2) {
    return {"N": N, "Rps": null};
  }
  let firstTime = timeRecords[0]['value'];
  let lastTime = timeRecords[N-1]['value'];
  let dt = new Date(lastTime).getTime() - new Date(firstTime).getTime();

  return {"N": N, "Rps": (N-1)/(dt/1000)};
}

function getGAttribute(cdfGAttributes, attributeName) {
  for (let attribute of cdfGAttributes['attribute']) {
    if (attribute['name'] === attributeName) {
      return attribute['entry'][0]['value'];
    }
  }
  return null;
}

function getVAttribute(cdfVAttributes, attributeName) {

  for (let attribute of cdfVAttributes['attribute']) {
    if (attribute['name'] === attributeName) {
      return attribute['entry'][0]['value'];
    }
  }
  return null;
}
