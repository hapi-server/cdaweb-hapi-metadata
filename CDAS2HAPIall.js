// Create a HAPI all.json catalog based on
//   https://spdf.gsfc.nasa.gov/pub/catalogs/all.xml
// and queries to
//   https://cdaweb.gsfc.nasa.gov/WS/cdasr
// CDASR documentation:
//   https://cdaweb.gsfc.nasa.gov/WebServices/REST/

const HAPI_VERSION = '3.2';

// Command line options
const argv = require('yargs').default({
  idregex: '^AC_',
  skip: '^ALOUETTE2,AIM_CIPS_SCI_3A',
  maxsockets: 3,
  maxage: 3600 * 24,
  cachedir: 'cache/bw',
  all: 'cache/bw/all-hapi.json',
  allfull: 'cache/bw/all-hapi-full.json',
  cdasr: 'https://cdaweb.gsfc.nasa.gov/WS/cdasr/1/dataviews/sp_phys/datasets/',
  allxml: 'https://spdf.gsfc.nasa.gov/pub/catalogs/all.xml',
  include: '',
  debug: false,
  cdf2cdfml: 'java CDF2CDFML',
  cdfexport: 'DYLD_LIBRARY_PATH=.:$CDF_BASE/lib $CDF_BASE/bin/cdfexport',
}).argv;

argv.include = argv.include.split(',');

// pool should be set in outer-most scope. See
// https://www.npmjs.com/package/request#requestoptions-callback
//argv.pool = {maxSockets: argv.maxsockets};
let http = require('http');
let pool = new http.Agent(); //Your pool/agent
pool['maxSockets'] = argv.maxsockets;
argv.pool = pool;

const { util } = require('./CDAS2HAPIall.jsm');

util.argv = argv;

if (!util.existsSync(argv.cachedir)) {
  util.mkdirSync(argv.cachedir);
}

main();

function main() {
  // Request all.xml to get dataset names.
  // Then call variables() to get list of variables for each dataset.

  let fnameAllXML = argv.cachedir + '/all.xml';
  let fnameAllJSON = argv.cachedir + '/all.xml.json';

  let CATALOG = { all: { url: argv.allxml } };
  util.get({ uri: argv.allxml, outFile: fnameAllXML }, function (err, obj) {
    if (argv.include.includes('allxml')) {
      CATALOG['all']['xml'] = obj['xml'];
    }
    CATALOG['all']['json'] = obj['json'];
    extractDatasets(obj['json']);
    util.log.debug("CATALOG['all']:");
    util.log.debug(CATALOG);
    util.log.debug("CATALOG['datasets']:");
    util.log.debug(CATALOG['datasets']);
    inventory(CATALOG, (CATALOG) => masters(CATALOG));
  });

  function extractDatasets(json) {
    CATALOG['datasets'] = [];
    let allIds = [];
    let datasets = json['sites']['datasite'][0]['dataset'];
    for (let dataset of datasets) {
      let id = dataset['$']['serviceprovider_ID'];
      let title = dataset['description'][0]['$']['short'];
      let mastercdf = dataset['mastercdf'][0]['$']['ID'];
      let masterskt = mastercdf
        .replace('0MASTERS', '0SKELTABLES')
        .replace('.cdf', '.skt');
      let masterjson = mastercdf
        .replace('0MASTERS', '0JSONS')
        .replace('.cdf', '.json');

      allIds.push(id);

      let re = new RegExp(argv.idregex);
      if (re.test(id) == false) {
        continue;
      }

      let skips = argv.skip.split(',');
      let omit = false;
      for (skip of skips) {
        let re = new RegExp(skip);
        if (re.test(id) == true) {
          util.log(
            'Note: Skipping ' +
              id +
              " b/c matches regex '" +
              skip +
              "' in skips."
          );
          omit = true;
          break;
        }
      }
      if (omit) {
        continue;
      }

      let startDate = dataset['$']['timerange_start'].replace(' ', 'T') + 'Z';
      let stopDate = dataset['$']['timerange_stop'].replace(' ', 'T') + 'Z';
      let contact =
        dataset['data_producer'][0]['$']['name'].trim() +
        ' @ ' +
        dataset['data_producer'][0]['$']['affiliation'].trim();
      CATALOG['datasets'].push({
        id: id,
        title: title,
        info: {
          HAPI: HAPI_VERSION,
          startDate: startDate,
          stopDate: stopDate,
          contact: contact,
          resourceURL: 'https://cdaweb.gsfc.nasa.gov/misc/Notes.html#' + id,
        },
        _dataset: dataset,
        _masters: {
          cdf: {
            url: mastercdf,
          },
          skt: {
            url: masterskt,
          },
          json: {
            url: masterjson,
          },
        },
      });
    }

    util.log.debug(
      'idregex = ' +
        argv.idregex +
        ' kept ' +
        CATALOG['datasets'].length +
        '/' +
        datasets.length +
        ' datasets.'
    );

    let allIdsFile = argv.cachedir + '/ids-cdasr.txt';
    util.log('Writing: ' + allIdsFile);
    util.writeSync(allIdsFile, allIds.join('\n'), 'utf8');

    if (CATALOG['datasets'].length == 0) {
      util.error(
        null,
        `Regex '${argv.idregex}' did not match and dataset ids.`,
        true
      );
    }
  }
}

function inventory(CATALOG, cb) {
  const cheerio = require('cheerio');

  for (let dataset of CATALOG['datasets']) {
    let rootURL = dataset['_dataset']['access'][0]['URL'][0];
    if (!rootURL.endsWith('/')) {
      rootURL = rootURL + '/';
    }
    getInventory(dataset, rootURL);
  }

  function getInventory(dataset, rootURL) {
    let id = dataset['id'];
    let inventory = {};
    inventory[id] = {};
    let inventory_flat = [];

    if (getInventory.finished === undefined) getInventory.finished = 0;
    util.log.debug(
      'Started a dir walk for ' +
        id +
        ' # walks dir walks left = ' +
        getInventory.finished
    );

    let outFile =
      argv.cachedir + '/' + id.split('_')[0] + '/' + id + '/files/index.html';
    listdir(id, rootURL, outFile, inventory[id]);

    function finished(id) {
      let baseDir = argv.cachedir + '/' + id.split('_')[0] + '/' + id;
      listdir[id].started = listdir[id].started - 1;
      util.log.debug(
        'Finished a dir listing for ' + id + '; # left = ' + listdir[id].started
      );

      if (listdir[id].started == 0) {
        util.log.debug('Finished walking dirs for ' + id);
        getInventory.finished = getInventory.finished + 1;
        dataset['_inventory'] = inventory[id];
        let inventoryJSON = baseDir + '/files/' + id + '-inventory.json';
        util.log('Writing: ' + inventoryJSON);
        util.writeSync(inventoryJSON, util.obj2json(inventory[id]));

        let inventoryCSV = baseDir + '/../' + id + '-files.csv';
        util.log('Writing: ' + inventoryCSV);
        util.writeSync(inventoryCSV, inventory_flat.join('\n'));

        if (getInventory.finished == CATALOG['datasets'].length) {
          util.log.debug('Finished inventory for all datasets');
          //util.log.debug(inventory);
          cb(CATALOG);
        }
      }
    }

    function listdir(id, url, outFile, parent) {
      if (listdir[id] === undefined) listdir[id] = {};
      if (listdir[id].started === undefined) listdir[id].started = 0;
      listdir[id].started = listdir[id].started + 1;

      let reqOpts = { uri: url, outFile: outFile };
      util.get(reqOpts, (err, html) => {
        parent['files'] = [];
        parent['dirs'] = {};
        if (err) {
          util.error(null, [url, err], false);
          finished();
          return;
        }
        const $ = cheerio.load(html);
        $('tr').each((i, elem) => {
          // .each is sync so no callback needed.
          if (i > 3) {
            // May break if Apache dir listing HTML changes.
            // First three links are not relevant.
            let cols = $(elem).find('td');
            let href = $($(cols[0]).find('a')[0]).attr('href');
            let mtime =
              $(cols[1])
                .text()
                .replace(/([0-9]) ([0-9])/, '$1T$2')
                .trim() + 'Z';
            let size = $(cols[2]).text().trim();
            if (href.endsWith('.cdf')) {
              let fileDate = href.replace(
                /.*([0-9]{4})([0-9]{2})([0-9]{2}).*/,
                '$1-$2-$3Z'
              );
              let fileVersion = href.replace(/.*(v.*)\.cdf/, '$1');
              parent['files'].push([
                fileDate,
                url + href,
                mtime,
                size,
                fileVersion,
              ]);
              inventory_flat.push(
                `${fileDate}, ${url + href}, ${mtime}, ${size}, ${fileVersion}`
              );
            }
            if (href.endsWith('/')) {
              let subdir = href.replace('/', '');
              let newOutDir =
                outFile.split('/').slice(0, -1).join('/') + '/' + href;
              let newOutFile = newOutDir + 'index.html';
              parent['dirs'][subdir] = {};

              listdir(id, url + href, newOutFile, parent['dirs'][subdir]);
            }
          }
        });

        let inventoryFile = outFile.replace('.html', '.json');
        const path = require('path');
        let outDir = path.dirname(inventoryFile);
        util.mkdirSync(outDir);
        util.log('Writing: ' + inventoryFile);
        util.writeSync(inventoryFile, util.obj2json(parent));
        finished(id);
      });
    }
  }
}

function masters(CATALOG) {
  // For each dataset in CATALOG, get its master CDF, convert to CDF to XML
  // and then convert XML to JSON. Place JSON in element CATALOG.
  //
  // When all master CDFs converted, HAPIInfo() to process
  // JSON for each CATALOG element and create new elements of CATALOG
  // if a dataset in CATALOG has more than one DEPEND_0. Final result
  // is CATALOG with elements of HAPI info responses for datasets
  // with only one DEPEND_0.
  let datasets = CATALOG['datasets'];
  let idx = 0;
  for (dataset of datasets) {
    let baseDir =
      argv.cachedir +
      '/' +
      dataset['id'].split('_')[0] +
      '/' +
      dataset['id'] +
      '/';
    util.mkdirSync(baseDir);
    let urlCDF = dataset['_masters']['cdf']['url'];
    let fnameMasterCDF = baseDir + urlCDF.split('/').slice(-1);
    let reqOpts = { uri: urlCDF, outFile: fnameMasterCDF };
    util.get(reqOpts, (err, obj) => {
      if (err) {
        util.error(dataset['id'], [urlCDF, 'Error message', err], false);
        obj = { json: null, xml: null };
      }
      datasets[idx]['_masters']['cdf']['json'] = obj['json'];
      if (argv.include.includes('cdfml')) {
        datasets[idx]['_masters']['cdf']['xml'] = obj['xml'];
      }
      finished(err);
    });

    let urlSKT = dataset['_masters']['skt']['url'];
    let fnameMasterSKT = baseDir + urlSKT.split('/').slice(-1);
    util.get({ uri: urlSKT, outFile: fnameMasterSKT }, (err, text) => {
      if (err) {
        util.error(dataset['id'], [urlSKT, err], false);
        text = null;
      }
      if (argv.include.includes('skt')) {
        datasets[idx]['_masters']['skt']['text'] = text;
      }
      finished(err);
    });

    let urlJSON = dataset['_masters']['json']['url'];
    let fnameMasterJSON = baseDir + urlJSON.split('/').slice(-1);
    util.get({ uri: urlJSON, outFile: fnameMasterJSON }, (err, json) => {
      if (err) {
        util.error(dataset['id'], [urlJSON, err], false);
        json = null;
      }
      if (argv.include.includes('json')) {
        datasets[idx]['_masters']['json']['json'] = json;
      }
      finished(err);
    });
  }

  function finished(err) {
    if (finished.N == undefined) {
      finished.N = 0;
    }
    finished.N = finished.N + 1;
    if (finished.N == 3 * datasets.length) {
      //datasets = datasets.filter(function (el) {return el != null;});
      util.log.debug('catalog after masters:');
      util.log.debug(CATALOG);
      variables(CATALOG);
    }
  }
}

function variables(CATALOG) {
  // Call /variables endpoint to get list of variables for each dataset.
  // Then call variableDetails() to get additional metadata for variables
  // by making a request to /data.

  let datasets = CATALOG['datasets'];
  for (dataset of datasets) {
    let id = dataset['id'];
    let url = argv.cdasr + dataset['id'] + '/variables';
    let dirId = argv.cachedir + '/' + id.split('_')[0] + '/' + id;
    util.mkdirSync(dirId);
    let fnameVariables = dirId + '/' + id + '-variables.json';
    requestVariables(url, fnameVariables, dataset);
  }

  function requestVariables(url, fnameVariables, dataset) {
    let id = dataset['id'];
    let headers = { Accept: 'application/json' };
    let opts = { uri: url, headers: headers, outFile: fnameVariables };
    util.get(opts, function (err, variables) {
      if (err) {
        util.error(id, [url, err], false);
        finished(err);
        return;
      }
      dataset['_variables'] = variables;
      extractParameterNames(dataset, variables);
      finished(err);
    });
  }

  function extractParameterNames(dataset, variables) {
    let parameters = {};
    for (let variable of variables['VariableDescription']) {
      let descr = variable['LongDescription'] || variable['ShortDescription'];
      let name = variable['Name'];
      parameters[name] = {
        name: name,
        description: descr,
      };
    }
    dataset['info']['parameters'] = parameters;
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
      variableDetails(CATALOG);
    }
  }
}

function variableDetails(CATALOG) {
  let datasets = CATALOG['datasets'];

  // Call /variables endpoint to get CDFML with data for all variables
  // in each dataset. Then call buildHAPI().

  for (let ididx = 0; ididx < datasets.length; ididx++) {
    parameters = null;
    let names = [];
    for (let name of Object.keys(datasets[ididx]['info']['parameters'])) {
      names.push(name);
    }
    names = names.join(',');
    requestVariableDetails(ididx, names);
  }

  function requestVariableDetails(ididx, names, timeRangeScalePower) {
    if (timeRangeScalePower === undefined) {
      timeRangeScalePower = 10;
    }

    if (!requestVariableDetails.tries) {
      requestVariableDetails.tries = {};
    }
    if (!requestVariableDetails.tries[ididx]) {
      requestVariableDetails.tries[ididx] = 0;
    }
    requestVariableDetails.tries[ididx] += 1;

    let seconds =
      100 *
      Math.pow(timeRangeScalePower, requestVariableDetails.tries[ididx] - 1);

    let stop = util.incrementTime(
      datasets[ididx]['info']['startDate'],
      seconds,
      'seconds'
    );
    let url = argv.cdasr + datasets[ididx]['id'] + '/variables';
    url =
      argv.cdasr +
      datasets[ididx]['id'] +
      '/data/' +
      datasets[ididx]['info']['startDate'].replace(/-|:/g, '') +
      ',' +
      stop.replace(/-|:/g, '') +
      '/' +
      names +
      '?format=json';

    let headers = { Accept: 'application/json' };
    let fnameCDFML =
      argv.cachedir +
      '/' +
      datasets[ididx]['id'].split('_')[0] +
      '/' +
      datasets[ididx]['id'] +
      '/' +
      datasets[ididx]['id'] +
      '-cdas.json';
    let reqOpts = { uri: url, headers: headers, outFile: fnameCDFML };
    util.get(reqOpts, function (err, body) {
      if (err) {
        util.error(datasets[ididx]['id'], err, true);
      } else {
        if (body === null) {
          err = true;
          reason = 'Empty body (usually non-200 HTTP status)';
        }
        let timeRangeScalePower = 10;
        if (typeof body === 'string') {
          if (
            body.match('Internal Server Error') ||
            body.match('Bad Request') ||
            body.match('No data available') ||
            body.match('Not Found')
          ) {
            err = true;
            reason = body;
          }
          if (body.match('Requested amount of data is too large')) {
            err = true;
            timeRangeScalePower = 1 / 10;
            reason = 'Requested amount of data is too large';
          }
        }
      }
      let maxRetries = 3;
      if (err && requestVariableDetails.tries[ididx] < maxRetries + 4) {
        util.rmSync(fnameCDFML);
        util.rmSync(fnameCDFML + '.httpheaders');
        util.log(
          'Retry (#' +
            requestVariableDetails.tries[ididx] +
            '/' +
            maxRetries +
            ') ' +
            datasets[ididx]['id'] +
            " due to '" +
            reason +
            "'."
        );
        requestVariableDetails(ididx, names, timeRangeScalePower);
      } else {
        finished(ididx, fnameCDFML, body, false);
      }
    });
  }

  function finished(ididx, fnameCDFML, body, fromCache) {
    if (!finished.N) {
      finished.N = 0;
    }
    finished.N = finished.N + 1;

    if (body && !body['CDF']) {
      util.error(
        datasets[ididx]['id'],
        'Problem with ' +
          datasets[ididx]['id'] +
          ': JSON has no CDF element. Omitting. Returned content: \n' +
          util.obj2json(body),
        false
      );
      //datasets[ididx]['_data'] = null;
      datasets[ididx] = null;
      if (body['Error'] && body['Error'].length > 0) {
        util.error(
          datasets[ididx]['id'],
          [
            'Request for ' + datasets[ididx]['id'] + ' gave',
            'Error: ' + body['Error'][0],
            'Message: ' + body['Message'][0],
            'Status: ' + body['Message'][0],
            'Omitting.',
          ],
          false
        );
      }
    } else if (body && body['CDF'] && !body['CDF'][0]) {
      util.error(
        datasets[ididx]['id'],
        'Problem with ' +
          datasets[ididx]['id'] +
          ': JSON has no CDF[0] element. Omitting. Returned content: \n' +
          util.obj2json(body),
        true
      );
    } else {
      let cdfVariables = body['CDF'][0]['cdfVariables'];
      if (cdfVariables.length > 1) {
        util.error(
          datasets[ididx]['id'],
          [
            'Case of more than one cdfVariable not implemented. Omitting.',
            cdfVariables,
          ],
          true
        );
      }

      let orphanAttributes = body['CDF'][0]['orphanAttributes'];
      if (orphanAttributes && orphanAttributes['attribute'].length > 0) {
        if (fromCache == false) {
          let fnameOrphan = fnameCDFML.replace('.json', '.orphan.json');
          util.log('Writing: ' + fnameOrphan);
          util.writeSync(
            fnameOrphan,
            util.obj2json(orphanAttributes['attribute'])
          );
        }
      }

      // Keep only first two data records.
      for (let [idx, variable] of Object.entries(cdfVariables['variable'])) {
        if (variable['cdfVarData']['record'].length > 2) {
          cdfVariables['variable'][idx]['cdfVarData']['record'] = cdfVariables[
            'variable'
          ][idx]['cdfVarData']['record'].slice(0, 2);
        }
      }

      if (fromCache == false) {
        util.log('Writing: ' + fnameCDFML);
        util.writeSync(fnameCDFML, util.obj2json(body), 'utf8');
        if (body['Warning'].length > 0) {
          let fnameCDFMLWarn = fnameCDFML.replace('.json', '.warning.json');
          util.log('Writing: ' + fnameCDFMLWarn);
          util.writeSync(
            fnameCDFMLWarn,
            util.obj2json(body['Warning']),
            'utf8'
          );
        }
      }

      datasets[ididx]['_data'] = body['CDF'][0];
    }

    if (finished.N == datasets.length) {
      util.log.debug('datasets after variableDetails:');
      util.log.debug(datasets);
      buildHAPI(CATALOG);
    }
  }
}

//////////////////////////////////////////////////////////////////////////////

function buildHAPI(CATALOG) {
  // Move HAPI-related parameter metadata from info['parameters'] to
  // info['parameters']. Then delete x_ keys.

  util.log('\nCreating HAPI catalog and info responses.\n');

  let datasets = CATALOG['datasets'];

  util.log.debug('Looking for datasets with more than one DEPEND_0.');
  datasets = subsetDatasets(datasets);

  for (let dataset of datasets) {
    if (!dataset['_variables']) {
      util.error(
        dataset['id'],
        'Omitting ' +
          dataset['id'] +
          ' from HAPI all.json because no variable attributes.',
        false
      );
      continue;
    }

    util.log(dataset['id']);

    if (/\s\s.*|\( | \)/.test(dataset['title'])) {
      util.warning(
        dataset['id'],
        "Check title formatting: '" + dataset['title'] + "'"
      );
    }

    extractParameterAttributes(dataset);
    extractDatasetAttributes(dataset);

    if (dataset['info']['cadence']) {
      let cadence = util.str2ISODuration(dataset['info']['cadence']);
      if (cadence !== undefined) {
        util.warning(
          dataset['id'],
          'Assumed TIME_RESOLUTION = ' +
            dataset['info']['cadence'] +
            ' => ' +
            cadence
        );
        dataset['info']['cadence'] = cadence;
      } else {
        util.warning(
          dataset['id'],
          'Could not parse TIME_RESOLUTION: ' +
            dataset['info']['cadence'] +
            ' to use for cadence.'
        );
      }
    } else {
      util.warning(
        dataset['id'],
        'No TIME_RESOLUTION found to use for cadence.'
      );
    }

    if (dataset['info']['x_creationDate']) {
      let creationDateo = dataset['info']['x_creationDate'];
      creationDate = util.str2ISODateTime(creationDateo);
      if (creationDate) {
        util.warning(
          dataset['id'],
          'Assumed GENERATION_DATE = ' + creationDateo + ' => ' + creationDate
        );
        dataset['info']['x_creationDate'] = creationDate;
      } else {
        util.warning(
          dataset['id'],
          'Could not parse GENERATION_DATE = ' + creationDateo
        );
      }
    }

    let parameters = dataset['info']['parameters'];

    let DEPEND_0s = [];
    let pidx = 0;
    parameter_array = [];
    for (let name of Object.keys(parameters)) {
      let parameter = parameters[name];

      let varType = parameter['_vAttributesKept']['_VAR_TYPE'];

      if (parameter['_vAttributesKept']['_DEPEND_2'] === null) {
        util.error(
          dataset['id'],
          name + ' has an un-handled DEPEND_2. Omitting dataset.',
          false
        );
        dataset = null;
        break;
      }
      if (dataset === null) {
        continue;
      }

      //let copy = JSON.parse(util.obj2json(parameters[parameter]));
      //parameters.push(copy);

      // Move kept vAttributes up
      for (let key of Object.keys(parameter['_vAttributesKept'])) {
        if (!key.startsWith('_'))
          parameter[key] = parameter['_vAttributesKept'][key];
      }

      if (!parameter['units'] && varType == 'data') {
        util.warning(dataset['id'], 'No units for ' + parameter['name']);
        parameter['units'] = null;
      }
      if (parameter['bins'] && !parameter['bins']['units']) {
        util.warning(dataset['id'], 'No bin units for ' + parameter['name']);
        parameter['bins']['units'] = null;
      }

      let DEPEND_0 = parameter['_vAttributesKept']['_DEPEND_0'];
      if (DEPEND_0 && !DEPEND_0.toLowerCase().startsWith('epoch')) {
        util.warning(
          dataset['id'],
          `${parameter['name']} has DEPEND_0 name of '${DEPEND_0}'; expected 'Epoch'`
        );
      }
      DEPEND_0s.push(DEPEND_0);

      // Extract DEPEND_1
      let vectorComponents = false;
      if (parameter['_vAttributesKept']['_DEPEND_1']) {
        let DEPEND_1 = parameter['_vAttributesKept']['_DEPEND_1'];
        let depend1 = extractDepend1(
          dataset['id'],
          parameters[DEPEND_1]['_variable']
        );
        if (Array.isArray(depend1)) {
          extractCoordinateSystemNameAndVectorComponents(
            dataset['id'],
            parameter,
            depend1
          );
        } else {
          parameter['bins'] = depend1;
        }
      }

      // Extract labels
      if (parameter['_vAttributesKept']['_LABL_PTR_1']) {
        let LABL_PTR_1 = parameter['_vAttributesKept']['_LABL_PTR_1'];
        let label = extractLabel(parameters[LABL_PTR_1]['_variable']);
        parameter['label'] = label;
      }

      if (varType === 'data') {
        parameter_array.push(JSON.parse(JSON.stringify(parameter)));
      }
    }

    let EpochName = DEPEND_0s[0];
    let firstTimeValue =
      parameters[EpochName]['_variable']['cdfVarData']['record'][0]['value'][0];
    let timePadValue =
      parameters[EpochName]['_variable']['cdfVarInfo']['padValue'];
    parameter_array.unshift({
      name: 'Time',
      type: 'isotime',
      units: 'UTC',
      length: firstTimeValue.length,
      fill: timePadValue,
    });

    dataset['info']['parameters'] = parameter_array;
  }

  util.log('\nCreated HAPI catalog and info responses.\n');

  writeFiles(datasets);

  function writeFiles(datasets) {
    util.log('Writing HAPI info and info-full files.\n');

    // Write one info file per dataset
    let allIds = [];
    let fnameInfo = '';
    let dsidx = 0;
    for (let dataset of datasets) {
      allIds.push(dataset['id']);

      let fnameInfoFull =
        argv.cachedir +
        '/' +
        dataset['id'].split('_')[0] +
        '/' +
        dataset['id'] +
        '-info-full.json';
      util.writeSync(fnameInfoFull, util.obj2json(dataset));
      if (dsidx <= 10) {
        util.log(`Wrote:   ${fnameInfoFull}`);
      }

      for (let parameter of dataset['info']['parameters']) {
        delete parameter['_variable'];
        delete parameter['_vAttributesKept'];
      }
      delete dataset['_dataset'];
      delete dataset['_masters'];
      delete dataset['_variables'];
      delete dataset['_data'];

      fnameInfo =
        argv.cachedir +
        '/' +
        dataset['id'].split('_')[0] +
        '/' +
        dataset['id'] +
        '-info.json';
      util.writeSync(fnameInfo, util.obj2json(dataset));
      if (dsidx <= 10) {
        util.log(`Wrote:   ${fnameInfo}`);
      }
      dsidx = dsidx + 1;
    }

    if (datasets.length > 10) {
      util.log(
        `Wrote:   ... ${datasets.length - 11} info and info-full files.`
      );
    } else {
      util.log(`Wrote:   ${datasets.length} HAPI info and info-full files.`);
    }

    util.log(`\nWrote ${datasets.length} HAPI info and info-full files.\n`);

    // Write HAPI all.json containing all content from all info files.
    let allIdsFile = argv.cachedir + '/ids-hapi.txt';
    util.log('Writing: ' + allIdsFile);
    util.writeSync(allIdsFile, allIds.join('\n'), 'utf8');

    // Write HAPI all.json containing all content from all info files.
    util.log('Writing: ' + argv.all);
    util.writeSync(argv.all, util.obj2json(datasets));
  }
}

function spase(CATALOG) {
  util.log('Getting SPASE records.\n');
  for (let dataset of CATALOG['datasets']) {
    let resourceID = dataset['info']['resourceID'];
    if (!resourceID || !resourceID.startsWith('spase://')) {
      util.warning(dataset['id'], 'No SPASE link in resourceID');
      finished(dataset, null);
    }
    let spaseURL = resourceID.replace('spase://', 'https://hpde.io/') + '.xml';
    let spaseFile =
      argv.cachedir +
      '/' +
      dataset['id'].split('_')[0] +
      '/' +
      dataset['id'] +
      '/' +
      dataset['id'] +
      '.spase.xml';
    getSPASE(dataset, spaseURL, spaseFile, writeFiles);
  }

  function finished(dataset, obj) {
    if (!finished.N) finished.N = 0;
    finished.N = finished.N + 1;
    if (obj === null) return;
    dataset['info']['x_additionalMetadata'] = { json: obj['json'] };
    if (finished.N == datasets.length) {
      util.log('\nGot SPASE records.\n');
      buildHAPI();
    }
  }

  function getSPASE(dataset, url, outFile) {
    util.get({ uri: url, outFile: outFile }, (err, obj) => {
      if (err) {
        util.error(dataset['id'], [url, err], false);
        obj = { json: null, xml: null };
      }
      finished(dataset, obj);
    });
  }
}

function subsetDatasets(datasets) {
  let datasetsExpanded = JSON.parse(JSON.stringify(datasets));

  for (let [dsidx, dataset] of Object.entries(datasets)) {
    util.log(dataset['id']);

    if (!dataset['_variables']) {
      util.error(
        dataset['id'],
        'Omitting ' +
          dataset['id'] +
          ' from HAPI all.json because no variable attributes.',
        false
      );
      continue;
    }

    extractParameterAttributes(dataset);
    extractDatasetAttributes(dataset);

    let subdatasets = subsetDataset(dataset);
    if (subdatasets !== undefined) {
      util.log('  Note: ' + subdatasets.length + ' sub-datasets');
      datasetsExpanded.splice(dsidx, 1, ...subdatasets);
    }
  }

  datasets = null;
  return datasetsExpanded;

  function subsetDataset(dataset) {
    // Look for parameters that have more than one DEPEND_0.
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
      return undefined;
    }

    //util.warning(dataset['id'], DEPEND_0s.length + " DEPEND_0s: " + DEPEND_0s.join(", "));
    let datasets = [];
    for ([sdsidx, DEPEND_0] of Object.entries(DEPEND_0s)) {
      newdataset = JSON.parse(JSON.stringify(dataset));
      newdataset['id'] = newdataset['id'] + '@' + sdsidx;
      for (parameter of Object.keys(newdataset['info']['parameters'])) {
        let depend_0 = parameters[parameter]['_vAttributesKept']['_DEPEND_0'];
        if (depend_0 !== DEPEND_0) {
          delete newdataset['info']['parameters'][parameter];
        }
      }
      datasets.push(newdataset);
    }
    return datasets;
  }
}

function extractDatasetAttributes(dataset) {
  let cdfGAttributes = dataset['_data']['cdfGAttributes']['attribute'];

  for (let attribute of cdfGAttributes) {
    if (attribute['name'] === 'TIME_RESOLUTION') {
      dataset['info']['cadence'] = attribute['entry'][0]['value'];
    }
    if (attribute['name'] === 'SPASE_DATASETRESOURCEID') {
      dataset['info']['resourceID'] = attribute['entry'][0]['value'];
    }
    if (attribute['name'] === 'GENERATION_DATE') {
      dataset['info']['x_creationDate'] = attribute['entry'][0]['value'];
    }
    if (attribute['name'] === 'ACKNOWLEDGEMENT') {
      dataset['info']['x_datasetCitation'] = catCharEntries(attribute['entry']);
    }
    if (attribute['name'] === 'RULES_OF_USE') {
      dataset['info']['x_datasetTermsOfUse'] = catCharEntries(
        attribute['entry']
      );
    }
  }

  function catCharEntries(entries) {
    let cat = '';
    for (let entry of entries) {
      cat = cat.trim() + ' ' + entry['value'];
    }
    return cat;
  }
}

function extractParameterAttributes(dataset) {
  let cdfVariables = dataset['_data']['cdfVariables']['variable'];
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
      parameters[variable['name']]['type'] = cdftype2hapitype(
        variable['cdfVarInfo']['cdfDatatype']
      );
    }
    parameters[variable['name']]['_vAttributesKept'] = vAttributesKept;
    parameters[variable['name']]['_variable'] = variable;
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
    } else {
      util.error(dataset['id'], 'Un-handled CDF datatype ' + cdftype);
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
      keptAttributes['_DEPEND_1'] = null;
    }
    if (attribute['name'] === 'LABL_PTR_1') {
      keptAttributes['_LABL_PTR_1'] = attribute['entry'][0]['value'];
    }
    if (attribute['name'] === 'VAR_TYPE') {
      keptAttributes['_VAR_TYPE'] = attribute['entry'][0]['value'];
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
    // Return a bins object;
    let bins = {};
    let keptAttributes = extractVariableAttributes(dsid, depend1Variable);
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
      util.error(
        null,
        'Un-handled DEPEND_1 type for bins variable ' +
          depend1Variable['name'] +
          DEPEND_1_TYPE,
        true
      );
    }
    bins['name'] = keptAttributes['name'];
    bins['units'] = keptAttributes['units'];
    bins['description'] = keptAttributes['description'];
    return bins;
  }

  // Return an array of strings.
  let delimiter =
    depend1Variable['cdfVarData']['record'][0]['elementDelimiter'];
  let depend1 = depend1Variable['cdfVarData']['record'][0]['value'][0]
    .replace(new RegExp(delimiter, 'g'), '')
    .replace(/[^\S\r\n]/g, '')
    .trim()
    .split('\n');
  return depend1;
}

function extractCoordinateSystemNameAndVectorComponents(
  dsid,
  parameter,
  depend1
) {
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
    util.warning(
      dsid,
      parameter['name'] +
        ': Assumed DEPEND_1 = [' +
        depend1.join(', ') +
        '] => vectorComponents = [x, y, z]'
    );
    parameter['vectorComponents'] = ['x', 'y', 'z'];
  }

  let foundName = false;
  if (foundComponents) {
    let knownNames = ['GSM', 'GCI', 'GSE', 'RTN', 'GEO', 'MAG'];
    for (let knownName of knownNames) {
      if (parameter['name'].includes(knownName)) {
        foundName = true;
        util.warning(
          dsid,
          parameter['name'] +
            ': Assumed parameter name = ' +
            parameter['name'] +
            ' => coordinateSystemName = ' +
            knownName
        );
        parameter['coordinateSystemName'] = knownName;
      }
    }
    if (foundName == false) {
      util.warning(
        dsid,
        'Could not infer coordinateSystemName from parameter name = ' +
          parameter['name']
      );
    }
  }
}
