// Create a HAPI all.json catalog based on
//   https://spdf.gsfc.nasa.gov/pub/catalogs/all.xml
// and queries to
//   https://cdaweb.gsfc.nasa.gov/WS/cdasr
// CDASR documentation:
//   https://cdaweb.gsfc.nasa.gov/WebServices/REST/

// Command line options
const yargs = require('yargs');
let argv = yargs
            .help()
            .describe('idregex', 'Comma-separated list of regex pattern to process')
            .describe('skipids', 'Comma-separated list of regex pattern to exclude')
            .describe('omit', 'Omit steps')
            .describe('maxsockets', 'Maximum open sockets per server')
            .describe('maxage', 'Do HEAD requests if file age < maxage (in seconds)')
            .describe('maxretries', 'Maximum # of retries for CDAS data requests')
            .describe('minrecords', 'Minimum # of records for CDAS data request to be successful')
            .describe('debug', 'Show additional logging information')
            .default(
              {
                idregex: '^AC_',    
                skipids: '^ALOUETTE2,AIM_CIPS_SCI_3A',
                omit: '',
                include: '',
                maxsockets: 3,
                maxage: 3600*24,
                all:     'hapi/bw/all.json',
                catalog: 'hapi/bw/catalog.json',
                infodir: 'hapi/bw/info',
                cachedir: 'cache/bw',
                allfull:  'cache/bw/all-hapi-full.json',
                cdasr:  'https://cdaweb.gsfc.nasa.gov/WS/cdasr/1/dataviews/sp_phys/datasets/',
                allxml: 'https://spdf.gsfc.nasa.gov/pub/catalogs/all.xml',
                debug: false,
                maxtries: 1,
                minrecords: 1440,
                hapiversion: '3.1',
                cdf2cdfml: 'java CDF2CDFML',
                cdfexport: 'DYLD_LIBRARY_PATH=.:$CDF_BASE/lib $CDF_BASE/bin/cdfexport',
              }).argv;

argv['include'] = argv['include'].split(',');
argv['omit'] = argv['omit'].split(',');
argv['skipids'] = argv['skipids'].split(',');

const {util} = require('./CDAS2HAPIall.util.js');
util.argv = argv;

const {meta} = require('./CDAS2HAPIall.meta.js');
meta.argv = argv;

// meta.run() gets all needed files.
meta.run(() => buildHAPI());
//meta.run();
//buildHAPI()
//////////////////////////////////////////////////////////////////////////////

function buildHAPI(CATALOG) {

  let datasets;
  if (CATALOG !== undefined) {
    datasets = CATALOG['datasets'];
  } else {
    let allIdsFile = argv["cachedir"] + '/ids-cdas-processed.txt';
    let ids = util.readSync(allIdsFile).toString().split("\n");

    datasets = [];
    for (id of ids) {
      let fnameCombined = util.baseDir(id) + "/" + id + ".combined.json";
      datasets.push(JSON.parse(util.readSync(fnameCombined)));
    }
  }

  util.log('\nCreating HAPI catalog and info responses.\n');

  for (let dataset of datasets) {
    // Create dataset['info']['parameters'] array of objects. Give each object
    // a name and description taken CDAS /variables request.
    extractParameterNames(dataset);
  }

  util.log.debug('Looking for datasets with more than one DEPEND_0.');
  datasets = subsetDatasets(datasets);

  let omsg = ' from HAPI all.json because no variable attributes.';
  for (let dataset of datasets) {

    let dsid = dataset['id'];

    let _allxml = dataset["_allxml"];

    dataset['title']   = _allxml['description'][0]['$']['short'];
    //dataset['HAPI'] = meta.argv['hapiversion'];
    let producer = _allxml['data_producer'][0]['$']
    dataset['info']['contact'] = producer['name'].trim() + ' @ ' + producer['affiliation'].trim();
    dataset['info']['resourceURL'] = 'https://cdaweb.gsfc.nasa.gov/misc/Notes.html#' + dataset["id"];

    if (!dataset['_variables']) {
      util.error(dsid, 'Omitting ' + dsid + omsg, false);
      continue;
    }

    util.log(dsid, 'blue');

    if (/\s\s.*|\( | \)/.test(dataset['title'])) {
      util.warning(dsid, "Check title formatting for extra spaces: '" + dataset['title'] + "'");
    }

    let _data = JSON.parse(util.readSync(dataset['_data']))['CDF'][0];
    extractParameterAttributes(dataset, _data);
    extractDatasetAttributes(dataset, _data);

    if (dataset['info']['cadence']) {
      let cadence = util.str2ISODuration(dataset['info']['cadence']);
      if (cadence !== undefined) {
        util.warning(dsid, 
                        'Assumed TIME_RESOLUTION = ' 
                        + dataset['info']['cadence'] 
                        + ' => ' + cadence);
        dataset['info']['cadence'] = cadence;
      } else {
        util.warning(dsid, 'Could not parse TIME_RESOLUTION: ' + dataset['info']['cadence'] + ' to use for cadence.');
      }
    } else {
      util.warning(dsid, 'No TIME_RESOLUTION found to use for cadence.');
    }

    if (dataset['info']['x_creationDate']) {
      let creationDateo = dataset['info']['x_creationDate'];
      creationDate = util.str2ISODateTime(creationDateo);
      if (creationDate) {
        util.warning(dsid,
                          'Assumed GENERATION_DATE = ' + creationDateo
                          + ' => ' + creationDate);
        dataset['info']['x_creationDate'] = creationDate;
      } else {
        util.warning(dsid, 'Could not parse GENERATION_DATE = ' + creationDateo);
      }
    }

    let parameters = dataset['info']['parameters'];

    let DEPEND_0s = [];
    let pidx = 0;
    parameterArray = [];
    for (let name of Object.keys(parameters)) {

      let parameter = parameters[name];
      let varType = parameter['_vAttributesKept']['_VAR_TYPE'];

      let omsg = ' has an un-handled DEPEND_2. Omitting dataset.';
      if (parameter['_vAttributesKept']['_DEPEND_2'] === null) {
        util.error(id, name + omsg, false);
        dataset = null;
        break;
      }

      if (dataset === null) {
        continue;
      }

      // Move kept vAttributes up
      for (let key of Object.keys(parameter['_vAttributesKept'])) {
        if (!key.startsWith('_'))
          parameter[key] = parameter['_vAttributesKept'][key];
      }

      if (!parameter['fill'] && varType == 'data') {
        util.warning(dataset['id'], 'No fill for ' + parameter['name']);
      }

      if (parameter['fill'] && varType == 'data' && parameter['fill'].toLowerCase() == "nan") {
        // Found in THA_L2_MOM
        util.warning(dataset['id'], 'FILLVAL cast to lower case is nan for ' + parameter['name'] + '. Setting to -1e31');
        parameter['units'] = "-1e31";
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
        util.warning( dataset['id'],
          `${parameter['name']} has DEPEND_0 name of '${DEPEND_0}'; expected 'Epoch'`);
      }
      DEPEND_0s.push(DEPEND_0);

      // Extract DEPEND_1
      let vectorComponents = false;
      if (parameter['_vAttributesKept']['_DEPEND_1']) {
        let DEPEND_1 = parameter['_vAttributesKept']['_DEPEND_1'];
        let depend1 = extractDepend1(dataset['id'], parameters[DEPEND_1]['_variable']);
        if (Array.isArray(depend1)) {
          extractCoordSysNameAndVecComps(dataset['id'], parameter, depend1);
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
        parameterArray.push(util.copy(parameter));
      }
    }

    let EpochName = DEPEND_0s[0];
    let firstTimeValue = parameters[EpochName]['_variable']['cdfVarData']['record'][0]['value'][0];
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
  }

  writeFiles(datasets);
}

function writeFiles(datasets) {

  // Write one info file per HAPI dataset.

  util.log('\nWriting HAPI info and info-full files.\n');

  let allIds = [];
  let catalog = [];
  let ididx = 0;
  for (let dataset of datasets) {

    let id = dataset['id'];
    allIds.push(id);

    // Re-order keys
    let order = {"id": null, "title": null, "info": null};
    datasets[ididx] = Object.assign(order, dataset);

    order = {
              "startDate": null,
              "stopDate": null,
              "sampleStartDate": null,
              "sampleStopDate": null,
              "x_sampleRecords": null,
              "cadence": null,
              "contact": null,
              "resourceURL": null,
              "resourceID": null,
              "x_creationDate": null,
              "x_datasetCitation": null,
              "x_datasetTermsOfUse": null,
              "parameters": null
            }
    datasets[ididx]['info'] = Object.assign(order, dataset['info']);

    let fnameInfoFull = util.baseDir(id) + '/' + id + '-info-full.json';

    util.writeSync(fnameInfoFull, util.obj2json(dataset));

    deleteUnderscoreKeys(datasets[ididx]);

    let fnameInfo = argv.infodir + '/' + id + '.json';

    util.writeSync(fnameInfo, util.obj2json(datasets[ididx]['info']));

    catalog.push({'id': dataset['id'], 'title': dataset['title']});
    ididx = ididx + 1;
  }

  // Write HAPI ids-hapi.txt containing all HAPI dataset ids.
  util.writeSync(argv.cachedir + '/ids-hapi.txt', allIds.join('\n'), 'utf8');

  // Write HAPI catalog.json containing /catalog response JSON
  util.writeSync(argv.catalog, util.obj2json(catalog));

  // Write HAPI all.json containing all content from all info files.
  util.writeSync(argv.all, util.obj2json(datasets));

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

  let datasetsExpanded = util.copy(datasets);

  let omsg = ' from HAPI all.json because no variable attributes.';
  for (let [dsidx, dataset] of Object.entries(datasets)) {
    if (!dataset['_variables']) {
      util.error(dataset['id'], 'Omitting ' + dataset['id'] + omsg, false);
      continue;
    }

    let _data = JSON.parse(util.readSync(dataset['_data']))['CDF'][0];
    extractParameterAttributes(dataset, _data);
    extractDatasetAttributes(dataset, _data);

    let subdatasets = subsetDataset(dataset);
    if (subdatasets.length > 1) {
      util.log('  Note: ' + subdatasets.length + ' sub-datasets');
    }
    datasetsExpanded.splice(dsidx, 1, ...subdatasets);
  }

  datasets = null;
  // This assumes first variable is always the primary time variable.
  // This assumption needs to be checked.
  // will not be true for datasets with multipl DEPEND_0s.

  return datasetsExpanded;

  function subsetDataset(dataset) {

    // Find parameters that have more than one DEPEND_0. Split associated
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
      newdataset = util.copy(dataset);
      newdataset['id'] = newdataset['id'] + '@' + sdsidx;
      newdataset['info']['x_DEPEND_0'] = DEPEND_0;
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

function extractParameterNames(dataset) {

  let variableDescriptions = dataset['_variables']['VariableDescription']
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

  let cdfGAttributes = _data['cdfGAttributes']['attribute'];

  let epochVariable = _data['cdfVariables']['variable'][0];
  if (!epochVariable['cdfVarInfo']['cdfDatatype'].startsWith('CDF_EPOCH')) {
    error(id,'First variable does not have cdfDatatype that starts with "CDF_EPOCH', true);    
  }
  let Nr = epochVariable['cdfVarData']['record'].length;
  dataset['info']['sampleStartDate'] = epochVariable['cdfVarData']['record'][0]['value'][0];
  let Nl = Nr;
  if (Nr > 10) {
    Nl = 10;
  }
  dataset['info']['sampleStopDate'] = epochVariable['cdfVarData']['record'][Nl-1]['value'][0];
  dataset['info']['x_sampleRecords'] = Nr;

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

function extractParameterAttributes(dataset) {

  let _data = JSON.parse(util.readSync(dataset['_data']))['CDF'][0];

  let cdfVariables = _data['cdfVariables']['variable'];
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
    util.warning(dsid, parameter['name'] + ': Assumed DEPEND_1 = [' 
                + depend1.join(', ') + '] => vectorComponents = [x, y, z]');
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
