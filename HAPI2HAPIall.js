// Create a HAPI all.json catalog using HAPI /catalog
// and /info responses from https://cdaweb.gsfc.nasa.gov/hapi.

const fs      = require('fs');
const request = require("request");
const xml2js  = require('xml2js').parseString;
const argv    = require('yargs')
                  .default
                    ({
                      'idregex': '^AC_',
                      'version': 'nl'
                    })
                  .argv;

let DATSET_ID_RE = new RegExp(argv.idregex);

// pool should be set outside of loop. See
// https://www.npmjs.com/package/request#requestoptions-callback
// Set max sockets to a single host.
let pool = {maxSockets: 3};

let hapiURL; 
if (argv.version === 'nl') {
  hapiURL = "https://cdaweb.gsfc.nasa.gov/hapi";
} else if (argv.version === 'bh') {
  pool = {maxSockets: 1};
  hapiURL  = "https://cdaweb.gsfc.nasa.gov/registry/hdp/hapi";
} else {
  console.error("version must be either 'nl' or 'bh'.");
  process.exit(1);
}

let outDir   = "cache/" + argv.version;
let fnameAll = "all-" + argv.version + ".json";

if (!fs.existsSync(outDir)) {fs.mkdirSync(outDir, {recursive: true})}

let CATALOG = {};

catalog();

function catalog(cb) {

  let fname = outDir + "/catalog.json";

  if (fs.existsSync(fname)) {
    console.log("Reading: " + fname);
    let body = fs.readFileSync(fname, 'utf-8');
    finished(fname, body, true);
    return;
  }

  let url = hapiURL + "/catalog"
  let reqOpts = {uri: url};
  console.log("Requesting: " + url);
  request(reqOpts, function (err,res,body) {
    if (err) console.log(err);
    console.log("Received: " + url);
    finished(fname, body, false);
  });

  function finished(fname, body, fromCache) {

    body = JSON.parse(body);

    if (fromCache == false) {
      // TODO: Don't write if error.
      console.log("Writing: " + fname);
      fs.writeFileSync(fname, JSON.stringify(body, null, 2), 'utf-8');
      //console.log("Wrote:   " + fname);
    }    
    let ids = [];
    for (dataset of body['catalog']) {
      ids.push(dataset['id']);
    }

    let fnameIds = outDir + "/ids-hapi.txt";
    console.log("Writing: " + fnameIds);
    fs.writeFileSync(fnameIds, ids.join("\n"));

    info(body['catalog']);
  }
}

function info(CATALOG) {

  let N = 0;
  for (ididx in CATALOG) {
    // ididx = datset id index

    let id = CATALOG[ididx]['id'];
    let idf = id; // id for file name
    if (argv.version === 'bh') {
      idf = CATALOG[ididx]['x_SPDF_ID'];
    }

    if (DATSET_ID_RE.test(idf) == false) {
      CATALOG[ididx] = null;
    }
  }

  CATALOG = CATALOG.filter(function (el) {return el != null;});

  for (ididx in CATALOG) {
    delete CATALOG[ididx]['status'];
    let id = CATALOG[ididx]['id'];
    let idf = id; // id for file name
    if (argv.version === 'bh') {
      idf = CATALOG[ididx]['x_SPDF_ID'];
    }
    let fname = outDir + "/" + idf + ".json";
    if (fs.existsSync(fname)) {
      console.log("Reading: " + fname);
      let body = fs.readFileSync(fname, 'utf-8');
      finished(fname, body, ididx, true)
    } else {
      getInfo(fname, id, ididx);
    }
  }

  function getInfo(fname, id, ididx) {

    let url = hapiURL + "/info?id="+id;
    let reqOpts = {uri: url, pool: pool};
    console.log("Requesting: " + url);
    request(reqOpts, function (err,res,body) {
      if (err) console.log(err);
      console.log("Received: " + url);
      finished(fname, body, ididx, false);
    });
  }

  function finished(fname, body, ididx, fromCache) {

    if (!finished.N) {finished.N = 0}
    finished.N = finished.N + 1;

    body = JSON.parse(body);

    CATALOG[ididx]['info'] = body;

    if (fromCache == false) {
      // TODO: Don't write if error.
      console.log("Writing: " + fname);
      fs.writeFileSync(fname, JSON.stringify(body, null, 2), 'utf-8');
    }

    if (finished.N == CATALOG.length) {
      if (argv.version === 'bh') {

        let fnameAllFull = fnameAll.replace('.json','-full.json');
        console.log("Writing: " + fnameAllFull);
        fs.writeFileSync(fnameAllFull, JSON.stringify(CATALOG, null, 2), 'utf-8');        

        for (ididx in CATALOG) {
          for (datasetkey of Object.keys(CATALOG[ididx])) {
            if (datasetkey.toLowerCase().startsWith('x_')) {
              if (datasetkey.toLowerCase() !== "x_spdf_id") {
                delete CATALOG[ididx][datasetkey];
              }
            }
          }
          for (infokey of Object.keys(CATALOG[ididx]['info'])) {

            if (infokey.toLowerCase().startsWith('x_')) {
              delete CATALOG[ididx]['info'][infokey];
            }
          }
        }

      }

      console.log("Writing: " + fnameAll);
      fs.writeFileSync(fnameAll, JSON.stringify(CATALOG, null, 2), 'utf-8');
    }
  }
}

