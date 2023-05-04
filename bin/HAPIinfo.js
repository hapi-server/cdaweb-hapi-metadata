// Create a HAPI all.json catalog using HAPI /catalog
// and /info responses from https://cdaweb.gsfc.nasa.gov/hapi.

const fs      = require('fs');
const request = require("request");
const xml2js  = require('xml2js').parseString;
const {exec}  = require('node:child_process');
const argv    = require('yargs')
                  .default
                    ({
                      'keepids': '^AC_',
                      'version': 'nl',
                      'hapiurl': '',
                      'maxsockets': 1
                    })
                  .argv;

let DATSET_ID_RE = new RegExp(argv.keepids);
let HAPIURL = argv.hapiurl;

// pool should be set outside of loop. See
// https://www.npmjs.com/package/request#requestoptions-callback
// Set max sockets to a single host.
let pool = {maxSockets: argv.maxsockets};

let hapiURL = HAPIURL;
if (hapiURL === '') { 
  if (argv.version === 'nljf' || argv.version === 'nl' || argv.version === 'jf') {
    // 'jf' method can not produce /catalog response, so
    // response from 'nl' is used.
    hapiURL = "https://cdaweb.gsfc.nasa.gov/hapi";
  } else if (argv.version === 'bh') {
    pool = {maxSockets: 1};
    hapiURL  = "https://cdaweb.gsfc.nasa.gov/registry/hdp/hapi";
  } else {
    console.error("version must be either 'nl' or 'bh'.");
    process.exit(1);
  }
}

let cacheDir = "cache/" + argv.version;
let outDir   = "hapi/" + argv.version;

if (!fs.existsSync(cacheDir + "/info")) {fs.mkdirSync(cacheDir + "/info", {recursive: true})}
if (!fs.existsSync(outDir + "/info")) {fs.mkdirSync(outDir + "/info", {recursive: true})}

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

    let fnameIds = cacheDir + "/ids.txt";
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
    let fname = outDir + '/info/' + idf + '.json';
    //let fname = cacheDir + "/" + idf + ".json";
    if (fs.existsSync(fname)) {
      console.log("Reading: " + fname);
      let body = fs.readFileSync(fname, 'utf-8');
      finished(fname, body, ididx, true)
    } else {
      getInfo(fname, id, ididx);
    }
  }

  function finished(fname, body, ididx, fromCache) {

    if (!finished.N) {finished.N = 0}
    finished.N = finished.N + 1;

    body = JSON.parse(body);

    CATALOG[ididx]['info'] = body;
    delete CATALOG[ididx]['info']['status'];

    if (fromCache == false) {
      // TODO: Don't write if error.
      console.log("Writing: " + fname);
      fs.writeFileSync(fname, JSON.stringify(body, null, 2), 'utf-8');
    }

    if (finished.N == CATALOG.length) {
      let fnameAll = outDir + "/all.json";
      if (argv.version === 'bh') {
        let fnameAllFull = cacheDir + "/all-full.json";
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

  function getInfo(fname, id, ididx) {

    let url = hapiURL + "/info?id="+id;
    let reqOpts = {uri: url, pool: pool};
    console.log("Requesting: " + url);
    request(reqOpts, function (err,res,body) {
      if (err) console.log(err);
      console.log("Received: " + url);
      if (argv.version == 'jf') {
        // AutoplotDataServer requires all parameter IDs to be given;
        // One can't request info for all parameters given a dataset ID.
        // So we get the IDs from 'nl' and pass them to AutoplotDataServer.
        let parameterArray = JSON.parse(body)['parameters'];
        getInfoAP(fname, id, ididx, parameterArray);
      } else {
        finished(fname, body, ididx, false);        
      }
    });
  }

  function getInfoAP(fname, id, ididx, parameterArray) {

    if (0) {
      // Should work, but Autoplot throws error.
      let parameters = [];
      for (let pidx in parameterArray) {
        let name = parameterArray[pidx]['name'];
        if (name == 'Time') {
          continue;
        }
        parameters.push(name);
      }
      parameters = parameters.join(",");

      let uri = `vap+cdaweb:ds=${id}&id=${parameters}`;
      let cmd = `java -Djava.awt.headless=true -cp bin/autoplot.jar org.autoplot.AutoplotDataServer -q --uri='${uri}' -f hapi-info`
      console.log("Requesting: " + cmd);
      exec(cmd, (err, body, stderr) => {
        if (err) console.log(err);
        console.log(combine(JSON.parse(body)));
        //combine(JSON.parse(body));
      });
    }

    if (1) {
      let psidx = 0;
      for (let parameterSet of parameterArray) {
        let name = parameterSet['name'];
        if (name == 'Time') {
          continue;
        }

        let uri = `vap+cdaweb:ds=${id}&id=${name}`;
        let cmd = `java -Djava.awt.headless=true -cp bin/autoplot.jar org.autoplot.AutoplotDataServer -q --uri='${uri}' -f hapi-info`
        doExec(cmd, psidx);
        psidx = psidx + 1;      
      }

      function doExec(cmd, psidx) {
        console.log("Requesting: " + cmd);
        exec(cmd, (err, body, stderr) => {
          if (err) console.log(err);
          combine(psidx, JSON.parse(body));
        });
      }

      function combine(psidx, body) {

      if (combine.N === undefined) {
        combine.N = 0;
      } else {
        combine.N = combine.N + 1;
      }

      if (!combine.all) {
        combine.all = body;
        combine.all['_parameters'] = [];
      }

      combine.all['_parameters'][psidx] = body['parameters'];

      if (combine.N == parameterArray.length - 2) {
        combine.all['parameters'] = [];
        for (psidx in combine.all['_parameters']) {
          //console.log(combine.all['_parameters'][psidx])
          if (psidx == 0) {
            combine.all['parameters'] = combine.all['parameters'].concat(combine.all['_parameters'][psidx]);
          } else {
            combine.all['parameters'] = combine.all['parameters'].concat(combine.all['_parameters'][psidx].slice(1));            
          }
        }
        delete combine.all['_parameters']
        finished(fname, JSON.stringify(combine.all), ididx, false);
      }
    }

    }
  }
}

