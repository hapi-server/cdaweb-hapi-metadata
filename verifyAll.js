// Create a HAPI all.json catalog using HAPI /catalog
// and /info responses from https://cdaweb.gsfc.nasa.gov/hapi.

const fs      = require('fs');
const request = require("request");
const xml2js  = require('xml2js').parseString;
const argv    = require('yargs')
                  .default
                    ({
                      'idregex': '^.*',
                      'url': 'http://hapi-server.org/servers/TestData2.0/hapi',
                      'vurl': 'http://localhost:9999/',
                      'dataset': '',
                      'maxsockets': 1
                    })
                  .argv;

//'vurl': 'http://hapi-server.org/verify/',
let outDir = "verify/"
              + argv["url"]
                .replace("://",".")
                .replace(/hapi(\/|)$/,"")
                .replace(/\//g,"-")
                .replace(/-$/g,"")

if (!fs.existsSync(outDir)) {
  fs.mkdirSync(outDir, {recursive: true});
}

let CATALOG = {};

catalog();

function catalog(cb) {

  let fname = outDir + "/catalog.json";

  let url = argv['url'] + "/catalog"
  let reqOpts = {uri: url};
  console.log("Requesting: " + url);
  request(reqOpts, function (err,res,body) {
    if (err) console.log(err);
    console.log("Received: " + url);
    info(JSON.parse(body)["catalog"]);
  });
}

function info(CATALOG) {

  for (ididx in CATALOG) {

    // ididx = datset id index
    let id = CATALOG[ididx]['id'];
    let idf = id; // id for file name
    if (argv.version === 'bh') {
      idf = CATALOG[ididx]['x_SPDF_ID'];
    }
    let re = new RegExp(argv['idregex']);
    if (re.test(idf) == false) {
      CATALOG[ididx] = null;
    }
  }

  // Remove nulled elements.
  CATALOG = CATALOG.filter(function (el) {return el !== null;});
  if (CATALOG.length === 0) {
    console.error("idregx selected zero ids. Exiting.");
    process.exit(1);
  }
  verify(0);

  function verify(ididx) {

    if (ididx === CATALOG.length) {
      return;
    }

    let id = CATALOG[ididx]['id'];
    let url = argv['vurl'] + "?url=" + argv['url'] + "&id=" + id + "&output=json";
    let reqOpts = {uri: url};
    console.log("Requesting: " + url);
      request(reqOpts, function (err,res,body) {
        if (err) console.log(err);
        console.log("Received: " + url);
        let result = JSON.parse(body);
        if (result['pass']) {
          console.log("PASS " +  CATALOG[ididx]['id'])
        } else {
          fs.writeFileSync(outDir + "/" + id + ".json", JSON.stringify(result, null, 2));
          console.log("FAIL " + id + " (" + result['fails'].length + " failures)");
        }
        verify(++ididx);
      });
  }
}

