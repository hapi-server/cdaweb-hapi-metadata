const fs      = require('fs');
const chalk   = require("chalk");
const request = require("request");
const xml2js  = require('xml2js').parseString;
const argv    = require('yargs')
                  .default
                    ({
                      'idregex': '^AC_OR',
                      'urls': 'http://localhost:8999/CDAWeb-cdas/hapi,https://cdaweb.gsfc.nasa.gov/hapi',
                      'vurl': 'http://localhost:9999/',
                      'showfails': false,
                      'dataset': '',
                      'maxsockets': 1
                    })
                  .argv;

function outDir(url) {
  let dir = "data/"
              + url
              .replace("://",".")
              .replace(/hapi(\/|)$/,"")
              .replace(/\//g,"-")
              .replace(/-$/g,"")
  if (!fs.existsSync(dir)) {
    fs.mkdirSync(dir, {recursive: true});
  }
  return dir;
}

let CATALOG = {};

let urls = argv.urls.split(",");
//urls = [urls[0]];

catalog();

function catalog(cb) {

  let fname = outDir(urls[0]) + "/catalog.json";
  let url = urls[0] + "/catalog"
  let reqOpts = {uri: url};
  console.log("Requesting: " + url);
  request(reqOpts, function (err,res,body) {
    if (err) console.log(err);
    console.log("Received: " + url);
    info(JSON.parse(body)["catalog"]);
  });
}

function info(CATALOG) {

  for (let ididx in CATALOG) {

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
    console.error(`idregx ${argv.idregex} selected zero id. Exiting.`);
    process.exit(1);
  } else {
    let plural = CATALOG.length > 1 ? "s" : "";
    console.log(`idregx ${argv.idregex} selected ${CATALOG.length} id${plural}. Each test takes 5-60 seconds.`);
  }

  verify(0);

  function verify(ididx) {

    if (ididx === CATALOG.length) {
      return;
    }

    get[ididx] = {};
    for (let url of urls) {
      get(ididx, url);
    }

  }

  function get(ididx, urlo) {

    let id = CATALOG[ididx]['id'];
    let url = argv['vurl'] 
            + "?url=" + urlo 
            + "&id=" + id 
            + "&output=json";

    let reqOpts = {uri: url};
    //console.log("Requesting: " + url);
    let msstart = Date.now();
    request(reqOpts, function (err,res,body) {

      if (err) console.log(err);
      let result = JSON.parse(body);

      let dt = ((Date.now()-msstart)/1000.0).toFixed(1);
      if (result['fails'].length == 0) {
        get[ididx][urlo] = chalk.green.bold(`✓ PASS: ${id} `) + `${dt} [s] ${urlo}`;
      } else {
        fs.writeFileSync(outDir(urlo) + "/" + id + "-fails.json", JSON.stringify(result['fails'], null, 2));
        let plural = result['fails'].length > 1 ? "s" : "";
        let fails = "\n  " + url;
        let see = `see ${outDir(urlo)}/${id}-fails.json`
        if (argv.showfails) {
          see = "";
          let num = 1;
          for (fail of result['fails']) {
            fails = fails + "\n  " + num + ". " + fail['description'].replace(/(<([^>]+)>)/gi, "") + "\n     Result: " + fail['got'];
            num++;
          }
        }
        get[ididx][urlo] = chalk.red.bold(`✗ FAIL: ${id} `) + `${dt} [s] ${urlo} ` + chalk.red(`| ${result['fails'].length} failure${plural} ${see} ` + fails);
      }
      if (Object.keys(get[ididx]).length == urls.length) {
        for (let urlo of urls) {
          console.log(get[ididx][urlo]);
        }
        verify(++ididx);
      }
    });
  }
}
