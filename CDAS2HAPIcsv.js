// Call the SPDF/CDAWeb API and respond with HAPI CSV.
// Or, pass through CDAS cdf or text responses un-altered.
const fs      = require('fs');
const request = require('request');
const moment  = require('moment');
const argv    = require('yargs')
                  .default
                    ({
                        'id': 'AC_H2_MFI',
                        'parameters': 'Magnitude',
                        'start': '2009-06-01T00:00:00.000000000Z',
                        'stop': '2009-06-01T00:00:01.000000000Z',
                        'format': 'csv',
                        'encoding': 'gzip',
                        'debug': false,
                    })
                  .option('debug',{'type': 'boolean'})
                  .argv;

// https://stackoverflow.com/questions/51226163/how-can-i-hide-moment-errors-in-nodejs
moment.suppressDeprecationWarnings = true;  
if (!moment(argv.start).isValid()) {
  console.error("Error: Invalid start: " + argv.start);
  process.exit(1);
}
if (!moment(argv.stop).isValid()) {
  console.error("Error: Invalid stop: " + argv.stop);
  process.exit(1);
}

let ID          = argv.id;
let START       = moment(argv.start);
let STOP        = moment(argv.stop);
let PARAMETERS  = argv.parameters.split(",");
let FORMAT      = argv.format;
let ENCODING    = argv.gzip;
let DEBUG       = argv.debug;

let START_STR = START.utc().format('YYYYMMDDTHHmmss') + "Z";
let STOP_STR  = STOP.utc().format('YYYYMMDDTHHmmss') + "Z";

let base = "https://cdaweb.gsfc.nasa.gov/WS/cdasr/1/dataviews/sp_phys/datasets";

let url = `${base}/${ID}/data/${START_STR},${STOP_STR}/${PARAMETERS}?format=${FORMAT}`;
if (FORMAT === 'csv') {
  // text output will be transformed into HAPI csv
  url = `${base}/${ID}/data/${START_STR},${STOP_STR}/${PARAMETERS}?format=text`;
}

makeRequest(url, false, extractURL);

function extractData(body) {
  body = body
          .toString()
          .split("\n")
          .map(function(line){
              if (line.search(/^[0-9]{2}-[0-9]{2}-[0-9]{4}/) != -1) {
                // First replace converts to restricted HAPI 8601
                // Second converts fractional sections from form
                // .xxx.yyyy to .xxxyyy.
                // Last replaces whitespace with comma (this assumes data are
                // never strings with spaces).
                return line
                        .replace(/^([0-9]{2})-([0-9]{2})-([0-9]{4}) ([0-9]{2}:[0-9]{2}:[0-9]{2})\.([0-9]{3})\.([0-9]{3})/, "$3-$2-$1T$4.$5$6Z")
                        .replace(/^([0-9]{2})-([0-9]{2})-([0-9]{4}) ([0-9]{2}:[0-9]{2}:[0-9]{2}\.[0-9]{3})\s/, "$3-$2-$1T$4Z ")
                        .replace(/\s+/g,",")
              } else {
                return "";
              }
          })
          .filter(function(s){ return s !== '' });
  console.log(body.join("\n"));
}

function extractURL(body) {

  if (FORMAT === 'json') {
    // TODO: Convert to HAPI JSON. Probably easier to convert
    // CSV output to JSON, however.
    console.log(JSON.stringify(JSON.parse(body), null, 2));
    return;
  }

  let m = body.match("<Name>(.*?)</Name>");
  if (m && m[1] && m[1].startsWith("http")) {
    makeRequest(m[1], true, extractData);    
  } else {
    let m = body.match("<Status>(.*?)</Status>");
    if (m && m[1]) {
      console.error("Status message in returned XML: " + m[1]);
    } else {
      console.error("Returned XML does not have URL to temporary file:");
      console.error(body);
    }
    process.exit(0);
  }
}

function makeRequest(url, data, cb) {

  if (DEBUG) {
    console.log("Requesting: \n  " + url);
  }

  let opts =
              {
                url: url,
                strictSSL: false,
                gzip: ENCODING
              };

  if (data == true && FORMAT !== 'csv') {
    // Pass through.
    let req = request(opts);
    req.pipe(process.stdout);
    return;
  }

  request(opts,
    function (error, response, body) {
      if (error) {
        console.log(error);
        process.exit(1);    
      }
      if (response && response.statusCode != 200) {
        console.error("Non-200 HTTP status code: " + response.statusCode);
        process.exit(1);
      }
      if (DEBUG) {
        console.log("Finished request:\n  " + url);
      }
      cb(body);
  })
}
