// Call the SPDF/CDAWeb API and respond with HAPI CSV.
// Or, pass through CDAS cdf or text responses un-altered.
// Test using defaults using
//    node CDAS2HAPIcsv.js
const fs      = require('fs');
const request = require('request');
const moment  = require('moment');
const argv    = require('yargs')
                  .default
                    ({
                        'id': 'AC_H2_MFI',
                        'parameters': 'Magnitude,BGSEc',
                        'start': '2009-06-01T00:00:00.000000000Z',
                        'stop':  '2009-06-01T12:00:00.000000000Z',
                        'format': 'csv',
                        'encoding': 'gzip',
                        'infodir': 'hapi/bw/info',
                        'debug': false,
                    })
                  .option('debug',{'type': 'boolean'})
                  .argv;

let info = fs.readFileSync(__dirname + "/" + argv.infodir + "/" + argv.id + ".json");

info = JSON.parse(info);

let timeOnly = false;
if (argv.parameters.trim() === '' || argv.parameters.trim() === "Time") {
  let names = [];
  for (parameter of info['parameters']) {
    let name = parameter["name"];
    if (name !== "Time") {
      names.push(name);
    }
  }
  if (argv.parameters.trim() === "Time") {
    timeOnly = true;
    argv.parameters = names[0];
  } else {
    argv.parameters = names.join(",");    
  }
}

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

let cdfFileDir = __dirname + "/tmp";
if (!fs.existsSync(cdfFileDir)) {
  fs.mkdirSync(cdfFileDir, {recursive: true});
}

let base = "https://cdaweb.gsfc.nasa.gov/WS/cdasr/1/dataviews/sp_phys/datasets";

let IDr = ID.replace(/@[0-9].*$/,"")
let url = `${base}/${IDr}/data/${START_STR},${STOP_STR}/${PARAMETERS}?format=${FORMAT}`;
if (FORMAT === 'csv') {
  // text output will be transformed into HAPI csv
  url = `${base}/${IDr}/data/${START_STR},${STOP_STR}/${PARAMETERS}?format=text`;
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
                line = line
                        .replace(/^([0-9]{2})-([0-9]{2})-([0-9]{4}) ([0-9]{2}:[0-9]{2}:[0-9]{2})\.([0-9]{3})\.([0-9]{3})/, "$3-$2-$1T$4.$5$6Z")
                        .replace(/^([0-9]{2})-([0-9]{2})-([0-9]{4}) ([0-9]{2}:[0-9]{2}:[0-9]{2}\.[0-9]{3})\s/, "$3-$2-$1T$4Z ")
                        .replace(/\s+/g,",");
                if (timeOnly) {
                  line = line.replace(/Z.*/,'Z');
                }
                return line;               
              } else {
                return "";
              }
          })
          .filter(function(s){ return s !== '' });

  if (body.length > 1) {
    // Remove last record if needed. 
    let lastDateTime;
    if (timeOnly) {
      lastDateTime = body[body.length - 1];
    } else {
      lastDateTime = body[body.length - 1].split(",")[0];
    }
    //console.log(lastDateTime.toISOString())
    if (moment(lastDateTime).isSame(STOP) || moment(lastDateTime).isAfter(STOP)) {
        //console.log("Removing last element")
        body = body.slice(0, -1);
    }
  }

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

  if (DEBUG) {console.error("Requesting: \n  " + url)}

  if (data == true) {
    if (FORMAT === 'text') {
      // Pass through.
      let opts = {"url": url, "strictSSL": false, "gzip": ENCODING};
      request(opts).pipe(process.stdout);
      return;
    } else if (FORMAT === 'csv-cdfdump') {
      cdfdump();
      return;
    } else if (FORMAT == 'csv-apds') {
    } else if (FORMAT == 'text2csv') {
    }
  }

  function cdfdump(cdfFileName) {

    let req = request(opts);
    if (DEBUG) {
      console.log("Writing: " + cdfFileName);
    }
    let cdfFileName = cdfFileDir + "/" + url.split("/").slice(-1)[0];
    let cdfFileStream = fs.createWriteStream(cdfFileName);
    req
      .on('end', () => {
        console.log("Wrote:   " + cdfFileName);
        dump(cdfFileName);
      })
      .pipe(cdfFileStream);

    let cmd = "python3 cdfdump.py"
            + " --file=" + cdfFileName
            + " --id=" + IDr
            + " --parameters='" + argv.parameters + "'"
            + " --start=" + argv.start
            + " --stop=" + argv.stop;
    let opts = {"encoding": "buffer"};
    let child = require('child_process').spawn('sh', ['-c', cmd], opts);

    child.stderr.on('data', function (err) {
      console.error("Command " + cmd + " gave stderr:\n" + err);
    });
    child.stdout.on('data', function (buffer) {
      console.log(buffer.toString());
    });
  }

  let opts = {"url": url, "strictSSL": false, "gzip": ENCODING};
  request(opts,
    function (error, response, body) {
      if (error) {
        console.log(error);
        process.exit(1);    
      }
      if (response && response.statusCode != 200) {
        if (response.statusCode == 503) {
          console.error(body)
        }
        console.error("Non-200 HTTP status code: " + response.statusCode);
        process.exit(1);
      }
      if (DEBUG) {console.error("Finished request:\n  " + url)}
      cb(body);
  });

}
