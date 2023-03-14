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
                        'format': 'text',
                        'encoding': 'gzip',
                        'infodir': 'hapi/bw/info',
                        'debug': false,
                    })
                  .option('debug',{'type': 'boolean'})
                  .argv;


let infoFile = argv.infodir.replace(__dirname,"") + "/" + argv.id + ".json";  
if (!argv.infodir.startsWith("/")) {
  infoFile = __dirname + "/" + argv.infodir.replace(__dirname,"") + "/" + argv.id + ".json";  
}
let info = fs.readFileSync(infoFile);

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

let IDr = ID.replace(/@[0-9].*$/,"")

let url;
if (FORMAT === 'csv-nl') {
  url = "https://cdaweb.gsfc.nasa.gov/hapi/data"
  url = url + `?id=${ID}&parameters=${argv.parameters}&time.min=${argv.start}&time.max=${argv.stop}`;
  let opts = {"url": url, "strictSSL": false};
  if (DEBUG) {console.log("Requesting: \n  " + url)}
  // Pass through.
  request(opts).pipe(process.stdout);
}

if (FORMAT === 'csv-bh') {
  let fname = __dirname + '/hapi/bw/info/' + argv.id + '.json';
  if (DEBUG) {console.log("Reading: " + fname);}
  let info = JSON.parse(fs.readFileSync(fname));
  let IDbh = info['resourceID'];
  url = "https://cdaweb.gsfc.nasa.gov/registry/hdp/hapi/data";
  url = url + `?id=${IDbh}&parameters=${argv.parameters}&start=${argv.start}&stop=${argv.stop}&format=x_json2`;
  let opts = {"url": url, "strictSSL": false};
  if (DEBUG) {console.log("Requesting: \n  " + url)}
  // Pass through.
  //request(opts).pipe(process.stdout);
  request(opts,
    function (error, response, body) {
      if (error) {
        console.log(error);
        process.exit(1);    
      }
      json2csv(JSON.parse(body));
  });    
}

if (FORMAT === 'csv-pycdas') {
  cdf2csv();
}

if (FORMAT === 'csv-jf') {
  cdf2csv();
}

let base = "https://cdaweb.gsfc.nasa.gov/WS/cdasr/1/dataviews/sp_phys/datasets";
if (FORMAT === 'csv-text2csv' || FORMAT === 'text') {
  url = `${base}/${IDr}/data/${START_STR},${STOP_STR}/${PARAMETERS}?format=text`;
  getFromCDAS(url);
}

if (FORMAT === 'csv-cdfdump') {
  url = `${base}/${IDr}/data/${START_STR},${STOP_STR}/${PARAMETERS}?format=cdf`;
  getFromCDAS(url);
}

function getFromCDAS(url) {

  if (DEBUG) {console.log("Requesting: \n  " + url)}

  let opts = {"url": url, "strictSSL": false};
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
      if (DEBUG) {console.log("Finished request:\n  " + url)}
      let linkURL = extractURL(body);
      if (DEBUG) {console.log("Link:\n  " + linkURL)}
      getAndProcess(linkURL);
  });
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
    return m[1];
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

function getAndProcess(url) {

  if (DEBUG) {console.log("Requesting: \n  " + url)}

  let opts = {"url": url, "strictSSL": false, "gzip": ENCODING};

  if (FORMAT === 'csv-cdfdump') {
    let req = request(opts);
    let cdfFileName = cdfFileDir + "/" + url.split("/").slice(-1)[0];
    if (DEBUG) {
      console.log("Writing: " + cdfFileName);
    }
    let cdfFileStream = fs.createWriteStream(cdfFileName);
    req
      .on('end', () => {
        if (DEBUG) {
          console.log("Wrote:   " + cdfFileName);
        }
        cdf2csv(cdfFileName);
      })
      .pipe(cdfFileStream);
    return;
  }
  if (FORMAT == 'csv-text2csv' || FORMAT === 'text') {
    let opts = {"url": url, "strictSSL": false};
    request(opts,
      function (error, response, body) {
        if (error) {
          console.log(error);
          process.exit(1);    
        }
        text2csv(body);
    });
  }
}

function text2csv(body) {

  if (FORMAT === 'text') {
    // Remove header and footer
    let idx = body.search(/^[0-9][0-9]/gm);
    body = body.slice(idx);
    idx = body.search(/^#/gm);
    console.log(body.slice(0,idx-1));
    return;
  }

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

function json2csv(json) {
  for (record of json['data']) {
    let line = [];
    for (col of record['record']) {
      // Only handles one level of nesting.
      if (typeof(col) === 'object' && typeof(col) !== null) {
        for (element of col['elements']) {
          line.push(element);
        }
      } else {
          line.push(col);
      }
    }
    console.log(line.join(","));
  }
}

function cdf2csv(cdfFileName) {

  let cmd;
  if (FORMAT === 'csv-jf') {
    cmd = "java -Djava.awt.headless=true"
        + " -cp bin/autoplot.jar"
        + " org.autoplot.AutoplotDataServer"
        + " -q"
        + " -f hapi-data"
        + ` --uri='vap+cdaweb:ds=${IDr}&id=${argv.parameters.replace(",",";")}&timerange=${argv.start}/${argv.stop}'`;
  }
  if (FORMAT === 'csv-cdfdump') {
    cmd = "python3 bin/cdf2csv.py"
        + " --lib=cdflib"
        + " --file=" + cdfFileName
        + " --id=" + ID
        + " --parameters='" + argv.parameters + "'"
        + " --start=" + argv.start
        + " --stop=" + argv.stop
        + " --infodir=hapi/bw/info"
  }
  if (FORMAT === 'csv-pycdas') {
    cmd = "python3 bin/cdf2csv.py"
        + " --lib=pycdaws"
        + " --id=" + ID
        + " --parameters='" + argv.parameters + "'"
        + " --start=" + argv.start
        + " --stop=" + argv.stop
        + " --infodir=hapi/bw/info"
  }

  if (DEBUG) {console.log("Executing: \n  " + cmd)}

  let copts = {"encoding": "buffer"};
  let child = require('child_process').spawn('sh', ['-c', cmd], copts);

  child.stderr.on('data', function (err) {
    console.error("Command " + cmd + " gave stderr:\n" + err);
  });
  child.stdout.on('data', function (buffer) {
    console.log(buffer.toString());
  });
}
