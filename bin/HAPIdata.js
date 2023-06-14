// Test using defaults using
//    node HAPIdata.js

const fs      = require('fs');
const request = require('request');
const moment  = require('moment');
const argv    = require('yargs')
                  .default
                    ({
                        'id': 'AC_H2_MFI',
                        'parameters': '',
                        'start': '2009-06-01T00:00:00.000000000Z',
                        'stop':  '2009-06-03T12:00:00.000000000Z',
                        'format': 'csv',
                        'from': 'cdas-text-using-js',
                        'encoding': 'gzip',
                        'infodir': '../hapi/bw/CDAWeb/info',
                        'debug': false,
                    })
                  .option('debug',{'type': 'boolean'})
                  .argv;


let url;

let infoFile = argv.infodir.replace(__dirname,"") + "/" + argv.id + ".json";  
if (!argv.infodir.startsWith("/")) {
  infoFile = __dirname + "/" + argv.infodir.replace(__dirname,"") + "/" + argv.id + ".json";  
}
let info = fs.readFileSync(infoFile);

info = JSON.parse(info);

let timeOnly = false;
if (argv.parameters.trim() === '' || argv.parameters.trim() === "Time") {
  let names = [];
  for (let parameter of info['parameters']) {
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
let FROM        = argv.from;
let ENCODING    = argv.gzip;
let DEBUG       = argv.debug;

let cdfFileDir = __dirname + "/tmp";
if (!fs.existsSync(cdfFileDir)) {
  fs.mkdirSync(cdfFileDir, {recursive: true});
}

if (FROM === 'nl-hapi') {
  url = "https://cdaweb.gsfc.nasa.gov/hapi/data"
  url = url + `?id=${ID}&parameters=${argv.parameters}&time.min=${argv.start}&time.max=${argv.stop}`;
  let opts = {"url": url, "strictSSL": false};
  if (DEBUG) {console.log("Requesting: \n  " + url)}
  // Pass through.
  request(opts).pipe(process.stdout);
}

if (FROM === 'bh-hapi') {
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

if (FROM === 'cdas-cdf-using-pycdaws') {
  cdf2csv();
}

if (FROM === 'apds') {
  cdf2csv();
}

// Remove "@N" from end of dataset names.
let IDr = ID.replace(/@[0-9].*$/,"")

let base = "https://cdaweb.gsfc.nasa.gov/WS/cdasr/1/dataviews/sp_phys/datasets";
if (FROM === 'cdas-text-using-js' || FROM === 'text-raw' || FROM === 'text-noheader') {
  let START_STR = START.utc().format('YYYYMMDDTHHmmss') + "Z";
  let STOP_STR  = STOP.utc().format('YYYYMMDDTHHmmss') + "Z";
  url = `${base}/${IDr}/data/${START_STR},${STOP_STR}/${PARAMETERS}?format=text`;
  getFromLink(url);
}

if (FROM === 'cdas-cdf-using-pycdf') {
  let START_STR = START.utc().format('YYYYMMDDTHHmmss') + "Z";
  let STOP_STR  = STOP.utc().format('YYYYMMDDTHHmmss') + "Z";
  url = `${base}/${IDr}/data/${START_STR},${STOP_STR}/${PARAMETERS}?format=cdf`;
  getFromLink(url);
}

if (FROM === 'cdaweb-csv-using-js') {
  // TODO: Append ms.
  START_STR = START.utc().format('YYYY/MM/DD+HH:mm:ss');
  STOP_STR  = STOP.utc().format('YYYY/MM/DD+HH:mm:ss');
  START_STR = START_STR.replace("/","%2F").replace(":","%3A")
  STOP_STR = STOP_STR.replace("/","%2F").replace(":","%3A")
  let parameters = `&var=${IDr}+` + PARAMETERS.join(`&var=${IDr}+`);
  base = `https://cdaweb.gsfc.nasa.gov/cgi-bin/eval3.cgi?dataset=+${IDr}&start=${START_STR}&stop=${STOP_STR}&bin_value=15&time_unit=min&missval=fillval&spike_opt=none&nobin_spike_opt=light&index=sp_phys&width_inp=3&spinner=2&autoChecking=on&action=list&csvOptions=1&csv=1`;
  url = base + parameters;
  getFromLink(url);
}

if (FROM === 'cdaweb-cdf-using-pycdf') {
  // TODO: Append ms.
  START_STR = START.utc().format('YYYY/MM/DD+HH:mm:ss');
  STOP_STR  = STOP.utc().format('YYYY/MM/DD+HH:mm:ss');
  START_STR = START_STR.replace("/","%2F").replace(":","%3A")
  STOP_STR = STOP_STR.replace("/","%2F").replace(":","%3A")
  let parameters = `&var=${IDr}+` + PARAMETERS.join(`&var=${IDr}+`);
  base = `https://cdaweb.gsfc.nasa.gov/cgi-bin/eval3.cgi?dataset=+${IDr}&start=${START_STR}&stop=${STOP_STR}&bin_value=15&time_unit=min&missval=fillval&spike_opt=none&nobin_spike_opt=light&index=sp_phys&width_inp=3&spinner=2&autoChecking=on&action=create&cdf27=cdf27`;
  url = base + parameters;
  getFromLink(url);
}


function getFromLink(url) {

  if (DEBUG) {console.log("Requesting: \n  " + url)}

  let opts = {"url": url, "strictSSL": false};
  if (argv.encoding === 'gzip') {
    opts['gzip'] = true;
  }
  request(opts,
    function (error, response, body) {
      if (error) {
        console.error("Request error:")
        console.error(error);
        process.exit(1);    
      }
      if (response && response.statusCode != 200) {
        console.error("Non-200 HTTP status code: " + response.statusCode);
        console.error("HTTP 503 error. Returned body:")
        console.error(body)
        process.exit(1);
      }
      if (DEBUG) {console.log("Finished request:\n  " + url)}
      extractLink(body, response.headers, getAndProcess);
  });
}

function extractLink(body, headers, getAndProcess) {

  if (FROM === 'json') {
    // TODO: Convert to HAPI JSON. Probably easier to convert
    // CSV output to JSON, however.
    console.log(JSON.stringify(JSON.parse(body), null, 2));
    return;
  }

  if (DEBUG) {
    //console.log(body);
  }

  if (FROM === 'cdaweb-csv-using-js' || FROM === 'cdaweb-cdf-using-pycdf') {
    let ext = 'csv';
    if (FROM === 'cdaweb-cdf-using-pycdf') {
      ext = 'cdf';
    }
    let m = body.match(`<a href="\/(.*?)\.${ext}">`);
    if (m && m[1]) {
      let linkURL = "https://cdaweb.gsfc.nasa.gov/" + m[1] + "." + ext;
      if (DEBUG) {console.log("Link:\n  " + linkURL)}
      getAndProcess(linkURL);
    } else {
      console.error("Returned HTML does not have URL to temporary file:");
      console.error(body);
      process.exit(0);
    }
    return;
  } 

  const xml2js = require('xml2js').parseString;

  xml2js(body, function (err, obj) {
    if (err) {
      console.error("Could not parse returned XML:");
      console.error(body);
      console.error(headers);
      process.exit(0);
    }
    let DataResult = obj["DataResult"];
    if (!DataResult) {
      console.error("Returned XML does not <DataResult>");
      console.error(body);
      console.error("HTTP headers:")
      console.error(headers);
      process.exit(0);
    }
    // TODO: Check for FileDescription.
    let Name = DataResult["FileDescription"][0]["Name"][0];
    let linkURL = Name;
    if (DEBUG) {console.log("Link:\n  " + linkURL)}
    let Error_ = DataResult["Error"];
    if (Error_) {
      console.error("Returned XML has error message:");
      console.error(Error_);
      let Status = DataResult["Status"];
      if (Status) {
        console.error("Status message:");
        console.error(Status);
      }
      console.error("HTTP headers:")
      console.error(headers);
      process.exit(0);
    }
    getAndProcess(linkURL);
  });
}

function getAndProcess(url) {

  if (DEBUG) {console.log("Requesting: \n  " + url)}

  let opts = {"url": url, "strictSSL": false, "gzip": ENCODING};

  if (FROM === 'cdas-cdf-using-pycdf' || FROM === 'cdaweb-cdf-using-pycdf') {
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
  if (FROM == 'cdas-text-using-js' || FROM == 'cdaweb-csv-using-js' || FROM.startsWith('text')) {
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

  if (FROM === 'text-raw') {
    console.log(body);
    return;
  }

  if (FROM === 'text-noheader') {
    // Remove header and footer
    let idx = body.search(/^[0-9][0-9]/gm);
    body = body.slice(idx);
    idx = body.search(/^#/gm);
    return body.slice(0,idx-1);
  } else {
    let re1 = new RegExp(/^[0-9]{2}-[0-9]{2}-[0-9]{4}/);
    if (FROM === "cdaweb-csv-using-js") {
      re1 = /^[0-9]{4}-[0-9]{2}-[0-9]{2}/;
    }
    body = 
      body
        .toString()
        .split("\n")
        .map(function(line) {
            if (line.search(re1) != -1) {
              if (FROM === "cdas-text-using-js") {
                // First replace converts to restricted HAPI 8601
                // Second converts fractional sections from form
                // .xxx.yyyy to .xxxyyy.
                // Last replaces whitespace with comma (this assumes data are
                // never strings with spaces).
                line = line
                        .replace(/^([0-9]{2})-([0-9]{2})-([0-9]{4}) ([0-9]{2}:[0-9]{2}:[0-9]{2})\.([0-9]{3})\.([0-9]{3})/, "$3-$2-$1T$4.$5$6Z")
                        .replace(/^([0-9]{2})-([0-9]{2})-([0-9]{4}) ([0-9]{2}:[0-9]{2}:[0-9]{2}\.[0-9]{3})\s/, "$3-$2-$1T$4Z ")
              }
              line = line.replace(/\s+/g,",");
              if (timeOnly) {
                line = line.replace(/Z.*/,'Z');
              }
              return line;               
            } else {
              return "";
            }
        })
        .filter(function(s){ return s !== '' });
  }

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
  let debugstr = "";
  if (DEBUG) {
    debugstr = " --debug";
  }
  if (FROM === 'apds') {
    cmd = "java -Djava.awt.headless=true"
        + ` -cp ${__dirname}/autoplot.jar`
        + " org.autoplot.AutoplotDataServer"
        + " -q"
        + " -f hapi-data"
        + ` --uri='vap+cdaweb:ds=${IDr}&id=${argv.parameters.replace(",",";")}&timerange=${argv.start}/${argv.stop}'`;
  }
  if (FROM === 'cdas-cdf-using-pycdf' || FROM === 'cdaweb-cdf-using-pycdf') {
    let lib = 'pycdf';
    let filearg = " --file=" + cdfFileName;
    cmd = `python3 ${__dirname}/cdf2csv.py`
        + ` --lib=${lib}`
        + filearg
        + " --id=" + ID
        + " --parameters='" + argv.parameters + "'"
        + " --start=" + argv.start
        + " --stop=" + argv.stop
        + debugstr;
  }
  if (FROM === 'cdas-cdf-using-pycdaws') {
    cmd = `python3 ${__dirname}/cdf2csv.py`
        + " --lib=pycdaws"
        + " --id=" + ID
        + " --parameters='" + argv.parameters + "'"
        + " --start=" + argv.start
        + " --stop=" + argv.stop
        + debugstr;
  }

  if (DEBUG) {console.log("Executing: \n  " + cmd)}

  let copts = {"encoding": "buffer"};
  let child = require('child_process').spawn('sh', ['-c', cmd], copts);

  child.stderr.on('data', function (err) {
    console.error("Command " + cmd + " gave stderr:\n" + err);
  });
  child.stdout.on('data', function (buffer) {
    process.stdout.write(buffer.toString());
  });
}
