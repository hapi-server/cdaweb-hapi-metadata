const fs = require('fs');
module.exports.util = {
  "get": get,
  "execSync": require('child_process').execSync,
  "existsSync": fs.existsSync,
  "mkdirSync": mkdirSync,
  "rmSync": rmSync,
  "writeSync": fs.writeFileSync,
  "writeAsync": fs.writeFile,
  "readSync": fs.readFileSync,
  "appendSync": appendSync,
  "log": log,
  "warning": warning,
  "error": error,
  "obj2json": obj2json,
  "xml2js": xml2js,
  "incrementTime": incrementTime,
  "str2ISODateTime": str2ISODateTime,
  "str2ISODuration": str2ISODuration
}

util = module.exports.util;

function mkdirSync(dir) {
  if (!fs.existsSync(dir)) {
    fs.mkdirSync(dir, {recursive: true});
  }
}
function rmSync(dir) {
  if (fs.existsSync(dir)) {
    fs.rmSync(dir);
  }  
}
function appendSync(fname, data) {
  fs.appendFileSync(fname, data, {flags: "a+"});
}


function get(opts, cb) {

  opts['pool'] = util.argv.pool;
  const request = require('request');

  let outFile = opts['outFile'];
  let outFileHeaders = outFile +  ".httpheader";
  const path = require("path");
  let outDir = path.dirname(outFile);
  mkdirSync(outDir);
  let hrs = Infinity;
  if (fs.existsSync(outFileHeaders)) {
    log.debug(`Stating: ${outFileHeaders}`);
    let stat = fs.statSync(outFileHeaders);
    hrs = (new Date().getTime() - stat['mtimeMs'])/3600000;
    let secs = hrs*3600;
    log.debug(`         file age    = ${secs.toFixed(0)} [s]`);
    log.debug(`         argv.maxage = ${util.argv.maxage} [s]`);
    let headersLast = JSON.parse(fs.readFileSync(outFileHeaders,'utf-8'));
    parse(outFile, null, headersLast, cb);
    return;
  }

  if (hrs < util.argv.maxage && !opts['ignoreCache'] && fs.existsSync(outFileHeaders)) {
    opts.method = "HEAD";
    log("Requesting (HEAD): " + opts.uri);
    request(opts, function (err, res, body) {

      if (err) {
        error(null, [opts.uri, err], true);
        cb(err, null);
        return;
      }

      if (res.headers['last-modified'] !== undefined) {
        let headersLast = JSON.parse(fs.readFileSync(outFileHeaders,'utf-8'));
        let lastLastModified = new Date(headersLast['last-modified']).getTime();
        let currLastModified = new Date(res.headers['last-modified']).getTime();
        let d = (currLastModified - lastLastModified)/86400000;
        if (currLastModified > lastLastModified) {
          log("File " + outFile + " is " + d.toFixed(2) + " days older than that reported by HEAD request");
          opts['ignoreCache'] = true;
          get(opts, cb);
        } else {
          log(outFile + " does not need to be updated.");
          parse(outFile, null, res.headers, cb);
        }
      }
    });
    return;
  }

  log("Requesting: " + opts.uri);
  opts.method = "GET";
  request(opts, function (err,res,body) {
    if (res && res.statusCode !== 200) {
      if (res.statusCode === 503 || res.statusCode === 429) {
        error(null, [opts.uri, "Status code " + res.statusCode, "Headers: ", obj2json(res.headers)], false);
      } else {
        error(null, [opts.uri, "Status code " + res.statusCode], false);
      }
      cb(null, null);
      return;      
    }
    if (err) {
      error(null, [opts.uri, err], false);
      cb(err, null);
      return;
    } else {
      log("Received:   " + opts.uri);
    }

    log("Writing: " + outFileHeaders);
    fs.writeFileSync(outFileHeaders, obj2json(res.headers), 'utf-8');

    parse(outFile, body, res.headers, cb);
  });


  function encoding(headers) {
    if (/json|xml/.test(headers['content-type'])) {
      return 'utf8';
    } else {
      return null;
    }
  }

  function parse(outFile, body, headers, cb) {

    if (body === null) {
      if (outFile.endsWith(".cdf") === false) {
        log("Reading: " + outFile);
        body = fs.readFileSync(outFile);
      }
    } else {
      log("Writing: " + outFile);
      fs.writeFileSync(outFile, body, encoding(headers));
    }

    if (/json/.test(headers['content-type'])) {
      cb(null, JSON.parse(body));
    } else if (/xml/.test(headers['content-type'])) {
      xml2js(body, (err, obj) => cb(null, obj));
    } else if (/cdf/.test(headers['content-type'])) {
      cdf2json(outFile, (err, obj) => cb(null, obj));
    } else {
      cb(null, body.toString());
    }    
  }
}

function cdf2json(fnameCDF, cb) {
  
  let fnameBase = fnameCDF.replace(/\.cdf$/,"");
  let fnameJSON = fnameBase + "-xml2js.json";
  let fnameXML  = fnameBase + "-cdfxdf.xml";

  if (fs.existsSync(fnameXML) && fs.existsSync(fnameJSON)) {
    let json = JSON.parse(util.readSync(fnameJSON));
    let xml = util.readSync(fnameJSON).toString();
    cb(null, {"json": json, "xml": xml});
    return;
  }

  let cdfxml;
  let args = " -withZ -mode:cdfxdf -output:STDOUT " 
  let cmd = util.argv.cdf2cdfml + args + fnameCDF;
  try {
    util.log("Executing: " + cmd);
    cdfxml = util.execSync(cmd, {maxBuffer: 8*1024*1024});
  } catch (err) {
    cb(err, null)
    return;
  }

  util.log("Writing: " + fnameXML);
  cdfxml = cdfxml.toString()
  util.writeSync(fnameXML, cdfxml, 'utf8');

  util.log("Converting " + fnameXML + " to JSON.");
  util.xml2js(cdfxml, function (err, obj) {
    util.log("Writing: " + fnameJSON);
    util.writeSync(fnameJSON, obj2json(obj), 'utf8');
    cb(err, {"json": obj, "xml": cdfxml});
  });
}

function xml2js(body, cb) {
  // Async xml2js function (used once).
  const _xml2js = require('xml2js').parseString;
  _xml2js(body, function (err, obj) {
    cb(null, {"json": obj, "xml": body.toString()});
  });
}

function obj2json(obj) {
  return JSON.stringify(obj, null, 2);
}

log.debug = function (msg) {
  if (util.argv.debug) {
    if (typeof(msg) === "string") {
      console.log("[debug] " + msg);        
    } else {
      console.log(msg);
    }
  }
}

function log(msg, msgf) {

  mkdirSync(util.argv.cachedir);
  let fname = util.argv.cachedir + "/log.txt";
  if (!log.fname) {
    fs.rmSync(fname, { recursive: true, force: true });
    log.fname = fname;
  }
  if (msgf) {
    appendSync(fname, msgf + "\n");
  } else {
    appendSync(fname, msg + "\n");    
  }
  console.log(msg);  
}

function warning(dsid, msg) {
  let warnDir = util.argv.cachedir + "/" + dsid.split("_")[0];
  mkdirSync(warnDir);
  let fname = warnDir + "/" + dsid + ".warning.txt";
  msgf = "  Warning: " + msg;
  if (!warning.fname) {
    fs.rmSync(fname, { recursive: true, force: true });
    warning.fname = fname;
  }
  appendSync(fname, msgf + "\n");
  const chalk = require("chalk");
  msg = "  " + chalk.yellow.bold("Warning: ") + msg;
  log(msg, msgf);
}

function error(dsid, msg, exit) {

  let errorDirAll;
  let errorDirDataset;
  let fnameAll;
  let fnameDataset;
  if (dsid) {
    errorDirDataset = util.argv.cachedir + "/" + dsid.split("_")[0];
    fnameDataset = util.argv.cachedir + "/" + dsid + ".error.txt";
    mkdirSync(errorDirDataset);
    if (!error.fnameDataset) {
      fs.rmSync(fnameDataset, { recursive: true, force: true });
      error.fnameDataset = fnameDataset;
    }
  }
  errorDirAll = util.argv.cachedir;
  fnameAll = errorDirAll + "/all.error.txt";
  mkdirSync(errorDirAll);
  if (!error.fnameAll) {
    fs.rmSync(fnameAll, { recursive: true, force: true });
    error.fnameAll = fnameAll;
  }

  if (Array.isArray(msg)) {
    msg = msg.join('\n').replace(/\n/g,'\n       ');
  } else {
    msg = msg.toString();
  }

  if (dsid) {
    appendSync(fnameDataset, "  " + msg);
  }
  appendSync(fnameAll, "  " + msg);

  const chalk = require("chalk");
  let msgf = msg.split("\n");
  msg = chalk.red.bold("Error: ") + msg;

  if (dsid) {
    msg = dsid + "\n" + msg;    
  }

  log(msg, msgf);

  if (exit === undefined || exit == true) {
    process.exit(1);
  }
}
// End convenience functions
/////////////////////////////////////////////////////////////////////////////

// Start time-related functions
const moment  = require('moment');
function incrementTime(timestr, incr, unit) {
  return moment(timestr).add(incr,unit).toISOString().slice(0,19) + "Z";
}
function sameDateTime(a, b) {
  return moment(a).isSame(b);
}
function str2ISODateTime(stro) {

  str = stro.trim();

  // e.g., 201707006
  str = str.replace(/^([0-9]{4})([0-9]{3})([0-9]{2})$/,"$1$2");
  if (str.length === 7) {    
    let y_doy = str.replace(/^([0-9]{4})([0-9]{3})$/,"$1-$2").split("-");
    str = new Date(new Date(y_doy[0]+'-01-01Z').getTime() + parseInt(y_doy[1])*86400000).toISOString().slice(0,10);
  } else {
    str = str.replace(/([0-9]{4})([0-9]{2})([0-9]{2})/,"$1-$2-$3");
    str = str.replace(/([0-9]{4})-([0-9]{2})-([0-9]{2})/,"$1-$2-$3Z");
  }

  // See if moment.js can parse string. If so, cast to ISO 8601.
  moment.suppressDeprecationWarnings = true
  let offset = ""
  if (str.length === 8) {
    let offset = " +0000";
  }
  let dateObj = moment(str + offset);
  if (dateObj.isValid()) {
    str = dateObj.toISOString().slice(0, 10);
    if (str.split("-")[0] === "8888") return undefined;
    return str;
  } else {
    return undefined;
  }  
}
function str2ISODuration(cadenceStr) {
  let cadence;
  if (cadenceStr.match(/day/)) {
    cadence = "P" + cadenceStr.replace(/\s.*days?/,'D');
  } else if (cadenceStr.match(/hour/)) {
    cadence = "PT" + cadenceStr.replace(/\s.*hours?/,'H');
  } else if (cadenceStr.match(/hr/)) {
    cadence = "PT" + cadenceStr.replace(/\s.*hrs?/,'H');
  } else if (cadenceStr.match(/minute/)) {
    cadence = "PT" + cadenceStr.replace(/\s.*minutes?/,'M');
  } else if (cadenceStr.match(/min/)) {
    cadence = "PT" + cadenceStr.replace(/\s.*mins?/,'M');
  } else if (cadenceStr.match(/second/)) {
    cadence = "PT" + cadenceStr.replace(/\s.*seconds?/,'S');
  } else if (cadenceStr.match(/sec/)) {
    cadence = "PT" + cadenceStr.replace(/\s.*secs?/,'S');
  } else if (cadenceStr.match(/[0-9]s/)) {
    cadence = "PT" + cadenceStr.replace(/([0-9].*)s/,'$1S');
  } else if (cadenceStr.match(/millisecond/)) {
    cadence = "PT" + cadenceStr.replace(/\s.*milliseconds?/,'S');
  } else if (cadenceStr.match(/ms/)) {
    cadence = "PT" + cadenceStr.replace(/\s.*ms/,'S');
  } else {
    return undefined;
  }
  return cadence.trim();
}
// End time-related functions
/////////////////////////////////////////////////////////////////////////////

