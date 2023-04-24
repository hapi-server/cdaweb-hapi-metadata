const fs = require('fs');
const chalk = require("chalk");

module.exports.util = {
  "get": get,
  "execSync": require('child_process').execSync,
  "existsSync": fs.existsSync,
  "rmSync": rmSync,
  "writeSync": writeSync,
  "readSync": fs.readFileSync,
  "appendSync": appendSync,
  "copy": function (obj) {return JSON.parse(JSON.stringify(obj))},
  "log": log,
  "cp": cp,  
  "warning": warning,
  "note": note,
  "error": error,
  "obj2json": obj2json,
  "xml2js": xml2js,
  "baseDir": baseDir,
  "incrementTime": incrementTime,
  "decrementTime": decrementTime,
  "sameDuration": sameDuration,
  "sameDateTime": sameDateTime,
  "str2ISODateTime": str2ISODateTime,
  "str2ISODuration": str2ISODuration
}
util = module.exports.util;

function rmSync(fname) {
  {if (fs.existsSync(fname)) {fs.unlinkSync(fname)}}  
}
function appendSync(fname, data) {
  const path = require('path');
  let dir = path.dirname(fname);
  if (!fs.existsSync(dir)) {
    fs.mkdirSync(dir, {recursive: true});
  }
  fs.appendFileSync(fname, data, {flags: "a+"})
}
function cp(src, dest, mode) {
  const path = require('path');
  let dir = path.dirname(dest);
  if (!fs.existsSync(dir)) {
    fs.mkdirSync(dir, {recursive: true});
  }
  fs.copyFileSync(src, dest, mode);
}
function writeSync(fname, data, opts) {
  const path = require('path');
  let dir = path.dirname(fname);
  if (!fs.existsSync(dir)) {
    fs.mkdirSync(dir, {recursive: true});
  }
  //util.log('Writing: ' + fname);
  fs.writeFileSync(fname, data, opts);
}

function baseDir(id) {
  return util.argv.cachedir + '/' + id.split('_')[0] + '/' + id;
}

// pool should be set in outer-most scope. See
// https://www.npmjs.com/package/request#requestoptions-callback
let http = require('http');
let pool = new http.Agent();
function get(opts, cb) {

  const path = require("path");
  const request = require('request');

  if (!opts["headers"]) {
    opts["headers"] = {};
  }
  opts["headers"]["User-Agent"] = "CDAS2HAPI; https://github.com/hapi-server/cdaweb-hapi-metadata";

  opts['pool'] = pool;
  pool['maxSockets'] = util.argv.maxsockets;

  let outFile = opts['outFile'];
  let outFileHeaders = outFile +  ".httpheader";
  let outFileURL = outFile +  ".url";
  let outDir = path.dirname(outFile);

  let hrs = Infinity;
  if (fs.existsSync(outFileHeaders) && fs.existsSync(outFile)) {
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
  request(opts, function (err, res, body) {

    if (res && res.statusCode !== 200) {
      cb(null, body);
      return;
    }

    if (err) {
      error(null, [opts.uri, err], false);
      cb(err, null);
      return;
    } else {
      log("Received:   " + opts.uri);
    }

    res.headers['x-Request-URL'] = opts.uri;
    log("Writing: " + outFileHeaders);
    util.writeSync(outFileHeaders, obj2json(res.headers), 'utf-8');

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
      // Cached local file
      if (outFile.endsWith(".cdf") === false) {
        // If CDF file, processing is done from command line
        // call, so don't need to read.
        log("Reading: " + outFile);
        body = fs.readFileSync(outFile);
      }
    } else {
      log("Writing: " + outFile);
      if (/application\/json/.test(headers['content-type'])) {
        util.writeSync(outFile, obj2json(JSON.parse(body)), encoding(headers));
      } else {
        util.writeSync(outFile, body, encoding(headers));
      }
    }

    if (opts["parse"] === false) {
      // This option is used when we only want to download the file.
      cb(null, body);
      return;
    }

    if (/application\/json/.test(headers['content-type'])) {
      cb(null, JSON.parse(body));
    } else if (/text\/xml/.test(headers['content-type'])) {
      xml2js(body, (err, obj) => cb(err, obj));
    } else if (/application\/xml/.test(headers['content-type'])) {
      xml2js(body, (err, obj) => cb(err, obj));
    } else if (/application\/x-cdf/.test(headers['content-type'])) {
      cdf2json(outFile, (err, obj) => cb(err, obj));
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
    cb(err, null);
    return;
  }

  util.log("Writing: " + fnameXML);
  cdfxml = cdfxml.toString()
  util.writeSync(fnameXML, cdfxml, 'utf8');

  util.log("Converting " + fnameXML + " to JSON.");
  util.xml2js(cdfxml,
    function (err, obj) {
      util.log("Writing: " + fnameJSON);
      util.writeSync(fnameJSON, obj2json(obj), 'utf8');
      cb(err, {"json": obj, "xml": cdfxml});
  });
}

function xml2js(body, cb) {
  const _xml2js = require('xml2js').parseString;
  _xml2js(body, function (err, obj) {
    cb(null, {"json": obj, "xml": body.toString()});
  });
}

function obj2json(obj) {
  return JSON.stringify(obj, null, 2);
}

function log(msg, color) {

  let fname = util.argv.cachedir + "/log.txt";
  if (!log.fname) {
    util.rmSync(fname, { recursive: true, force: true });
    log.fname = fname;
  }
  appendSync(fname, msg + "\n");
  let inverse = msg.trim().startsWith("*") && msg.trim().endsWith("*");
  if (inverse && !color) {
    color = 'yellow';
  }
  if (color && chalk[color]) {
    if (inverse) {
      console.log(chalk[color].inverse(msg));
    } else {
      console.log(chalk[color].bold(msg));
    }
  } else {
    console.log(msg);
  }
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

function warning(dsid, msg) {
  msg = "  Warning: " + msg;
  log(msg, "yellow");
}

function note(dsid, msg) {
  let pad = "";
  if (dsid !== null) {
    pad = "  ";
  }
  log(pad + "Note:    " + msg);
}

function error(dsid, msg, exit, prefix) {
  let errorDir = util.argv.cachedir;
  let fname = undefined;
  if (dsid) {
    errorDir = util.argv.cachedir + "/" + dsid.split("_")[0];
    fname = errorDir + "/" + dsid + "/" + dsid + ".error.txt";
  }
  if (fname && !error.fname) {
    fs.rmSync(fname, {"recursive": true, "force": true});
    error.fname = fname;
  }
  if (Array.isArray(msg)) {
    msg = msg.join('\n').replace(/\n/g,'\n       ');
  } else {
    msg = msg.toString();
  }
  if (fname) {
    appendSync(fname, "  " + msg);
  }
  msg = "  Error:   " + msg;
  if (prefix !== false) {
    msg = dsid + "\n" + msg;
  }
  log(msg, "red");
  if (exit === undefined || exit == true) {
    process.exit(1);
  }
}

/////////////////////////////////////////////////////////////////////////////
// Start time-related functions
const moment  = require('moment');
function incrementTime(timestr, incr, unit) {
  return moment(timestr).add(incr,unit).toISOString().slice(0,19) + "Z";
}
function decrementTime(timestr, incr, unit) {
  return moment(timestr).subtract(incr,unit).toISOString().slice(0,19) + "Z";
}
function sameDateTime(a, b) {
  return moment(a).isSame(b);
}
function sameDuration(a, b) {
  a = moment.duration(a)['_data'];
  b = moment.duration(a)['_data'];
  let match = true;
  for (key in Object.keys(a)) {
    if (a[key] !== b[key]) {
      match = false;
      break;
    }
  }
  return match;
}
function str2ISODateTime(stro) {

  let str = stro.trim();

  // e.g., 201707006
  str = str.replace(/^([0-9]{4})([0-9]{3})([0-9]{2})$/,"$1$2");
  if (str.length === 7) {    
    let y_doy = str.replace(/^([0-9]{4})([0-9]{3})$/,"$1-$2").split("-");
    str = new Date(new Date(y_doy[0]+'-01-01Z').getTime() 
                  + parseInt(y_doy[1])*86400000).toISOString().slice(0,10);
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

  let cadence = "";
  cadenceStr = cadenceStr.toLowerCase();
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
    let ms = cadenceStr.match(/(\d.*\d+)/)[0];
    let S = parseFloat(ms)/1000;
    cadence = "PT" + S + 'S';
  } else if (cadenceStr.match(/ms/)) {
    let ms = cadenceStr.match(/(\d.*\d+)/)[0];
    let S = parseFloat(ms)/1000;
    cadence = "PT" + S + 'S';
  } else {
    return undefined;
  }
  return cadence.trim();
}
// End time-related functions
/////////////////////////////////////////////////////////////////////////////