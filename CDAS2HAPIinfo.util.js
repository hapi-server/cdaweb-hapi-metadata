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
  "debug": debug,
  "error": error,
  "obj2json": obj2json,
  "xml2js": xml2js,
  "baseDir": baseDir,
  "incrementTime": incrementTime,
  "decrementTime": decrementTime,
  "sameDuration": sameDuration,
  "sameDateTime": sameDateTime,
  "str2ISODateTime": str2ISODateTime,
  "str2ISODuration": str2ISODuration,
  "idFilter": idFilter,
  "sizeOf": sizeOf
}
util = module.exports.util;

let logExt = "request";

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
  util.debug(null, 'Writing: ' + fname, logExt);
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
    util.debug(opts.id, `Stating: ${outFileHeaders}`, logExt);
    let stat = fs.statSync(outFileHeaders);
    hrs = (new Date().getTime() - stat['mtimeMs'])/3600000;
    let secs = hrs*3600;
    util.debug(opts.id, `         file age    = ${secs.toFixed(0)} [s]`, logExt);
    util.debug(opts.id, `         argv.maxage = ${util.argv.maxage} [s]`, logExt);
    let headersLast = JSON.parse(fs.readFileSync(outFileHeaders,'utf-8'));
    parse(outFile, null, headersLast, cb);
    return;
  }

  if (hrs < util.argv.maxage && !opts['ignoreCache'] && fs.existsSync(outFileHeaders)) {

    opts.method = "HEAD";
    util.debug(opts.id, "Requesting (HEAD): " + opts.uri, logExt);
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
          util.debug(null, "File " + outFile + " is " + d.toFixed(2) + " days older than that reported by HEAD request");
          opts['ignoreCache'] = true;
          get(opts, cb);
        } else {
          util.debug(null, outFile + " does not need to be updated.");
          parse(outFile, null, res.headers, cb);
        }
      }
    });
    return;
  }

  util.log(opts.id, "Requesting: " + opts.uri, "", null, logExt);
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
      util.log(opts.id, "Received:   " + opts.uri, "", null, logExt);
    }

    res.headers['x-Request-URL'] = opts.uri;
    util.debug(null, "Writing: " + outFileHeaders);
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
      if (outFile.endsWith(".cdf") === false && opts["parse"] !== false) {
        // If CDF file, processing is done from command line
        // call, so don't need to read.
        util.log(opts.id, "Reading: " + outFile, null, logExt);
        body = fs.readFileSync(outFile);
      }
    } else {
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
    util.debug(opts.id, "Executing: " + cmd, logExt);
    cdfxml = util.execSync(cmd, {maxBuffer: 8*1024*1024});
  } catch (err) {
    cb(err, null);
    return;
  }

  util.debug(opts.id, "Writing: " + fnameXML, logExt);
  cdfxml = cdfxml.toString()
  util.writeSync(fnameXML, cdfxml, 'utf8');

  util.debug(opts.id, "Converting " + fnameXML + " to JSON.", logExt);
  util.xml2js(cdfxml,
    function (err, obj) {
      util.debug(opts.id, "Writing: " + fnameJSON, logExt);
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

function log(dsid, msg, prefix, color, fext) {

  if (!fext) {
    fext = "log";
  }
  if (dsid) {
    if (prefix) {
      prefix = "  " + prefix;
    } else {
      prefix = "";
    }
  }
  if (!prefix) prefix = "";

  if (Array.isArray(msg)) {
    msg[0] = prefix + msg[0];
    msg = msg.join('\n').replace(/\n/g,'\n' + " ".repeat(msg[0].length));
  } else {
    msg = prefix + msg;
  }

  let fname1 = util.argv.cachedir + "/" + fext + ".txt";
  log["logFileName"] = fname1;
  if (!log[fname1]) {
    util.rmSync(fname1, { recursive: true, force: true });
    log[fname1] = fname1;
  }
  if (fext === "error" && dsid) {
    appendSync(fname1, dsid + "\n" + msg + "\n");
  } else {
    appendSync(fname1, msg + "\n");
  }

  if (dsid) {
    let fname2 = baseDir(dsid) + "/" + dsid + "." + fext + ".txt";
    if (!log[fname2]) {
      util.rmSync(fname2, { recursive: true, force: true });
      log[fname2] = fname2;
    }
    appendSync(fname2, msg + "\n");
  }

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

function msgtype(mtype1, mtype2) {
  if (mtype2) {return mtype2 + "." + mtype1} else {return mtype1}
}

function debug(dsid, msg, mtype) {
  if (!util.argv.debug) return;
  if (typeof msg !== 'string') {
    msg = "\n" + JSON.stringify(msg, null, 2);
  }
  log(dsid, msg, "[debug]: ", "", msgtype("log", mtype));
}

function warning(dsid, msg, mtype) {
  log(dsid, msg, "Warning: ", "yellow", msgtype("log", mtype));
}

function note(dsid, msg, mtype) {
  log(dsid, msg, "Note:    ", null, msgtype("log", mtype));
}

function error(dsid, msg, exit, mtype) {
  log(dsid, msg, "Error:   ", "red", msgtype("error", mtype));
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
  let re = /.*?([0-9]*\.?[0-9]+).*/;
  cadenceStr = cadenceStr.toLowerCase();
  if (cadenceStr.match(/day/)) {
    cadence = "P" + cadenceStr.replace(re,'$1D');
  } else if (cadenceStr.match(/hour|hr/)) {
    cadence = "PT" + cadenceStr.replace(re,'$1H');
  } else if (cadenceStr.match(/minute|min/)) {
    cadence = "PT" + cadenceStr.replace(re,'$1M');
  } else if (cadenceStr.match(/second|sec/)) {
    cadence = "PT" + cadenceStr.replace(re,'$1S');
  } else if (cadenceStr.match(/[0-9]s\s?/)) {
    cadence = "PT" + cadenceStr.replace(re,'$1S');
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

function idFilter(id, keeps, omits) {

  if (Array.isArray(id)) {
    let keeps = [];
    for (let i of id) {
      if (idFilter(i)) {
        keeps.push(i)
      }
    }
    return keeps;
  }

  for (let keep of keeps) {
    let re = new RegExp(keep);
    if (re.test(id) == true) {
      return true;
    }
  }

  for (let omit of omits) {
    let re = new RegExp(omit);
    if (re.test(id) == true) {
      return false;
    }
  }

  return false;
}

function sizeOf(bytes) {
  // https://stackoverflow.com/a/28120564
  if (bytes == 0) { return "0.00 B"; }
  var e = Math.floor(Math.log(bytes) / Math.log(1000));
  return (bytes/Math.pow(1000, e)).toFixed(2)+' '+' KMGTP'.charAt(e)+'B';
}
