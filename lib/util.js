const fs = require('fs');
const chalk = require("chalk");

module.exports.util = {
  "get": get,
  "execSync": require('child_process').execSync,
  "existsSync": fs.existsSync,
  "rmSync": rmSync,
  "writeSync": writeSync,
  "readSync": readSync,
  "appendSync": appendSync,
  "copy": function (obj) {return JSON.parse(JSON.stringify(obj))},
  "log": log,
  "cp": cp,  
  "warning": warning,
  "note": note,
  "debug": debug,
  "error": error,
  "obj2json": obj2json,
  "baseDir": baseDir,
  "incrementTime": incrementTime,
  "decrementTime": decrementTime,
  "sameDuration": sameDuration,
  "sameDateTime": sameDateTime,
  "str2ISODateTime": str2ISODateTime,
  "str2ISODuration": str2ISODuration,
  "idFilter": idFilter,
  "sizeOf": sizeOf,
  "plural": plural,
  "heapUsed": heapUsed
}
const util = module.exports.util;
const argv = require('./cli.js').argv();

let logExt = "request";

function plural(arr) {
  if (Array.isArray(arr)) {return arr.length == 1 ? "" : "s"}
  return arr == 1 ? "" : "s";
}
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
function readSync(fname, opts) {
  util.debug(null, 'Reading: ' + fname, logExt);
  return fs.readFileSync(fname, opts);
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
  return argv.cachedir + '/' + id.split('_')[0] + '/' + id;
}

// pool should be set in outer-most scope. See
// https://www.npmjs.com/package/request#requestoptions-callback
let http = require('http');
let pool = new http.Agent();
function get(opts, cb) {

  const request = require('request');

  opts = util.copy(opts);

  if (opts["headers"] === undefined) opts["headers"] = {};
  if (opts['ignoreCache'] === undefined) opts['ignoreCache'] = false
  if (opts['parse'] === undefined) opts['parse'] = true;

  let ignoreCache = opts['ignoreCache'] || argv['ignorecache']

  let userAgent = "CDAS2HAPI; https://github.com/hapi-server/cdaweb-hapi-metadata";
  opts["headers"]["User-Agent"] = userAgent;

  opts['pool'] = pool;
  pool['maxSockets'] = argv.maxsockets;

  let outFile = opts['outFile'];
  let outFileHeaders = outFile +  ".httpheader";

  util.debug(null, `argv.ignorecache = ${argv.ignorecache}`, logExt);
  util.debug(null, `opts.ignoreCache = ${opts.ignoreCache}`, logExt);
  
  if (fs.existsSync(outFileHeaders) && ignoreCache === false) {

    let secs = Infinity;
    if (fs.existsSync(outFileHeaders)) {

      util.debug(null, `Checking: ${outFileHeaders}`, logExt);
      let stat = fs.statSync(outFileHeaders);
      secs = (new Date().getTime() - stat['mtimeMs'])/1000;
      util.debug(null, `file age         = ${secs.toFixed(0)} [s]`, logExt);
      util.debug(null, `argv.maxheadage  = ${argv.maxheadage} [s]`, logExt);
      util.debug(null, `argv.maxfileage  = ${argv.maxfileage} [s]`, logExt);
      util.debug(null, `opts.maxFileAge  = ${opts.maxFileAge} [s]`, logExt);
      util.debug(null, `opts.maxHeadAge  = ${opts.maxHeadAge} [s]`, logExt);
      if (opts.maxFileAge === undefined) opts.maxFileAge = Infinity;
      if (opts.maxHeadAge === undefined) opts.maxHeadAge = Infinity;
      let fresh = secs < Math.min(opts.maxHeadAge, argv.maxheadage);
      if (fresh) {
        let headersLast = JSON.parse(fs.readFileSync(outFileHeaders,'utf-8'));
        util.debug(null, `HEAD check not needed`, logExt);
        parse(outFile, null, headersLast['content-type'], cb);
        return;
      }
    }

    let fresh = secs < Math.min(opts.maxFileAge, argv.maxfileage);
    if (fresh && ignoreCache === false) {

      opts.method = "HEAD";
      util.debug(null, "Requesting (HEAD): " + opts.uri, logExt);
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
            let msg = "File " + outFile + " is " + d.toFixed(2) 
                    + " days older than that reported by HEAD request. Updating.";
            util.debug(null, msg, logExt);
            let optsx = util.copy(opts);
            optsx.ignoreCache = true;
            get(optsx, cb);
          } else {
            util.debug(null, outFile + " does not need to be updated.");
            parse(outFile, null, res.headers['content-type'], cb);
          }
        }
      });
      return;
    }
  }

  util.log(opts.id, "Requesting: " + opts.uri, "", null, logExt);
  opts.method = "GET";
  opts.timeout = 120*1000;
  request(opts, function (err, res, body) {

    if (res && res.statusCode !== 200) {
      let retry = ""; 
      if (res.headers["retry-after"]) {
        retry = ". Retry After: " + res.headers["retry-after"];
      }
      cb("HTTP " + res.statusCode + retry, res, body);
      return;
    }

    if (err) {
      if (err.code === 'ETIMEDOUT') {
        // https://github.com/request/request#readme
        if (err.connect === true) {
          err = "Connect took longer than " + opts.timeout + " [ms]";
        } else {
          err = "First response took longer than " + opts.timeout + " [ms]";
        }
      }
      error(null, [opts.uri, err], false);
      cb([opts.uri, err], null);
      return;
    } else {
      util.log(opts.id, "Received: " + opts.uri, "", null, logExt);
    }

    res.headers['x-Request-URL'] = opts.uri;
    util.debug(null, "Writing: " + outFileHeaders, logExt);
    util.writeSync(outFileHeaders, obj2json(res.headers), 'utf-8');

    parse(outFile, body, res.headers['content-type'], cb);
  });

  function encoding(contentType) {
    if (/json|xml/.test(contentType)) {
      return 'utf8';
    } else {
      return null;
    }
  }

  function parse(outFile, body, contentType, cb) {

    if (body === null) {
      // Cached local file if body === null
      if (opts['parse'] === true && /application\/x-cdf/.test(contentType) === false) {
        // If CDF file, processing is done from command line
        // call, so don't need to read.
        util.log(opts.id, "Reading: " + outFile, null, logExt);
        body = fs.readFileSync(outFile);
      }
    } else {
      if (/application\/json/.test(contentType)) {
        util.writeSync(outFile, obj2json(JSON.parse(body)), encoding(contentType));
      } else {
        //console.log(headers)
        //console.log(encoding(headers))
        //console.log(body.length)
        util.writeSync(outFile, body, encoding(contentType));
      }
    }

    if (opts['parse'] === false) {
      // This option is used when we only want to download the file.
      cb(null, body);
      return;
    }

    if (/application\/json/.test(contentType)) {
      cb(null, JSON.parse(body));
    } else if (/text\/xml/.test(contentType)) {
      xml2json(body, (err, obj) => cb(err, obj));
    } else if (/application\/xml/.test(contentType)) {
      xml2json(body, (err, obj) => cb(err, obj));
    } else if (/application\/x-cdf/.test(contentType)) {
      if (body) {
        cdf2json(outFile, false, (err, obj) => cb(err, obj));
      } else {
        cdf2json(outFile, true, (err, obj) => cb(err, obj));
      }
    } else {
      cb(null, body.toString());
    }    
  }
}

function cdf2json(fnameCDF, useCache, cb) {

  let fnameBase = fnameCDF.replace(/\.cdf$/,"");
  let method = argv["cdf2json-method"];
  let fnameJSON = fnameBase + "-" + method + ".json";

  if (util.existsSync(fnameJSON) && useCache) {
    util.debug(null, "Returning cached " + fnameJSON, logExt);
    cb(null, {
      "file": fnameJSON,
      "json": JSON.parse(fs.readFileSync(fnameJSON))
    });
    return;
  }

  if (method === "python-cdf2json") {
    let opts = {maxBuffer: 1e8};
    let cmd = argv["python-cdf2json"] + ' --in=' + fnameCDF + ' --out=' + fnameJSON;
    util.debug(null, "Executing " + cmd, logExt)
    require('child_process').exec(cmd, opts, function (err, stdout, stderr) {
      util.debug(null, "Executed " + cmd, logExt)
      if (err) {
        cb([cmd, "gave", err.message], null);
        return;
      }
      cb(stderr, {
          "file": fnameJSON,
          "json": JSON.parse(fs.readFileSync(fnameJSON))
        });
    });
    return;
  }

  if (method === "java-CDF2Json") {
    // Much slower than Python. Would require normalizing names to match that
    // used by CDASR, which upstream codes assumes.
    let opts = {maxBuffer: 1e8};
    let cmd = argv["java-CDF2Json"] + ' -delete -withZ -output:' + fnameJSON + " " + fnameCDF ;
    util.debug(null, "Executing " + cmd, logExt)
    require('child_process').exec(cmd, opts, function (err, stdout, stderr) {
      if (err) {
        cb(err, null);
        return;
      }
      let json = JSON.parse(fs.readFileSync(fnameJSON));
      let tmp = JSON.stringify(json, null, 2);
      console.log("Writing: " + fnameJSON);
      fs.writeFileSync(fnameJSON, tmp);
      cb(stderr, {
          "file": fnameJSON,
          "json": json
        });
    });
    return;
  }

  if (method === "java-CDF2CDFML") {
    // Old method using CDF2CDFML + xml2js. Much slower than Python. Would
    // require normalizing names to match that used by CDASR, which upstream
    // codes assumes.
    fnameBase = fnameCDF.replace(/\.cdf$/,"");
    let fnameXML  = fnameBase + "-cdfxdf.xml";

    if (fs.existsSync(fnameXML) && fs.existsSync(fnameJSON)) {
      let json = JSON.parse(util.readSync(fnameJSON));
      let xml = util.readSync(fnameJSON).toString();
      cb(null, {"json": json, "xml": xml});
      return;
    }

    let cdfxml;
    let args = " -withZ -mode:cdfxdf -output:STDOUT ";
    let cmd = argv["java-CDF2CDFML"] + args + fnameCDF;
    try {
      util.debug(null, "Executing: " + cmd, logExt);
      cdfxml = util.execSync(cmd, {maxBuffer: 8*1024*1024});
    } catch (err) {
      cb(err, null);
      return;
    }

    util.debug(null, "Writing: " + fnameXML, logExt);
    cdfxml = cdfxml.toString()
    util.writeSync(fnameXML, cdfxml, 'utf8');

    util.debug(null, "Converting " + fnameXML + " to JSON.", logExt);
    xml2json(cdfxml,
      function (err, obj) {
        util.debug(null, "Writing: " + fnameJSON, logExt);
        util.writeSync(fnameJSON, obj2json(obj), 'utf8');
        cb(err, {"file": fnameJSON, "json": obj, "xml": cdfxml});
    });
  }
}

function xml2json(body, cb) {
  const xml2js = require('xml2js').parseString;
  xml2js(body, function (err, obj) {
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
  prefix = prefix || "";

  if (Array.isArray(msg)) {
    msg = util.copy(msg);
    for (let i in msg) {
      if (typeof msg[i] !== 'string') {
        msg[i] = JSON.stringify(msg[i]);
      }
    }
    msg[0] = prefix + msg[0];
    msg = msg.join('\n').replace(/\n/g,'\n' + " ".repeat(4));
  } else {
    msg = prefix + msg;
  }

  let fname1 = argv.cachedir + "/" + fext + ".txt";
  log["logFileName"] = fname1;
  if (!log[fname1]) {
    util.rmSync(fname1, {recursive: true, force: true});
    log[fname1] = fname1;
  }
  if (dsid) {
    appendSync(fname1, dsid + "\n" + msg.trim() + "\n");
  } else {
    appendSync(fname1, msg + "\n");
  }

  if (dsid) {
    let fname2 = baseDir(dsid) + "/" + dsid + "." + fext + ".txt";
    if (!log[fname2]) {
      util.rmSync(fname2, {recursive: true, force: true});
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
  if (!argv.debug) return;
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
  if (exit === undefined || exit == true) {
    log(dsid, msg + "\nExiting.", "", "red", msgtype("error", mtype));
    process.exit(1);
  }
  log(dsid, msg, "Error:   ", "red", msgtype("error", mtype));
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
  for (let key in Object.keys(a)) {
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
    offset = " +0000";
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

  for (let omit of omits) {
    if (omit == '') continue;
    let re = new RegExp(omit);
    if (re.test(id) == true) {
      return false;
    }
  }

  for (let keep of keeps) {
    if (keep === '') return true;
    let re = new RegExp(keep);
    if (re.test(id) == true) {
      return true;
    }
  }
  return false;
}

function heapUsed() {
  const used = process.memoryUsage().heapUsed / 1024 / 1024;
  console.log(`heapUsed ${Math.round(used * 100) / 100} MB`);
}

function sizeOf(bytes) {
  // https://stackoverflow.com/a/28120564
  if (bytes == 0) { return "0.00 B"; }
  var e = Math.floor(Math.log(bytes) / Math.log(1000));
  return (bytes/Math.pow(1000, e)).toFixed(2)+' '+' KMGTP'.charAt(e)+'B';
}
