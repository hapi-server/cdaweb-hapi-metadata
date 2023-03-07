const request = require('request');
const moment  = require('moment');

let DEBUG = true;

let opts = {
  "start": "1990-01-01T00Z", 
  "stop":"1990-01-20T00Z",
  "format": "YYYY+DDDD+HH:mm:ss",
  "formatAppendZ": true,
  "url": "start=${start}&stop=${stop}",
  "maxDays": 3
};

let jobs = createJobs(opts);
parallelBatch(opts, jobs, (err) => {
  console.log("parallelBatch callback. Error: '" + err + "'");
});

function createJobs(opts) {

  let start = moment(opts["start"]);
  let stop_final  = moment(opts["stop"]);

  if (stop_final.diff(start,'days') > opts["maxDays"]) {
    stop = start.clone().add(opts["maxDays"], 'days');
  } else {
    stop = moment(opts["stop"]);
  }

  let urls = [];
  let starts = [];
  let stops = [];

  while (1) {

    let Z = "";
    if (opts["formatAppendZ"]) {Z = "Z"};
    let startFormatted = moment(start).utc().format(opts["format"]) + Z;
    let stopFormatted = moment(stop).utc().format(opts["format"]) + Z;

    let url = opts["url"]
                .replace("${start}", startFormatted)
                .replace("${stop}", stopFormatted)

    urls.push(url);
    starts.push(start.clone());
    stops.push(stop.clone());

    if (stop.isSame(stop_final)) {
      break;
    }

    start.add(opts["maxDays"],'days')
    stop.add(opts["maxDays"], 'days')

    if (stop.isAfter(stop_final)) {
      stop = stop_final;
    }
  }

  let chunks = [];
  let jobs = [];
  let jidx = 0;
  for (url of urls) {
    chunks[jidx] = undefined;
    let job = {
                "url": url, 
                "start": starts[jidx], 
                "stop": stops[jidx],
                "data": undefined,
                "doJob": doJob
              };
    jobs.push(job);
    jidx = jidx + 1;
  }
  return jobs;
}

function doJob(job, cb) {

  let jidx = job['jidx'];
  setTimeout( () => {
    if (jidx == -1) {
      cb(true, job);
    } else {
      job["data"] = jidx;
      cb(false, job);
    }
  }, 100/(jidx+1));
}

function parallelBatch(opts, jobs, cb) {

  let MAX_FAILS = 2;
  let MAX_JOBS  = 3;
  let ORDERED   = true;

  for (let idx in jobs) {
    jobs[idx]["jidx"] = parseInt(idx);
  }

  submitJob(jobs[0]);

  function submitJob(job) {

    let jidx = job["jidx"];

    if (submitJob.inflight === undefined) {
      submitJob.inflight = [];
    }
    submitJob.inflight.push(jidx);

    if (job["fails"] === undefined) {
      job["fails"] = 0;
    }

    job['inflight'] = true;
    if (job["fails"] == 0) {
      console.log(`Starting job #${jidx}; ${submitJob.inflight.length} jobs in flight: [${submitJob.inflight.join(", ")}]`);
    } else {
      console.log(`Retrying job #${jidx}; ${submitJob.inflight.length} jobs in flight: [${submitJob.inflight.join(", ")}]`);
    }

    submitNext(jidx);

    job['inflight'] = true;

    job['doJob'](job, finishedJob);

  }

  function submitNext(jidx) {
    if (submitJob.inflight.length < MAX_JOBS && jidx < jobs.length - 1) {
      if (!jobs[jidx+1]["done"] && !jobs[jidx+1]["inflight"]) {
        submitJob(jobs[jidx+1]);
      }
    }
  }

  function finishedJob(err, job) {

    let jidx = job['jidx'];

    submitJob.inflight =
      submitJob.inflight
        .filter(function (idx) {
            return idx !== jidx;
          });

    let verb = err ? "failed": "passed";
    let plural = submitJob.inflight.length > 1 ? "s" : "";
    console.log(`Job #${jidx} ${verb}; ${submitJob.inflight.length} job${plural} in flight: [${submitJob.inflight.join(", ")}]`);
    if (err) {
      if (job["fails"] < MAX_FAILS) {
        job["fails"] = job["fails"] + 1; 
        submitJob(job);
      } else {
        console.log(`Job #${jidx} failed ${MAX_FAILS} times. Exiting.`);
        if (cb)
          cb("Too many failures.");
      }
    } else {
      job["done"] = true;
      submitNext(jidx);
      finished(job);
    }
  }

  function finished(job) {

    if (finished.N === undefined) {
      finished.N = 0;
    }
    finished.N = finished.N + 1;

    let jidx = job["jidx"];

    if (ORDERED == false) {
      if (DEBUG) {
        console.log(`  Dumping data for job #${jidx}.`);
      }
      console.log(job.data);
      job.data = null;
      if (finished.N == jobs.length && cb)
        cb(null);
      return;
    }

    console.log(`Attempting dump of job #${jidx} data.`);

    for ([jidx, job] of Object.entries(jobs)) {
      if (job.data === undefined) {
        if (DEBUG) {
          console.log(`  Job #${jidx} not finished. Delaying dump.`);
        }
        break;
      } else if (job.data === null) {
        if (DEBUG) {
          console.log(`  Job #${jidx} already dumped.`);
        }
        continue;
      } else {
        if (DEBUG) {
          console.log(`  Dumping data for job #${jidx}.`);
        }
        console.log(job.data);
        job.data = null;
      }
    }

    if (finished.N == jobs.length && cb)
      cb(null);

  }
}

if (0) {
  if (DEBUG) {
    console.log(`Starting request #${dropts["cidx"]} ${dropts["start"].toISOString()} to ${dropts["stop"].toISOString()}`);
  }
  let ropts = {
                "url": dropts["url"],
                "strictSSL": false,
                "gzip": true
              };

  request(ropts,
    function (err, res, body) {  
      if (err) {
        let msg = "Upstream server returned error.";
        console.log(msg);
        console.error(msg);
        console.error(error);
        process.exit(1);    
      }
      if (res && res.statusCode != 200) {
        if (res.statusCode == 429 || res.statusCode == 503) {
          console.log(res.headers['retry-after']);
        }
        process.exit(1);
      }
      if (DEBUG) {       
        console.log("Finished request " + cidx);
      }
      finished(opts["cidx"], body);
  })
}