// Perform jobs in parallel in batches and dump data from jobs in order.
let async = require("async");

let ordered = true;
let N = 4;
let asyncLimit = 2;  // batch size

let chunks = [];
let jobs = [];
let err = null;

function finished(err, j) {

  if (err) {
    console.log('Job #' + j + " errored.");
  }
  console.log('Finished job #' + j);

  if (ordered === false) {
    console.log(  "Dumping data from job #" + j + ": " + chunks[j]);
    return;
  }

  for (let c = 0; c < chunks.length; c++) {
    // Dump until a null found.
    if (chunks[c] === undefined) {
      console.log("  Job " + c + " data not ready for dump.");
      break;
    } else if (chunks[c] === -1) {
      console.log("  Job " + c + " errored.");
      break;
    } else if (chunks[c] === null) {
      console.log("  Chunk " + c + " data already dumped.");
    } else {
      // Dump data
      console.log("  Dumping data from job #" + j + ": " + chunks[j]);
      chunks[c] = null;
    }
  }
}

function delay(ms, j) {
  //let asyncTask = (r) => setTimeout(r, ms);
  //return new Promise(r => asyncTask(r));
  const myPromise = new Promise(
    (resolve, reject) => {
      setTimeout(() => {
        if (j == 2) {
          reject("x");
          chunks[j] = -1;
          console.log(chunks)
          return;
        }
        chunks[j] = j;
        resolve();
      }, ms);
  });
  return myPromise;
}

function job(j, cb) {
  return async (err) => {
    console.log("Starting job #" + j);
    try {
      await delay(1000, j);
    } catch (err) {
      console.log("Job #" + j + " gave error: " + err);      
    }
    cb(err, j);
  }
}

for (let j = 0; j < N; j++) {
  chunks.push(undefined);
  jobs.push(job(j, finished))
}

async.parallelLimit(jobs, asyncLimit,
  (err, results) => {
    if (err) {
      console.error('Error: ', err);
    } else {
      console.log(chunks);
      console.log('Done.');
    }
});