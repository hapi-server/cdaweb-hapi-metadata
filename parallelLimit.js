let async = require("async");

let N = 10;
let asyncLimit = 2;

let chunks = [];
let jobs = [];
let err = null;

function finished(err, n) {
  if (err) {
    console.log('#' + n + " errored.");
    return;
  }
  console.log('Finished #' + n)
  for (let c = 0; c < chunks.length; c++) {
    if (chunks[c] === null) {
      break;
    }
    if (chunks[c] !== '') {
      console.log(chunks[c]);
      chunks[c] = '';
    }
  }
}

function delay(ms, p) {
  //let asyncTask = (r) => setTimeout(r, ms);
  //return new Promise(r => asyncTask(r));
  const myPromise = new Promise((resolve, reject) => {
    setTimeout(() => {
      resolve();
    }, 300);
  });
  return myPromise;
}

function job(p) {
  return async () => {
    console.log("Starting #" + p)
    await delay(400, p);
    finished(err, p);
  }
}

for (let j = 0; j < 10; j++) {
  chunks.push(null);
  jobs.push(job(j))
}

// Finally execute the tasks
async.parallelLimit(jobs, asyncLimit, (err, result) => {
  if (err) {
    console.error('Error: ', err);
  } else {
    console.log('Done.');
  }
});