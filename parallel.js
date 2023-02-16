const async = require('async');

let N = 10;
let chunks = [];
let err = null;

function finished(err, n) {
  if (err) {
    console.log('Errored: #' + n);
    return;
  }
  console.log('Finished #' + n)
  for (let c = 0; c < chunks.length; c++) {
    if (chunks[c] === null) {
      break;
    }
    if (chunks[c] !== '') {
      chunks[c] = '';
    }
  }
}

let jobs = [];
function job(n) {

  function func(callback) {
    console.log("Starting #" + n);
     setTimeout(
      function() {
          chunks[n] = n;
          finished(err, n);
          callback(err, n);
          if (err) {
            console.log(err)
            err = null;
          }
        }
        , 1000)
  }

  return function(callback) {
            async
              .retry(
                {
                  times: 10,
                  interval: 200
                },
                (callback) => func(callback)
              )
          }
}

for (let j = 0; j < N; j++) {
  chunks.push(null);
  jobs.push(job(j));
}

async
  .parallel(jobs)
  .catch(err => {
    console.log(err);
  });
