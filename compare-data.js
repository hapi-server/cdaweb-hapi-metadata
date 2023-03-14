const fs    = require("fs");
const chalk = require("chalk");
const argv  = require('yargs')
                  .default
                    ({
                        'id': 'AC_H2_MFI',
                        'parameters': '',
                        'start': '',
                        'stop': '',
                        'infodir': 'hapi/bw/info'
                    })
                  .argv;

let infoFile = __dirname + "/" + argv.infodir + "/" + argv.id + ".json";
let info = fs.readFileSync(infoFile);
info = JSON.parse(info);
if (!argv.start) {
  argv.start = info['sampleStartDate'];
}
if (!argv.stop) {
  argv.stop = info['sampleStopDate'];
}
if (!argv.parameters) {
  argv.parameters = info['parameters'][1]['name'];
}

let base = `node HAPIcsv.js --debug --id ${argv.id} --parameters ${argv.parameters} --start ${argv.start} --stop ${argv.stop}`;
let fmts = [
              "text        ",
              "csv-text2csv", 
              "csv-cdfdump ", 
              "csv-pycdas  ", 
              "csv-nl      ", 
              "csv-bh      ", 
              "csv-jf      "
            ];

let timing = [];
exec(0);

function exec(f) {

  let cmd = base + " --format " + fmts[f].trim();
  console.log(chalk.blue.bold("Executing: \n  " + cmd));
  console.log(chalk.blue.bold("-".repeat(80)));

  let copts = {"encoding": "buffer"};
  let child = require('child_process').spawn('sh', ['-c', cmd], copts);

  let msstart = Date.now();
  child.stderr.on('data', function (err) {
    console.error("Command " + cmd + " gave stderr:\n" + err);
  });
  child.stdout.on('data', function (buffer) {
    process.stdout.write(buffer.toString());
  });
  child.stdout.on('end', function () {
    let dt = (Date.now()-msstart);
    timing.push(dt)
    console.log(chalk.blue(dt + " [ms]"));
    console.log(chalk.blue.bold("-".repeat(80)));
    if (f < fmts.length - 1) {
      exec(++f);
    } else {
      for (f in fmts) {
        console.log(fmts[f] + "\t" + timing[f] + "\t[ms]");
      }
    }
  });
}