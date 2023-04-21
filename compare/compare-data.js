const fs    = require("fs");
const chalk = require("chalk");
const argv  = require('yargs')
                  .default
                    ({
                        'id': 'AC_H2_MFI',
                        'parameters': '',
                        'start': '',
                        'stop': '',
                        'infodir': '../hapi/bw/CDAWeb/info',
                        'debug': true,
                        'showdata': false
                    })
                  .option('debug',{'type': 'boolean'})
                  .option('showdata',{'type': 'boolean'})
                  .argv;

let fmts = [
    'cdaweb-csv-using-js   ',
    'cdaweb-cdf-using-pycdf',
    'cdas-text-using-js    ',
    'cdas-cdf-using-pycdaws ',
    'cdas-cdf-using-pycdf  ',
    'nl-hapi               ',
//  'bh-hapi               ',
//  'apds                  ',
];


let infoFile = __dirname + "/" + argv.infodir + "/" + argv.id + ".json";
let info = fs.readFileSync(infoFile);
info = JSON.parse(info);
if (!argv.start) {
  argv.start = info['sampleStartDate'];
}
if (!argv.stop) {
  argv.stop = info['sampleStopDate'];
}
let parameters = [];
if (argv.parameters) {
  parameters = argv.parameters;
} else {
  for (p = 1; p < info['parameters'].length; p++) {
    parameters.push(info['parameters'][p]['name']);
  }
  parameters = parameters.join(",");
}
let DEBUG = "";
if (argv.debug) {
  DEBUG = "--debug";
}

let base = `node ../bin/HAPIdata.js ${DEBUG} --id ${argv.id} --parameters ${parameters} --start ${argv.start} --stop ${argv.stop}`;

let timing = new Array(fmts.length);
let Data = new Array(fmts.length);
exec(0);

function exec(f) {

  let cmd = base + " --from " + fmts[f].trim();
  console.log(chalk.blue.bold("Executing: \n  " + cmd));
  console.log(chalk.blue.bold("-".repeat(80)));

  let copts = {"encoding": "buffer"};
  let child = require('child_process').spawn('sh', ['-c', cmd], copts);

  let data = '';
  let stderr = '';

  let msstart = Date.now();
  child.stderr.on('data', function (err) {
    stderr = stderr + err;
    timing[f] = -1;
  });
  child.stdout.on('data', function (buffer) {
    data = data + buffer;      
  });
  child.stdout.on('end', function () {

    if (timing[f] == -1) {
      //console.error(chalk.red("Command " + cmd + " gave stderr:"));
      console.error(chalk.red(stderr));
    }

    let dt = (Date.now()-msstart);
    if (timing[f] !== -1) {
      timing[f] = dt;
    }

    if (timing[f] !== -1) {
      Data[f] = data;
    }

    if (data && (argv.debug || argv.showdata)) {
      let xdata = data.toString();
      if (argv.debug === true && argv.showdata === false) {
        xdata = xdata.split("\n");
        for (d of xdata) {
          if (d !== "" && !/^[0-9]{2}/.test(d)) {
            console.log(d);
          }
        }
      } else {
        console.log(xdata);
      }
    }

    console.log(chalk.blue.bold("-".repeat(80)));
    if (f < fmts.length - 1) {
      exec(++f);
    } else {
      for (f in fmts) {
        if (timing[f] !== -1) {
          datas = Data[f].split("\n")
          let msg = fmts[f] + "\t"
                  + timing[f] + "\t[ms]\t"
          if (!argv.debug) {
            msg = msg 
                  + (datas.length-1) + " records\t"
                  + (datas[0].split(",").length) + " columns\t"
                  + (Data[f].length/1000).toFixed(1) + " [KB]\t"
                  + (Data[f].length/(1000*timing[f])).toFixed(1) + "\t[KB/s]";
          }
          console.log(msg);
        } else {
          console.log(chalk.red(fmts[f] + "\t" + "error"));
        }
      }
    }
  });
}