const fs = require('fs');

let outFile = __dirname + 'compare-meta.json';

let alls = {};
//let versions = ['bw','nl','bh', 'jf'];
let versions = ['bw','nl','bh'];

for (version of versions) {
  alls[version] = {};
  let fname = __dirname + '/../hapi/' + version + '/CDAWeb/all.json';
  console.log("Reading: " + fname);
  let all = JSON.parse(fs.readFileSync(fname));

  for (dsidx in all) {
    let id = all[dsidx]['id'];
    if (version === 'bh') {
      id = all[dsidx]['x_SPDF_ID'];
    }
    alls[version][id] = all[dsidx];
  }
}

let combined = {};
for (version of versions) {

  for (dsid of Object.keys(alls[version])) {

    if (!combined[dsid]) {
      combined[dsid] = {};      
    }

    for (dskey of Object.keys(alls[version][dsid])) {
      if (dskey === 'info') continue;
      if (!combined[dsid][dskey]) {
          combined[dsid][dskey] = {};
      }
      combined[dsid][dskey][version] = alls[version][dsid][dskey];
    }

    if (!combined[dsid]['info']) {
      combined[dsid]['info'] = {};
    }

    for (infokey of Object.keys(alls[version][dsid]['info'])) {
      if (infokey === 'parameters') continue;
      if (!combined[dsid]['info'][infokey]) {
          combined[dsid]['info'][infokey] = {};
      }
      combined[dsid]['info'][infokey][version] = alls[version][dsid]['info'][infokey];
    }

    for (paramidx in alls[version][dsid]['info']['parameters']) {
      let paramName = alls[version][dsid]['info']['parameters'][paramidx]['name'];
      if (!combined[dsid]['info']['parameters']) {
          combined[dsid]['info']['parameters'] = {};
      }
      if (!combined[dsid]['info']['parameters'][paramName]) {
          combined[dsid]['info']['parameters'][paramName] = {};
      }
      combined[dsid]['info']['parameters'][paramName][version] = alls[version][dsid]['info']['parameters'][paramidx];
    }

  }
}

// Move info node to end to make easier to read.
for (dsid in combined) {
  // Deep copy
  let info = JSON.parse(JSON.stringify(combined[dsid]['info']));
  delete combined[dsid]['info'];
  combined[dsid]['info'] = info;
}

console.log("Writing: " + outFile);
fs.writeFileSync(outFile, JSON.stringify(combined, null, 2), 'utf-8');        

