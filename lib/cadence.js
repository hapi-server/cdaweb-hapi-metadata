const spase  = require('./spase.js');
const {util}  = require('./util.js');

// TODO: extractRecords is copy of what is in CDAS2HAPIinfo.js
function extractRecords(recordArray) {
  let records = [];
  if (records['value']) {
    for (let r of recordArray) {
      records.append(r['value']);
    }
  } else {
    return recordArray;
  }
}

function getAttribute(attributes, attributeName) {
  for (let attribute of attributes['attribute']) {
    if (attribute['name'] === attributeName) {
      return attribute['entry'][0]['value'];
    }
  }
  return null;
}

function extractCadence(dataset, _data) {

  let dsid = dataset['id'];

  // Get cadence from data sample
  let cadenceData = undefined;
  let timeRecords = undefined;
  let MONOTON = undefined;
  for (let variable of _data['cdfVariables']['variable']) {
    if (variable['name'] === dataset['info']['x_DEPEND_0']) {
      MONOTON = getAttribute(variable['cdfVAttributes'],'MONOTON');
      timeRecords = extractRecords(variable['cdfVarData']['record']);
      break;
    }
  }
  if (timeRecords === undefined) {
    util.warning(dsid, "No DEPEND_0 records.");
  } else {
    cadenceData = inferCadence(timeRecords, dsid, MONOTON);
  }

  // Get cadence from CDF
  let cadenceCDF = undefined;
  if (dataset['info']['cadence']) {
    cadenceCDF = util.str2ISODuration(dataset['info']['cadence']);
    if (cadenceCDF !== undefined) {
      let msg = `Inferred cadence of ${cadenceCDF} from CDF attribute `
      msg += `TIME_RESOLUTION = '${dataset['info']['cadence']}'`;
      util.note(dsid, msg);
    } else {
      let msg = "Could not parse CDF attribute TIME_RESOLUTION: '"
      msg += dataset['info']['cadence'] + "' to use for cadence.";
      util.warning(dsid, msg);
    }
  } else {
    let msg = 'No CDF attribute TIME_RESOLUTION';
    util.note(dsid, msg);    
  }

  // Get cadence from SPASE
  let cadenceSPASE = undefined;
  if (dataset['_spase']) {
    cadenceSPASE = spase.extractSPASECadence(dataset['_spase']);
    if (cadenceSPASE !== undefined) {
      util.note(dsid, 'Cadence from SPASE: ' + cadenceSPASE);
    } else {
      util.warning(dsid, 'No Cadence node in SPASE');
    }
  }

  // Summarize
  if (cadenceSPASE !== undefined && cadenceData !== undefined ) {
    if (!util.sameDuration(cadenceSPASE, cadenceData)) {
      util.warning(dsid, "Cadence mis-match between SPASE and data sample.");
    }
  }
  if (cadenceSPASE !== undefined && cadenceCDF !== undefined ) {
    if (!util.sameDuration(cadenceSPASE, cadenceCDF)) {
      let msg = "Cadence mis-match between SPASE and parsed "
      msg += "TIME_RESOLUTION from CDF.";
      util.warning(dsid, msg);
    }
  }
  if (cadenceCDF !== undefined && cadenceData !== undefined ) {
    if (!util.sameDuration(cadenceCDF, cadenceData)) {
      let msg = "Cadence mis-match between data sample and parsed "
      msg += "TIME_RESOLUTION from CDF.";
      util.warning(dsid, msg);
    }
  }

  // Select
  if (cadenceSPASE !== undefined) {
    util.note(dsid, `Using cadence ${cadenceSPASE} from SPASE`);
    dataset['info']['cadence'] = cadenceSPASE;
  } else if (cadenceCDF !== undefined) {
    let msg = `Using cadence ${cadenceCDF} from parsed TIME_RESOLUTION from CDF`;
    util.note(dsid, msg);
    dataset['info']['cadence'] = cadenceCDF;
  } else if (cadenceData !== undefined) {
    util.note(dsid, `Using cadence ${cadenceData} from sample data`);
    dataset['info']['cadence'] = cadenceData;
  }
}
module.exports.extractCadence = extractCadence;

function inferCadence(timeRecords, dsid, MONTON) {

  let monoton = false;
  if (MONTON && MONTON.toLowerCase() === 'increase') {
    monoton = true;
  }
  let dts = [];
  for (let r = 0; r < timeRecords.length - 1; r++) {
    let dt = new Date(timeRecords[r+1]).getTime() 
           - new Date(timeRecords[r]).getTime();
    dts.push(dt);
  }

  let dthist = histogram(dts);

  let cadenceData = undefined;

  let Nu = Object.keys(dthist).length;
  let Nr = timeRecords.length;
  if (Nu == 1) {
    let S = dthist[0][0];
    cadenceData = "PT" + S + "S";
    util.note(dsid, `Inferred cadence of ${cadenceData} based on ${Nr} records.`);
  } else {
    if (Nr > 1) {
      let msg = `Could not infer cadence from ${Nr} time record${util.plural(Nr)} because `;
      msg += `more than one (${Nu}) unique Δt values.`;
      util.note(dsid, msg);
    } else {
      util.note(dsid, `Could not infer cadence from ${Nr} time record${util.plural(Nr)}.`);      
    }
    let dtmsg = "";
    let n = 1;
    for (let i in dthist) {
      let p = (100*dthist[i][1]/Nr);
      if (n == 5) break;
      dtmsg += `${p.toFixed(1)}% @ ${dthist[i][0]}s, `;
      n++;
    }
    if (dthist.length >= n) dtmsg += "...";
    if (Nr > 1 && dtmsg !== "") util.note(dsid, "Δt histogram: " + dtmsg);
  }

  if (dthist.length > 0) {
    dthist.sort(function(a, b) {return a[0] - b[0]});
    if (dthist[0][0] <= 0) {
      if (monoton) {
        let emsg = `(CDAWeb) Time records not monotonically `
        emsg += `increasing but MONOTON = '${MONTON}'.`;
        util.error(dsid, emsg, false, false);
      } else {
        util.error(dsid, "Time records not monotonic.", false, false);
      }
    }
  }
  return cadenceData;

  function histogram(dts) {
    let counts = {};
    for (let i = 0; i < dts.length; i++) {
      counts[dts[i]] = 1 + (counts[dts[i]] || 0);
    }
    let sorted = [];
    for (let entry in counts) {
      sorted.push([parseFloat(entry)/1000, counts[entry]]);
    }
    sorted.sort(function(a, b) {return b[1] - a[1]});
    return sorted;
  }
}
module.exports.inferCadence = inferCadence;
