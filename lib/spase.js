function extractSPASEUnits(jsonSPASE, parameterID) {
  try {
    let parameters = jsonSPASE['NumericalData'][0]['Parameter'];
    for (let parameter of parameters) {
      if (parameter['ParameterKey'][0] === parameterID && parameter['Units']) {
        return parameter['Units'][0];
      }
    }
  } catch (e) {
    return undefined;
  }
}
module.exports.extractSPASEUnits = extractSPASEUnits;

function extractSPASECadence(jsonSPASE) {
    try {
      return jsonSPASE['NumericalData'][0]['TemporalDescription'][0]['Cadence'][0];
    } catch (e) {
      return undefined;
    }
  }
  module.exports.extractSPASECadence = extractSPASECadence;
  