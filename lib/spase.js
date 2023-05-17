function extractSPASEUnits(jsonSPASE, parameterID) {
  if (!jsonSPASE) {return undefined}
  let parameters = jsonSPASE['NumericalData'][0]['Parameter'];
  if (!parameters) {return undefined}
  for (let parameter of parameters) {
    if (parameter['ParameterKey'] && parameter['ParameterKey'][0] === parameterID) {
      if (parameter['Units']) {
        return parameter['Units'][0];
      }
    }
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
  