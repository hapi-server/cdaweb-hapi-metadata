function cdfType2hapiType(cdftype) {
    if (floatType(cdftype)) {
      return 'double';
    } else if (intType(cdftype)) {
      return 'integer';
    } else if (timeType(cdftype)) {
      return 'isotime';
    } else if (charType(cdftype)) {
      return 'string';
    } else {
      return null;
    }
}
module.exports.cdfType2hapiType = cdfType2hapiType;

function handledType(cdfType) {
  return floatType(cdfType) || intType(cdfType) || charType(cdfType) || timeType(cdfType)
}
module.exports.handledType = handledType;

function charType(cdfType) {
  return ['CDF_CHAR', 'CDF_UCHAR'].includes(cdfType);
}
module.exports.charType = charType;

function timeType(cdfType) {
  return cdfType.startsWith('CDF_EPOCH') || cdfType.startsWith('CDF_TIME');
}
module.exports.timeType = timeType;

function intType(cdfType) {
  return cdfType.startsWith('CDF_INT') || cdfType.startsWith('CDF_UINT') || cdfType.startsWith('CDF_BYTE');
}
module.exports.intType = intType;

function floatType(cdfType) {
  return ['CDF_FLOAT', 'CDF_DOUBLE', 'CDF_REAL4', 'CDF_REAL8'].includes(cdfType);
}
module.exports.floatType = floatType;
