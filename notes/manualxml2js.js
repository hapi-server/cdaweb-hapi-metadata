const fs = require('fs');

// read file
const xmlFile = fs.readFileSync('../cache/bw/AC/ac_or_def_00000000_v01-cdfxdf.xml','utf-8');
const { DOMParser, XMLSerializer } = require('@xmldom/xmldom')

let xmlDoc = new DOMParser().parseFromString(xmlFile, 'text/xml')

console.log(xmlToJson(xmlDoc))

// Changes XML to JSON
function xmlToJson(xml) {
  
  // Create the return object
  var obj = {};

  if (xml.nodeType == 1) { // element
    // do attributes
    if (xml.attributes.length > 0) {
      obj["$"] = {};
      for (var j = 0; j < xml.attributes.length; j++) {
        var attribute = xml.attributes.item(j);
        obj["$"][attribute.nodeName] = attribute.nodeValue;
      }
    }
  } else if (xml.nodeType == 3) { // text
    //obj = xml.nodeValue;
  }

  // do children
  if (xml.hasChildNodes()) {
    for (var i = 0; i < xml.childNodes.length; i++) {
      var item = xml.childNodes.item(i);
      var nodeName = item.nodeName;
      console.log(nodeName)
      console.log(obj[nodeName])
      if (nodeName === undefined) continue;
      if (nodeName === "#text") continue;
      if (typeof(obj[nodeName]) == "undefined") {
        tmp = xmlToJson(item);
        if (Object.keys(tmp).length != 0)
          obj[nodeName] = tmp
      } else {
        if (typeof(obj[nodeName].push) == "undefined") {
          var old = obj[nodeName];

          console.log(old)
          obj[nodeName] = [];
          obj[nodeName].push(old);
        }
        obj[nodeName].push(xmlToJson(item));
      }
    }
  }
  return obj;
};

