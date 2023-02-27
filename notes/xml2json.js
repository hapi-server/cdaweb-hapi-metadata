const fs = require('fs');
const convert = require('xml-js');
const xml2js = require('xml2js').parseString;

// read file
const xmlFile = fs.readFileSync('../cache/bw/AC/ac_or_def_00000000_v01-cdfxdf.xml','utf-8');
const { DOMParser, XMLSerializer } = require('@xmldom/xmldom')

xmlDoc = new DOMParser().parseFromString(xmlFile, 'text/xml')

console.log(xmlDoc);


process.exit()
//console.log(xmlFile.toString())

xml2js(xmlFile, function (err, jsonObj) {
  //console.log(jsonObj)
});

//let jsObj = convert.xml2json(xmlFile, {compact: true, spaces: 2});

//console.log(jsObj);


cdfGAttributes = {};

for (i = 0; i < x.length ;i++) {
  if (x[i].nodeName === '#text') continue;
  console.log(x[i].nodeName)
  if (x[i].nodeName === 'cdfGAttributes') {
    for (j = 0; j < x[i].childNodes.length; j++) {
      // Loop over cdfGAttributes, which are <parameter name="NAME"> nodes.
      if (x[i].childNodes[j].nodeName === '#text') continue;
      // Extract name attribute value, NAME. Assumes only attribute is "name".
      let name = x[i].childNodes[j].attributes[0].value;
      console.log(" " + name);
      cdfGAttributes[name] = [];
      for (k = 0; k < x[i].childNodes[j].childNodes.length; k++) {
        // Loop over children of <parameter> nodes that are <value> nodes.
        if (x[i].childNodes[j].childNodes[k].nodeName !== 'value') continue;
          // <value> node under <parameter>. Get its content.
          let value = x[i].childNodes[j].childNodes[k].childNodes[0].nodeValue;
          cdfGAttributes[name].push(value)
          console.log("    " + value);
      }
      if (cdfGAttributes[name].length == 1) {
        // If only one <value> node, store as string not in one-element array.
        cdfGAttributes[name] = cdfGAttributes[name][0];
      }
    }
  }
}

//process.exit()

console.log(cdfGAttributes)

const source = `<xml xmlns="a"><child1>test</child1><child2>b</child2></xml>`

const xmlDoc = new DOMParser().parseFromString(xmlFile, 'text/xml')
//console.log(doc)

x = xmlDoc.documentElement.childNodes;

//cdfGAttributes = xmlDoc.getElementsByTagName("parameter");

//console.log(cdfGAttributes[0])
//console.log(cdfGAttributes.getElementsByTagName("parameter"));

var xpath = require('xpath')
var dom = require('xmldom').DOMParser
 
var xml = "<book><title>Harry Potter</title><title>xHarry Potter</title></book>"
var doc = new dom().parseFromString(xml)
var nodes = xpath.select("string(//title)", doc)
console.log(nodes)
