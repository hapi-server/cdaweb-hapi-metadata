function argv() {
    let argv = 
    require('yargs')
        .help()
        .option('keepids', {
            describe: 'Comma-separated list of regex patterns to include',
            default: '^AC_',
            type: 'string'
        })
        .option('omitids', {
            describe: 'Comma-separated list of regex patterns to exclude (ignored for ids that match keepids pattern)',
            default: '^AIM',
            type: 'string'
        })
        .option('debug', {
            describe: "Show additional logging information",
            default: false
        })
        .option("omit", {
            describe: "Comma-separated list of steps to omit from: {inventory, masters, spase}",
            default: "",
            type: "string"
        })
        .option("maxsockets", {
            describe: "Maximum open sockets per server",
            default: 2, // More than 3 and requests start failing w/ 429 or 503.
            type: "number"
        })
        .option('maxheadage', {
            describe: 'Skip HEAD request and use cached file if header age < maxheadage',
            default: 100*3600*24,
            type: "number"
        })
        .option('maxfileage', {
            describe: 'Request file if age < maxage (in seconds) and HEAD indicates expired',
            default: 100*3600*24,
            type: "number"
        })
        .option('infodir', {
            describe: '',
            default: "hapi/bw",
            type: "string"
        })
        .option('cachedir', {
            describe: '',
            default: "cache/bw",
            type: "string"
        })
        .option('cdasr', {
            describe: "",
            default: "https://cdaweb.gsfc.nasa.gov/WS/cdasr/1/dataviews/sp_phys/datasets/",
            type: "string"
        })
        .option('allxml', {
            describe: "",
            default: "https://spdf.gsfc.nasa.gov/pub/catalogs/all.xml",
            type: "string"
        })
        .option('allxml', {
            describe: "",
            default: "https://spdf.gsfc.nasa.gov/pub/catalogs/all.xml",
            type: "string"
        })
        .option('ignorecache', {
            describe: "",
            default: false,
            type: "boolean"
        })
        .option('hapiversion', {
            describe: "",
            default: "3.1",
            type: "number"
        })
        .option('cdf2json-method', {
            describe: "python-cdf2json, java-CDF2Json, or java-CDF2CDFML",
            default: "python-cdf2json",
            type: "string"
        })
        .option('python-cdf2json', {
            describe: "",
            default: "python3 " + __dirname + "/../bin/cdf2json.py --maxrecs=1440",
            type: "string"
        })
        .option('java-CDF2CDFML', {
            describe: "",
            default: "CDF_BASE=/Applications/cdf/cdf39_0-dist; CLASSPATH=.:$CDF_BASE/cdfjava/classes/cdfjava.jar:$CDF_BASE/cdfjava/classes/cdfjson.jar:$CDF_BASE/cdfjava/classes/gson-2.8.6.jar:$CDF_BASE/cdfjava/classes/javax.json-1.0.4.jar:$CDF_BASE/cdfjava/cdftools/CDFToolsDriver.jar:$CDF_BASE/cdfjava/cdfml/cdfml.jar java CDF2CDFML",
            type: "string"
        })
        .option('java-CDF2Json', {
            describe: "",
            default: "CDF_BASE=/Applications/cdf/cdf39_0-dist; CLASSPATH=.:$CDF_BASE/cdfjava/classes/cdfjava.jar:$CDF_BASE/cdfjava/classes/cdfjson.jar:$CDF_BASE/cdfjava/classes/gson-2.8.6.jar:$CDF_BASE/cdfjava/classes/javax.json-1.0.4.jar:$CDF_BASE/cdfjava/cdftools/CDFToolsDriver.jar:$CDF_BASE/cdfjava/cdfml/cdfml.jar java CDF2Json",
            type: "string"
        })
        .argv;

    argv['keepids'] = argv['keepids'].split(',');
    argv['omitids'] = argv['omitids'].split(',');
    argv['omit'] = argv['omit'].split(',');
    return argv;
}
module.exports.argv = argv;
