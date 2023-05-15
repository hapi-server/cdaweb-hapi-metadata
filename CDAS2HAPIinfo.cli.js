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
            default: '^ALOUETTE2,AIM_CIPS_SCI_3A,^MMS,PO_.*_UVI',
            default: '',
            type: 'string'
        })
        .option('minrecords', {
            describe: 'Minimum # of records for CDAS data request to be successful',
            minrecords: 1440,
            type: 'number'
        })
        .option('debug', {
            describe: "Show additional logging information",
            default: false
        })
        .option("omit", {
            describe: "Comma-separated list of steps to omit from: {inventory, files1, masters, spase}",
            default: "",
            type: "string"
        })
        .option("maxsockets", {
            describe: "Maximum open sockets per server",
            default: 3,
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
        .option('maxtries', {
            describe: 'Maximum # of tries for CDAS data requests',
            default: 4,
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
        .option('hapiversion', {
            describe: "",
            default: "3.1",
            type: "number"
        })
        .option('cdf2json-method', {
            describe: "cdf2json.py or CDF2CDFML+xml2json",
            default: "cdf2json.py",
            type: "string"
        })
        .option('cdf2cdfml', {
            describe: "",
            default: "CDF_BASE=/Applications/cdf/cdf38_1-dist; CLASSPATH=.:$CDF_BASE/cdfjava/classes/cdfjava.jar:$CDF_BASE/cdfjava/classes/cdfjson.jar:$CDF_BASE/cdfjava/classes/gson-2.8.6.jar:$CDF_BASE/cdfjava/classes/javax.json-1.0.4.jar:$CDF_BASE/cdfjava/cdftools/CDFToolsDriver.jar:$CDF_BASE/cdfjava/cdfml/cdfml.jar java CDF2CDFML",
            type: "string"
        })
        .option('cdf2json', {
            describe: "",
            default: "python3 " + __dirname + "/bin/cdf2json.py --maxrecs=1440",
            type: "string"
        })
        .argv;

    argv['keepids'] = argv['keepids'].split(',');
    argv['omitids'] = argv['omitids'].split(',');
    argv['omit'] = argv['omit'].split(',');
    return argv;
}
module.exports.argv = argv;