# Motivation

This code was developed to improve the HAPI metadata served at https://cdaweb.gsfc.nasa.gov/hapi.

Three developers have written software to produce HAPI metadata using different methods. Each approach taken has limitations and differences exist in the produced metadata. This repository can be used to compare metadata generated by each method.

This respository also contains code to compare HAPI CSV generated using five different methods.

This repository contains

* scripts for a fourth method that uses the [CDAS REST service](https://cdaweb.gsfc.nasa.gov/WebServices/REST/). The script that generates metadata is `CDAS2HAPIinfo.js` ($\sim$1000 lines). The output from running this script is placed in `cache/bw/`. The output is not stored in the repository, but is visible at http://mag.gmu.edu/git-data/cdaweb-hapi-metadata/.

* a script, `compare-meta.js`, that compares the metadata results. The file `compare/compare-meta.json` contains the content from the four `all` files with keys to indicate the method. The keys are `bw`, `nl`, `bh`, and `jf`, which are the initials of the person who developed the software that generates the HAPI metatada. See below for additional details.

* a script, `HAPIcsv.js` that compares HAPI CSV produced by methods `nl`, `bh`, and `jf` described below along with HAPI CSV produced by: transforming a CDAS `text` response (using Node.js), transforming a CDAS `cdf` response using the Python CDFlib, and using the CDAS Python CDAS client.

The four methods that produce HAPI metadata are

1. `bw`, which uses the new code in this repository that 

   1. Extracts dataset ids and their start and stop dates from https://spdf.gsfc.nasa.gov/pub/catalogs/all.xml

   2. For each dataset, makes a `/variables` request to https://cdaweb.gsfc.nasa.gov/WebServices/REST/ to get the list of variable in the dataset that is needed for the next step

   3. Makes a `/data` request to https://cdaweb.gsfc.nasa.gov/WebServices/REST/ to obtain a sample data file (in JSON format, but could be modified to use CDF) with the final needed pieces of metadata. Note that often several requests are needed because the web service will not return data if the time range of the request is such that there are no data. In addition, if the time range of the request is too large, the request is rejected.

   `CDAS2HAPIinfo.js` starts with a 100-second timespan starting at the start (or stop) date given in CDAWeb's `all.xml` and increases (or decreases) this timespan by a factor of $10^n$ for up to $n=4$. If the timespan is to large, it decreases the timespan by $1/10^n$. Ideally this trial-and-error approach would not be required and there would be an endpoint where parameter-level metadata was returned without the need to form a data request that works. (Note: this approach should be replaced with a request for a CDF file, assuming that this will work for files with virtual variables.)

   4. After step 3., all of the metadata needed to form a HAPI response is available. The final step is to generate HAPI `/info` responses for each dataset. There is one complication. HAPI mandates that all variables in a dataset have the same time tags. Some CDAWeb datasets have datasets with variables with different time tags. So prior to creating `/info` responses, new HAPI datasets are formed. These new datasets have ids that match the CDAWeb ids but have an "@0", "@1", ... appended, where the number indicates the time tag variable index in the original dataset.

   The initial generation of the the HAPI `all-info.json` file using `CDAS2HAPIinfo.js` can take up to 30 minutes, which is similar to the update time required daily by the `nl` server. In contrast, subsequent updates using `CDAS2HAPIinfo.js` takes less than a second; on a daily basis, only the `startDate` and `stopDate` must be updated, which requires reading `all.xml` and updating `all-info.json`. When CDAWeb adds datasets or the master CDF changes, the process outlined above is only required for those dataset; this process typically takes less than 10 seconds per dataset.

2. `nl`, which uses an approach similar to the above for datasets with virtual variables and an approach similar to `jf` below otherwise.

   [This code](https://git.mysmce.com/spdf/hapi-nand) is used for the [production CDAWeb HAPI server](https://cdaweb.gsfc.nasa.gov/hapi).

   This production HAPI server has many datasets for which the metadata or data responses are not valid. It appears the [HAPI verifier](https://hapi-server.org/verify) was never run on all datasets. I have found that when randomly selecting datasets and parameters at https://hapi-server.org/servers, one frequently encounters issues.

   The production HAPI server becomes unresponsive at 9 am daily due to a similar update that appears to block the meain thread. However, in general, a full update is only needed when content other than the `startDate` and `stopDate` changes. 

3. `bh`, which uses [SPASE](https://spase-group.org/) records. 

   [This server](https://cdaweb.gsfc.nasa.gov/registry/hdp/hapi/) is a prototype and it serves only [CDAWeb datasets for which a SPASE record is available](https://github.com/hpde/SMWG/tree/master/Repository/NASA).

4. `jf`, which uses [master CDFs](https://cdaweb.gsfc.nasa.gov/pub/software/cdawlib/0MASTERS/), raw CDF files, and code from [Autoplot](https://sourceforge.net/p/autoplot/code/HEAD/tree/).

   The code that produces HAPI metadata is also a prototype and is not indended for production use.

# Use

Requires [`Node.js`](https://nodejs.org/en/).

In reference to the above four options, to create metadata for all CDAWeb IDs that start with `"AC_"`, use

1. `node CDAS2HAPIinfo.js --idregex '^AC_'`, which creates 

   `hapi/bw/all.json`, which contains all of the info responses placed in `hapi/bw/info/`.

2. `node HAPI2HAPIinfo.js --version 'nl' --idregex '^AC_'`, which creates

   `hapi/nl/all.json` and `hapi/nl/info/`

3. `node HAPI2HAPIinfo.js --version 'bh' --idregex '^AC_'`, which creates

   `hapi/bh/all.json`, which contains all of the info responses placed in `hapi/bh/info/`.

4. `node HAPI2HAPIinfo.js --version 'jf' --idregex '^AC_'`, which creates

   `hapi/jf/all.json`, which contains all of the info responses placed in `hapi/jf/info/`.

After these files are created, the program `compare-meta.js` can be executed to generate the file `compare/compare-meta.json` that shows the metadata created by the four approaches in a single file.

# SPASE Comments

* The process of creating HAPI metadata is complex. If CDAWeb had a complete set of _validated_ SPASE records _that were continously updated_, we would not have needed to write the code for this process. In fact, much of the code in `CDAS2HAPIinfo.js` duplicates functionality in the (unpublished) code used to generate CDAWeb SPASE Numerical data records.

* Ideally, CDAWeb would publish `all-SPASE.xml`, which contained all of the SPASE Numerical data records. The transformation from this to HAPI metadata would then be trivial.

# Recommendation

The output of the CDAS Rest server is straightforward to transform into HAPI CSV responses.

A "data adapter" that takes an input of `dataset`, `parameters`, `start`, and `stop` that can be used for a generic server requires approximately $100$ lines of code.

I see several two paths forward:

1. Find and fix all of the issues in the production server. The code base for the production HAPI server is complex and fixes take a significant amount of time. Once the issues are fixed, we would also want to update the metadata it produces so that it contains important information required for validation, such as sample start/stop and proper labels for multi-dimensional parameters. In addition, we would probably want this server to be updated to support HAPI 3.0 syntax. Given how long this code took to develop and the rate at which issues are fixed, I regard this as a labor intensive option.

2. Have a generic HAPI server use the HAPI metdata generated by `CDAS2HAPIinfo.js` and a command line data adapter. The primary development required would be optimization and testing of the data adapter.

