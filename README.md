# Motivation

This code was developed to improve the HAPI metadata served at https://cdaweb.gsfc.nasa.gov/hapi by using the CDAS REST service https://cdaweb.gsfc.nasa.gov/WebServices/REST/.

Four methods are used to create a HAPI `all.json` file. A HAPI `all.json` file is array of datasets from the `/catalog` endpoint with an additional `info` node for each dataset that contains the response to `/info` for that dataset. 

The methods use

1. https://spdf.gsfc.nasa.gov/pub/catalogs/all.xml
   
   and queries for JSON CDFML to

   https://cdaweb.gsfc.nasa.gov/WebServices/REST/

2. queries to

   https://cdaweb.gsfc.nasa.gov/hapi

   The metadata for this server are created using master CDFs.

3. queries to

   https://cdaweb.gsfc.nasa.gov/registry/hdp/hapi

   The metadata for this server is created using SPASE records.

4. Using command line calls to `AutplotDataserver`, which can output HAPI `info` responses given a dataset id.

   The metadata for this server are created using master CDFs.

# Use

Requires [`Node.js`](https://nodejs.org/en/).

# To Do

1. Handle non-string `DEPEND_1`
1. Validate using hapi-server.org/verify