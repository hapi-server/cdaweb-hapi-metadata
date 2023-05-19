# TODO:

Add virtual variable discussion. See Virtual.md

naming conventions in JSON

# CDAWeb Comments

Point of json response if difficult to generate request that does not fail.

## Mixing of science and code metadata

GZIP, padValue, etc.

## Time to generate

It should not take so long to generate the metadata that is needed to get 
all of the relevant metadata information, except the recordy varying numbers,
needed to be able to develop a comprehensive set of metadata for search.

Nand's server takes 30 minutes. Mine takes longer. 

For each dataset, there should be a "master metadata file" that contains
all of the non-record varying metadata.

## Metadata overlay

The overlay problem. People using the `orig_data` endpoint will need to overlay
masters corrections. I heard someone in the PyHC meeting mention it. SPASE has
normalized expressions of units an cadence. So that is a second overlay. A 
third "overlay" is the expansion of virtual variables. Either one has to go
through the web service or do it yourself. In principle, one can go through the
web service to resolve this. However, what about MMS in the cloud? It would have
taken 5x longer or more to mirror to cloud if going through web service.

Why make users do so much patching? One argument could be "they should not
be using files". But why not patch the original files? Or hide the original
files? Or provide a "patch my cdf service"?

CDF2Json uses different conventions than CDASR. The Python library `cdflib` uses
different conventions for case and also in some cases different words.
cdflib uses keys that differ than standard usage. CDFJson uses StudlyCaps while CDASR uses camelCase. It seems that one should be able to do CDASR CDF | java CDF2Json and get the same result as a CDAS JSON request. (which has elementDelimiter ...) 

# SPASE Comments

* The process of creating HAPI metadata is complex. If CDAWeb had a complete set of _validated_ SPASE records _that were continously updated_, we would not have needed to write much of the code for this process. In fact, much of the code in `CDAS2HAPIinfo.js` duplicates functionality in the (unpublished?) code used to generate CDAWeb SPASE Numerical data records.

## Issues

* SPASE records have parameters that are not returned by request for data from the SPDF APIs that are backed by data provider-generated CDF files (CDAWeb and CDAS). The only way to access these variables is by reading the data provider-generated CDF files. It is not possible to determine which variables are available from the API vs. only from data provider-generated CDF files. Example where the primary time variable is virtual [THEMIS/A/EFI](https://hpde.io/NASA/NumericalData/THEMIS/A/EFI/PT3S.html). The SPASE record mentions "This parameter is virtual. It is calculated by calling the function COMP_THEMIS_EPOCH with the following input parameters: tha_efs_dot0_epoch0, tha_efs_dot0_time." Without mentioning where the "function" exists. This is the primary reason why SPASE records cannot be used to build HAPI metadata
* Stop time is relative
* Data Variable Descriptions
* Data provided metadata is modified. For example, at https://cdaweb.gsfc.nasa.gov/misc/NotesA.html under `AC_H5_SWI`, there is the sentence
  
  "The charge state distribution is the abundance of each charge state relative tothe total abundance for analyzed ions of the specified element."

  In the corresponding [SPASE record](https://hpde.io/NASA/NumericalData/ACE/SWICS_SWIMS/L2/ChargeState/PT2H) under parameter 2, I see

  "The Charge State Distribution is the Abundance of each Charge State relative to the total Abundance for analyzed Ions of the specified Element."

  If SPASE is making changes, these changes should be propagated back into the CDF metadata (in this case, the reason for the change is not clear, however).

# Virtual Variables

See
https://mail.google.com/mail/u/0/#search/jeremy/QgrcJHrjBQxNxwTckbhqnscttvqszHNjSrl
and
https://mail.google.com/mail/u/0/#sent/QgrcJHsTfRRbLXlQzkDnqxfpFdwhrCFRVXb


```
{
  "FileDescription": [
    {
      "Name": "https://cdaweb.gsfc.nasa.gov/sp_phys/data/themis/tha/l2/efi/2007/tha_l2_efi_20070224_v01.cdf",
      "MimeType": "application/x-cdf",
      "StartTime": "2007-02-24T00:00:00.000Z",
      "EndTime": "2007-02-25T00:00:00.000Z",
*     "Length": 103014,
      "LastModified": "2017-04-20T04:43:37.414Z"
    },
...
    {
      "Name": "https://cdaweb.gsfc.nasa.gov/sp_phys/data/themis/tha/l2/efi/2023/tha_l2_efi_20230321_v01.cdf",
      "MimeType": "application/x-cdf",
      "StartTime": "2023-03-21T00:00:00.000Z",
      "EndTime": "2023-03-22T00:00:00.000Z",
*     "Length": 47424390, 
      "LastModified": "2023-03-27T12:17:39.890Z"
    }

```

Virtual variable expansion causes request for file to be (12.8 + 4.5)/4.1 ~4.2x slower.

time curl -O "https://cdaweb.gsfc.nasa.gov/sp_phys/data/themis/tha/l2/efi/2023/tha_l2_efi_20230321_v01.cdf"
4.139 total

time curl "https://cdaweb.gsfc.nasa.gov/WS/cdasr/1/dataviews/sp_phys/datasets/THA_L2_EFI/data/20230321T000000Z,20230322T000000Z/ALL-VARIABLES?format=cdf"
12.830 total
time curl -O "https://cdaweb.gsfc.nasa.gov/tmp/wsAqQ4pW/tha_l2s_efi_20230321000001_20230321235957.cdf"
4.494 total



time curl -O "https://cdaweb.gsfc.nasa.gov/sp_phys/data/ace/mag/level_2_cdaweb/mfi_h0/1997/ac_h0_mfi_19970902_v04.cdf"

time curl "https://cdaweb.gsfc.nasa.gov/WS/cdasr/1/dataviews/sp_phys/datasets/AC_H0_MFI/data/19970902T000012Z,19970902T235956Z/ALL-VARIABLES?format=cdf"


```
time curl -O https://cdaweb.gsfc.nasa.gov/sp_phys/data/themis/tha/l2/efi/2007/tha_l2_efi_20070224_v01.cdf
```

```
time curl --silent -H "Content-Type: application/xml" \
    -H "Accept: application/json" \
    --data-ascii @Request_THA_L2_EFI.xml \
    https://cdaweb.gsfc.nasa.gov/WS/cdasr/1/dataviews/sp_phys/datasets
```

```
<?xml version="1.0" encoding="UTF-8" standalone="yes"?>
<DataRequest xmlns="http://cdaweb.gsfc.nasa.gov/schema">
  <CdfRequest>
    <TimeInterval>
      <Start>2023-03-21T00:00:00.000Z</Start>
        <End>2023-03-22T00:00:00.000Z</End>
    </TimeInterval>
    <DatasetRequest>
      <DatasetId>THA_L2_EFI</DatasetId>
      <VariableName>ALL-VARIABLES</VariableName>
    </DatasetRequest>
    <CdfVersion>3</CdfVersion>
    <CdfFormat>CDF</CdfFormat>
  </CdfRequest>
</DataRequest>
```
