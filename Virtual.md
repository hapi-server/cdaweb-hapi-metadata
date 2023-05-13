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
