'''
Last tested with cdflib 0.4.4.

Build dict with structure similar^[1] to that returned by `format=json` `/data` 
endpoint response of [CDASR](https://cdaweb.gsfc.nasa.gov/WebServices/REST/)

[1] Not enough information is returned by `cdflib` to match exactly. In addition,

I. Although enough information is available to reproduce the [CDASR](https://cdaweb.gsfc.nasa.gov/WebServices/REST/) form, which has
the form
  "cdfVarData": {
    "record": [
      {
        "recNum": 0,
        "value": [
          "-0.863 1.227 2.553"
        ]
      },...
this script uses the more efficient form
"cdfVarData": [-0.863***], 1.227***], 2.553***]
where the ***] indicates extra precision that is removed when casting values
to a string based on FORMAT.

II. The structure returned by this script has less unnecessary depth. For example,
instead of "CDF": [{ ... }], this script uses "CDF": {...} because its use case
is for a single CDF only.

TODO: Modify to match CDASR form exactly and add an option "--format=strict".
'''

import os
import re
import sys
import json
import argparse

import cdflib

def cl_args():
  parser = argparse.ArgumentParser()
  parser.add_argument('--in', default='notes/cdfs/ac_h2s_mfi_20090601000000_20090601120000.cdf')
  parser.add_argument('--out', default='')
  parser.add_argument('--stdout', default=False, action='store_true')
  parser.add_argument('--maxrecs', default=None)
  argv = vars(parser.parse_args())
  return argv

def cdf2json(file):

  def normalize(d):

    key_removes = [
                    "Sparse", "Data_Type", "Block_Factor", "Compression",
                    "Variable", "CDF", "Majority", "rDim_sizes", "rDim_sizes",
                    "Encoding", "Version", "rVariables", "zVariables", "Compress",
                    "Var_Type", "varNum", "Copyright", "Num"
                  ]

    key_renames = {
                    "Last_Rec": "numRecordsAllocate",
                    "Data_Type_Description": "cdfDatatype",
                    "Num_Dims": "dim",
                    "Pad": "padValue",
                    "Dim_Sizes": "dimSizes",
                    "Rec_Vary": "recVariance",
                    "Dim_Vary": "dimVariances",
                    "Num_Elements": "numElements",
                    "Attributes": "attribute"
                  }

    for key in list(d.keys()):

      if False:
        # https://cdaweb.gsfc.nasa.gov/WebServices/REST/ represents values
        # as strings using FORMAT for display. To match their output
        # we would need to do the same.
        if key == "FILLVAL":
          if d[key] == -9999999848243207295109594873856:
            d[key] = "-1e31"
          if d[key] == -1e31:
            d[key] = "-1e31"

        if key == "Pad":
          if d[key] == -1000000015047466219876688855040:
            d[key] = "-1e30"
          if d[key] == -1e30:
            d[key] = "-1e30"

      # Change True/False to "VARY"/"NOVARY"
      if key == "Rec_Vary":
        d[key] = "VARY" if d[key] else "NOVARY"

      if key == "Last_Rec":
        # key is renamed to numRecordsAllocate, so add 1.
        d[key] = d[key] + 1

      # Convert from array of ints to comma separated list of ints
      if key == "Dim_Sizes":
          d[key] = ",".join(str(el) for el in d[key])

      # Convert from list of -1 and +1 elements to list with "NOVARY" and "VARY"
      if key == "Dim_Vary":
        for i, val in enumerate(d[key]):
          if val == -1:
            d[key][i] = "NOVARY"
          if val == 1:
            d[key][i] = "VARY"

      # Remove i/o related metadata
      if key in key_removes:
        del d[key]

      # Rename keys
      if key in key_renames:
        d[key_renames[key]] = d[key]
        del d[key]

    for i, ikey in enumerate(list(d.keys())):
      if isinstance(d[ikey], list):
        if len(d[ikey]) == 0:          
          del d[ikey]
        elif len(d[ikey]) == 1:
          # Unwrap lists with one element.
          d[ikey] = d[ikey][0]
      elif isinstance(d[ikey], str) and d[ikey] == "":
        del d[ikey]

    return d

  def lists2entries(d):
    darray = []
    for i, ikey in enumerate(list(d.keys())):
      entries = []
      for j, jkey in enumerate(list(d[ikey].keys())):
        value = d[ikey][jkey][0]
        if isinstance(value, list):
          value = " ".join(value)
        else:
          value = str(value)
        entries.append({
                        "entryNum": j,
                        "cdfDatatype": d[ikey][jkey][1],
                        "value": value
                      })
      darray.append({"name": ikey, "entry": entries})

    return darray

  def list2records(l):
    pad1 = " "*15
    pad2 = " "*12
    if isinstance(l, list):
      l = "".join("\n" + pad1 + "@~" + str(el) + "@~" for el in l) + "\n" + pad2
    else:
      l = str(l)
    return l

  def from_np(cdfVarInfo):
    """
    Deal with https://github.com/MAVENSDC/cdflib/issues/176
    and https://github.com/MAVENSDC/cdflib/issues/177
    No to_np=False option for varinq b/c of 176 and to_np=False can't
    be used in varattsget b/c of 177.
    """
    import numpy
    for k, v in cdfVarInfo.items():
      if isinstance(v, numpy.ndarray):
        cdfVarInfo[k] = v.tolist()
      elif isinstance(v, numpy.generic):
        cdfVarInfo[k] = v.item()
      # TODO: Anything else?

    return cdfVarInfo

  cdffile = cdflib.CDF(file)

  cdfGAttributes = cdffile.globalattsget(expand=True);

  #print(json.dumps(cdfGAttributes, indent=2))
  #sys.exit()
  cdfInfo  = cdffile.cdf_info()
  cdfInfo['name'] = file

  variables = [*cdfInfo['rVariables'], *cdfInfo['zVariables']]

  cdfdict = {"CDF":
              [
                {
                  "name": file,
                  "cdfGAttributes": {
                      "attribute": lists2entries(normalize(cdfGAttributes))
                  },
                  "cdfVariables": {
                      "variable": []
                  }
                }
              ]
            }

  #print(json.dumps(normalize(cdfGAttributes), indent=2))
  for variable in variables:

    cdfVarInfo = from_np(cdffile.varinq(variable))
    #print(cdffile.varattsget(variable, expand=True, to_np=True))
    #print(cdffile.varattsget(variable, expand=False, to_np=True))
    cdfVAttributes = from_np(cdffile.varattsget(variable, expand=True, to_np=True))

    #print(variable)
    #print(cdfVarInfo)
    cdfVarInfo = normalize(cdfVarInfo)
    #print(json.dumps(cdfVarInfo, indent=2))

    #if variable == ''
    #sys.exit()

    if argv['maxrecs'] is None:
      cdfVarData = cdffile.varget(variable, expand=True, to_np=False)
    else:
      numrecs = cdfVarInfo['numRecordsAllocate']
      endrec = numrecs
      if cdfVarInfo["recVariance"] == "VARY":
        endrec = min(numrecs, int(argv['maxrecs']))

      cdfVarInfo['numRecordsAllocate'] = endrec

      cdfVarData = cdffile.varget(variable, startrec=0, endrec=endrec-1, expand=True, to_np=False)
    #print(cdfVarData)

    #print(json.dumps(cdfVarData, indent=2))
    data = cdfVarData['Data']
    #print(json.dumps(data, indent=2))

    if cdfVarInfo['cdfDatatype'].startswith('CDF_EPOCH'):
      data = cdflib.cdfepoch.encode(data, iso_8601=True)
      if isinstance(data, list):
        for i in range(len(data)):
          data[i] += "Z"
      else:
        data = data + "Z"

      cdfVarInfo["padValue"] = cdflib.cdfepoch.encode(cdfVarInfo["padValue"], iso_8601=True) 
      cdfVarInfo["padValue"] += "Z"

    if cdfVarInfo["cdfDatatype"] == "CDF_CHAR" and cdfVarInfo["recVariance"] == "NOVARY":
      cdfVarInfo["padValue"] = " "*cdfVarInfo["numElements"]
      data = [{
                  "recNum": 0,
                  "value": list2records(data),
                  "elementDelimiter": "@~"
              }]
    
    #print(json.dumps(normalize(cdfVAttributes), indent=2))
    aarray = []
    obj = normalize(cdfVAttributes)
    for key in obj.keys():
      if obj[key] == None:
        continue
      value = obj[key][0]
      if isinstance(value, list):
        value = map(str, value)
        value = " ".join(value)
      else:
        value = str(value)

      aarray.append({
                  "name": key,
                  "entry": [
                    {
                      "cdfDatatype": obj[key][1],
                      "value": value
                    }
                  ]
                })
    cdfVAttributes = aarray  
    cdfVariable = {
                    "name": variable,
                    "cdfVarInfo": cdfVarInfo,
                    "cdfVAttributes": {
                        "attribute": aarray
                    },
                    "cdfVarData": {
                        "record": data
                    }
                  }


    cdfdict["CDF"][0]["cdfVariables"]["variable"].append(cdfVariable)

  return cdfdict

argv = cl_args()

filecdf = argv['in']
cdfdict = cdf2json(filecdf)

if argv['stdout'] == True:
  print(json.dumps(cdfdict, indent=2))

if argv['out'] == '':
  argv['out'] = os.path.splitext(filecdf)[0] + '-pycdf2json.json'

with open(argv['out'], 'w') as f:
  json.dump(cdfdict, f, indent=2)