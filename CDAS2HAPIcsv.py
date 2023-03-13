#pip install xarray cdflib cdasws
import time as tm
#begin = tm.time()
import sys
import re
import numpy as np
import argparse
#print(tm.time() - begin)

#begin = tm.time()
from cdasws import CdasWs
from cdasws.datarepresentation import DataRepresentation
cdas = CdasWs()
#print(tm.time() - begin)

parser = argparse.ArgumentParser()
parser.add_argument('--id', default='AC_H3_CRIS')
parser.add_argument('--parameters', default='flux_B,flux_C')
parser.add_argument('--start', default='1997-08-26T00:00:00.000000000Z')
parser.add_argument('--stop', default='1997-09-04T00:00:00.000000000Z')
parser.add_argument('--fmt', default='csv')
parser.add_argument('--debug', action='store_true', default=False)
parser.add_argument('--infodir', default='')

argv   = vars(parser.parse_args())

dataset    = argv['id']
parameters = argv['parameters']
start      = argv['start']
stop       = argv['stop']

import json

if argv['debug']:
  begin = tm.time()

if parameters.strip() == '' or parameters.strip() == 'Time':
  infofile = argv['infodir'] + "/" + argv['id'] + ".json"
  with open(infofile, 'r') as f:
    info = json.load(f)

  if parameters.strip() == 'Time':
    # Must request at least one parameter, so choose first.
    # TODO: Choose one with smallest number of columns.
    parameters = [info['parameters'][1]['name']]
  else:
    names = []
    for parameter in info['parameters']:
      if parameter['name'] != 'Time':
        names.append(parameter['name'])

    parameters = names
else:
  parameters = parameters.split(",")

if parameters[0] == 'Time':
  parameters = parameters[1:]

#print(parameters)
#sys.exit(0)
#print(tm.time() - begin)

if argv['debug']:
  begin = tm.time()


# Library sends warnings to stdout instead of using logging infrastructure.
# We need to capture to prevent it from being sent out.
from contextlib import redirect_stdout
import io
stdout_ = io.StringIO()
with redirect_stdout(stdout_):
  status, data = cdas.get_data(dataset, parameters, start, stop,
                               dataRepresentation=DataRepresentation.XARRAY)
#print(stdout_.getvalue())

if not isinstance(data['Epoch'].values, np.datetime64):
  # If no data returned, get_data returns a single value that
  # has type datetime. 
  sys.exit(0)

if status and hasattr(status, 'http') and status['http']['status_code'] != 200:
  sys.stderr.write(status['cdas'])
  sys.exit(1)

if argv['debug']:
  end = tm.time()
  read_time = end - begin
  begin = tm.time()

startdt64 = np.datetime64(start.replace("Z",""))
stopdt64 = np.datetime64(stop.replace("Z",""))
for i in range(data['Epoch'].shape[0]):

  if data['Epoch'].values[i] < startdt64:
    continue
  if data['Epoch'].values[i] >= stopdt64:
    break

  sys.stdout.write('%sZ' % str(data['Epoch'].values[i]))

  for p in range(0,len(parameters)):

    fmt = re.sub(r"([i|f|e])([0-9].*)\.([0-9].*)", r".\2\1", data[parameters[p]].FORMAT.lower())
    fmt = fmt.replace("i","d")

    fmtc = ",%"+fmt
    FILLVAL = str(data[parameters[p]].FILLVAL[0])
    if len(data[parameters[p]].values.shape) == 1:
      el = fmtc % data[parameters[p]].values[i]
      el = el.replace("nan", FILLVAL)
      sys.stdout.write(el)
    else:
      for j in range(data[parameters[p]].values.shape[1]):
        el = fmtc % data[parameters[p]].values[i,j]
        el = el.replace("nan", FILLVAL)
        sys.stdout.write(el)

  sys.stdout.write("\n")

if argv['debug']:
  end = tm.time()
  write_time = end - begin
  print("(Get and Read)/(Write CSV) Time: {0:f}".format(write_time/read_time))
