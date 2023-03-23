import re
import sys
import time as _time
import argparse

import cdflib
import numpy as np

parser = argparse.ArgumentParser()
parser.add_argument('--id', default='AC_H2_MFI')
parser.add_argument('--parameters', default='Magnitude,BGSEc')
# TODO: We assume start and stop always given to ns precision.
parser.add_argument('--start', default='1997-08-26T00:00:00.000000000Z')
parser.add_argument('--stop', default='1997-09-04T00:00:00.000000000Z')
parser.add_argument('--file', default='notes/cdfs/ac_h2s_mfi_20090601000000_20090601120000.cdf')
parser.add_argument('--infodir', default='hapi/bw/info')
parser.add_argument('--debug', action='store_true', default=False)
parser.add_argument('--lib', default='cdflib')

argv   = vars(parser.parse_args())

dataset    = argv['id']
parameters = argv['parameters']
start      = argv['start']
stop       = argv['stop']

parameters = parameters.split(",")

if argv['debug'] == True:
  # So piping to head works.
  # https://stackoverflow.com/a/30091579
  from signal import signal, SIGPIPE, SIG_DFL
  signal(SIGPIPE, SIG_DFL)


def hapi_info(infodir, dataset):

  import json
  infofile = infodir + "/" + dataset + ".json"
  with open(infofile, 'r') as f:
    info = json.load(f)
  return info

def _print(msg, end=None):
  print(msg,end=end)
  return len('{}'.format(msg))

def dump(time, meta, data):

  nbytes = 0

  for i in range(len(time)):

    tstr = str(time[i])
    if tstr >= stop[0:len(tstr)]:
      nbytes = nbytes + 1
      break

    nbytes = nbytes + len(str(time[i]))
    sys.stdout.write('%sZ' % tstr)
    for p in range(0,len(data)):

      # e.g., 10s => s and 10a => s
      fmt = re.sub(r"([0-9].*)([a|s])", r"s", meta[p]['FORMAT'].lower())

      # e.g., i4 => d
      fmt = re.sub(r"([i])([0-9].*)", r"d", meta[p]['FORMAT'].lower())

      # e.g., E11.4 => %.4e
      fmt = re.sub(r"([f|e])([0-9].*)\.([0-9].*)", r".\2\1", fmt)

      # e.g., d => ,%d
      fmtc = ",%" + fmt

      if len(data[p].shape) == 1:
        # Commented out code leads to data that appears as, e.g., 5.27 being
        # printed as 5.26999999948.
        #el = fmtc % data[p][i]
        #el = el.replace("nan", FILLVAL)
        #nbytes = nbytes + len(el)
        nbytes = nbytes + _print(",", end='')
        nbytes = nbytes + _print(data[p][i], end='')
        #sys.stdout.write(el)
      else:
        for j in range(data[p].shape[1]):
          #el = fmtc % data[p][i,j]
          #el = el.replace("nan", FILLVAL)
          #nbytes = nbytes + len(el)
          #sys.stdout.write(el)
          nbytes = nbytes + _print(",", end='')
          nbytes = nbytes + _print(data[p][i,j], end='')

    if i < len(time) - 1:
      nbytes = nbytes + 1
      sys.stdout.write("\n")

  return nbytes


def report(begin, nbytes, nrecords, what=None):

  if argv['debug'] == False:
    return

  dt = _time.time() - begin

  if what == 'read':
    fmtstr = "Read  {0:.1f} KB | {1:.1f} KB/s | {2:d} records/s"
    print(fmtstr.format(nbytes/1000., nbytes/(1000.*dt), int(nrecords/dt)))

  if what == 'write':
    fmtstr = "Wrote {0:.1f} KB | {1:.1f} KB/s | {2:d} records/s"
    print(fmtstr.format(nbytes/1000., nbytes/(1000.*dt), int(nrecords/dt)))


def tick():
  if argv['debug'] == False:
    return
  return _time.time()


def depend_0():
  hapiinfo = hapi_info(argv['infodir'], dataset)
  return re.sub(r'@[0-9].*$', "", hapiinfo['x_DEPEND_0'])


def data():

  meta = []
  data = []
  size = []

  if argv['lib'] == 'pycdaws':

    from cdasws import CdasWs
    from cdasws.datarepresentation import DataRepresentation
    cdas = CdasWs()

    # CdasWs() sends warnings to stdout instead of using Python logging
    # module. We need to capture to prevent it from being sent out.
    import io
    stdout_ = io.StringIO()
    datasetr = re.sub(r"@[0-9].*$","",dataset)
    from contextlib import redirect_stdout
    with redirect_stdout(stdout_):
      status, xrdata = cdas.get_data(\
                        datasetr, parameters, start, stop,
                        dataRepresentation=DataRepresentation.XARRAY)

    time = xrdata[depend_0()].values

    for p in range(len(parameters)):
      v = xrdata[parameters[p]].values
      data.append(v)
      meta.append({
                    "FORMAT": xrdata[parameters[p]].FORMAT
                  })
      size.append(v.size*v.itemsize)

  else:

    cdffile  = cdflib.CDF(argv['file'])
    cdfinfo  = cdffile.cdf_info()

    epoch = cdffile.varget(variable=depend_0())
    time  = cdflib.cdfepoch.encode(epoch, iso_8601=True) 

    for p in range(len(parameters)):
      v  = cdffile.varget(variable=parameters[p])
      va = cdffile.varattsget(variable=parameters[p])
      data.append(v)
      meta.append(va)
      size.append(v.size*v.itemsize)

  nrecords = len(time)

  return time, data, meta, nrecords, sum(size)


begin = tick()
time, data, meta, nrecords, size = data()
report(begin, size, nrecords, what='read')

begin = tick()
nbytes = dump(time, meta, data)
report(begin, nbytes, nrecords, what='write')
