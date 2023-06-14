import re
import sys
import time as _time
import argparse

import cdflib
import numpy as np

def cli():
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

  return vars(parser.parse_args())


def write(time, meta, data):

  # https://stackoverflow.com/a/30091579
  from signal import signal, SIGPIPE, SIG_DFL
  signal(SIGPIPE, SIG_DFL)

  nrecords = 0

  for t in range(len(time)):

    tstr = str(time[t])

    if tstr >= stop[0:len(tstr)]:
      break
    
    sys.stdout.write('%sZ' % tstr)

    for p, parameter in enumerate(data):

      fmt = c_template(meta[p]['FORMAT'])
      FILLVAL = np.array(meta[p]["FILLVAL"], dtype=parameter.dtype)

      # TODO: Check that this row-major (order-'C') is correct
      #  and not column major ('F').
      for value in parameter[t].flatten(order='C'):
        if value == FILLVAL:
          sys.stdout.write("," + str(FILLVAL))
        else:
          sys.stdout.write(fmt.format(value))
      
    # Omit trailing newline by executing if t < len(time) - 1?
    # Does spec say last character of CSV is newline?
    sys.stdout.write("\n")

    nrecords += 1

  return nrecords  


def c_template(f_template):
  
  # TODO: If invalid, return {}

  f_template = f_template.lower().strip(' ')

  # e.g., 10s => s and 10a => s
  fmt = re.sub(r"([0-9].*)([a|s])", r",{:s}", f_template)

  # e.g., i4 => d
  fmt = re.sub(r"([i])([0-9].*)", r",{:d}", f_template)

  # e.g., E11.4 => %.4e, F8.1 => %.1f
  fmt = re.sub(r"([f|e])([0-9].*)\.([0-9].*)", r",{:.\3\1}", f_template)

  return fmt


def report(begin, nrecords, what=None, size=None):

  if argv['debug'] == False:
    return

  dt = _time.time() - begin

  if what == 'read':
    fmtstr = "Read   {0:d} records | {1:6d} records/s | {2:d} B"
    print(fmtstr.format(nrecords, int(nrecords/dt), size))

  if what == 'write':
    fmtstr = "\nWrote  {0:d} records | {1:6d} records/s"
    print(fmtstr.format(nrecords, int(nrecords/dt)))


def tick():
  if argv['debug'] == False:
    return
  return _time.time()


def read():

  meta = []
  data = []

  if argv['lib'] == 'pycdaws':

    # No longer working.
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
    # Note: Assumes DEPEND_0 = 'Epoch', which is not always true.
    # TODO: Determine how to extract DEPEND_0. Not easy b/c can't save
    # xrdata due to a bug, so need to run request each time or modify
    # source of CdasWs() to use cache.
    time = xrdata['Epoch'].values
    size = -1

    for parameter in parameters:
      data.append(xrdata[parameter].values)
      meta.append({"FORMAT": xrdata[parameter].FORMAT})

  else:

    cdffile  = cdflib.CDF(argv['file'])

    size = 0
    depend_0s = []
    for parameter in parameters:
      data.append(cdffile.varget(variable=parameter))
      meta.append(cdffile.varattsget(variable=parameter))
      size = size + data[-1].size*data[-1].itemsize
      depend_0s.append(meta[-1]['DEPEND_0'])

    udepend_0s = list(set(depend_0s)); # Unique depend_0s
    assert len(udepend_0s) == 1, 'Multiple DEPEND0s not implemented. Found: ' + ", ".join(udepend_0s)

    epoch = cdffile.varget(variable=depend_0s[0])
    time  = cdflib.cdfepoch.encode(epoch, iso_8601=True) 

  if isinstance(time, str):
    time = [time]

  nrecords = len(time)

  return time, data, meta, nrecords, size

argv = cli()

dataset    = argv['id']
parameters = argv['parameters']
start      = argv['start']
stop       = argv['stop']

parameters = parameters.split(",")

begin = tick()
time, data, meta, nrecords, size = read()
report(begin, nrecords, size=size, what='read')

begin = tick()
nrecords = write(time, meta, data)
report(begin, nrecords, what='write')
