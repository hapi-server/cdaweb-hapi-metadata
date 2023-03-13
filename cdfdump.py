import re
import sys
import time as _time
import argparse

import cdflib
import numpy as np

parser = argparse.ArgumentParser()
#parser.add_argument('--id', default='../cdfs/ac_h2s_mfi_20090601000000_20090601120000.cdf')
#parser.add_argument('--parameters', default='Magnitude,BGSEc')
parser.add_argument('--id', default='THA_L2_MOM@0')
parser.add_argument('--file', default='notes/cdfs/tha_l2s_mom_20070802000030_20070802011213.cdf')
parser.add_argument('--parameters', default='tha_peim_data_quality,tha_peim_densityQ,tha_peim_ptotQ,tha_peim_fluxQ,tha_peim_mftensQ,tha_peim_efluxQ,tha_peim_velocity_dslQ,tha_peim_velocity_gseQ,tha_peim_velocity_gsmQ,tha_peim_ptensQ,tha_peim_magQ,tha_peim_velocity_magQ,tha_peim_t3_magQ,tha_peim_ptens_magQ,tha_peem_data_quality,tha_peem_densityQ,tha_peem_ptotQ,tha_peem_fluxQ,tha_peem_mftensQ,tha_peem_efluxQ,tha_peem_velocity_dslQ,tha_peem_velocity_gseQ,tha_peem_velocity_gsmQ,tha_peem_ptensQ,tha_peem_magQ,tha_peem_velocity_magQ,tha_peem_t3_magQ,tha_peem_ptens_magQ,tha_peim_density,tha_peim_ptot,tha_peim_flux,tha_peim_mftens,tha_peim_eflux,tha_peim_velocity_dsl,tha_peim_velocity_gse,tha_peim_velocity_gsm,tha_peim_ptens,tha_peim_mag,tha_peim_velocity_mag,tha_peim_t3_mag,tha_peim_ptens_mag,tha_peem_density,tha_peem_ptot,tha_peem_flux,tha_peem_mftens,tha_peem_eflux,tha_peem_velocity_dsl,tha_peem_velocity_gse,tha_peem_velocity_gsm,tha_peem_ptens,tha_peem_mag,tha_peem_velocity_mag,tha_peem_t3_mag,tha_peem_ptens_mag,tha_pxxm_pot,tha_eesa_solarwind_flag,tha_iesa_solarwind_flag')
#parser.add_argument('--parameters', default='tha_peim_data_quality')
parser.add_argument('--start', default='1997-08-26T00:00:00.000000000Z')
parser.add_argument('--stop', default='1997-09-04T00:00:00.000000000Z')
parser.add_argument('--debug', action='store_true', default=False)
parser.add_argument('--infodir', default='hapi/bw/info')

argv   = vars(parser.parse_args())

dataset    = argv['id']
parameters = argv['parameters']
start      = argv['start']
stop       = argv['stop']

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


def dump(time, meta, data):

  nbytes = 0

  for i in range(len(time)):

    nbytes = nbytes + len(str(time[i]))
    sys.stdout.write('%sZ' % str(time[i]))

    for p in range(0,len(data)):

      # e.g., i4 => d
      fmt = re.sub(r"([i])([0-9].*)", r"d", meta[p]['FORMAT'].lower())

      # e.g., E11.4 => %.4e
      fmt = re.sub(r"([f|e])([0-9].*)\.([0-9].*)", r".\2\1", fmt)

      # e.g., d => ,%d
      fmtc = ",%" + fmt

      FILLVAL = str(meta[p]['FILLVAL'])
      if FILLVAL == 'nan':
        # cdflib converts fill values to np.nan.
        # not sure how to get it to return original.
        # Could get from hapiinfo, which has original
        # FILLVAL
        FILLVAL = '1e31'

      if len(data[p].shape) == 1:
        el = fmtc % data[p][i]
        el = el.replace("nan", FILLVAL)
        nbytes = nbytes + len(el)
        sys.stdout.write(el)
      else:
        for j in range(data[p].shape[1]):
          el = fmtc % data[p][i,j]
          el = el.replace("nan", FILLVAL)
          nbytes = nbytes + len(el)
          sys.stdout.write(el)

    nbytes = nbytes + 1
    sys.stdout.write("\n")

  return nbytes


def report(begin, end, nbytes, what=None):

  if argv['debug'] == False:
    return

  dt = end - begin

  if what == 'read':
    fmtstr = "Read  {0:.1f} MB | {1:.1f} MB/s | {2:d} records/s\n"
    sys.stderr.write(fmtstr.format(nbytes/1000000., nbytes/(1000000.*dt), int(len(time)/dt)))

  if what == 'write':
    fmtstr = "Wrote {0:.1f} MB | {1:.1f} MB/s | {2:d} records/s\n"
    sys.stderr.write(fmtstr.format(nbytes/1000000., nbytes/(1000000.*dt), int(len(time)/dt)))


def tick():

  if argv['debug'] == False:
    return

  return _time.time()

cdffile  = cdflib.CDF(argv['file'])
cdfinfo  = cdffile.cdf_info()

hapiinfo = hapi_info(argv['infodir'], dataset)
DEPEND_0 = re.sub(r'@[0-9].*$', "", hapiinfo['x_DEPEND_0'])

begin = tick()

epoch = cdffile.varget(variable=DEPEND_0)
time  = cdflib.cdfepoch.encode(epoch, iso_8601=True) 

meta = []
data = []
size = []
parameters = parameters.split(",")
for p in range(len(parameters)):
  v  = cdffile.varget(variable=parameters[p])
  va = cdffile.varattsget(variable=parameters[p])
  data.append(v)
  meta.append(va)
  size.append(v.size*v.itemsize)

report(begin, _time.time(), sum(size), what='read')

begin = tick()

nbytes = dump(time, meta, data)

report(begin, _time.time(), nbytes, what='write')
