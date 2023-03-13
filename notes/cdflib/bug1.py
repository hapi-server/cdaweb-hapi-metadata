import cdflib

fname = 'ac_h2s_swi_19980204000916_19980204000916.cdf'
cdf_file = cdflib.CDF(fname)
data = cdflib.cdf_to_xarray(fname)

