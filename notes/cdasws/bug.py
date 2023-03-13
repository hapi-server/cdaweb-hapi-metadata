from cdasws import CdasWs
from cdasws.datarepresentation import DataRepresentation
cdas = CdasWs()

dataset = 'AC_OR_SSC'
parameters = 'GSE_LAT'
to = '1997-08-27T00:00:00Z'
tf = '1997-08-27T01:00:00Z'

parameters = parameters.split(",")

status, data = cdas.get_data(dataset, parameters, to, tf,
                             dataRepresentation=DataRepresentation.XARRAY)

data.to_netcdf("data.nc")
import xarray as xr
data = xr.load("data.nc")

