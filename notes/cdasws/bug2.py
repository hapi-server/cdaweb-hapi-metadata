from cdasws import CdasWs
from cdasws.datarepresentation import DataRepresentation
cdas = CdasWs()

dataset = 'AC_OR_SSC'
parameters = 'GSE_LAT,GSE_LON,RADIUS,XYZ_GSE,XYZ_GSEO'
to = '1997-08-27T00:00:00Z'
tf = '1997-09-03T01:00:00Z'

parameters = parameters.split(",")

status, data = cdas.get_data(dataset, parameters, to, tf,
                             dataRepresentation=DataRepresentation.XARRAY)

print(status)
print(data)