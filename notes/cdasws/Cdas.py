# https://hub.gke2.mybinder.org/user/berniegsfc-cdasws-notebooks-id6nuend/doc/tree/CdasWsExampleXarray.ipynb

#%pip install xarray cdflib cdasws

from cdasws import CdasWs
import inspect
print(inspect.getfile(CdasWs))

from cdasws.datarepresentation import DataRepresentation
import matplotlib.pyplot as plt
cdas = CdasWs()

groups = cdas.get_observatory_groups()
for gidx, group in enumerate(groups):
    datasets = cdas.get_datasets(observatoryGroup=group['Name'])
    for didx, dataset in enumerate(datasets):
        print(group['Name'] + "," + dataset['Id'] + "," +  dataset['Label'])
        if didx == 2:
            break
    if gidx == 2:
        break