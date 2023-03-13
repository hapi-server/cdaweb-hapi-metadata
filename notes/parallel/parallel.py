import time
data = 10*[None]

from joblib import Parallel, delayed

def process(i):
  if i == 0:
    time.sleep(1)
  data[i] = i  
  #print(data)
  for k in range(len(data)):
    #print(k, data[k])
    if data[k] == None: break
    print(k, data[k])
  return 0
    
results = Parallel(n_jobs=2, prefer="threads", require='sharedmem')(delayed(process)(i) for i in range(10))
print(results)  # prints [0, 1, 4, 9, 16, 25, 36, 49, 64, 81]