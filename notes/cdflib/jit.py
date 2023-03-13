import sys
from numba import jit
import numpy as np
import time

@jit
def f(n):
  for i in range(n):
    print(i)

f(1)
a = time.time()
print(a)
f(1000000)
dt = time.time() - a
sys.stderr.write("%f\n" % (dt))

a = time.time()
print(a)
f(1000000)
dt = time.time() - a
sys.stderr.write("%f\n" % (dt))
