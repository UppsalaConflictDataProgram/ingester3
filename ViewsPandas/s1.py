import sqlalchemy as sa
import warnings
import time
from random import randint
from diskcache import Cache


def mmm():
    print(1)

def init_cache():
    dc = Cache('~/local2')
    return dc
dc = init_cache()


@dc.memoize(typed=True, expire=None, tag='r1')
def xrand(i):
    time.sleep(3)
    a = randint(0, i)
    return a


class xCached:
    def __init__(self,i):
        self.dcx = init_cache()
        self.i=i

    @dc.memoize(typed=True, expire=None, tag='r2')
    def rand_gen(self):
        a=randint(0,self.i)
        return a


#print(a)
if __name__ == '__main__':
    xc = xCached(11)
    d = xc.rand_gen(); print(d)
    y = xrand(12); print(y)
    z = xrand(14); print(z)
