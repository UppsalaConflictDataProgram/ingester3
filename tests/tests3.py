import pandas as pd

from ingester3.extensions import *
import numpy as np


###

a = pd.DataFrame({'c_id': [218, 117, 234], 'values': [1, 2, 9]})
b = pd.DataFrame({'c_id': [218, 117, 234, 117], 'year_id': [1991, 1991, 1991, 1992], 'values': [1, 2, 9, 11]})
assert(a.c.pg_id.shape == (5195,3))
assert(b.cy.pg_id.shape == (5512,4))

a['priogrid_gid'] = a.apply(lambda x: [j.id for j in Country(x.c_id).priogrids()], axis=1)
a = a.explode('priogrid_gid')

####

c1 = Country.from_iso('ISR')
c1 = c1.priogrids()
x = abs(int(np.min(np.array([i.lat for i in c1])) - np.max(np.array([i.lat for i in c1]))))
assert x == 3 #Israel should be 3 degrees long


c2 = Country.from_priogrid(157011)
assert c2.isoab == 'USA'

p2 = Priogrid(157011)
assert p2.country() == c2

# The system should know that any priogrid that is today in Russia was in USSR in 1989.
# Test crosslevel inheritance and db consistency as well as year specs.
c3 = Country.from_iso('RUS').priogrids()[0].country(1987).isoab
assert c3 == 'SUN'

c4 = Country.from_priogrid(1)
assert c4.id is None

x1 = pd.DataFrame({'pg_id': [173950, 193388, 157011], 'expected': [218, 117, 234]})
assert (sum(x1.pg.c_id == x1.expected) == 3)

x2 = x1.copy(deep=True)
x2['year_id'] = [2007, 1988, 1992]

assert (sum(x2.pgy.c_id == x1.expected) == 2)
assert x2.pgy.c_id.loc[1] == 189
