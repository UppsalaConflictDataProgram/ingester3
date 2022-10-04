from ingester3.extensions import *
import numpy as np

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
assert c4 is None