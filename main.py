import pandas as pd
import numpy as np

from ViewsPandas.extensions import *

test(100)
x = pd.DataFrame({"pg_id":[145294,145295], 'value':[100,300]})
print(x.pg.is_unique)

