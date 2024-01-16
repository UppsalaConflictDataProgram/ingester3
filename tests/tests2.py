from ingester3.extensions import *
import pandas as pd
from ingester3.DBWriter import DBWriter
pgm_base = pd.DataFrame(
    {'month_id':[121, 122], #'' saves as integers (121 no decimals)
    'pg_id':[131460, 131460],
    'txgt50p':[None, 1.0],
    'txm':[None, 1.0],
    'txn':[None, 1.0],
    'txx':[None, 1.0]}
 )
pgm_base = pgm_base.pgm.db_id()
#push it as empty table
climate_indices_writer = DBWriter(pgm_base,
                   level = 'pgm',
                   in_panel_wipe = True, #default, updates the in_panel when new data are inserted w/o touching the past month_ids
                   out_panel_wipe = True,
                   in_panel_zero = False, # set to Na if empty
                   out_panel_zero = False)
climate_indices_writer.set_time_extents_min_max(115, 130)
climate_indices_writer.transfer(tname = 'test_xxx')
