from ingester3.extensions import *
import pandas as pd
from ingester3.DBWriter import DBWriter
pgm_base = pd.DataFrame(
    {'month_id':[121, 122], #'' saves as integers (121 no decimals)
    'pg_id':[131460, 131460],
    'consecutive_dry_days_index_per_time_period':[1.0, None],
    'number_of_cdd_periods_with_more_than_5days_per_time_period':[1.0, None],
    'consecutive_wet_days_index_per_time_period':[1.0, None],
    'number_of_cwd_periods_with_more_than_5days_per_time_period':[1.0, None],
    'dtr':[None, 1.0],
    'fd':[None, 1.0],
    'ice_days':[None, 1.0],
    'prcptot':[None, 1.0],
    'r10mm':[1.0, None],
    'r20mm':[1.0, None],
    'r30mm':[1.0, None],
    'rx1day':[1.0, None],
    'rx5day':[1.0, None],
    'rx7day':[1.0, None],
    'spei3':[1.0, None],
    'spei6':[1.0, None],
    'spei12':[1.0, None],
    'spi3':[1.0, 1.0],
    'spi6':[1.0, None],
    'spi12':[1.0, None],
    'su':[1.0, None],
    'tmge5':[1.0, None],
    'tmge10':[1.0, None],
    'tmlt5':[1.0, None],
    'tmlt10':[1.0, None],
    'tmm':[1.0, None],
    'tn10p':[1.0, None],
    'tn90p':[1.0, None],
    'tnlt2':[1.0, None],
    'tnltm2':[2.0, None],
    'tnltm20':[1.0, None],
    'tnm':[1.0, None],
    'tnn':[None, 1.0],
    'tnx':[None, 1.0],
    'tr':[None, 1.0],
    'tx10p':[None, 1.0],
    'tx90p':[None, 1.0],
    'txge30':[None, 1.0],
    'txge35':[None, 1.0],
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
                   out_panel_wipe = False,
                   in_panel_zero = False, # set to Na if empty
                   out_panel_zero = False)
climate_indices_writer.set_time_extents_min_max(1,900)
climate_indices_writer.transfer(tname = 'gknvsgtgs')
