from ingester3.extensions import *
from ingester3.DBWriter import DBWriter

pgm_base = pd.DataFrame(
    {'latitude':[None, None],
    'longitude':[None, None],
    'month_id':[121, 122],
    'pg_id':[131460, 131460],
    'time':['1-1-1990', '1-2-1990'],
    'cdd_mon':[1, None],
    'cwd_mon':[1, None],
    'dtr_mon':[None, 1],
    'fd_mon':[None, 1],
    'id_mon':[None, 1],
    'prcptot_mon':[None, 1],
    'r10mm_mon':[1, None],
    'r20mm_mon':[1, None],
    'r30mm_mon':[1, None],
    'rx1day_mon':[1, None],
    'rx5day_mon':[1, None],
    'rx7day_mon':[1, None],
    'spei3_mon':[1, None],
    'spei6_mon':[1, None],
    'spei12_mon':[1, None],
    'spi3_mon':[1, None],
    'spi6_mon':[1, None],
    'spi12_mon':[1, None],
    'su_mon':[1, None],
    'tmge5_mon':[1, None],
    'tmge10_mon':[1, None],
    'tmlt5_mon':[1, None],
    'tmlt10_mon':[1, None],
    'tmm_mon':[1, None],
    'tn10p_mon':[1, None],
    'tn90p_mon':[1, None],
    'tnlt2_mon':[1, None],
    'tnltm2_mon':[2, None],
    'tnltm20_mon':[1, None],
    'tnm_mon':[1, None],
    'tnn_mon':[None, 1],
    'tnx_mon':[None, 1],
    'tr_mon':[None, 1],
    'tx10p_mon':[None, 1],
    'tx90p_mon':[None, 1],
    'txge30_mon':[None, 1],
    'txge35_mon':[None, 1],
    'txgt50p_mon':[None, 1],
    'txm_mon':[None, 1],
    'txn_mon':[None, 1],
    'txx_mon':[None, 1]}
)
pgm_base = pgm_base.pgm.db_id()
#push it as empty table
climate_indices_writer  = DBWriter(pgm_base,
                   level = 'pgm',
                   in_panel_wipe = True, #default, updates the in_panel when new data are inserted w/o touching the past month_ids
                   out_panel_wipe = False,
                   in_panel_zero = False, # set to Na if empty
                   out_panel_zero = False)
climate_indices_writer.set_time_extents_min_max(121,122)
climate_indices_writer.transfer(tname = 'climate') #I will ingest the 'climate' table in DB