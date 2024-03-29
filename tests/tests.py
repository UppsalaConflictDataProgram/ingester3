# TODO: move this to a proper testing library one day

import pandas as pd
from ingester3.extensions import *
from ingester3.Country import Country
from ingester3.scratch import fetch_ids_df
from warnings import catch_warnings
import numpy as np

cache_manager(True)

#Let's populate the caches

pgm_ids = fetch_ids_df('priogrid_month')
assert pgm_ids.shape[0] == 11169720
pgy_ids = fetch_ids_df('priogrid_year')
assert pgy_ids.shape[0] == 4602078
cm_ids = fetch_ids_df('country_month')
assert cm_ids.shape[0] == 158230
cy_ids = fetch_ids_df('country_year')
assert cy_ids.shape[0] == 20392


# Test the m accessor

m1 = pd.DataFrame({"month_id":[100,101,102]})
m2 = pd.DataFrame({"month_id":[100,100,102]})
m3 = pd.DataFrame({"month_id":['8','349']})
m4 = pd.DataFrame({"month_id":['8','9229','e']})
m5 = pd.DataFrame({"year":[1989,1990,1991],"month":[11,12,10]})
m6 = pd.DataFrame({"year":[1989,1990,1991],"month":[13,12,0]})

assert m1.m.is_unique is True

assert m2.m.is_unique is False

# m3 should coerce to ints. Should throw a warning, but otherwise be correct.
with catch_warnings(record=True) as w:
    assert m3.m.is_unique == True
    assert len(w) == 1

# m4 should generate both a ValueError (since e is not a known int)
# as well as a Warning, as strings are coerced to ints. Testing both

with catch_warnings(record=True) as w:
    try:
        m4.m.is_unique
    except ValueError:
        assert len(w) == 1

# m5 should convert in ids 119,132,142
m5 = pd.DataFrame.m.from_year_month(m5)
assert m5.month_id[0] == 119
assert m5.month_id[2] == 142

# m6 should return a column with FTF
print("Assert FTF")
m6 = pd.DataFrame.m.soft_validate_year_month(m6)

assert m6.valid_year_month[0] == False
assert m6.valid_year_month[1] == True
assert m6.valid_year_month[2] == False


print("Assert PGM")
pgm1 = pd.DataFrame({"pg_id":[49141,39218,53959,53959],"month_id":[294,293,294,294],"data":[1,2,4,6]})
pgm2 = pd.DataFrame({"pg_id":[49141,39218,891,53959],"month_id":[294,293,294,294],"data":[1,2,4,6]})
pgm3 = pd.DataFrame({'year':[1991,1995,1981],'month':[11,12,1],'lat':[9.2,4.2,11.2],'lon':[20.5,22.5,29.5]})
pgm5 = pd.DataFrame({'year':[1991,1995,1981],'month':[11,12,1],'lat':[9.2,4.2,11.2],'lon':[-221.5,22.5,29.5]})

print("Assert PGM1")
assert pgm1.pgm.is_unique == False
print("Assert PGM2")
assert pgm2.pgm.is_unique == True
print("Assert PGM3")
assert pd.DataFrame.pgm.soft_validate_year_month_latlon(pgm5).valid_year_month_latlon[0] == False
print("Assert PGM5")
assert pd.DataFrame.pgm.from_year_month_latlon(pgm3).pg_id[0] == 142962
print("Assert PGM6")
dataset = pd.DataFrame.pgm.from_year_month_latlon(pgm3)
print("Assert PGM7")
dataset = dataset.pgm.db_id()
assert dataset.pgm_id[0] == 118534163

#test the Country Module
print("Assert C")
c = Country(85)
assert c.isoab == 'ITA'

c = Country.from_gwcode(666)
assert c.name == 'Israel'

x = Country.from_iso('ROU').neighbors(month_id=400)
assert set([i.isoab for i in x]) == {'BGR','HUN','MDA','UKR','SRB'}

#x = Country.from_gwcode(666)
#print(x)


c_list = ['SDN','VEN','PER','FJI','ISR','ETH','ZAF','TZA','SUN','SUN','YUG','BEN','AND',
        'MCO','VUT','ARG','BOL','SYR','MNE','DEU','DDR','DMA','GRD','LCA','GBR','CSK','SUN','YEM','SCG','SRB',
        'YEM','SAU','KIR','MLI','MNG','ETH','KAZ','NOR','RUS','LUX','GUY','SUR','TTO','WSM','TON','ITA','MKD','NZL',
        'NAM','PRY','MUS','URY','BLZ','SYC','DOM','BRN','HTI','JAM','CRI','SLV','BFA','HND','GNQ','NIC',
        'PAN','ATG','NRU','SLB','ALB','JOR','CPV','GNB','IRQ','MDV','BIH','SMR','ARM','BGR','CYP','GEO','LBN',
        'AUT','CZE','KWT','HUN','QAT','SVK','SVN','COG','COD','BEL','DJI','YMD','LTU','TGO','YUG','IDN','SGP','IRL',
        'JPN','BHS','CUB','FSM','CIV','ZMB','PRT','ESP','UGA','SUN','SUN','SUN','SUN','SUN','SUN','SWE','DZA','CMR',
        'CAF','NGA','HRV','GRC','MDG','IRN','DNK','POL','SAU','FRA','LIE','NLD','AUS','FIN','CHE','PNG','TUV','SOM',
        'TKM','UZB','AFG','TLS','KGZ','PAK','TJK','BGD','PHL','KOR','LAO','MMR','THA','SRB','VNM','LBR',
        'SLE','MOZ','ZAF','AGO','CAN','DEU','GHA','LBY','MYS','USA','TZA','MAR','SDN','BLR','EST','LVA','MDA',
        'ROU','UKR','OMN','LKA','TWN','CHN','YEM','BDI','SWZ','RWA','GMB','AZE','BHR','ARE','BTN','GTM',
        'VCT','KNA','NPL','PRK','PLW','COL','MEX','BRB','ISL','KHM','BRA','CHL','ECU','MLT','TUR','MHL','IDN',
        'IND','EGY','GIN','SEN','ERI','LSO','TUN','NER','STP','BWA','ZWE','COM','MWI','GAB','TCD','KEN','MRT','SSD']

c_df = pd.DataFrame({'isoab': c_list,
                     'var1': np.random.random(len(c_list)),
                     'var2': np.random.randint(1,100,len(c_list))})

filled_c_df = pd.DataFrame.c.from_iso(c_df, iso_col='isoab')
assert filled_c_df[filled_c_df.isoab == 'EGY'].c_id.iloc[0] == 222

print("Assert SD")
assert ViewsMonth(300).start_date == '2004-12-01'
assert ViewsMonth(300).end_date == '2004-12-31'
assert ViewsMonth(302).end_date == '2005-02-28'

cm6 = pd.DataFrame({"isocc":['SEN','SEN','GDP'],"month":[500,501,502]})
cm6 = pd.DataFrame.cm.soft_validate_iso(cm6,'isocc','month')
assert cm6.valid_id[2] == False
assert cm6.valid_id[1] == True

print("Assert ISOCC")

cm_empty = cm6.drop([0,1,2])
cm_empty = pd.DataFrame.cm.soft_validate_iso(cm_empty,'isocc','month')
cm_empty = pd.DataFrame.cm.from_iso(cm_empty,'isocc','month')
assert 'c_id' in set(cm_empty.columns)
assert 'valid_id' in set(cm_empty.columns)
assert cm_empty.shape[0] == 0

pg_valid = pd.DataFrame({"pg_id":[592112, -1, 20245, None]})
pg_valid = pd.DataFrame.pg.soft_validate(pg_valid)
assert pg_valid.valid_id.sum() == 1

m_valid = pd.DataFrame({"month_id":[100,200,None, -1]})
m_valid = pd.DataFrame.m.soft_validate(m_valid)
assert m_valid.valid_id.sum() == 2


print("ASSERT ISO->CY")

# Test if the country-year creation toolkit works using the country year panels we have

cc0 = pd.DataFrame({'code':['SUN','DDR','SRB','RUS','SCG','SRB','SAU','SCG','SUN','DDR'],
'year':[1989,1986,2009,2015,1996,2020,2019,2020,2019,2017],
'data':[1,2,9,4,5,6,0,4,5,6]})

cc1 = pd.DataFrame({'code':[553,'212',625,625,365,365,365,626,626],
                    'year':[2000,2000,1986,2015,1982,1991,2010,2000,2020],
                    'data':[1,2,4,5,2,1,4,2,9]})

cc0 = pd.DataFrame.c.soft_validate_iso_year(cc0, iso_col='code', year_col='year', at_month=6)
assert cc0[cc0.valid_id].reset_index()['index'].max() == 6
cc0 = pd.DataFrame.c.from_iso_year(cc0.head(7), iso_col='code', year_col='year', at_month=6)
assert cc0.c_id.sum() == 1264

cc1 = pd.DataFrame.c.soft_validate_gwcode_year(cc1, gw_col='code', year_col='year', at_month=11)
assert cc1[cc1.valid_id == True].reset_index()['index'].sum() == 29

# This should fail, since there is one country in the df that does not exist in our panels
# Catch and pass, if passes without tripping raise.
try:
    not_ok = False
    cc2 = pd.DataFrame.c.from_gwcode_year(cc1, gw_col='code', year_col='year', at_month=11)
    not_ok = True
except ValueError:
    pass
finally:
    if not_ok:
        raise ValueError

cc1 = pd.DataFrame.c.from_gwcode_year(cc1.head(7), gw_col='code', year_col='year', at_month=11)
assert cc1.c_id.sum() == 1079



print("Assert PGM_S")

pgm_valid = pd.DataFrame({"pg_id":[21219, -1, 20245, 30211], 'month_id':[100,100,-1,None]})
pgm_valid = pd.DataFrame.pgm.soft_validate(pgm_valid)
assert pg_valid.valid_id.sum() == 1

c_valid = pd.DataFrame({"c_id":[64,67,-1,910,None]})
c_valid = pd.DataFrame.c.soft_validate(c_valid)
assert c_valid.valid_id.sum() == 2

c_valid = pd.DataFrame({"c_id":[64,67,-1,910,None], "month_id":[404,None,-1,2,700]})
c_valid = pd.DataFrame.cm.soft_validate(c_valid)
assert c_valid.valid_id.sum() == 1


c_valid = pd.DataFrame({"c_id":[254,254,254],"year_id":[1990,1991,1992]})
c_valid = pd.DataFrame.cy.soft_validate(c_valid)
assert c_valid.loc[0].valid_id == False
assert c_valid.loc[1].valid_id == True
assert c_valid.loc[2].valid_id == False


c_cy = pd.DataFrame({'c_id':[116,73,74,75], 'year_id':[2000,2000,-1,2001]})
c_cy = pd.DataFrame.cy.soft_validate(c_cy)
assert c_cy.loc[0:1].valid_id.sum() == 2
assert c_cy.loc[2:3].valid_id.sum() == 0


new_africa = pd.DataFrame.c.new_africa()
assert new_africa.c.full_set(in_africa = True) == True

new_me = pd.DataFrame.c.new_me()
assert new_me.c.full_set(in_me = True) == True

print("Assert STRUCT")

new_africa = pd.DataFrame.c.new_africa()
new_africa['month_id'] = 100
x = new_africa.cm.full_set(in_africa=True, min_month=100, max_month=100)
assert x==True

print ("Assert datetime_lat_lon")
c_pgm = pd.DataFrame({'lat':[25.29,11.29,-2.74,-0.75], 'lon':[110,-49,-32.44,7.32],
                     'date':['2020-01-09','2019-12-20','2021-11-03','2020-01-01'],
                     'x_data':[1,2,5,9]})

c_pgm['date'] = pd.to_datetime(c_pgm.date)

c_pgm = pd.DataFrame.pgm.from_datetime_latlon(c_pgm, datetime_col='date', lat_col='lat', lon_col='lon')

assert c_pgm.loc[3].pg_id == 128535
assert c_pgm.loc[0].pg_id == 166181
assert c_pgm.loc[1].month_id == 480
assert c_pgm.loc[2].month_id == 503
assert c_pgm.loc[3].month_id == 481

assert c_pgm.pgm.full_set() == False
