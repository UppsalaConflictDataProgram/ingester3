import pandas as pd
import requests
from ingester3.extensions import *
from ingester3.DBWriter import DBWriter
from ingester3.scratch import cache_manager

from diskcache import Cache
ged_cache = Cache('ged.cache')

def _get_ged_slice(next_page_url: str):
    r = requests.get(next_page_url)
    output = r.json()
    next_page_url = (
        output["NextPageUrl"] if output["NextPageUrl"] != "" else None
    )
    ged = pd.DataFrame(output["Result"])
    page_count = output["TotalPages"]
    return next_page_url, ged, page_count

@ged_cache.memoize(typed=True, expire=1000, tag='ged')
def fetch_ged(api_version):
    cur_page = 1
    next_page_url = (
        "http://ucdpapi.pcr.uu.se/api/gedevents/"
        + api_version
        + "?pagesize=1000"
    )

    df = pd.DataFrame()
    while next_page_url:
        next_page_url, ged_slice, total_pages = _get_ged_slice(
            next_page_url=next_page_url
        )
        df = df.append(ged_slice, ignore_index=True)
        print(f"{cur_page} of {total_pages} pages loaded.")
        cur_page += 1

    return df

if __name__=='__main__':
    ged = fetch_ged('21.0.5')
    print(ged.shape)

