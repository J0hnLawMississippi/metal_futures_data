#!/usr/bin/env python3                                                                                                

import os
import databento as db
import pandas as pd
import numpy as np
import warnings
from datetime import datetime,timedelta
from numba import njit

warnings.filterwarnings('ignore', module='pandas', category=UserWarning)


def custom_resample(group):
    return pd.Series({
        'rtype': group['rtype'].iloc[0],
        'publisher_id': group['publisher_id'].iloc[0],
        'instrument_id': group['instrument_id'].iloc[0],
        'open': group['open'].iloc[0],
        'high': group['high'].max(),
        'low': group['low'].min(),
        'close': group['close'].iloc[-1],
        'volume': group['volume'].sum(),
        'symbol': group['symbol'].iloc[0]
    })




def update():
    client = db.Live("$DB_KEY$")
    GC_df = pd.read_pickle("GC_latest.pkl")
    SI_df = pd.read_pickle("SI_latest.pkl")
    PL_df = pd.read_pickle("PL_latest.pkl")
    HG_df = pd.read_pickle("HG_latest.pkl")
    now = datetime.now()
    last_hour_start = now - timedelta(hours=1)
    last_hour_start = last_hour_start.replace(minute=0).strftime("%Y-%m-%dT%H:%M")
    #print(now,last_hour_start)                                                                                       


    client.subscribe(
        dataset="GLBX.MDP3",
        schema="ohlcv-1m",
        stype_in="continuous",
        symbols=["GC.c.2","SI.c.1","PL.c.2","HG.c.1"],
        start=last_hour_start
    )

    # Now, we will open a file for writing and start streaming the data for the last 24hrs                            
    now_str = now.strftime("%Y-%m-%dT%H:%M")

    client.add_stream(f"GC_SI_PL_HG_{now_str}.dbn")
    client.start()

    client.block_for_close(timeout=5)


    # Finally, we will open the DBN file                                                                              
    dbn_store = db.read_dbn(f"GC_SI_PL_HG_{now_str}.dbn")
    new_GC_SI_PL_HG = dbn_store.to_df(schema="ohlcv-1m").reset_index()

    new_GC = new_GC_SI_PL_HG[new_GC_SI_PL_HG['symbol'].str.startswith('G')]
    new_SI = new_GC_SI_PL_HG[new_GC_SI_PL_HG['symbol'].str.startswith('S')]
    new_PL = new_GC_SI_PL_HG[new_GC_SI_PL_HG['symbol'].str.startswith('P')]
    new_HG = new_GC_SI_PL_HG[new_GC_SI_PL_HG['symbol'].str.startswith('H')]

    new_GC['ts_event'] = pd.to_datetime(new_GC['ts_event'])
    new_GC['symbol'] = 'GC.v.0'
    new_GC = new_GC[['ts_event', 'rtype', 'publisher_id','instrument_id', 'open', 'high', 'low', 'close', 'volume', '\
symbol']]
    new_SI['ts_event'] = pd.to_datetime(new_SI['ts_event'])
    new_SI['symbol'] = 'SI.v.0'
    new_SI = new_SI[['ts_event', 'rtype', 'publisher_id','instrument_id', 'open', 'high', 'low', 'close', 'volume', '\
symbol']]
    new_PL['ts_event'] = pd.to_datetime(new_PL['ts_event'])
    new_PL['symbol'] = 'PL.v.0'
    new_PL = new_PL[['ts_event', 'rtype', 'publisher_id','instrument_id', 'open', 'high', 'low', 'close', 'volume', '\
symbol']]
    new_HG['ts_event'] = pd.to_datetime(new_HG['ts_event'])
    new_HG['symbol'] = 'HG.v.0'
    new_HG = new_HG[['ts_event', 'rtype', 'publisher_id','instrument_id', 'open', 'high', 'low', 'close', 'volume', '\
symbol']]
    #print(new_SI)                                                                                                    
    GC_hourly = new_GC.set_index('ts_event').groupby(pd.Grouper(freq='h')).apply(custom_resample)
    SI_hourly = new_SI.set_index('ts_event').groupby(pd.Grouper(freq='h')).apply(custom_resample)
    PL_hourly = new_PL.set_index('ts_event').groupby(pd.Grouper(freq='h')).apply(custom_resample)
    HG_hourly = new_HG.set_index('ts_event').groupby(pd.Grouper(freq='h')).apply(custom_resample)

    #print(hourly)                                                                                                    

    # Get last hours and format                                                                                       
    GC_last_hour = GC_hourly.iloc[-1:].reset_index()
    SI_last_hour = SI_hourly.iloc[-1:].reset_index()
    PL_last_hour = PL_hourly.iloc[-1:].reset_index()
    HG_last_hour = HG_hourly.iloc[-1:].reset_index()

    #print(GC_last_hour,SI_last_hour)                                                                                 


    GC_latest = pd.concat([GC_df,GC_last_hour], ignore_index=True)
    SI_latest = pd.concat([SI_df,SI_last_hour], ignore_index=True)
    PL_latest = pd.concat([PL_df,PL_last_hour], ignore_index=True)
    HG_latest = pd.concat([HG_df,HG_last_hour], ignore_index=True)

    GC_latest.to_pickle("GC_data_temp.pkl")
    os.replace("GC_data_temp.pkl", "GC_latest.pkl")
    SI_latest.to_pickle("SI_data_temp.pkl")
    os.replace("SI_data_temp.pkl", "SI_latest.pkl")
    PL_latest.to_pickle("PL_data_temp.pkl")
    os.replace("PL_data_temp.pkl", "PL_latest.pkl")
    HG_latest.to_pickle("HG_data_temp.pkl")
    os.replace("HG_data_temp.pkl", "HG_latest.pkl")
    return GC_latest,SI_latest,PL_latest,HG_latest
