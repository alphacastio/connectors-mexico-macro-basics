#!/usr/bin/env python
# coding: utf-8

# In[1]:


import tabula
import pandas as pd
import requests
from urllib.request import urlopen

from alphacast import Alphacast
from dotenv import dotenv_values
API_KEY = dotenv_values(".env").get("API_KEY")
alphacast = Alphacast(API_KEY)


from tqdm import tqdm


# In[2]:


headers = {
"Host": "www.inegi.org.mx",
"Connection": "keep-alive",
"Pragma": "no-cache",
"Cache-Control": "no-cache",
"sec-ch-ua": '"Google Chrome";v="87", " Not;A Brand";v="99", "Chromium";v="87"',
"sec-ch-ua-mobile": "?0",
"Upgrade-Insecure-Requests": "1",
"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/87.0.4280.141 Safari/537.36",
"Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.9",
"Sec-Fetch-Site": "none",
"Sec-Fetch-Mode": "navigate",
"Sec-Fetch-User": "?1",
"Sec-Fetch-Dest": "document",
"Accept-Encoding": "gzip, deflate, br",
"Accept-Language": "es-ES,es;q=0.9",
"Cookie": "ASP.NET_SessionId=hqduz3ticvzl3e00aiuc5j3t; _ga=GA1.3.916147448.1610701197; _gid=GA1.3.942867910.1611092720; NSC_MC_bqjt=ffffffff09911cc745525d5f4f58455e445a4a423660; NSC_MC_OvfwpQpsubm=ffffffff0991142a45525d5f4f58455e445a4a423660; NSC_MC_dpoufojept2=ffffffff09911c5645525d5f4f58455e445a4a4229a2"
}


# In[3]:


groups_subgroups = [627610,627611,627612,627715,627726,627727,627737,627740,627741,627767,627776,627777,627780,627783,627786,627791,627797,627798,627809,627857,627858,627877,627884,627890,627891,627898,627915,627926,627927,627930,627933,627939,627940,627951,627954,627966,627973,627982,627985,627986,627990,627993,627996,627999,628003,628004,628014,628015,628017,628018,628037,628040,628042,628045,628048]
all_codes = [628052,628051,628050,628049,628048,628047,628046,628045,628044,628043,628042,628041,628040,628039,628038,628037,628036,628035,628034,628033,628032,628031,628030,628029,628028,628027,628026,628025,628024,628023,628022,628021,628020,628019,628018,628017,628016,628015,628014,628013,628012,628011,628010,628009,628008,628007,628006,628005,628004,628003,628002,628001,628000,627999,627998,627997,627996,627995,627994,627993,627992,627991,627990,627989,627988,627987,627986,627985,627983,627982,627980,627977,627974,627973,627972,627971,627970,627969,627968,627967,627966,627965,627964,627963,627962,627961,627960,627959,627958,627957,627956,627955,627954,627953,627952,627951,627950,627949,627948,627947,627946,627945,627944,627943,627942,627941,627940,627939,627938,627937,627936,627935,627934,627933,627932,627931,627930,627929,627928,627927,627926,627925,627924,627923,627922,627921,627920,627919,627918,627917,627916,627915,627914,627913,627912,627911,627910,627909,627908,627907,627906,627905,627904,627903,627902,627901,627900,627899,627898,627897,627896,627895,627894,627893,627892,627891,627890,627889,627888,627887,627886,627885,627884,627883,627882,627881,627880,627879,627878,627877,627876,627875,627874,627873,627872,627871,627870,627869,627868,627867,627866,627865,627864,627863,627862,627861,627860,627859,627858,627857,627856,627855,627854,627853,627852,627851,627850,627849,627848,627847,627846,627845,627844,627843,627842,627841,627840,627839,627838,627837,627836,627835,627834,627833,627832,627831,627830,627829,627828,627827,627826,627825,627824,627823,627822,627821,627820,627819,627818,627817,627816,627815,627814,627813,627812,627811,627810,627809,627808,627807,627806,627805,627804,627803,627802,627801,627800,627799,627798,627797,627796,627795,627794,627793,627792,627791,627790,627789,627788,627787,627786,627785,627784,627783,627782,627781,627780,627779,627778,627777,627776,627775,627774,627773,627772,627771,627770,627769,627768,627767,627766,627765,627764,627763,627762,627761,627760,627759,627758,627757,627756,627755,627754,627753,627752,627751,627750,627749,627748,627747,627746,627745,627744,627743,627742,627741,627740,627739,627738,627737,627736,627735,627734,627733,627732,627731,627730,627729,627728,627727,627726,627725,627724,627723,627722,627721,627720,627719,627718,627717,627716,627715,627714,627713,627712,627711,627710,627709,627708,627707,627706,627705,627704,627703,627702,627701,627700,627699,627698,627697,627696,627695,627694,627693,627692,627691,627690,627689,627688,627687,627686,627685,627684,627683,627682,627681,627680,627679,627678,627677,627676,627675,627674,627673,627672,627671,627670,627669,627668,627667,627666,627665,627664,627663,627662,627661,627660,627659,627658,627657,627656,627655,627654,627653,627652,627651,627650,627649,627648,627647,627646,627645,627644,627643,627642,627641,627640,627639,627638,627637,627636,627635,627634,627633,627632,627631,627630,627629,627628,627627,627626,627625,627624,627623,627622,627621,627620,627619,627618,627617,627616,627615,627614,627613,627612,627611,627610]
details_codes = [x for x in all_codes if x not in groups_subgroups]

# print(len(groups_subgroups))
# print(len(all_codes))
# print(len(details_codes))


# In[4]:


import json
from datetime import datetime
from dateutil.relativedelta import relativedelta


# In[5]:


def get_data(code):
#          "https://www.inegi.org.mx/app/api/indicadores/interna_v1_1//ValorIndicador/627610/0700/null/es/null/null/3/pd/0/null/null/null/null/json/563cbaa8-58bb-fef8-6763-1f1dae318f99"
    url = "https://www.inegi.org.mx/app/api/indicadores/interna_v1_1//ValorIndicador/" + str(code) + "/0700/null/es/null/null/3/pd/0/null/null/null/null/json/563cbaa8-58bb-fef8-6763-1f1dae318f99?"
    response = requests.get(url, headers=headers)
    html = response.content
    
    #print(response.text)
    json_text = response.text
    #[response.text.find("(")+1:-2]
    #print(json_text)
    json_text = json.loads(json_text)

    date_string = json_text["dimension"]["periods"]["category"]["label"][0]["Value"] + "/01"
    last_date = datetime.strptime(date_string, "%Y/%m/%d")
    num_months = len(json_text["value"]) -1
    first_date = last_date + relativedelta(months=-num_months)
    d = pd.date_range(start=first_date, end=last_date, freq='MS')
    col_name = json_text["dimension"]["indicator"]["category"]["label"][0]["Value"]
    values = json_text["value"]
    values.reverse()
#     print(str(code) + ": "+ col_name)
    df = pd.DataFrame(values, index=d, columns={col_name})
    df[col_name] = pd.to_numeric(df[col_name], errors="coerce")
    return df



# In[6]:


import time

df_agg = pd.DataFrame()
for code in tqdm(groups_subgroups, desc='Looping over groups/subgroups'):
#     print(code)
    df_agg = df_agg.merge(get_data(code), left_index=True, right_index=True, how="outer")
    time.sleep(3)

df_agg["Date"] = df_agg.index
df_agg = df_agg.set_index("Date")    
df_agg["country"] = "Mexico"

df_agg_2 = pd.DataFrame()
for code in tqdm(details_codes, desc='Looping over Code Details'):
    df_agg_2 = df_agg_2.merge(get_data(code), left_index=True, right_index=True, how="outer")
    time.sleep(1)

df_agg_2["Date"] = df_agg_2.index
df_agg_2 = df_agg_2.set_index("Date")    
df_agg_2["country"] = "Mexico"

alphacast.datasets.dataset(94).upload_data_from_df(df_agg, 
    deleteMissingFromDB = True, onConflictUpdateDB = True, uploadIndex=True)

alphacast.datasets.dataset(95).upload_data_from_df(df_agg_2, 
    deleteMissingFromDB = True, onConflictUpdateDB = True, uploadIndex=True)
