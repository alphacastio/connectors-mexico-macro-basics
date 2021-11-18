#!/usr/bin/env python
# coding: utf-8

# In[45]:


import pandas as pd

import numpy as np
from alphacast import Alphacast
from dotenv import dotenv_values
API_KEY = dotenv_values(".env").get("API_KEY")
alphacast = Alphacast(API_KEY)



# In[46]:


#codigos = ["496152","496153","496154","496155","496156","496157","496158","499227","496159","496160","496161","496162","496163","496164","496165","496150","496151"]
codigos = ["6207061409","6207061412","6207061413","6207061414","6207061417","6207061459","6207061423","6207061433","6207061425","6207061426","6207061446","6207061457","6207061472","6207061445","6207061466","6207061448","6207061461"]
string_codigos =  ",".join(codigos)
#url = "https://www.inegi.org.mx/app/indicadores/exportacion.aspx?cveser=,{},&bie=true&ordena=a&ordenaPeriodo=ap&orientacion=v&frecuencia=Todo&estadistico=false&esquema=&bdesplaza=False&FileFormat=xls&subapp=&tematica=0".format(string_codigos)
url = "https://www.inegi.org.mx/app/indicadores/exportacion.aspx?cveser=,{},&ag=0700&bie=false&aamin=1900&aamax=9999&ordena=a&ordenaPeriodo=ap&orientacion=v&frecuencia=Todo&estadistico=false&esquema=&bdesplaza=False&FileFormat=xls&tc=indicadorVertical&subapp=&tematica=6&name=Estados%20Unidos%20Mexicanos".format(string_codigos)

df = pd.read_excel(url, skiprows=4, header=[0])
df = df.replace("2021/01 /r1", "2021/01")
df = df.replace("2021/06 /r1", "2021/06")
df["Date"] = pd.to_datetime(df["Periodos"], errors= "coerce", format= "%Y/%m")
del df["Periodos"]
df.dropna(axis=0, how='all',inplace=True)


# In[47]:


#df.columns.to_frame().to_csv("inegi.csv", encoding="latin-1")
df.columns = df.columns.str.replace(" /f1  ", " ").     str.replace("Banco de Indicadores > PIB y Cuentas Nacionales > Indicadores macroeconómicos nacionales > Indicador Global de la Actividad Económica > IGAE. Índice de volumen físico base 2013=100.", "").     str.replace("(Índice de volumen físico base 2013=100)", "").     str.replace("Mensual", "").     str.replace("\(\)", "").     str.replace(" /a /f1", "").     str.replace(" /f1", "").     str.strip()

df = df.rename(columns={'Total':'IGAE'})
df = df.set_index("Date")


# In[48]:


#IGAE desest
url2 = "https://www.inegi.org.mx/app/indicadores/exportacion.aspx?cveser=496216&ag=0700&bie=false&aamin=1900&aamax=9999&ordena=a&ordenaPeriodo=ap&orientacion=v&frecuencia=Todo&estadistico=false&esquema=&bdesplaza=False&FileFormat=xls&tc=indicadorVertical&subapp=&tematica=6&name=Estados%20Unidos%20Mexicanos"
df1 = pd.read_excel(url2, skiprows=4, header=[0])
df1["Date"] = pd.to_datetime(df1["Periodos"], errors= "coerce", format= "%Y/%m")
del df1["Periodos"]
df1.dropna(axis=0, how='all',inplace=True)

df1.columns = df1.columns.str.replace(" /f1  ", " ").     str.replace("Indicadores económicos de coyuntura > Indicador global de la actividad económica, base 2013 > Series desestacionalizadas y tendencia-ciclo > Total > Serie desestacionalizada >", "").     str.replace("(Índice base 2013=100)", "").     str.replace("Mensual", "").     str.replace("\(\)", "").     str.replace(" /f1", "").     str.strip()



df1 = df1.rename(columns={'Índice':'IGAE - sa_orig'})

df1 = df1.set_index("Date")


dfFinal = df.merge(df1, how='right', left_index=True, right_index=True)
dfFinal["country"] = "Mexico"

alphacast.datasets.dataset(42).upload_data_from_df(dfFinal, 
    deleteMissingFromDB = True, onConflictUpdateDB = True, uploadIndex=True)
