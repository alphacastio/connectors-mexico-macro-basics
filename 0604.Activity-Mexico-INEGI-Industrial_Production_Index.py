#!/usr/bin/env python
# coding: utf-8

# In[5]:


import pandas as pd

from alphacast import Alphacast
from dotenv import dotenv_values
API_KEY = dotenv_values(".env").get("API_KEY")
alphacast = Alphacast(API_KEY)



# In[6]:


codigos = ['496326','496327','496331','496334','496338','496462','496464','496467','496471','496499','496520','496548']
string_codigos =  ",".join(codigos)
url = "https://www.inegi.org.mx/app/indicadores/exportacion.aspx?cveser=,{},&ag=0700&bie=false&aamin=1900&aamax=9999&ordena=a&ordenaPeriodo=ap&orientacion=v&frecuencia=Todo&estadistico=false&esquema=&bdesplaza=False&FileFormat=xls&tc=indicadorVertical&subapp=&tematica=6&name=Estados%20Unidos%20Mexicanos".format(string_codigos)

df = pd.read_excel(url, skiprows=4, header=[0])
df["Date"] = pd.to_datetime(df["Periodos"], errors= "coerce", format= "%Y/%m")
del df["Periodos"]
df.dropna(axis=0, how='all',inplace=True)


# In[7]:


df.columns = df.columns.str.replace(" /p1 /f1 ", " ").     str.replace(" /f2 ", " ").     str.replace("Indicadores económicos de coyuntura > Actividad industrial, base 2013 > Series Originales > Índice de volumen físico > ", "").     str.replace("Indicadores económicos de coyuntura > Actividad industrial, base 2013 > Series desestacionalizadas y tendencia-ciclo > ", "").     str.replace("21", "").     str.replace("22", "").     str.replace("23", "").     str.replace("31-33", "").     str.strip()

df = df.set_index("Date")
df['country'] = 'Mexico'

alphacast.datasets.dataset(604).upload_data_from_df(df, 
    deleteMissingFromDB = True, onConflictUpdateDB = True, uploadIndex=True)

