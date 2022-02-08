#!/usr/bin/env python
# coding: utf-8

# In[1]:


import pandas as pd


# In[2]:


dir_archivo = "/home/tonatiuh/Documents/Desarrollo/ZophiaLearning/datasets/"


# ### Tabla products

# In[3]:


nombre_archivo = "amazon/products.csv"
df = pd.read_csv(dir_archivo+nombre_archivo, delimiter='|')
nombre_destino = "stage/products.parquet"
df.to_parquet(nombre_destino)
df.head(3)


# ### Tabla tasas_cambio_pais_anual

# In[4]:


nombre_archivo = "amazon/tasas_cambio_pais_anual.csv"
df = pd.read_csv(dir_archivo+nombre_archivo, delimiter=',')
nombre_destino = "stage/tasas_cambio_pais_anual.parquet"
df.to_parquet(nombre_destino)
df.head(3)

