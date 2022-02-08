#!/usr/bin/env python
# coding: utf-8

# In[1]:


from pyspark.sql import SparkSession
from pyspark.context import SparkContext
import pyspark.sql.functions as F
from pyspark.sql.types import *


# In[2]:


sc = SparkContext.getOrCreate()
spark = SparkSession(sc)


# ### Tabla products_standard_price

# In[3]:


dir_archivo = "/home/tonatiuh/Documents/Desarrollo/ZophiaLearning/ejercicios/"
nombre_archivo = "amazon/curated/products_standard_price.parquet"


# In[4]:


df = spark.read.format("parquet")        .load(dir_archivo+nombre_archivo)


# In[5]:


df = df.select([
    'product_id',
    'country',
    'app_sale_price_us'])


# In[6]:


df.printSchema()


# In[7]:


df.show(n=5)
#df.show(n=5, vertical=True, truncate=False)


# ### Tabla agrupación product_id

# In[8]:


df_id = df.groupby('product_id')    .agg(F.count('country').alias('country_count'),
         F.mean('app_sale_price_us').alias('avg_price_us'))
df_id = df_id.withColumn('country_count', F.col('country_count').cast(IntegerType()))
df_id.cache()


# In[9]:


df_id.show(3)


# ### Almacenamiento

# In[10]:


nombre_destino = "amazon/curated/products_avg_price.parquet"


# In[11]:


df_id.write.mode('overwrite').parquet(dir_archivo+nombre_destino)


# #### Adicional

# In[12]:


df_filtrado = df_id.limit(10)
df_filtrado.show(3)


# In[13]:


df_pandas = df_filtrado.toPandas()
nombre_csv = "output/products_avg_price.csv"
df_pandas.to_csv(nombre_csv, index=False)


# ### Unpersist

# In[14]:


df_id.unpersist()

