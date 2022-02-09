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


# ### Tabla avg, min, max

# In[8]:


df_price_max = df.groupby('product_id')    .agg(F.max('app_sale_price_us').alias('app_sale_price_us'))

df_max = df.join(df_price_max, how='inner', on=['product_id', 'app_sale_price_us'])
df_max = df_max.distinct()
df_max = df_max.withColumnRenamed('app_sale_price_us', 'max_price_us')
df_max = df_max.withColumnRenamed('country', 'max_country')
df_max.cache()


# In[9]:


df_max.show(3)


# In[10]:


df_price_min = df.groupby('product_id')    .agg(F.min('app_sale_price_us').alias('app_sale_price_us'))

df_min = df.join(df_price_min, how='inner', on=['product_id', 'app_sale_price_us'])
df_min = df_min.distinct()
df_min = df_min.withColumnRenamed('app_sale_price_us', 'min_price_us')
df_min = df_min.withColumnRenamed('country', 'min_country')
df_min.cache()


# In[11]:


df_min.show(3)


# In[12]:


df_id = df.groupby('product_id')    .agg(F.count('country').alias('country_count'),
         F.mean('app_sale_price_us').alias('avg_price_us'))
df_id = df_id.withColumn('country_count', F.col('country_count').cast(IntegerType()))
df_id.cache()


# In[13]:


df_id.show(3)


# ### Cruce de tablas

# In[14]:


df_id_max = df_id.join(df_max, how='left', on=['product_id'])
df_agg = df_id_max.join(df_min, how='left', on=['product_id'])
df_agg.cache()


# In[15]:


df_agg.show(3)


# ### Almacenamiento

# In[16]:


nombre_destino = "amazon/curated/products_price_ranges.parquet"


# In[17]:


df_id.write.mode('overwrite').parquet(dir_archivo+nombre_destino)


# #### Adicional

# In[18]:


df_filtrado = df_agg.limit(10)
df_filtrado.show(3)


# In[19]:


df_pandas = df_filtrado.toPandas()
nombre_csv = "output/products_price_ranges.csv"
df_pandas.to_csv(nombre_csv, index=False)


# ### Unpersist

# In[20]:


df_max.unpersist()
df_min.unpersist()
df_id.unpersist()
df_agg.unpersist()

