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


# ### Tabla productos

# In[3]:


dir_archivo = "/home/tonatiuh/Documents/Desarrollo/ZophiaLearning/ejercicios/"
nombre_archivo = "amazon/stage/products.parquet"


# In[4]:


df = spark.read.format("parquet")        .load(dir_archivo+nombre_archivo)
df = df.select([
    'isbestseller',
    'product_id',
    'app_sale_price',
    'app_sale_price_currency',
    'isprime',
    'evaluate_rate',
    'country'])


# In[5]:


df.printSchema()


# In[6]:


df = df.withColumn('app_sale_price', F.col('app_sale_price').cast(FloatType()))


# In[7]:


df.show(n=5)
#df.show(n=5, vertical=True, truncate=False)


# ### Tabla tasas de cambio

# In[8]:


nombre_archivo = "amazon/stage/tasas_cambio_pais_anual.parquet"
df_tc = spark.read.format("parquet")                    .load(dir_archivo+nombre_archivo)


# In[9]:


df_tc.show(3)


# In[10]:


df_tc_max_year = df_tc.groupby('Alpha-2-code').agg(F.max('Year').alias('Year'))
df_tc_max = df_tc.join(df_tc_max_year, how='inner', on=['Alpha-2-code', 'Year'])
df_tc_max = df_tc_max.select([
    'Alpha-2-code', 
    'Value'])
df_tc_max = df_tc_max.withColumnRenamed('Alpha-2-code', 'country')
df_tc_max.cache()


# In[11]:


df_tc_max.show(3)


# In[12]:


#df_tc_max.groupby('Alpha-2-code').agg(F.count('Value').alias('conteo')).sort('conteo', ascending=False).show()


# ### Cruce de tablas

# In[13]:


df_p_tc = df.join(df_tc_max, how='left', on=['country'])
df_p_tc = df_p_tc.withColumn('app_sale_price_us',
                             F.col('app_sale_price')*F.col('Value'))
df_p_tc = df_p_tc.select([
    'isbestseller',
    'product_id',
    'app_sale_price',
    'app_sale_price_currency',
    'isprime',
    'evaluate_rate',
    'country',
    'app_sale_price_us'])
df_p_tc.cache()


# In[14]:


df_p_tc.filter(F.col('app_sale_price').isNotNull()).show(3)


# ### Almacenamiento

# In[15]:


nombre_destino = "amazon/curated/products_standard_price.parquet"


# In[16]:


df_p_tc.write.mode('overwrite').parquet(dir_archivo+nombre_destino)


# #### Adicional

# In[17]:


df_filtrado = df_p_tc.filter(F.col('app_sale_price').isNotNull()).limit(10)


# In[18]:


df_filtrado.show(3)


# In[19]:


df_pandas = df_filtrado.toPandas()
nombre_csv = "output/products_standard_price_sample.csv"
df_pandas.to_csv(nombre_csv, index=False)


# ### Unpersist

# In[20]:


df_tc_max.unpersist()
df_p_tc.unpersist()

