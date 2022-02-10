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
    'evaluate_rate',
    'country'])


# In[6]:


df.printSchema()


# In[7]:


df.show(n=5)
#df.show(n=5, vertical=True, truncate=False)


# ### Columna evaluation rate

# In[8]:


df = df.withColumn('evaluation_rate', F.split(F.col('evaluate_rate'), '[^0-9.,]').getItem(0))
df = df.withColumn('evaluation_rate', F.regexp_replace(F.col('evaluation_rate'), ',', '.'))
df = df.withColumn('evaluation_rate', F.col('evaluation_rate').cast(FloatType()))


# In[9]:


df.show(3)


# In[10]:


#df.filter(F.col('evaluation_rate').isNull()).filter(F.col('evaluate_rate').isNotNull()).show(5)


# ### Tabla avg rate

# In[11]:


df_rate = df.groupby('product_id')    .agg(F.mean('evaluation_rate').alias('avg_evaluation_rate'),
         F.count('country').alias('country_count'))
df_rate = df_rate.withColumn('country_count', F.col('country_count').cast(IntegerType()))
df_rate.cache()


# In[12]:


df_rate.show(3)


# ### Almacenamiento

# In[13]:


nombre_destino = "amazon/curated/product_rate_avg.parquet"


# In[14]:


df_rate.write.mode('overwrite').parquet(dir_archivo+nombre_destino)


# #### Adicional

# In[15]:


df_filtrado = df_rate.limit(10)
df_filtrado.show(3)


# In[16]:


df_pandas = df_filtrado.toPandas()
nombre_csv = "output/product_rate_avg.csv"
df_pandas.to_csv(nombre_csv, index=False)


# ### Unpersist

# In[17]:


df_rate.unpersist()

