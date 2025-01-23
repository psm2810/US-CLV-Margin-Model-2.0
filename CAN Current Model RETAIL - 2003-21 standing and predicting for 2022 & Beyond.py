
# coding: utf-8

# In[1]:


import numpy as np
import csv
import decimal
import math
import os
import re
import time
import random
from pyspark.sql.functions import udf
from pyspark.sql.functions import isnan, when, count, col, to_date, row_number
import pyspark.sql.functions
from pyspark.sql.window import Window
from pyspark.sql.types import DoubleType
from pyspark.sql.types import StringType
from pyspark.sql.types import IntegerType
from pyspark.sql.types import LongType
from pyspark.sql.types import DateType
from pyspark.sql import HiveContext
from pyspark.storagelevel import StorageLevel


# In[2]:


from pyspark.sql.types import *

from pyspark_llap.sql import HiveWarehouseBuilder
from pyspark_llap.sql.session import CreateTableBuilder, HiveWarehouseSessionImpl
hive = HiveWarehouseBuilder.session(spark).build()


# In[3]:


from pyspark.conf import SparkConf
from pyspark.sql import SparkSession
spark.sparkContext._conf.getAll()


# In[4]:


hive_context = HiveContext(spark)
hive_context.setConf("hive.exec.dynamic.partition", "true")
hive_context.setConf("hive.execution.engine","spark")
hive_context.setConf("hive.prewarm.enabled","true")
hive_context.setConf("hive.vectorized.execution.enabled","true")
hive_context.setConf("hive.exec.dynamic.partition.mode", "nonstrict")


# ## VOMART Data Pull between 2003 to 2021 for FORD CAN

# In[5]:


Query1="""select vin, 
consumer_id, 
vehicle_ownership_cycle_num,
vehicle_model, 
vehicle_model_year, 
acquisition_date, 
acquisition_year,
vehicle_age, 
fmcc_lease_purchase_ind, 
platform, 
lifestage_value,
consumer_type_code,
contribution_margin
from dsc60180_ici_sde_tz_db.vomart_history
where to_date(last_update_timestamp) = '2022-01-02'
and acquisition_date >= '2003-01-01'
and acquisition_date <= '2021-12-31'
and country_code  = 'CAN' 
and vehicle_make = 'FORD'
and consumer_id is not NULL
and vin is not NULL
"""
mpartha9_CAN_data_1_external_p1=hive.executeQuery(Query1)


# In[6]:


#Save the original extract as parquet file
#mpartha9_CAN_data_1_external_p1.write.parquet("mpartha9_CAN_data_1_external_p1.parquet")


# In[7]:


#Read the parquet file
#mpartha9_CAN_data_1_external_p1=spark.read.parquet('mpartha9_CAN_data_1_external_p1.parquet')


# In[8]:


mpartha9_CAN_data_1_external_p1.createOrReplaceTempView("mpartha9_CAN_data_1_external_p1")


# In[9]:


print(mpartha9_CAN_data_1_external_p1.count(), mpartha9_CAN_data_1_external_p1.select("consumer_id").distinct().count())


# In[10]:


mpartha9_CAN_data_1_external_p1.printSchema()

Query_retail_only="""select *
from mpartha9_CAN_data_1_external_p1
where consumer_type_code='I'
"""
mpartha9_CAN_data_1_external_p1_retail=spark.sql(Query_retail_only)
mpartha9_CAN_data_1_external_p1_retail.createOrReplaceTempView("mpartha9_CAN_data_1_external_p1_retail")print(mpartha9_CAN_data_1_external_p1_retail.count(), mpartha9_CAN_data_1_external_p1_retail.select("consumer_id").distinct().count())
#4413558 3239521
# ### CURRENT MODEL :  All NON-ZERO CM

# In[11]:


#All NON-ZERO data records
Query2="""select consumer_id, avg(contribution_margin) as avg_cm 
from mpartha9_CAN_data_1_external_p1
where contribution_margin !=0
and contribution_margin is NOT NULL
and consumer_type_code='I'
group by consumer_id
"""
mpartha9_CAN_combo_all_nonzero_records_p2=spark.sql(Query2)
mpartha9_CAN_combo_all_nonzero_records_p2.createOrReplaceTempView("mpartha9_CAN_combo_all_nonzero_records_p2")


# In[12]:


print(mpartha9_CAN_combo_all_nonzero_records_p2.count(), mpartha9_CAN_combo_all_nonzero_records_p2.select("consumer_id").distinct().count())


# In[13]:


mpartha9_CAN_combo_all_nonzero_records_p2.show(2)


# ### CURRENT MODEL :  Only zero CM

# In[14]:


#only zero CM  records
Query3="""select avg(case when acquisition_date>='2021-01-01' and acquisition_date<='2021-12-31'
then contribution_margin else NULL end) as cm_365 
from mpartha9_CAN_data_1_external_p1
where contribution_margin !=0
and consumer_type_code='I' 
"""
mpartha9_CAN_combo_all_zero_records_p3=spark.sql(Query3)
mpartha9_CAN_combo_all_zero_records_p3.createOrReplaceTempView("mpartha9_CAN_combo_all_zero_records_p3")


# In[15]:


print(mpartha9_CAN_combo_all_zero_records_p3.count())


# In[16]:


mpartha9_CAN_combo_all_zero_records_p3.show(2)


# ### Distinct Customers

# In[17]:


Query4="""select distinct consumer_id
from mpartha9_CAN_data_1_external_p1
where consumer_type_code='I'
"""
mpartha9_CAN_distinct_records_p4=spark.sql(Query4)
mpartha9_CAN_distinct_records_p4.createOrReplaceTempView("mpartha9_CAN_distinct_records_p4")


# In[18]:


mpartha9_CAN_distinct_records_p4.show(2)


# In[19]:


mpartha9_CAN_distinct_records_p4.count()


# ### Join both tables

# In[20]:


Query5="""select a.consumer_id, b.avg_cm
from mpartha9_CAN_distinct_records_p4 a
left join mpartha9_CAN_combo_all_nonzero_records_p2 b
on a.consumer_id = b.consumer_id
"""
mpartha9_CAN_vomart_2003_21_with_avg_cm=spark.sql(Query5)
mpartha9_CAN_vomart_2003_21_with_avg_cm.createOrReplaceTempView("mpartha9_CAN_vomart_2003_21_with_avg_cm")


# In[21]:


print(mpartha9_CAN_vomart_2003_21_with_avg_cm.count(), mpartha9_CAN_vomart_2003_21_with_avg_cm.select("consumer_id").distinct().count())


# In[22]:


mpartha9_CAN_vomart_2003_21_with_avg_cm.show(2)


# In[23]:


mpartha9_CAN_vomart_2003_21_with_avg_cm.where(col('avg_cm').isNull()).count()


# In[24]:


mpartha9_CAN_vomart_2003_21_with_avg_cm.where(col('avg_cm')==0).count()


# ### Impute fixed single value for NULL CMs

# In[25]:


Query6="""select consumer_id,
case when avg_cm is NULL then 10708.813996 else avg_cm end as avg_cm_imputed
from mpartha9_CAN_vomart_2003_21_with_avg_cm
"""
mpartha9_CAN_vomart_2003_21_with_avg_cm_imputed=spark.sql(Query6)
mpartha9_CAN_vomart_2003_21_with_avg_cm_imputed.createOrReplaceTempView("mpartha9_CAN_vomart_2003_21_with_avg_cm_imputed")


# In[26]:


print(mpartha9_CAN_vomart_2003_21_with_avg_cm_imputed.count(), mpartha9_CAN_vomart_2003_21_with_avg_cm_imputed.select("consumer_id").distinct().count())


# In[27]:


mpartha9_CAN_vomart_2003_21_with_avg_cm_imputed.show(2)


# In[28]:


mpartha9_CAN_vomart_2003_21_with_avg_cm_imputed.where(col('avg_cm_imputed').isNull()).count()


# In[29]:


mpartha9_CAN_vomart_2003_21_with_avg_cm_imputed.where(col('avg_cm_imputed')==0).count()


# ### Customers who bought a new vehicle in 2022 or later from Ford CAN

# In[30]:


Query_2022_beyond="""select vin, 
consumer_id, 
vehicle_ownership_cycle_num,
vehicle_model, 
vehicle_model_year, 
acquisition_date, 
acquisition_year,
vehicle_age, 
fmcc_lease_purchase_ind, 
platform, 
lifestage_value,
consumer_type_code,
contribution_margin
from dsc60180_ici_sde_tz_db.vomart
where acquisition_date >= '2022-01-01'
and country_code  = 'CAN' 
and vehicle_make = 'FORD'
and consumer_id is not NULL
and vin is not NULL
and contribution_margin is NOT NULL
and contribution_margin != 0
"""
mpartha9_vomart_CAN_2022_beyond=hive.executeQuery(Query_2022_beyond)


# In[31]:


#Save the original extract as parquet file
#mpartha9_vomart_2022_beyond.write.parquet("mpartha9_vomart_2022_beyond.parquet")


# In[32]:


#Read the parquet file
#mpartha9_vomart_2022_beyond=spark.read.parquet('mpartha9_vomart_2022_beyond.parquet')


# In[33]:


mpartha9_vomart_CAN_2022_beyond.createOrReplaceTempView("mpartha9_vomart_CAN_2022_beyond")


# In[34]:


print(mpartha9_vomart_CAN_2022_beyond.count(), mpartha9_vomart_CAN_2022_beyond.select("consumer_id").distinct().count())


# ### Only Individual customers

# In[35]:


Query_retail="""select *
from mpartha9_vomart_CAN_2022_beyond
where consumer_type_code='I'
"""
mpartha9_vomart_CAN_2022_beyond_retail=spark.sql(Query_retail)
mpartha9_vomart_CAN_2022_beyond_retail.createOrReplaceTempView("mpartha9_vomart_CAN_2022_beyond_retail")


# In[36]:


print(mpartha9_vomart_CAN_2022_beyond_retail.count(), mpartha9_vomart_CAN_2022_beyond_retail.select("consumer_id").distinct().count())


# ### Row Numbering based on Acquisition date

# In[37]:


Query_row_numbering = """select *
from 
(select *,
ROW_NUMBER() OVER (PARTITION BY consumer_id ORDER BY acquisition_date) as ROW_numbering
from mpartha9_vomart_CAN_2022_beyond_retail
) a
"""
mpartha9_CAN_row_numbering = spark.sql(Query_row_numbering)
mpartha9_CAN_row_numbering.createOrReplaceTempView("mpartha9_CAN_row_numbering")


# In[38]:


print(mpartha9_CAN_row_numbering.count(), mpartha9_CAN_row_numbering.select("consumer_id").distinct().count())


# In[39]:


mpartha9_CAN_row_numbering.show(2)


# ### Only First Transaction in 2022 and beyond

# In[40]:


Query_only_latest_transaction = """select consumer_id, acquisition_date, 
contribution_margin as Latest_CM
from mpartha9_CAN_row_numbering
where ROW_numbering = 1
"""
mpartha9_CAN_only_latest_transaction = spark.sql(Query_only_latest_transaction)
mpartha9_CAN_only_latest_transaction.createOrReplaceTempView("mpartha9_CAN_only_latest_transaction")


# In[41]:


print(mpartha9_CAN_only_latest_transaction.count(), mpartha9_CAN_only_latest_transaction.select("consumer_id").distinct().count())


# In[42]:


mpartha9_CAN_only_latest_transaction.show(5)


# ### Match back beyond_2022 with consumer_ids before 2022

# In[43]:


Match_query = """
select a.consumer_id, a.avg_cm_imputed, b.Latest_CM
from mpartha9_CAN_vomart_2003_21_with_avg_cm_imputed a
inner join mpartha9_CAN_only_latest_transaction b
on a.consumer_id = b.consumer_id
"""
mpartha9_CAN_Match_query = spark.sql(Match_query)
mpartha9_CAN_Match_query.createOrReplaceTempView("mpartha9_CAN_Match_query")


# In[44]:


print(mpartha9_CAN_Match_query.count(), mpartha9_CAN_Match_query.select("consumer_id").distinct().count())


# In[45]:


mpartha9_CAN_Match_query.show(5)


# In[46]:


mpartha9_CAN_Match_query.toPandas().to_csv('/s/mpartha9/CAN_RETAIL_current_model_performance_2022_beyond.csv', index=False)


# ### Read CSV in pyspark

# In[47]:


df = sqlContext.read.csv("file:///s/mpartha9/CAN_RETAIL_NEW_developed_model_performance_2022_beyond.csv", header=True)


# In[48]:


print(df.count(), df.select("consumer_id").distinct().count())


# In[49]:


df.createOrReplaceTempView("New_model_consumers")


# In[50]:


Match = """
select a.*
from mpartha9_CAN_Match_query a
inner join New_model_consumers b
on a.consumer_id = b.consumer_id
"""
mpartha9_match = spark.sql(Match)
mpartha9_match.createOrReplaceTempView("mpartha9_match")


# In[51]:


print(mpartha9_match.count(), mpartha9_match.select("consumer_id").distinct().count())


# In[52]:


mpartha9_match.show(5)


# In[ ]:


#------------ CHECK


# In[53]:


Check1 = """
select a.consumer_id, b.avg_cm
from mpartha9_match a
inner join mpartha9_CAN_vomart_2003_21_with_avg_cm b
on a.consumer_id = b.consumer_id
"""
mpartha9_check1 = spark.sql(Check1)
mpartha9_check1.createOrReplaceTempView("mpartha9_check1")


# In[54]:


print(mpartha9_check1.count(), mpartha9_check1.select("consumer_id").distinct().count())


# In[55]:


mpartha9_check1.show(5)


# In[56]:


mpartha9_check1.where(col('avg_cm').isNull()).count()


# In[ ]:


#Around 8.5% percent of ID’s (of the 54,854  )  who’s previous purchase had 0 or missing CM

