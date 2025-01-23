
# coding: utf-8

# --- Written by Partha Sarathi Misra (mpartha9)

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


from pyspark.sql.types import *

from pyspark_llap.sql import HiveWarehouseBuilder
from pyspark_llap.sql.session import CreateTableBuilder, HiveWarehouseSessionImpl
hive = HiveWarehouseBuilder.session(spark).build()



from pyspark.conf import SparkConf
from pyspark.sql import SparkSession
spark.sparkContext._conf.getAll()



hive_context = HiveContext(spark)
hive_context.setConf("hive.exec.dynamic.partition", "true")
hive_context.setConf("hive.execution.engine","spark")
hive_context.setConf("hive.prewarm.enabled","true")
hive_context.setConf("hive.vectorized.execution.enabled","true")
hive_context.setConf("hive.exec.dynamic.partition.mode", "nonstrict")


# ## Importing Packages for DataRobot Scoring


from py4j.java_gateway import java_import
java_import(spark._jvm, 'com.datarobot.prediction.Predictors')
java_import(spark._jvm, 'com.datarobot.prediction.spark.CodegenModel')
java_import(spark._jvm, 'com.datarobot.prediction.spark.Predictors')



codeGenModel = spark._jvm.com.datarobot.prediction.spark.Predictors.getPredictor('64119c37f60675e3f6972a3d')


# ## VOMART Data Pull for FORD CANADA


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
contribution_margin
from dsc60180_ici_sde_tz_db.vomart
where consumer_id is not NULL
and vin is not NULL
and country_code  = 'CAN' 
and vehicle_make = 'FORD'
and vehicle_model is NOT NULL
"""
mpartha9_CAN_data_1_external_p1=hive.executeQuery(Query1)



mpartha9_CAN_data_1_external_p1.createOrReplaceTempView("mpartha9_CAN_data_1_external_p1")



# ### All NON-ZERO data records


#All NON-ZERO data records
Query2="""select vin, 
consumer_id,
vehicle_ownership_cycle_num,
trim(vehicle_model) as vehicle_model, 
int(vehicle_model_year) as vehicle_model_year, 
acquisition_date, 
acquisition_year,
vehicle_age, 
fmcc_lease_purchase_ind, 
trim(platform) as platform,
trim(lifestage_value) as lifestage_value,
cast(contribution_margin as decimal(18,2)) as contribution_margin
from mpartha9_CAN_data_1_external_p1
where contribution_margin != 0
"""
mpartha9_CAN_combo_all_nonzero_records_p2=spark.sql(Query2)
mpartha9_CAN_combo_all_nonzero_records_p2.createOrReplaceTempView("mpartha9_CAN_combo_all_nonzero_records_p2")


# ### median of non zero CM values


import pyspark.sql.functions as F


# getting median of contribution_margin at model+model_year level
mpartha9_CAN_nonzero_median_p3=mpartha9_CAN_combo_all_nonzero_records_p2.groupBy(['vehicle_model', 'acquisition_year']).agg(F.expr('percentile_approx(contribution_margin, 0.5)').alias("contribution_margin"))


mpartha9_CAN_nonzero_median_p3=mpartha9_CAN_nonzero_median_p3.orderBy(['vehicle_model','acquisition_year'],ascending=True)


# ### Only zero CM  records

#only zero CM  records
Query3="""select vin, 
consumer_id,
vehicle_ownership_cycle_num,
trim(vehicle_model) as vehicle_model, 
int(vehicle_model_year) as vehicle_model_year, 
acquisition_date, 
acquisition_year,
vehicle_age, 
fmcc_lease_purchase_ind, 
trim(platform) as platform,
trim(lifestage_value) as lifestage_value,
cast(contribution_margin as decimal(18,2)) as contribution_margin_0
from mpartha9_CAN_data_1_external_p1
where contribution_margin = 0
"""
mpartha9_CAN_combo_all_zero_records_p3=spark.sql(Query3)
mpartha9_CAN_combo_all_zero_records_p3.createOrReplaceTempView("mpartha9_CAN_combo_all_zero_records_p3")


# ### Replace all zero CM records with median imputation - acc to vehicle model and model year
df_join = mpartha9_CAN_combo_all_zero_records_p3.join(mpartha9_CAN_nonzero_median_p3, on=['vehicle_model', 'acquisition_year'],how='inner') 


# dropping unneccessary columns 
df_clean = df_join.drop('contribution_margin_0')


df_clean.createOrReplaceTempView("mpartha9_CAN_df_clean")


df_clean_rearrange="""select vin, consumer_id, vehicle_ownership_cycle_num, vehicle_model, vehicle_model_year, acquisition_date, acquisition_year,
vehicle_age, fmcc_lease_purchase_ind, platform, lifestage_value, contribution_margin
from mpartha9_CAN_df_clean
"""
df_clean_final=spark.sql(df_clean_rearrange)


# ### APPEND NONZERO AND IMPUTED ZERO CM

## APPEND NONZERO AND IMPUTED ZERO CM
mpartha9_CAN_df_append = mpartha9_CAN_combo_all_nonzero_records_p2.union(df_clean_final)
mpartha9_CAN_df_append.createOrReplaceTempView("mpartha9_CAN_df_append")


# ### Vehicle Age


Query_vehicle_age = """select consumer_id, avg(vehicle_age) as avg_vehicle_age
from mpartha9_CAN_df_append
group by consumer_id
order by consumer_id
"""
mpartha9_CAN_veh_age = spark.sql(Query_vehicle_age)
mpartha9_CAN_veh_age.createOrReplaceTempView("mpartha9_CAN_veh_age")


# ### fmcc_lease_purchase_ind


Query_fmcc_lease_purchase_ind = """select consumer_id, sum(fmcc_lease_purchase_ind) as no_of_lease_purchases
from mpartha9_CAN_df_append
group by consumer_id
order by consumer_id
"""
mpartha9_CAN_fmcc_lease_purchase_ind = spark.sql(Query_fmcc_lease_purchase_ind)
mpartha9_CAN_fmcc_lease_purchase_ind.createOrReplaceTempView("mpartha9_CAN_fmcc_lease_purchase_ind")

# ### Frequency


Query_freq = """select consumer_id, Count(acquisition_date) as freq_excluding_latest
from mpartha9_CAN_df_append
group by consumer_id
order by consumer_id
"""
mpartha9_freq = spark.sql(Query_freq)
mpartha9_freq.createOrReplaceTempView("mpartha9_freq")


# ### avg_contribution_margin_new


Query_monetary_CM = """select consumer_id, avg(contribution_margin) as avg_contribution_margin_new
from mpartha9_CAN_df_append
group by consumer_id
order by consumer_id
"""
mpartha9_monetary_CM = spark.sql(Query_monetary_CM)
mpartha9_monetary_CM.createOrReplaceTempView("mpartha9_monetary_CM")


# ### AVERAGE TIME TO PURCHASE (FORMERLY  RECENCY)


Query_Recency = """Select consumer_id, acquisition_date,
-((DateDiff(acquisition_date, Lag(acquisition_date) Over (Partition by consumer_id Order By acquisition_date desc)))/365) as moving_diff
FROM mpartha9_CAN_df_append
ORDER BY consumer_id
"""
mpartha9_Recency = spark.sql(Query_Recency)
mpartha9_Recency.createOrReplaceTempView("mpartha9_Recency")


Query_Recency_1 = """select consumer_id, avg(moving_diff) as AVERAGE_TIME_TO_PURCHASE
from mpartha9_Recency
group by consumer_id
"""
mpartha9_Recency_1 = spark.sql(Query_Recency_1)
mpartha9_Recency_1.createOrReplaceTempView("mpartha9_Recency_1")


Query_Recency_2 = """select consumer_id,
case when AVERAGE_TIME_TO_PURCHASE is NULL then 'Purchased_only_once'
	when (AVERAGE_TIME_TO_PURCHASE >=0 and AVERAGE_TIME_TO_PURCHASE<=3) then 'Repurchased_WITHIN_3_yrs'
	when (AVERAGE_TIME_TO_PURCHASE >3 and AVERAGE_TIME_TO_PURCHASE<=5) then 'Repurchased_BTWN_3_5_yrs'
	when AVERAGE_TIME_TO_PURCHASE >5 then 'Repurchased_AFTER_5_yrs'
	end as AVERAGE_TIME_TO_PURCHASE
from mpartha9_Recency_1
"""
mpartha9_Recency_2 = spark.sql(Query_Recency_2)
mpartha9_Recency_2.createOrReplaceTempView("mpartha9_Recency_2")


# ### COMBINED TABLE for all feature variables

Query_combined_table = """select a.consumer_id, a.freq_excluding_latest, 
b.avg_contribution_margin_new, 
c.AVERAGE_TIME_TO_PURCHASE, 
e.avg_vehicle_age, 
g.no_of_lease_purchases
from mpartha9_freq a
inner join mpartha9_monetary_CM b
on a.consumer_id=b.consumer_id
inner join mpartha9_Recency_2 c
on a.consumer_id=c.consumer_id
inner join mpartha9_CAN_veh_age e
on a.consumer_id=e.consumer_id
inner join mpartha9_CAN_fmcc_lease_purchase_ind g
on a.consumer_id=g.consumer_id
order by consumer_id
"""
mpartha9_CAN_combined_table = spark.sql(Query_combined_table)
mpartha9_CAN_combined_table.createOrReplaceTempView("mpartha9_CAN_combined_table")


# ### Final X Variables  final formattings

Query_RFM_new_non_NaN = """select consumer_id, 
case when freq_excluding_latest=1 then 'One'
	when freq_excluding_latest=2 then 'Two'
	when freq_excluding_latest=3 then 'Three'
	else 'More_than_Three' end as Freq_purchase,
avg_contribution_margin_new as avg_imputed_contribution_margin,
AVERAGE_TIME_TO_PURCHASE,
avg_vehicle_age,
case when no_of_lease_purchases=0 then 'Zero' else 'One' end as lease_purchase_flag
from mpartha9_CAN_combined_table
"""
mpartha9_CAN_RFM_new_non_NaN = spark.sql(Query_RFM_new_non_NaN)
mpartha9_CAN_RFM_new_non_NaN.createOrReplaceTempView("mpartha9_CAN_RFM_new_non_NaN")


# ### Formatting as in DataRobot

Final_data = """select consumer_id, 
Freq_purchase, 
float(avg_imputed_contribution_margin) as avg_imputed_contribution_margin,
AVERAGE_TIME_TO_PURCHASE,
avg_vehicle_age,
lease_purchase_flag
from mpartha9_CAN_RFM_new_non_NaN
order by consumer_id
"""
mpartha9_CAN_Scoring_Margin_model = spark.sql(Final_data)
mpartha9_CAN_Scoring_Margin_model.createOrReplaceTempView("mpartha9_CAN_Scoring_Margin_model")


# --- Completed by Partha Sarathi Misra (mpartha9)
