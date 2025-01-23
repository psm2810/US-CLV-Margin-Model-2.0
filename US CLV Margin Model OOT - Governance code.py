
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


# ## VOMART Data Pull between 2003 to 2021 for FORD USA

#Read the parquet file
#mpartha9_NA_data_1_external_p1=spark.read.parquet('mpartha9_NA_data_1_external_p1.parquet')


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
and household_demogrphics_key is not NULL
and country_code  = 'USA' 
and acquisition_date >= '2003-01-01'
and acquisition_date <= '2021-12-31'
and vehicle_make = 'FORD'
and contribution_margin is not NULL
and vehicle_model is NOT NULL
"""
mpartha9_NA_data_1_external_p1=hive.executeQuery(Query1)
mpartha9_NA_data_1_external_p1.createOrReplaceTempView("mpartha9_NA_data_1_external_p1")

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
from mpartha9_NA_data_1_external_p1
where contribution_margin != 0
"""
mpartha9_NA_combo_all_nonzero_records_p2=spark.sql(Query2)
mpartha9_NA_combo_all_nonzero_records_p2.createOrReplaceTempView("mpartha9_NA_combo_all_nonzero_records_p2")

# ### median of non zero CM values

import pyspark.sql.functions as F

# getting median of contribution_margin at model+model_year level
mpartha9_NA_nonzero_median_p3=mpartha9_NA_combo_all_nonzero_records_p2.groupBy(['vehicle_model', 'acquisition_year']).agg(F.expr('percentile_approx(contribution_margin, 0.5)').alias("contribution_margin"))

mpartha9_NA_nonzero_median_p3=mpartha9_NA_nonzero_median_p3.orderBy(['vehicle_model','acquisition_year'],ascending=True)


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
from mpartha9_NA_data_1_external_p1
where contribution_margin = 0
"""
mpartha9_NA_combo_all_zero_records_p3=spark.sql(Query3)
mpartha9_NA_combo_all_zero_records_p3.createOrReplaceTempView("mpartha9_NA_combo_all_zero_records_p3")


# ### Replace all zero CM records with median imputation - acc to vehicle model and model year

df_join = mpartha9_NA_combo_all_zero_records_p3.join(mpartha9_NA_nonzero_median_p3, on=['vehicle_model', 'acquisition_year'],how='inner') 

# dropping unneccessary columns 
df_clean = df_join.drop('contribution_margin_0')

df_clean.createOrReplaceTempView("mpartha9_NA_df_clean")


df_clean_rearrange="""select vin, consumer_id, vehicle_ownership_cycle_num, vehicle_model, vehicle_model_year, acquisition_date, acquisition_year,
vehicle_age, fmcc_lease_purchase_ind, platform, lifestage_value, contribution_margin
from mpartha9_NA_df_clean
"""
df_clean_final=spark.sql(df_clean_rearrange)


# ### APPEND NONZERO AND IMPUTED ZERO CM

## APPEND NONZERO AND IMPUTED ZERO CM
mpartha9_NA_df_append = mpartha9_NA_combo_all_nonzero_records_p2.union(df_clean_final)
mpartha9_NA_df_append.createOrReplaceTempView("mpartha9_NA_df_append")


# ### DISTINCT CUSTOMERS
Distinct_customers = mpartha9_NA_df_append.select("consumer_id").distinct()

Distinct_customers.count()


# ## SPILT the data  into train and test

# splitting dataset into train and test set
train, test = Distinct_customers.randomSplit([0.7, 0.3], seed=42)

print(train.count(), test.count())


train.createOrReplaceTempView("train_customers")
test.createOrReplaceTempView("test_customers")


Train_df = """select a.*
from mpartha9_NA_df_append a
inner join train_customers b
on a.consumer_id=b.consumer_id
"""
df_train = spark.sql(Train_df)
df_train.createOrReplaceTempView("df_train")


print(df_train.count(), df_train.select("consumer_id").distinct().count())


Test_df = """select a.*
from mpartha9_NA_df_append a
inner join test_customers b
on a.consumer_id=b.consumer_id
"""
df_test = spark.sql(Test_df)
df_test.createOrReplaceTempView("df_test")


print(df_test.count(), df_test.select("consumer_id").distinct().count())

# ### test


Query_Test = """select *
from 
df_test
where acquisition_date > '2017-12-31'
"""
mpartha9_NA_df_test = spark.sql(Query_Test)
mpartha9_NA_df_test.createOrReplaceTempView("mpartha9_NA_df_test")


print(mpartha9_NA_df_test.count(), mpartha9_NA_df_test.select("consumer_id").distinct().count())


# # TRAIN

Query_Train = """select *
from 
df_train
where acquisition_date <= '2017-12-31'
ORDER BY acquisition_date desc
"""
mpartha9_NA_df_train = spark.sql(Query_Train)
mpartha9_NA_df_train.createOrReplaceTempView("mpartha9_NA_df_train")


print(mpartha9_NA_df_train.count(), mpartha9_NA_df_train.select("consumer_id").distinct().count())


# ## SPECIAL STEP to filter out Used cars from the Latest purchases in such a way that if they had purchased a new car before in the lifetime then that transaction would be the latest transaction

Query_special_step = """select *,
      sum(case when vehicle_ownership_cycle_num = 1 then 1 else 0 end) over (partition by consumer_id order by consumer_id, acquisition_date desc) as grp
from mpartha9_NA_df_train
"""
mpartha9_NA_special_step = spark.sql(Query_special_step)
mpartha9_NA_special_step.createOrReplaceTempView("mpartha9_NA_special_step")

print(mpartha9_NA_special_step.count(), mpartha9_NA_special_step.select("consumer_id").distinct().count())


Query_special_step_filter = """select *
from mpartha9_NA_special_step
where grp>=1
"""
mpartha9_NA_special_step_filter = spark.sql(Query_special_step_filter)
mpartha9_NA_special_step_filter.createOrReplaceTempView("mpartha9_NA_special_step_filter")


print(mpartha9_NA_special_step_filter.count(), mpartha9_NA_special_step_filter.select("consumer_id").distinct().count())


# ### Row Numbering

Query_row_numbering = """select *
from 
(select *,
ROW_NUMBER() OVER (PARTITION BY consumer_id ORDER BY acquisition_date desc) as ROW_numbering
from mpartha9_NA_special_step_filter
) a
"""
mpartha9_NA_row_numbering = spark.sql(Query_row_numbering)
mpartha9_NA_row_numbering.createOrReplaceTempView("mpartha9_NA_row_numbering")


print(mpartha9_NA_row_numbering.count(), mpartha9_NA_row_numbering.select("consumer_id").distinct().count())


# ### Table for No_of_purchases by each customer


Query_no_of_purchases = """select consumer_id, Count(acquisition_date) as no_of_purc
from mpartha9_NA_row_numbering
group by consumer_id
order by consumer_id
"""
mpartha9_NA_no_of_purchases = spark.sql(Query_no_of_purchases)
mpartha9_NA_no_of_purchases.createOrReplaceTempView("mpartha9_NA_no_of_purchases")


print(mpartha9_NA_no_of_purchases.count(), mpartha9_NA_no_of_purchases.select("consumer_id").distinct().count())


# ### Selecting No_of_purchases > 1

Query_no_of_purchases_more_than_1 = """select *
from mpartha9_NA_no_of_purchases
where no_of_purc > 1
"""
mpartha9_NA_no_of_purchases_more_than_1 = spark.sql(Query_no_of_purchases_more_than_1)
mpartha9_NA_no_of_purchases_more_than_1.createOrReplaceTempView("mpartha9_NA_no_of_purchases_more_than_1")

print(mpartha9_NA_no_of_purchases_more_than_1.count(), mpartha9_NA_no_of_purchases_more_than_1.select("consumer_id").distinct().count())

# ### Final Special step with more than single purchase transactions


Query_final_table_Special_step = """select a.*
from mpartha9_NA_row_numbering a
inner join mpartha9_NA_no_of_purchases_more_than_1 b
on a.consumer_id=b.consumer_id
"""
mpartha9_NA_final_table_Special_step = spark.sql(Query_final_table_Special_step)
mpartha9_NA_final_table_Special_step.createOrReplaceTempView("mpartha9_NA_final_table_Special_step")


print(mpartha9_NA_final_table_Special_step.count(), mpartha9_NA_final_table_Special_step.select("consumer_id").distinct().count())


# # Excluding_first_transaction : (N-1) TRANSACTION


Query_excluding_first_transaction = """select *
from mpartha9_NA_final_table_Special_step
where ROW_numbering != 1
order by consumer_id, acquisition_date desc
"""
mpartha9_NA_excluding_first_transaction = spark.sql(Query_excluding_first_transaction)
mpartha9_NA_excluding_first_transaction.createOrReplaceTempView("mpartha9_NA_excluding_first_transaction")

print(mpartha9_NA_excluding_first_transaction.count(), mpartha9_NA_excluding_first_transaction.select("consumer_id").distinct().count())


# ### Vehicle Age


Query_vehicle_age = """select consumer_id, avg(vehicle_age) as avg_vehicle_age
from mpartha9_NA_excluding_first_transaction
group by consumer_id
order by consumer_id
"""
mpartha9_NA_veh_age = spark.sql(Query_vehicle_age)
mpartha9_NA_veh_age.createOrReplaceTempView("mpartha9_NA_veh_age")

print(mpartha9_NA_veh_age.count(), mpartha9_NA_veh_age.select("consumer_id").distinct().count())


# ### fmcc_lease_purchase_ind


Query_fmcc_lease_purchase_ind = """select consumer_id, sum(fmcc_lease_purchase_ind) as no_of_lease_purchases
from mpartha9_NA_excluding_first_transaction
group by consumer_id
order by consumer_id
"""
mpartha9_NA_fmcc_lease_purchase_ind = spark.sql(Query_fmcc_lease_purchase_ind)
mpartha9_NA_fmcc_lease_purchase_ind.createOrReplaceTempView("mpartha9_NA_fmcc_lease_purchase_ind")


print(mpartha9_NA_fmcc_lease_purchase_ind.count(), mpartha9_NA_fmcc_lease_purchase_ind.select("consumer_id").distinct().count())


# ### PLATFORM SEGMENTATION

Query_platform = """select *,
case when platform = 'CAR' then 1 else 0 end as count_CAR,
case when platform = 'SUV' then 1 else 0 end as count_SUV,
case when platform = 'TRUCK' then 1 else 0 end as count_TRUCK
from mpartha9_NA_excluding_first_transaction
"""
mpartha9_platform = spark.sql(Query_platform)
mpartha9_platform.createOrReplaceTempView("mpartha9_platform")

Query_platform_1 = """select consumer_id, 
sum(count_CAR) as seg_car,
sum(count_SUV) as seg_suv,
sum(count_TRUCK) as seg_truck
from mpartha9_platform
group by consumer_id
order by consumer_id
"""
mpartha9_platform_1 = spark.sql(Query_platform_1)
mpartha9_platform_1.createOrReplaceTempView("mpartha9_platform_1")

print(mpartha9_platform_1.count(), mpartha9_platform_1.select("consumer_id").distinct().count())


# ### Frequency

Query_freq = """select consumer_id, Count(acquisition_date) as freq_excluding_latest
from mpartha9_NA_excluding_first_transaction
group by consumer_id
order by consumer_id
"""
mpartha9_freq = spark.sql(Query_freq)
mpartha9_freq.createOrReplaceTempView("mpartha9_freq")

print(mpartha9_freq.count(), mpartha9_freq.select("consumer_id").distinct().count())


# ### avg_contribution_margin_new

Query_monetary_CM = """select consumer_id, avg(contribution_margin) as avg_contribution_margin_new
from mpartha9_NA_excluding_first_transaction
group by consumer_id
order by consumer_id
"""
mpartha9_monetary_CM = spark.sql(Query_monetary_CM)
mpartha9_monetary_CM.createOrReplaceTempView("mpartha9_monetary_CM")


print(mpartha9_monetary_CM.count(), mpartha9_monetary_CM.select("consumer_id").distinct().count())

# ### AVERAGE TIME TO PURCHASE (FORMERLY  RECENCY)


Query_Recency = """Select consumer_id, acquisition_date, ROW_numbering
,-((DateDiff(acquisition_date, Lag(acquisition_date) Over (Partition by consumer_id Order By acquisition_date desc)))/365) as moving_diff
FROM mpartha9_NA_excluding_first_transaction
ORDER BY consumer_id, ROW_numbering
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


print(mpartha9_Recency_2.count(), mpartha9_Recency_2.select("consumer_id").distinct().count())

# ### lifestage_value


Query_lifestage_value = """select consumer_id, lifestage_value
from
(
select consumer_id, lifestage_value,
ROW_NUMBER() OVER (PARTITION BY consumer_id ORDER BY acquisition_date DESC) as lifestage_flag
from mpartha9_NA_excluding_first_transaction
) a
where lifestage_flag =1
"""
mpartha9_lifestage_value = spark.sql(Query_lifestage_value)
mpartha9_lifestage_value.createOrReplaceTempView("mpartha9_lifestage_value")


print(mpartha9_lifestage_value.count(), mpartha9_lifestage_value.select("consumer_id").distinct().count())


# ### COMBINED TABLE for all feature variables

Query_combined_table = """select a.consumer_id, a.freq_excluding_latest, b.avg_contribution_margin_new, c.AVERAGE_TIME_TO_PURCHASE, 
d.seg_car, d.seg_suv, d.seg_truck,
e.avg_vehicle_age, g.no_of_lease_purchases, k.lifestage_value
from mpartha9_freq a
inner join mpartha9_monetary_CM b
on a.consumer_id=b.consumer_id
inner join mpartha9_Recency_2 c
on a.consumer_id=c.consumer_id
inner join mpartha9_platform_1 d
on a.consumer_id=d.consumer_id
inner join mpartha9_NA_veh_age e
on a.consumer_id=e.consumer_id
inner join mpartha9_NA_fmcc_lease_purchase_ind g
on a.consumer_id=g.consumer_id
inner join mpartha9_lifestage_value k
on a.consumer_id=k.consumer_id
order by consumer_id
"""
mpartha9_NA_combined_table = spark.sql(Query_combined_table)
mpartha9_NA_combined_table.createOrReplaceTempView("mpartha9_NA_combined_table")


print(mpartha9_NA_combined_table.count(), mpartha9_NA_combined_table.select("consumer_id").distinct().count())

mpartha9_NA_combined_table.show(5)


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
from mpartha9_NA_combined_table
"""
mpartha9_NA_RFM_new_non_NaN = spark.sql(Query_RFM_new_non_NaN)
mpartha9_NA_RFM_new_non_NaN.createOrReplaceTempView("mpartha9_NA_RFM_new_non_NaN")

print(mpartha9_NA_RFM_new_non_NaN.count(), mpartha9_NA_RFM_new_non_NaN.select("consumer_id").distinct().count())

mpartha9_NA_RFM_new_non_NaN.show(5)

## Value Counts for Freq_purchase
mpartha9_NA_RFM_new_non_NaN.groupBy('Freq_purchase').count().show()

## Value Counts for Freq_purchase
mpartha9_NA_RFM_new_non_NaN.groupBy('lease_purchase_flag').count().show()


## Value Counts for Freq_purchase
mpartha9_NA_RFM_new_non_NaN.groupBy('AVERAGE_TIME_TO_PURCHASE').count().show()


# ## Latest Transaction CM = y
# ### Nth TRANSACTION

Query_only_first_transaction = """select consumer_id, acquisition_year as Latest_CM_purchase_year, 
contribution_margin as Latest_CM
from mpartha9_NA_final_table_Special_step
where ROW_numbering = 1
"""
mpartha9_NA_only_first_transaction = spark.sql(Query_only_first_transaction)
mpartha9_NA_only_first_transaction.createOrReplaceTempView("mpartha9_NA_only_first_transaction")

print(mpartha9_NA_only_first_transaction.count(), mpartha9_NA_only_first_transaction.select("consumer_id").distinct().count())


# ### Adding Target y = CM of latest transaction


Query_RFM_with_y = """select a.consumer_id, 
a.Freq_purchase, 
float(a.avg_imputed_contribution_margin) as avg_imputed_contribution_margin,
a.AVERAGE_TIME_TO_PURCHASE,
a.avg_vehicle_age,
a.lease_purchase_flag, 
b.Latest_CM_purchase_year,
float(b.Latest_CM) as Latest_CM
from mpartha9_NA_RFM_new_non_NaN a
inner join mpartha9_NA_only_first_transaction b
on a.consumer_id=b.consumer_id
order by consumer_id
"""
mpartha9_NA_RFM_with_y = spark.sql(Query_RFM_with_y)
mpartha9_NA_RFM_with_y.createOrReplaceTempView("mpartha9_NA_RFM_with_y")

print(mpartha9_NA_RFM_with_y.count(), mpartha9_NA_RFM_with_y.select("consumer_id").distinct().count())

mpartha9_NA_RFM_with_y.show(5)






# # TEST

# ## SPECIAL STEP


Query_special_step_test = """select *,
      sum(case when vehicle_ownership_cycle_num = 1 then 1 else 0 end) over (partition by consumer_id order by consumer_id, acquisition_date desc) as grp
from mpartha9_NA_df_test
"""
mpartha9_NA_special_step_test = spark.sql(Query_special_step_test)
mpartha9_NA_special_step_test.createOrReplaceTempView("mpartha9_NA_special_step_test")


print(mpartha9_NA_special_step_test.count(), mpartha9_NA_special_step_test.select("consumer_id").distinct().count())


Query_special_step_filter_test = """select *
from mpartha9_NA_special_step_test
where grp>=1
"""
mpartha9_NA_special_step_filter_test = spark.sql(Query_special_step_filter_test)
mpartha9_NA_special_step_filter_test.createOrReplaceTempView("mpartha9_NA_special_step_filter_test")


print(mpartha9_NA_special_step_filter_test.count(), mpartha9_NA_special_step_filter_test.select("consumer_id").distinct().count())


# ### Row Numbering


Query_row_numbering_test = """select *
from 
(select *,
ROW_NUMBER() OVER (PARTITION BY consumer_id ORDER BY acquisition_date desc) as ROW_numbering
from mpartha9_NA_special_step_filter_test
) a
"""
mpartha9_NA_row_numbering_test = spark.sql(Query_row_numbering_test)
mpartha9_NA_row_numbering_test.createOrReplaceTempView("mpartha9_NA_row_numbering_test")


print(mpartha9_NA_row_numbering_test.count(), mpartha9_NA_row_numbering_test.select("consumer_id").distinct().count())


# ### Table for No_of_purchases by each customer


Query_no_of_purchases_test = """select consumer_id, Count(acquisition_date) as no_of_purc
from mpartha9_NA_row_numbering_test
group by consumer_id
order by consumer_id
"""
mpartha9_NA_no_of_purchases_test = spark.sql(Query_no_of_purchases_test)
mpartha9_NA_no_of_purchases_test.createOrReplaceTempView("mpartha9_NA_no_of_purchases_test")


print(mpartha9_NA_no_of_purchases_test.count(), mpartha9_NA_no_of_purchases_test.select("consumer_id").distinct().count())


# ### Selecting No_of_purchases > 1


Query_no_of_purchases_more_than_1_test = """select *
from mpartha9_NA_no_of_purchases_test
where no_of_purc > 1
"""
mpartha9_NA_no_of_purchases_more_than_1_test = spark.sql(Query_no_of_purchases_more_than_1_test)
mpartha9_NA_no_of_purchases_more_than_1_test.createOrReplaceTempView("mpartha9_NA_no_of_purchases_more_than_1_test")


print(mpartha9_NA_no_of_purchases_more_than_1_test.count(), mpartha9_NA_no_of_purchases_more_than_1_test.select("consumer_id").distinct().count())


# ### Final Special step with more than single purchase transactions


Query_final_table_Special_step_test = """select a.*
from mpartha9_NA_row_numbering_test a
inner join mpartha9_NA_no_of_purchases_more_than_1_test b
on a.consumer_id=b.consumer_id
"""
mpartha9_NA_final_table_Special_step_test = spark.sql(Query_final_table_Special_step_test)
mpartha9_NA_final_table_Special_step_test.createOrReplaceTempView("mpartha9_NA_final_table_Special_step_test")


print(mpartha9_NA_final_table_Special_step_test.count(), mpartha9_NA_final_table_Special_step_test.select("consumer_id").distinct().count())


# # Excluding_first_transaction : (N-1) TRANSACTION


Query_excluding_first_transaction_test = """select *
from mpartha9_NA_final_table_Special_step_test
where ROW_numbering != 1
order by consumer_id, acquisition_date desc
"""
mpartha9_NA_excluding_first_transaction_test = spark.sql(Query_excluding_first_transaction_test)
mpartha9_NA_excluding_first_transaction_test.createOrReplaceTempView("mpartha9_NA_excluding_first_transaction_test")

print(mpartha9_NA_excluding_first_transaction_test.count(), mpartha9_NA_excluding_first_transaction_test.select("consumer_id").distinct().count())


# ### Vehicle Age

Query_vehicle_age_test = """select consumer_id, avg(vehicle_age) as avg_vehicle_age
from mpartha9_NA_excluding_first_transaction_test
group by consumer_id
order by consumer_id
"""
mpartha9_NA_veh_age_test = spark.sql(Query_vehicle_age_test)
mpartha9_NA_veh_age_test.createOrReplaceTempView("mpartha9_NA_veh_age_test")

print(mpartha9_NA_veh_age_test.count(), mpartha9_NA_veh_age_test.select("consumer_id").distinct().count())


# ### fmcc_lease_purchase_ind

Query_fmcc_lease_purchase_ind_test = """select consumer_id, sum(fmcc_lease_purchase_ind) as no_of_lease_purchases
from mpartha9_NA_excluding_first_transaction_test
group by consumer_id
order by consumer_id
"""
mpartha9_NA_fmcc_lease_purchase_ind_test = spark.sql(Query_fmcc_lease_purchase_ind_test)
mpartha9_NA_fmcc_lease_purchase_ind_test.createOrReplaceTempView("mpartha9_NA_fmcc_lease_purchase_ind_test")

print(mpartha9_NA_fmcc_lease_purchase_ind_test.count(), mpartha9_NA_fmcc_lease_purchase_ind_test.select("consumer_id").distinct().count())


# ### PLATFORM SEGMENTATION


Query_platform_test = """select *,
case when platform = 'CAR' then 1 else 0 end as count_CAR,
case when platform = 'SUV' then 1 else 0 end as count_SUV,
case when platform = 'TRUCK' then 1 else 0 end as count_TRUCK
from mpartha9_NA_excluding_first_transaction_test
"""
mpartha9_platform_test = spark.sql(Query_platform_test)
mpartha9_platform_test.createOrReplaceTempView("mpartha9_platform_test")


Query_platform_1_test = """select consumer_id, 
sum(count_CAR) as seg_car,
sum(count_SUV) as seg_suv,
sum(count_TRUCK) as seg_truck
from mpartha9_platform_test
group by consumer_id
order by consumer_id
"""
mpartha9_platform_1_test = spark.sql(Query_platform_1_test)
mpartha9_platform_1_test.createOrReplaceTempView("mpartha9_platform_1_test")


# ### Frequency

Query_freq_test = """select consumer_id, Count(acquisition_date) as freq_excluding_latest
from mpartha9_NA_excluding_first_transaction_test
group by consumer_id
order by consumer_id
"""
mpartha9_freq_test = spark.sql(Query_freq_test)
mpartha9_freq_test.createOrReplaceTempView("mpartha9_freq_test")


print(mpartha9_freq_test.count(), mpartha9_freq_test.select("consumer_id").distinct().count())


# ### avg_contribution_margin_new

Query_monetary_CM_test = """select consumer_id, avg(contribution_margin) as avg_contribution_margin_new
from mpartha9_NA_excluding_first_transaction_test
group by consumer_id
order by consumer_id
"""
mpartha9_monetary_CM_test = spark.sql(Query_monetary_CM_test)
mpartha9_monetary_CM_test.createOrReplaceTempView("mpartha9_monetary_CM_test")


print(mpartha9_monetary_CM_test.count(), mpartha9_monetary_CM_test.select("consumer_id").distinct().count())


# ### AVERAGE TIME TO PURCHASE (FORMERLY  RECENCY)

Query_Recency_test = """Select consumer_id, acquisition_date, ROW_numbering
,-((DateDiff(acquisition_date, Lag(acquisition_date) Over (Partition by consumer_id Order By acquisition_date desc)))/365) as moving_diff
FROM mpartha9_NA_excluding_first_transaction_test
ORDER BY consumer_id, ROW_numbering
"""
mpartha9_Recency_test = spark.sql(Query_Recency_test)
mpartha9_Recency_test.createOrReplaceTempView("mpartha9_Recency_test")


Query_Recency_1_test = """select consumer_id, avg(moving_diff) as AVERAGE_TIME_TO_PURCHASE
from mpartha9_Recency_test
group by consumer_id
"""
mpartha9_Recency_1_test = spark.sql(Query_Recency_1_test)
mpartha9_Recency_1_test.createOrReplaceTempView("mpartha9_Recency_1_test")


Query_Recency_2_test = """select consumer_id,
case when AVERAGE_TIME_TO_PURCHASE is NULL then 'Purchased_only_once'
	when (AVERAGE_TIME_TO_PURCHASE >=0 and AVERAGE_TIME_TO_PURCHASE<=3) then 'Repurchased_WITHIN_3_yrs'
	when (AVERAGE_TIME_TO_PURCHASE >3 and AVERAGE_TIME_TO_PURCHASE<=5) then 'Repurchased_BTWN_3_5_yrs'
	when AVERAGE_TIME_TO_PURCHASE >5 then 'Repurchased_AFTER_5_yrs'
	end as AVERAGE_TIME_TO_PURCHASE
from mpartha9_Recency_1_test
"""
mpartha9_Recency_2_test = spark.sql(Query_Recency_2_test)
mpartha9_Recency_2_test.createOrReplaceTempView("mpartha9_Recency_2_test")


print(mpartha9_Recency_2_test.count(), mpartha9_Recency_2_test.select("consumer_id").distinct().count())


# ### lifestage_value


Query_lifestage_value_test = """select consumer_id, lifestage_value
from
(
select consumer_id, lifestage_value,
ROW_NUMBER() OVER (PARTITION BY consumer_id ORDER BY acquisition_date DESC) as lifestage_flag
from mpartha9_NA_excluding_first_transaction_test
) a
where lifestage_flag =1
"""
mpartha9_lifestage_value_test = spark.sql(Query_lifestage_value_test)
mpartha9_lifestage_value_test.createOrReplaceTempView("mpartha9_lifestage_value_test")


# ### COMBINED TABLE for all feature variables


Query_combined_table_test = """select a.consumer_id, a.freq_excluding_latest, b.avg_contribution_margin_new, c.AVERAGE_TIME_TO_PURCHASE, 
d.seg_car, d.seg_suv, d.seg_truck,
e.avg_vehicle_age, g.no_of_lease_purchases, k.lifestage_value
from mpartha9_freq_test a
inner join mpartha9_monetary_CM_test b
on a.consumer_id=b.consumer_id
inner join mpartha9_Recency_2_test c
on a.consumer_id=c.consumer_id
inner join mpartha9_platform_1_test d
on a.consumer_id=d.consumer_id
inner join mpartha9_NA_veh_age_test e
on a.consumer_id=e.consumer_id
inner join mpartha9_NA_fmcc_lease_purchase_ind_test g
on a.consumer_id=g.consumer_id
inner join mpartha9_lifestage_value_test k
on a.consumer_id=k.consumer_id
order by consumer_id
"""
mpartha9_NA_combined_table_test = spark.sql(Query_combined_table_test)
mpartha9_NA_combined_table_test.createOrReplaceTempView("mpartha9_NA_combined_table_test")


# ### Final X Variables  final formattings


Query_RFM_new_non_NaN_test = """select consumer_id, 
case when freq_excluding_latest=1 then 'One'
	when freq_excluding_latest=2 then 'Two'
	when freq_excluding_latest=3 then 'Three'
	else 'More_than_Three' end as Freq_purchase,
avg_contribution_margin_new as avg_imputed_contribution_margin,
AVERAGE_TIME_TO_PURCHASE,
avg_vehicle_age,
case when no_of_lease_purchases=0 then 'Zero' else 'One' end as lease_purchase_flag
from mpartha9_NA_combined_table_test
"""
mpartha9_NA_RFM_new_non_NaN_test = spark.sql(Query_RFM_new_non_NaN_test)
mpartha9_NA_RFM_new_non_NaN_test.createOrReplaceTempView("mpartha9_NA_RFM_new_non_NaN_test")


# ## Latest Transaction CM = y
# ### Nth TRANSACTION



Query_only_first_transaction_test = """select consumer_id, acquisition_year as Latest_CM_purchase_year, 
contribution_margin as Latest_CM
from mpartha9_NA_final_table_Special_step_test
where ROW_numbering = 1
"""
mpartha9_NA_only_first_transaction_test = spark.sql(Query_only_first_transaction_test)
mpartha9_NA_only_first_transaction_test.createOrReplaceTempView("mpartha9_NA_only_first_transaction_test")



print(mpartha9_NA_only_first_transaction_test.count(), mpartha9_NA_only_first_transaction_test.select("consumer_id").distinct().count())


# ### Adding Target y = CM of latest transaction


Query_RFM_with_y_test = """select a.consumer_id, 
a.Freq_purchase, 
float(a.avg_imputed_contribution_margin) as avg_imputed_contribution_margin,
a.AVERAGE_TIME_TO_PURCHASE,
a.avg_vehicle_age,
a.lease_purchase_flag,
b.Latest_CM_purchase_year,
float(b.Latest_CM) as Latest_CM
from mpartha9_NA_RFM_new_non_NaN_test a
inner join mpartha9_NA_only_first_transaction_test b
on a.consumer_id=b.consumer_id
order by consumer_id
"""
mpartha9_NA_RFM_with_y_test = spark.sql(Query_RFM_with_y_test)
mpartha9_NA_RFM_with_y_test.createOrReplaceTempView("mpartha9_NA_RFM_with_y_test")


print(mpartha9_NA_RFM_with_y_test.count(), mpartha9_NA_RFM_with_y_test.select("consumer_id").distinct().count())


mpartha9_NA_RFM_with_y_test.show(5)


# --- Completed by Partha Sarathi Misra (mpartha9)
