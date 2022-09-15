# Databricks notebook source
import pandas as pd
import datetime
df_merg_wall_us_op_pur_produc = pd.read_csv("/dbfs/mnt/working/data_join/mergered_user_updatedWallet_op_pur_prod_out.csv",sep= "|",infer_datetime_format = True)
df_merg_wall_us_op_pur_produc["OccuredAt_operations"] = df_merg_wall_us_op_pur_produc["OccuredAt_operations"].astype('datetime64[ns]') 

df_merg_wall_us_op_pur_produc_scope_whitout_ramandan  = df_merg_wall_us_op_pur_produc.loc[(df_merg_wall_us_op_pur_produc["OccuredAt_operations"]>datetime.datetime(2022,2,15)) & (df_merg_wall_us_op_pur_produc["OccuredAt_operations"]<datetime.datetime(2022,4,1)) ] 
df_merg_wall_us_op_pur_produc_scope_whitout_ramandan = pd.concat([df_merg_wall_us_op_pur_produc_scope_whitout_ramandan, df_merg_wall_us_op_pur_produc[df_merg_wall_us_op_pur_produc["OccuredAt_operations"]>datetime.datetime(2022,5,1)]], axis = 0)

# COMMAND ----------

df_merg_wall_us_op_pur_produc_scope_whitout_ramandan.to_csv("/dbfs/mnt/working/data_join/mergered_user_updatedWallet_op_pur_prod_out_ramadan.csv",sep= "|",index =False)

# COMMAND ----------

import pandas as pd

df_merg_wall_us_op_pur_produc_scope_whitout_ramandan =pd.read_csv("/dbfs/mnt/working/data_join/mergered_user_updatedWallet_op_pur_prod_out_ramadan.csv", sep ='|')

# COMMAND ----------

1+1

# COMMAND ----------

df_merg_wall_us_op_pur_produc_scope_whitout_ramandan[df_merg_wall_us_op_pur_produc_scope_whitout_ramandan["Id_wallets_source"] == 2]

# COMMAND ----------

df_merg_wall_us_op_pur_produc_scope_whitout_ramandan.unique()

# COMMAND ----------

import pandas as pd
a=  pd.read_csv("/dbfs/mnt/working/data_fusion/df_merg_wall_us_days_per_5.csv",sep= "|",infer_datetime_format = True)
a

# COMMAND ----------

import pandas as pd
df_merg_wall_us_op_pur_produc = pd.read_csv("/dbfs/mnt/working/data_join/mergered_user_updatedWallet_op_pur_prod_out_ramadan.csv" ,sep ='|',infer_datetime_format =True ) 


# COMMAND ----------

df_merg_wall_us_op_pur_produc

# COMMAND ----------

from functools import reduce
from pyspark.sql.functions import col, lit, when
from graphframes import *

# COMMAND ----------

!pip show networkx 

# COMMAND ----------

!pip show scipy

# COMMAND ----------

dbutils.library.installPyPI("scipy", version="1.8.1")

# COMMAND ----------

# MAGIC  %pip install --upgrade scipy

# COMMAND ----------

# MAGIC %pip freeze

# COMMAND ----------

1+1

# COMMAND ----------

