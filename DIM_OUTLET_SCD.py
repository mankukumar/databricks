# Databricks notebook source
#Sync Test 1#####################################################################SOURCE##################################################################################
#Loading the Source data in Dataframe
file_location = "/mnt/APAC/SOURCE_FILES/DIMENSION/DIM_OUTLET/LIVE/DIM_OUTLET.csv"
file_type = "csv"

# CSV options
infer_schema = "true"
first_row_is_header = "true"
delimiter = ","

# The applied options are for CSV files. For other file types, these will be ignored.
df_src = spark.read.format(file_type) \
  .option("inferSchema", infer_schema) \
  .option("header", first_row_is_header) \
  .option("sep", delimiter) \
  .load(file_location)
##############################################################################################################################################################

# COMMAND ----------

df_src = df_src.select('store_id',	'store_name',	'store_code',	'storetype',	'store_level',	'assortmentcode',	'assortmentname',	'distributionsegmentlcode',	'distributionsegmentlname',	'subchannelcode',	'subchannelname',	'channelcode',	'channelname',	'contactpersonname',	'address1',	'citycode',	'city',	'fax',	'phonenumber',	'emailaddress',	'outletotiscode',	'is_active',	'is_assorment',	'last_call_time',	'lon',	'lat',	'iscontracted',	'provincecode',	'province',	'subregioncode', 'emp_id', 'emp_code',	'dist_name',	'org_id',	'org_name',	'org_code',	'pricinggroup','emp_name','subregion','regioncode','region','state','created','updated','coret2','exclusived','priority','focuschannel','battleground','keydealer_flag','keycustname','salesprod_flag','wholesaler_flag','corestorescanchannel','coret2distributionchannel','is_need_visit','type_id','type_name','level_id','level_name')

# COMMAND ----------

df_src = df_src.withColumnRenamed('store_id','SRC_STORE_ID').withColumnRenamed('store_name','SRC_STORE_NAME').withColumnRenamed('store_code','SRC_STORE_CODE').withColumnRenamed('storetype','SRC_STORE_TYPE').withColumnRenamed('store_level','SRC_STORE_LEVEL').withColumnRenamed('assortmentcode','SRC_ASSORTMENT_CODE').withColumnRenamed('assortmentname','SRC_ASSORTMENT_NAME').withColumnRenamed('distributionsegmentlcode','SRC_DISTRIBUTION_SEGEMENT_LCODE').withColumnRenamed('distributionsegmentlname','SRC_DISTRIBUTION_SEGEMENT_LNAME').withColumnRenamed('subchannelcode','SRC_SUB_CHANNEL_CODE').withColumnRenamed('subchannelname','SRC_SUB_CHANNEL_NAME').withColumnRenamed('channelcode','SRC_CHANNEL_CODE').withColumnRenamed('channelname','SRC_CHANNEL_NAME').withColumnRenamed('contactpersonname','SRC_CONTACT_PERSON_NAME').withColumnRenamed('address1','SRC_ADDRESS_1').withColumnRenamed('citycode','SRC_CITY_CODE').withColumnRenamed('city','SRC_CITY').withColumnRenamed('fax','SRC_FAX').withColumnRenamed('phonenumber','SRC_PHONE_NUMBER').withColumnRenamed('emailaddress','SRC_EMAIL_ADDRESS').withColumnRenamed('outletotiscode','SRC_OUTLET_OTIS_CODE').withColumnRenamed('is_active','SRC_IS_ACTIVE').withColumnRenamed('is_assorment','SRC_IS_ASSORMENT').withColumnRenamed('last_call_time','SRC_LAST_CALL_TIME').withColumnRenamed('lon','SRC_LONGITUDE').withColumnRenamed('lat','SRC_LATITUDE').withColumnRenamed('iscontracted','SRC_IS_CONTRACTED').withColumnRenamed('provincecode','SRC_PROVINCE_CODE').withColumnRenamed('province','SRC_PROVINCE').withColumnRenamed('subregioncode','SRC_SUB_REGION_CODE').withColumnRenamed('emp_id','SRC_EMP_ID').withColumnRenamed('dist_name','SRC_DISTRIBUTOR_NAME').withColumnRenamed('org_id','SRC_ORG_ID').withColumnRenamed('org_name','SRC_ORG_NAME').withColumnRenamed('org_code','SRC_ORG_CODE').withColumnRenamed('pricinggroup','SRC_PRICING_GROUP').withColumnRenamed('emp_code','SRC_EMP_CODE').withColumnRenamed('emp_name','SRC_EMP_NAME').withColumnRenamed('subregion','SRC_SUB_REGION').withColumnRenamed('regioncode','SRC_REGION_CODE').withColumnRenamed('region','SRC_REGION').withColumnRenamed('state','SRC_STATE').withColumnRenamed('created','SRC_CREATED').withColumnRenamed('updated','SRC_UPDATED').withColumnRenamed('coret2','SRC_CORET_2').withColumnRenamed('exclusived','SRC_EXCLUSIVED').withColumnRenamed('priority','SRC_PRIORITY').withColumnRenamed('focuschannel','SRC_FOCUS_CHANNEL').withColumnRenamed('battleground','SRC_BATTLE_GROUND').withColumnRenamed('keydealer_flag','SRC_KEY_DEALER_FLAG').withColumnRenamed('keycustname','SRC_KEY_CUSTOMER_NAME').withColumnRenamed('salesprod_flag','SRC_SALES_PROD_FLAG').withColumnRenamed('wholesaler_flag','SRC_WHOLESALER_FLAG').withColumnRenamed('corestorescanchannel','SRC_CORE_STORE_SCAN_CHANNEL').withColumnRenamed('coret2distributionchannel','SRC_CORE_T2_DISTRIBUTION_CHANNEL').withColumnRenamed('is_need_visit','SRC_IS_NEED_VISIT').withColumnRenamed('type_id','SRC_TYPE_ID').withColumnRenamed('type_name','SRC_TYPE_NAME').withColumnRenamed('level_id','SRC_LEVEL_ID').withColumnRenamed('level_name','SRC_LEVEL_NAME')


from pyspark.sql.functions import col , column
from pyspark.sql.functions import trim

df_src=df_src.withColumn('SRC_STORE_LEVEL',col('SRC_STORE_LEVEL').cast("string"))
df_src=df_src.withColumn('SRC_LONGITUDE',col('SRC_LONGITUDE').cast("string"))
df_src=df_src.withColumn('SRC_LATITUDE',col('SRC_LATITUDE').cast("string"))
df_src=df_src.withColumn('SRC_ORG_ID',trim(col('SRC_ORG_ID')))


# COMMAND ----------

df_src = df_src.fillna({'SRC_STORE_ID' : -1,'SRC_STORE_NAME' : 'NA','SRC_STORE_CODE' : 'NA','SRC_STORE_TYPE' : 'NA',  'SRC_STORE_LEVEL' : 'NA','SRC_ASSORTMENT_CODE' : 'NA',  'SRC_ASSORTMENT_NAME' : 'NA',  'SRC_DISTRIBUTION_SEGEMENT_LCODE' : 'NA',  'SRC_DISTRIBUTION_SEGEMENT_LNAME' : 'NA',  'SRC_SUB_CHANNEL_CODE' : 'NA',  'SRC_SUB_CHANNEL_NAME' : 'NA',  'SRC_CHANNEL_CODE' : 'NA',  'SRC_CHANNEL_NAME' : 'NA','SRC_CONTACT_PERSON_NAME' : 'NA','SRC_ADDRESS_1' : 'NA','SRC_CITY_CODE' : 'NA','SRC_CITY' : 'NA','SRC_FAX' : -1,'SRC_PHONE_NUMBER' : -1,    'SRC_EMAIL_ADDRESS' : 'NA','SRC_OUTLET_OTIS_CODE' : 'NA','SRC_IS_ACTIVE' : -1,'SRC_IS_ASSORMENT' : -1,'SRC_LAST_CALL_TIME' : '1900-01-01','SRC_LONGITUDE' : 0,'SRC_LATITUDE' : 0,'SRC_IS_CONTRACTED' : -1,'SRC_PROVINCE_CODE' : 'NA','SRC_PROVINCE' : 'NA','SRC_SUB_REGION_CODE' : -1,'SRC_EMP_ID' : -1,'SRC_DISTRIBUTOR_NAME': -1,'SRC_ORG_ID': -1,'SRC_ORG_NAME': 'NA','SRC_ORG_CODE':'NA','SRC_PRICING_GROUP': 'NA','SRC_EMP_CODE':'NA', 'SRC_EMP_NAME' : 'NA', 'SRC_SUB_REGION' : 'NA', 'SRC_REGION_CODE' : 'NA', 'SRC_REGION' : 'NA', 'SRC_STATE' : -1, 'SRC_CREATED' : '1900-01-01', 'SRC_UPDATED' : '1900-01-01', 'SRC_CORET_2' : -1, 'SRC_EXCLUSIVED' : 'NA', 'SRC_PRIORITY' : -1, 'SRC_FOCUS_CHANNEL' : -1, 'SRC_BATTLE_GROUND' : 'NA', 'SRC_KEY_DEALER_FLAG' : -1, 'SRC_KEY_CUSTOMER_NAME' : 'NA', 'SRC_SALES_PROD_FLAG' : -1, 'SRC_WHOLESALER_FLAG' : -1, 'SRC_CORE_STORE_SCAN_CHANNEL' : -1, 'SRC_CORE_T2_DISTRIBUTION_CHANNEL' : -1, 'SRC_IS_NEED_VISIT' : -1, 'SRC_TYPE_ID' : -1, 'SRC_TYPE_NAME' : 'NA', 'SRC_LEVEL_ID' : -1, 'SRC_LEVEL_NAME' : 'NA'})

# COMMAND ----------

from pyspark.sql.functions import unix_timestamp, from_unixtime
#Load the latest saved CSV in a dataframe
file_location = "/mnt/APAC/EXTERNAL_TABLES/DIMENSION/DIM_OUTLET/*.csv"
file_type = "csv"

# CSV options
infer_schema = "true"
first_row_is_header = "true"
delimiter = ","

# The applied options are for CSV files. For other file types, these will be ignored.
df_tgt = spark.read.format(file_type) \
  .option("inferSchema", infer_schema) \
  .option("header", first_row_is_header) \
  .option("sep", delimiter) \
  .load(file_location)

df_tgt = df_tgt.withColumnRenamed('ORD_ID','ORG_ID')
df_tgt = df_tgt.withColumn('DW_CREATED_DATE',from_unixtime(unix_timestamp('dw_created_date','MM/dd/yyy HH:mm:ss'))) \
.withColumn('DW_UPDATED_DATE',from_unixtime(unix_timestamp('DW_UPDATED_DATE','MM/dd/yyy HH:mm:ss'))).withColumn('SALES_REP_START_DATE',from_unixtime(unix_timestamp('SALES_REP_START_DATE','MM/dd/yyy HH:mm:ss'))).withColumn('SALES_REP_END_DATE',from_unixtime(unix_timestamp('SALES_REP_END_DATE','MM/dd/yyy HH:mm:ss'))).withColumn('CREATED',from_unixtime(unix_timestamp('CREATED','MM/dd/yyy HH:mm:ss'))).withColumn('UPDATED',from_unixtime(unix_timestamp('UPDATED','MM/dd/yyy HH:mm:ss'))).withColumn('LAST_CALL_TIME',from_unixtime(unix_timestamp('LAST_CALL_TIME','MM/dd/yyy HH:mm:ss')))


# COMMAND ----------

from pyspark.sql import functions as F

df_tgt = df_tgt.withColumn('STORE_KEY',df_tgt.STORE_KEY.cast('integer')).withColumn('STORE_ID',df_tgt.STORE_ID.cast('integer')).withColumn('SUB_REGION_KEY',df_tgt.SUB_REGION_KEY.cast('integer')).withColumn('EMP_KEY',df_tgt.EMP_KEY.cast('integer')).withColumn('EMP_ID',df_tgt.EMP_ID.cast('integer')).withColumn('DIST_STORE_KEY',df_tgt.DIST_STORE_KEY.cast('integer')).withColumn('DISTRIBUTOR_NAME',df_tgt.DISTRIBUTOR_NAME.cast('integer')).withColumn('IS_CONTRACTED',df_tgt.IS_CONTRACTED.cast('integer')).withColumn('FAX',df_tgt.FAX.cast('integer')).withColumn('ORG_ID',df_tgt.ORG_ID.cast('integer')).withColumn('STATE',df_tgt.STATE.cast('integer')).withColumn('IS_ACTIVE',df_tgt.IS_ACTIVE.cast('integer')).withColumn('IS_ASSORMENT',df_tgt.IS_ASSORMENT.cast('integer')).withColumn('CORET_2',df_tgt.CORET_2.cast('integer')).withColumn('PRIORITY',df_tgt.PRIORITY.cast('integer')).withColumn('FOCUS_CHANNEL',df_tgt.FOCUS_CHANNEL.cast('integer')).withColumn('KEY_DEALER_FLAG',df_tgt.KEY_DEALER_FLAG.cast('integer')).withColumn('SALES_PROD_FLAG',df_tgt.SALES_PROD_FLAG.cast('integer')).withColumn('WHOLESALER_FLAG',df_tgt.WHOLESALER_FLAG.cast('integer')).withColumn('CORE_STORE_SCAN_CHANNEL',df_tgt.CORE_STORE_SCAN_CHANNEL.cast('integer')).withColumn('CORE_T2_DISTRIBUTION_CHANNEL',df_tgt.CORE_T2_DISTRIBUTION_CHANNEL.cast('integer')).withColumn('IS_NEED_VISIT',df_tgt.IS_NEED_VISIT.cast('integer')).withColumn('TYPE_ID',df_tgt.TYPE_ID.cast('integer')).withColumn('LEVEL_ID',df_tgt.LEVEL_ID.cast('integer')).withColumn('ACTIVE_FLAG',df_tgt.ACTIVE_FLAG.cast('integer'))

# COMMAND ----------

#filter the active records from the target data frame

df_tgt_a = df_tgt.filter(F.col('ACTIVE_FLAG') == 1)
df_tgt_ia = df_tgt.filter(F.col('ACTIVE_FLAG') == 0)

# COMMAND ----------

df_merge = df_tgt_a.join(df_src, df_tgt_a.STORE_CODE == df_src.SRC_STORE_CODE,how = 'left')

# COMMAND ----------

#SCD 1 changes to be done*******************
df_merge = df_merge.withColumn("DW_UPDATED_DATE",F.when(df_merge.STORE_CODE.isNotNull() & ((df_merge.STORE_NAME != df_merge.SRC_STORE_NAME)| (df_merge.CONTACT_PERSON_NAME != df_merge.SRC_CONTACT_PERSON_NAME) | (df_merge.ADDRESS_1 != df_merge.SRC_ADDRESS_1) | (df_merge.ADDRESS_1 != df_merge.SRC_ADDRESS_1) | (df_merge.CITY_CODE != df_merge.SRC_CITY_CODE) | (df_merge.FAX != df_merge.SRC_FAX) | (df_merge.PHONE_NUMBER != df_merge.SRC_PHONE_NUMBER) | (df_merge.EMAIL_ADDRESS != df_merge.SRC_EMAIL_ADDRESS) | (df_merge.OUTLET_OTIS_CODE != df_merge.SRC_OUTLET_OTIS_CODE) | (df_merge.LAST_CALL_TIME != df_merge.SRC_LAST_CALL_TIME)), F.current_timestamp()).otherwise(df_merge.DW_UPDATED_DATE)).withColumn("STORE_NAME",F.when(df_merge.STORE_CODE.isNotNull() & (df_merge.STORE_NAME != df_merge.SRC_STORE_NAME), df_merge.SRC_STORE_NAME).otherwise(df_merge.STORE_NAME)).withColumn("CONTACT_PERSON_NAME",F.when(df_merge.STORE_CODE.isNotNull() & (df_merge.CONTACT_PERSON_NAME != df_merge.SRC_CONTACT_PERSON_NAME), df_merge.SRC_CONTACT_PERSON_NAME).otherwise(df_merge.CONTACT_PERSON_NAME)).withColumn("ADDRESS_1",F.when(df_merge.STORE_CODE.isNotNull() & (df_merge.ADDRESS_1 != df_merge.SRC_ADDRESS_1), df_merge.SRC_ADDRESS_1).otherwise(df_merge.ADDRESS_1)).withColumn("CITY_CODE",F.when(df_merge.STORE_CODE.isNotNull() & (df_merge.CITY_CODE != df_merge.SRC_CITY_CODE), df_merge.SRC_CITY_CODE).otherwise(df_merge.CITY_CODE)).withColumn("CITY",F.when(df_merge.STORE_CODE.isNotNull() & (df_merge.CITY != df_merge.SRC_CITY), df_merge.SRC_CITY).otherwise(df_merge.CITY)).withColumn("FAX",F.when(df_merge.STORE_CODE.isNotNull() & (df_merge.FAX != df_merge.SRC_FAX), df_merge.SRC_FAX).otherwise(df_merge.FAX)).withColumn("PHONE_NUMBER",F.when(df_merge.STORE_CODE.isNotNull() & (df_merge.PHONE_NUMBER != df_merge.SRC_PHONE_NUMBER), df_merge.SRC_PHONE_NUMBER).otherwise(df_merge.PHONE_NUMBER)).withColumn("EMAIL_ADDRESS",F.when(df_merge.STORE_CODE.isNotNull() & (df_merge.EMAIL_ADDRESS != df_merge.SRC_EMAIL_ADDRESS), df_merge.SRC_EMAIL_ADDRESS).otherwise(df_merge.EMAIL_ADDRESS)).withColumn("OUTLET_OTIS_CODE",F.when(df_merge.STORE_CODE.isNotNull() & (df_merge.OUTLET_OTIS_CODE != df_merge.SRC_OUTLET_OTIS_CODE), df_merge.SRC_OUTLET_OTIS_CODE).otherwise(df_merge.OUTLET_OTIS_CODE)).withColumn("LAST_CALL_TIME",F.when(df_merge.STORE_CODE.isNotNull() & (df_merge.LAST_CALL_TIME != df_merge.SRC_LAST_CALL_TIME), df_merge.SRC_LAST_CALL_TIME).otherwise(df_merge.LAST_CALL_TIME))

# COMMAND ----------

df_tgt_a = df_merge.select('STORE_KEY','STORE_ID','STORE_NAME','STORE_CODE','STORE_TYPE','STORE_LEVEL','ASSORTMENT_CODE','ASSORTMENT_NAME','DISTRIBUTION_SEGEMENT_LCODE','DISTRIBUTION_SEGEMENT_LNAME','SUB_CHANNEL_CODE','SUB_CHANNEL_NAME','CHANNEL_CODE','CHANNEL_NAME','CITY_CODE','CITY','PROVINCE_CODE','PROVINCE',  'SUB_REGION_KEY','SUB_REGION_CODE','SUB_REGION','REGION_CODE','REGION','EMP_KEY','EMP_ID','EMP_CODE','EMP_NAME','SALES_REP_START_DATE','SALES_REP_END_DATE','DIST_STORE_KEY','DISTRIBUTOR_NAME','IS_CONTRACTED','PRICING_GROUP','CONTACT_PERSON_NAME','ADDRESS_1','FAX','PHONE_NUMBER','EMAIL_ADDRESS','OUTLET_OTIS_CODE','ORG_ID','ORG_NAME','ORG_CODE','STATE','IS_ACTIVE','IS_ASSORMENT','LAST_CALL_TIME','LONGITUDE','LATITUDE','CREATED','UPDATED','CORET_2','EXCLUSIVED','PRIORITY','FOCUS_CHANNEL','BATTLE_GROUND','KEY_DEALER_FLAG','KEY_CUSTOMER_NAME','SALES_PROD_FLAG','WHOLESALER_FLAG','CORE_STORE_SCAN_CHANNEL','CORE_T2_DISTRIBUTION_CHANNEL','IS_NEED_VISIT','TYPE_ID','TYPE_NAME','LEVEL_ID','LEVEL_NAME','ACTIVE_FLAG','DW_CREATED_DATE','DW_UPDATED_DATE','DW_CREATED_BY','DW_UPDATED_BY')

df_tgt = df_tgt_a.union(df_tgt_ia)

# COMMAND ----------

df_tgt_a = df_tgt_a.withColumnRenamed('STORE_KEY','TGT_STORE_KEY').withColumnRenamed('STORE_ID','TGT_STORE_ID').withColumnRenamed('STORE_NAME','TGT_STORE_NAME').withColumnRenamed('STORE_CODE','TGT_STORE_CODE').withColumnRenamed('STORE_TYPE','TGT_STORE_TYPE').withColumnRenamed('STORE_LEVEL','TGT_STORE_LEVEL').withColumnRenamed('ASSORTMENT_CODE','TGT_ASSORTMENT_CODE').withColumnRenamed('ASSORTMENT_NAME','TGT_ASSORTMENT_NAME').withColumnRenamed('DISTRIBUTION_SEGEMENT_LCODE','TGT_DISTRIBUTION_SEGEMENT_LCODE').withColumnRenamed('DISTRIBUTION_SEGEMENT_LNAME','TGT_DISTRIBUTION_SEGEMENT_LNAME').withColumnRenamed('SUB_CHANNEL_CODE','TGT_SUB_CHANNEL_CODE').withColumnRenamed('SUB_CHANNEL_NAME','TGT_SUB_CHANNEL_NAME').withColumnRenamed('CHANNEL_CODE','TGT_CHANNEL_CODE').withColumnRenamed('CHANNEL_NAME','TGT_CHANNEL_NAME').withColumnRenamed('CONTACT_PERSON_NAME','TGT_CONTACT_PERSON_NAME').withColumnRenamed('ADDRESS_1','TGT_ADDRESS_1').withColumnRenamed('CITY_CODE','TGT_CITY_CODE').withColumnRenamed('CITY','TGT_CITY').withColumnRenamed('FAX','TGT_FAX').withColumnRenamed('PHONE_NUMBER','TGT_PHONE_NUMBER').withColumnRenamed('EMAIL_ADDRESS','TGT_EMAIL_ADDRESS').withColumnRenamed('OUTLET_OTIS_CODE','TGT_OUTLET_OTIS_CODE').withColumnRenamed('IS_ACTIVE','TGT_IS_ACTIVE').withColumnRenamed('IS_ASSORMENT','TGT_IS_ASSORMENT').withColumnRenamed('LAST_CALL_TIME','TGT_LAST_CALL_TIME').withColumnRenamed('LONGITUDE','TGT_LONGITUDE').withColumnRenamed('LATITUDE','TGT_LATITUDE').withColumnRenamed('IS_CONTRACTED','TGT_IS_CONTRACTED').withColumnRenamed('PROVINCE_CODE','TGT_PROVINCE_CODE').withColumnRenamed('PROVINCE','TGT_PROVINCE').withColumnRenamed('SUB_REGION_KEY','TGT_SUB_REGION_KEY').withColumnRenamed( 'EMP_KEY','TGT_EMP_KEY').withColumnRenamed('DIST_STORE_KEY','TGT_DIST_STORE_KEY').withColumnRenamed('DISTRIBUTOR_NAME','TGT_DISTRIBUTOR_NAME').withColumnRenamed('ORG_ID','TGT_ORG_ID').withColumnRenamed('ORG_NAME','TGT_ORG_NAME').withColumnRenamed('ORG_CODE','TGT_ORG_CODE').withColumnRenamed('PRICING_GROUP','TGT_PRICING_GROUP').withColumnRenamed('DW_CREATED_DATE','TGT_DW_CREATED_DATE').withColumnRenamed('DW_UPDATED_DATE','TGT_DW_UPDATED_DATE').withColumnRenamed('ACTIVE_FLAG','TGT_ACTIVE_FLAG').withColumnRenamed('EMP_CODE','TGT_EMP_CODE').withColumnRenamed('SALES_REP_START_DATE','TGT_SALES_REP_START_DATE').withColumnRenamed('SALES_REP_END_DATE','TGT_SALES_REP_END_DATE').withColumnRenamed('DW_CREATED_BY','TGT_DW_CREATED_BY').withColumnRenamed('DW_UPDATED_BY','TGT_DW_UPDATED_BY').withColumnRenamed('EMP_NAME','TGT_EMP_NAME').withColumnRenamed('SUB_REGION','TGT_SUB_REGION').withColumnRenamed('REGION_CODE','TGT_REGION_CODE').withColumnRenamed('REGION','TGT_REGION').withColumnRenamed('STATE','TGT_STATE').withColumnRenamed('CREATED','TGT_CREATED').withColumnRenamed('UPDATED','TGT_UPDATED').withColumnRenamed('CORET_2','TGT_CORET_2').withColumnRenamed('EXCLUSIVED','TGT_EXCLUSIVED').withColumnRenamed('PRIORITY','TGT_PRIORITY').withColumnRenamed('FOCUS_CHANNEL','TGT_FOCUS_CHANNEL').withColumnRenamed('BATTLE_GROUND','TGT_BATTLE_GROUND').withColumnRenamed('KEY_DEALER_FLAG','TGT_KEY_DEALER_FLAG').withColumnRenamed('KEY_CUSTOMER_NAME','TGT_KEY_CUSTOMER_NAME').withColumnRenamed('SALES_PROD_FLAG','TGT_SALES_PROD_FLAG').withColumnRenamed('WHOLESALER_FLAG','TGT_WHOLESALER_FLAG').withColumnRenamed('CORE_STORE_SCAN_CHANNEL','TGT_CORE_STORE_SCAN_CHANNEL').withColumnRenamed('CORE_T2_DISTRIBUTION_CHANNEL','TGT_CORE_T2_DISTRIBUTION_CHANNEL').withColumnRenamed('IS_NEED_VISIT','TGT_IS_NEED_VISIT').withColumnRenamed('TYPE_ID','TGT_TYPE_ID').withColumnRenamed('TYPE_NAME','TGT_TYPE_NAME').withColumnRenamed('LEVEL_ID','TGT_LEVEL_ID').withColumnRenamed('LEVEL_NAME','TGT_LEVEL_NAME').withColumnRenamed('SUB_REGION_CODE','TGT_SUB_REGION_CODE').withColumnRenamed('EMP_ID','TGT_EMP_ID')

# COMMAND ----------

#Join Target Outlet dimension with sales rep to find the changes in src
#Load the latest saved CSV in a dataframe
file_location = "/mnt/APAC/EXTERNAL_TABLES/DIMENSION/DIM_SFA_EMPLOYEE/*.csv"
file_type = "csv"

# CSV options
infer_schema = "true"
first_row_is_header = "true"
delimiter = ","

# The applied options are for CSV files. For other file types, these will be ignored.
df_sales_rep = spark.read.format(file_type) \
  .option("inferSchema", infer_schema) \
  .option("header", first_row_is_header) \
  .option("sep", delimiter) \
  .option("encoding", "UTF-8") \
  .load(file_location)

#df_sales_rep = df_sales_rep.filter(df_sales_rep.ACTIVE_FLAG == 1)
df_sales_rep = df_sales_rep.select('EMP_KEY','EMP_CODE')
df_sales_rep = df_sales_rep.withColumnRenamed('EMP_KEY','DIM_EMP_KEY').withColumnRenamed('EMP_CODE','DIM_EMP_CODE')

# COMMAND ----------

#Join Sales rep dim with Outlet tgt to retrieve sales rep code
df_tgt_a = df_tgt_a.join(df_sales_rep, df_tgt_a.TGT_EMP_KEY == df_sales_rep.DIM_EMP_KEY,how = 'left')

df_tgt_a = df_tgt_a.fillna({'DIM_EMP_CODE' : 'NA'})

# COMMAND ----------

df_dist_ol = df_tgt_a.select('TGT_STORE_KEY','TGT_STORE_CODE')

df_dist_ol = df_dist_ol.withColumnRenamed('TGT_STORE_KEY','DIM_STORE_KEY').withColumnRenamed('TGT_STORE_CODE','DIM_STORE_CODE')

# COMMAND ----------

#Join Sales rep dim with Outlet tgt to retrieve sales rep code
df_tgt_a = df_tgt_a.join(df_dist_ol, df_tgt_a.TGT_STORE_KEY == df_dist_ol.DIM_STORE_KEY,how = 'left')

df_tgt_a = df_tgt_a.fillna({'DIM_STORE_CODE' : 'NA'})

# COMMAND ----------

#Join Target and Source and create a new dataframe for perfroming SCD
df_merge = df_tgt_a.join(df_src, df_tgt_a.TGT_STORE_CODE == df_src.SRC_STORE_CODE, how = 'fullouter')

# COMMAND ----------

#create a new column "ACTION" to classify the type of action to be done
df_merge = df_merge\
.withColumn('ACTION',F.when(df_merge.TGT_STORE_CODE.isNotNull() & ((df_merge.TGT_STORE_TYPE != df_merge.SRC_STORE_TYPE) | 
                                               (df_merge.TGT_STORE_LEVEL != df_merge.SRC_STORE_LEVEL) | 
                                               (df_merge.TGT_ASSORTMENT_CODE != df_merge.SRC_ASSORTMENT_CODE)| 
                                               (df_merge.TGT_ASSORTMENT_NAME != df_merge.SRC_ASSORTMENT_NAME)| 
                                               (df_merge.TGT_DISTRIBUTION_SEGEMENT_LCODE != df_merge.SRC_DISTRIBUTION_SEGEMENT_LCODE)| 
                                               (df_merge.TGT_DISTRIBUTION_SEGEMENT_LNAME != df_merge.SRC_DISTRIBUTION_SEGEMENT_LNAME)| 
                                               (df_merge.TGT_SUB_CHANNEL_CODE != df_merge.SRC_SUB_CHANNEL_CODE)| 
                                               (df_merge.TGT_SUB_CHANNEL_NAME != df_merge.SRC_SUB_CHANNEL_NAME)| 
                                               (df_merge.TGT_CHANNEL_CODE != df_merge.SRC_CHANNEL_CODE)| 
                                               (df_merge.TGT_CHANNEL_NAME != df_merge.SRC_CHANNEL_NAME)| 
                                               (df_merge.TGT_IS_ASSORMENT != df_merge.SRC_IS_ASSORMENT)| 
                                               (df_merge.TGT_IS_CONTRACTED != df_merge.SRC_IS_CONTRACTED) |
                                               (df_merge.TGT_DISTRIBUTOR_NAME != df_merge.SRC_DISTRIBUTOR_NAME) |
                                               (df_merge.TGT_EMP_CODE != df_merge.SRC_EMP_CODE)), 'UPSERT')\
.when(df_merge.TGT_STORE_CODE.isNull(),'INSERT')\
.when(df_merge.SRC_STORE_CODE.isNull(),'INACTIVE')\
.otherwise('NO_ACTION')).withColumn('SALES_REP_CHANGE',F.when(df_merge.TGT_STORE_CODE.isNotNull() & ((df_merge.TGT_EMP_CODE != df_merge.SRC_EMP_CODE) | df_merge.DIM_EMP_CODE.isNull()),'UPDATE').otherwise('NO_ACTION'))

# COMMAND ----------

#create a new dataframe with the action as "insert"
df_insert = df_merge.filter(df_merge.ACTION == 'INSERT').select(df_merge.SRC_STORE_ID.alias('STORE_ID'),df_merge.SRC_STORE_NAME.alias('STORE_NAME'),df_merge.SRC_STORE_CODE.alias('STORE_CODE'),df_merge.SRC_STORE_TYPE.alias('STORE_TYPE'),df_merge.SRC_STORE_LEVEL.alias('STORE_LEVEL'),df_merge.SRC_ASSORTMENT_CODE.alias('ASSORTMENT_CODE'),df_merge.SRC_ASSORTMENT_NAME.alias('ASSORTMENT_NAME'),df_merge.SRC_DISTRIBUTION_SEGEMENT_LCODE.alias('DISTRIBUTION_SEGEMENT_LCODE'),df_merge.SRC_DISTRIBUTION_SEGEMENT_LNAME.alias('DISTRIBUTION_SEGEMENT_LNAME'),df_merge.SRC_SUB_CHANNEL_CODE.alias('SUB_CHANNEL_CODE'),df_merge.SRC_SUB_CHANNEL_NAME.alias('SUB_CHANNEL_NAME'),df_merge.SRC_CHANNEL_CODE.alias('CHANNEL_CODE'),df_merge.SRC_CHANNEL_NAME.alias('CHANNEL_NAME'),df_merge.SRC_CONTACT_PERSON_NAME.alias('CONTACT_PERSON_NAME'),df_merge.SRC_ADDRESS_1.alias('ADDRESS_1'),df_merge.SRC_CITY_CODE.alias('CITY_CODE'),df_merge.SRC_CITY.alias('CITY'),df_merge.SRC_FAX.alias('FAX'),df_merge.SRC_PHONE_NUMBER.alias('PHONE_NUMBER'),df_merge.SRC_EMAIL_ADDRESS.alias('EMAIL_ADDRESS'),df_merge.SRC_OUTLET_OTIS_CODE.alias('OUTLET_OTIS_CODE'),df_merge.SRC_IS_ACTIVE.alias('IS_ACTIVE'),df_merge.SRC_IS_ASSORMENT.alias('IS_ASSORMENT'),df_merge.SRC_LAST_CALL_TIME.alias('LAST_CALL_TIME'),df_merge.SRC_LONGITUDE.alias('LONGITUDE'),df_merge.SRC_LATITUDE.alias('LATITUDE'),df_merge.SRC_IS_CONTRACTED.alias('IS_CONTRACTED'),df_merge.SRC_PROVINCE_CODE.alias('PROVINCE_CODE'),df_merge.SRC_PROVINCE.alias('PROVINCE'),df_merge.SRC_SUB_REGION_CODE.alias('SUB_REGION_CODE'),df_merge.SRC_EMP_CODE.alias('EMP_CODE'),df_merge.SRC_DISTRIBUTOR_NAME.alias('DISTRIBUTOR_NAME'),df_merge.SRC_ORG_ID.alias('ORG_ID'),df_merge.SRC_ORG_NAME.alias('ORG_NAME'),df_merge.SRC_ORG_CODE.alias('ORG_CODE'),df_merge.SRC_PRICING_GROUP.alias('PRICING_GROUP'),df_merge.SRC_EMP_NAME.alias('EMP_NAME'),df_merge.SRC_SUB_REGION.alias('SUB_REGION'),df_merge.SRC_REGION_CODE.alias('REGION_CODE'),df_merge.SRC_REGION.alias('REGION'),df_merge.SRC_STATE.alias('STATE'),df_merge.SRC_CREATED.alias('CREATED'),df_merge.SRC_UPDATED.alias('UPDATED'),df_merge.SRC_CORET_2.alias('CORET_2'),df_merge.SRC_EXCLUSIVED.alias('EXCLUSIVED'),df_merge.SRC_PRIORITY.alias('PRIORITY'),df_merge.SRC_FOCUS_CHANNEL.alias('FOCUS_CHANNEL'),df_merge.SRC_BATTLE_GROUND.alias('BATTLE_GROUND'),df_merge.SRC_KEY_DEALER_FLAG.alias('KEY_DEALER_FLAG'),df_merge.SRC_KEY_CUSTOMER_NAME.alias('KEY_CUSTOMER_NAME'),df_merge.SRC_SALES_PROD_FLAG.alias('SALES_PROD_FLAG'),df_merge.SRC_WHOLESALER_FLAG.alias('WHOLESALER_FLAG'),df_merge.SRC_CORE_STORE_SCAN_CHANNEL.alias('CORE_STORE_SCAN_CHANNEL'),df_merge.SRC_CORE_T2_DISTRIBUTION_CHANNEL.alias('CORE_T2_DISTRIBUTION_CHANNEL'),df_merge.SRC_IS_NEED_VISIT.alias('IS_NEED_VISIT'),df_merge.SRC_TYPE_ID.alias('TYPE_ID'),df_merge.SRC_TYPE_NAME.alias('TYPE_NAME'),df_merge.SRC_LEVEL_ID.alias('LEVEL_ID'),df_merge.SRC_LEVEL_NAME.alias('LEVEL_NAME'),df_merge.SRC_EMP_ID.alias('EMP_ID'))



#create a new dataframe with the action as "update"
df_upsert = df_merge.filter(df_merge.ACTION == 'UPSERT').select(df_merge.SRC_STORE_ID.alias('STORE_ID'),df_merge.SRC_STORE_NAME.alias('STORE_NAME'),df_merge.SRC_STORE_CODE.alias('STORE_CODE'),df_merge.SRC_STORE_TYPE.alias('STORE_TYPE'),df_merge.SRC_STORE_LEVEL.alias('STORE_LEVEL'),df_merge.SRC_ASSORTMENT_CODE.alias('ASSORTMENT_CODE'),df_merge.SRC_ASSORTMENT_NAME.alias('ASSORTMENT_NAME'),df_merge.SRC_DISTRIBUTION_SEGEMENT_LCODE.alias('DISTRIBUTION_SEGEMENT_LCODE'),df_merge.SRC_DISTRIBUTION_SEGEMENT_LNAME.alias('DISTRIBUTION_SEGEMENT_LNAME'),df_merge.SRC_SUB_CHANNEL_CODE.alias('SUB_CHANNEL_CODE'),df_merge.SRC_SUB_CHANNEL_NAME.alias('SUB_CHANNEL_NAME'),df_merge.SRC_CHANNEL_CODE.alias('CHANNEL_CODE'),df_merge.SRC_CHANNEL_NAME.alias('CHANNEL_NAME'),df_merge.SRC_CONTACT_PERSON_NAME.alias('CONTACT_PERSON_NAME'),df_merge.SRC_ADDRESS_1.alias('ADDRESS_1'),df_merge.SRC_CITY_CODE.alias('CITY_CODE'),df_merge.SRC_CITY.alias('CITY'),df_merge.SRC_FAX.alias('FAX'),df_merge.SRC_PHONE_NUMBER.alias('PHONE_NUMBER'),df_merge.SRC_EMAIL_ADDRESS.alias('EMAIL_ADDRESS'),df_merge.SRC_OUTLET_OTIS_CODE.alias('OUTLET_OTIS_CODE'),df_merge.SRC_IS_ACTIVE.alias('IS_ACTIVE'),df_merge.SRC_IS_ASSORMENT.alias('IS_ASSORMENT'),df_merge.SRC_LAST_CALL_TIME.alias('LAST_CALL_TIME'),df_merge.SRC_LONGITUDE.alias('LONGITUDE'),df_merge.SRC_LATITUDE.alias('LATITUDE'),df_merge.SRC_IS_CONTRACTED.alias('IS_CONTRACTED'),df_merge.SRC_PROVINCE_CODE.alias('PROVINCE_CODE'),df_merge.SRC_PROVINCE.alias('PROVINCE'),df_merge.SRC_SUB_REGION_CODE.alias('SUB_REGION_CODE'),df_merge.SRC_EMP_CODE.alias('EMP_CODE'),df_merge.SRC_DISTRIBUTOR_NAME.alias('DISTRIBUTOR_NAME'),df_merge.SRC_ORG_ID.alias('ORG_ID'),df_merge.SRC_ORG_NAME.alias('ORG_NAME'),df_merge.SRC_ORG_CODE.alias('ORG_CODE'),df_merge.SRC_PRICING_GROUP.alias('PRICING_GROUP'),df_merge.SRC_EMP_NAME.alias('EMP_NAME'),df_merge.SRC_SUB_REGION.alias('SUB_REGION'),df_merge.SRC_REGION_CODE.alias('REGION_CODE'),df_merge.SRC_REGION.alias('REGION'),df_merge.SRC_STATE.alias('STATE'),df_merge.SRC_CREATED.alias('CREATED'),df_merge.SRC_UPDATED.alias('UPDATED'),df_merge.SRC_CORET_2.alias('CORET_2'),df_merge.SRC_EXCLUSIVED.alias('EXCLUSIVED'),df_merge.SRC_PRIORITY.alias('PRIORITY'),df_merge.SRC_FOCUS_CHANNEL.alias('FOCUS_CHANNEL'),df_merge.SRC_BATTLE_GROUND.alias('BATTLE_GROUND'),df_merge.SRC_KEY_DEALER_FLAG.alias('KEY_DEALER_FLAG'),df_merge.SRC_KEY_CUSTOMER_NAME.alias('KEY_CUSTOMER_NAME'),df_merge.SRC_SALES_PROD_FLAG.alias('SALES_PROD_FLAG'),df_merge.SRC_WHOLESALER_FLAG.alias('WHOLESALER_FLAG'),df_merge.SRC_CORE_STORE_SCAN_CHANNEL.alias('CORE_STORE_SCAN_CHANNEL'),df_merge.SRC_CORE_T2_DISTRIBUTION_CHANNEL.alias('CORE_T2_DISTRIBUTION_CHANNEL'),df_merge.SRC_IS_NEED_VISIT.alias('IS_NEED_VISIT'),df_merge.SRC_TYPE_ID.alias('TYPE_ID'),df_merge.SRC_TYPE_NAME.alias('TYPE_NAME'),df_merge.SRC_LEVEL_ID.alias('LEVEL_ID'),df_merge.SRC_LEVEL_NAME.alias('LEVEL_NAME'),df_merge.SRC_EMP_ID.alias('EMP_ID'),df_merge.TGT_SALES_REP_START_DATE.alias('OLD_SALES_REP_START_DATE'),df_merge.SALES_REP_CHANGE.alias('SALES_REP_CHANGE'))

#create a new dataframe for updating the tgt table old records
df_update = df_merge.filter(df_merge.ACTION == 'UPSERT')

# COMMAND ----------

#join the target with the dataframe with action to update the target table
df_final = df_tgt.join(df_merge, (df_merge.TGT_STORE_CODE == df_tgt.STORE_CODE) & (df_merge.TGT_ACTIVE_FLAG == 1), how = 'left_outer')

# COMMAND ----------

#update the target table inactive records and create a new final dataframe to be writted as target
df_final = df_final.withColumn('ACTIVE_FLAG',F.when((df_final.ACTION == 'UPSERT') | (df_final.ACTION == 'INACTIVE'),0).otherwise(df_final.ACTIVE_FLAG)).withColumn('DW_UPDATED_DATE',F.when((df_final.ACTION == 'UPSERT') | (df_final.ACTION == 'INACTIVE'),F.current_timestamp()).otherwise(df_final.DW_UPDATED_DATE)).withColumn('DW_UPDATED_BY',F.when((df_final.ACTION == 'UPSERT') | (df_final.ACTION == 'INACTIVE'),F.lit(dbutils.notebook.entry_point.getDbutils().notebook().getContext().notebookPath().get())).otherwise(df_final.DW_UPDATED_BY)).withColumn('SALES_REP_END_DATE',F.when(df_final.SALES_REP_CHANGE == 'UPDATE',F.current_timestamp()).otherwise(df_final.SALES_REP_END_DATE))

# COMMAND ----------

df_final = df_final.select('STORE_KEY','STORE_ID','STORE_NAME','STORE_CODE','STORE_TYPE','STORE_LEVEL','ASSORTMENT_CODE','ASSORTMENT_NAME','DISTRIBUTION_SEGEMENT_LCODE','DISTRIBUTION_SEGEMENT_LNAME','SUB_CHANNEL_CODE','SUB_CHANNEL_NAME','CHANNEL_CODE','CHANNEL_NAME','CITY_CODE','CITY','PROVINCE_CODE','PROVINCE',  'SUB_REGION_KEY','SUB_REGION_CODE','SUB_REGION','REGION_CODE','REGION','EMP_KEY','EMP_ID','EMP_CODE','EMP_NAME','SALES_REP_START_DATE','SALES_REP_END_DATE','DIST_STORE_KEY','DISTRIBUTOR_NAME','IS_CONTRACTED','PRICING_GROUP','CONTACT_PERSON_NAME','ADDRESS_1','FAX','PHONE_NUMBER','EMAIL_ADDRESS','OUTLET_OTIS_CODE','ORG_ID','ORG_NAME','ORG_CODE','STATE','IS_ACTIVE','IS_ASSORMENT','LAST_CALL_TIME','LONGITUDE','LATITUDE','CREATED','UPDATED','CORET_2','EXCLUSIVED','PRIORITY','FOCUS_CHANNEL','BATTLE_GROUND','KEY_DEALER_FLAG','KEY_CUSTOMER_NAME','SALES_PROD_FLAG','WHOLESALER_FLAG','CORE_STORE_SCAN_CHANNEL','CORE_T2_DISTRIBUTION_CHANNEL','IS_NEED_VISIT','TYPE_ID','TYPE_NAME','LEVEL_ID','LEVEL_NAME','ACTIVE_FLAG','DW_CREATED_DATE','DW_UPDATED_DATE','DW_CREATED_BY','DW_UPDATED_BY')

# COMMAND ----------

#Load the latest saved CSV in a dataframe
file_location = "/mnt/APAC/EXTERNAL_TABLES/DIMENSION/DIM_REGION/*.csv"
file_type = "csv"

# CSV options
infer_schema = "true"
first_row_is_header = "true"
delimiter = ","

# The applied options are for CSV files. For other file types, these will be ignored.
df_region = spark.read.format(file_type) \
  .option("inferSchema", infer_schema) \
  .option("header", first_row_is_header) \
  .option("sep", delimiter) \
  .load(file_location)

df_region = df_region.select('SUB_REGION_KEY','SUB_REGION_CODE','ACTIVE_FLAG')

df_region = df_region.withColumnRenamed('SUB_REGION_KEY','REGION_SUB_REGION_KEY').withColumnRenamed('SUB_REGION_CODE','REGION_SUB_REGION_CODE')\
.withColumnRenamed('ACTIVE_FLAG','REGION_ACTIVE_FLAG')

# COMMAND ----------

df_insert = df_insert.join(df_region, (df_insert.SUB_REGION_CODE == df_region.REGION_SUB_REGION_CODE) & (df_region.REGION_ACTIVE_FLAG == 1),how='left')

df_upsert = df_upsert.join(df_region, (df_upsert.SUB_REGION_CODE == df_region.REGION_SUB_REGION_CODE) & (df_region.REGION_ACTIVE_FLAG == 1),how='left')

# COMMAND ----------

from pyspark.sql.functions import lit

df_insert = df_insert.withColumn('STORE_KEY',lit(None))
df_upsert = df_upsert.withColumn('STORE_KEY',lit(None))

df_insert = df_insert.select('STORE_KEY','STORE_ID','STORE_NAME','STORE_CODE','STORE_TYPE','STORE_LEVEL','ASSORTMENT_CODE','ASSORTMENT_NAME','DISTRIBUTION_SEGEMENT_LCODE','DISTRIBUTION_SEGEMENT_LNAME','SUB_CHANNEL_CODE','SUB_CHANNEL_NAME','CHANNEL_CODE','CHANNEL_NAME','CITY_CODE','CITY','PROVINCE_CODE','PROVINCE',  'REGION_SUB_REGION_KEY','SUB_REGION_CODE','SUB_REGION','REGION_CODE','REGION','EMP_ID','EMP_CODE','EMP_NAME','DISTRIBUTOR_NAME','IS_CONTRACTED','PRICING_GROUP','CONTACT_PERSON_NAME','ADDRESS_1','FAX','PHONE_NUMBER','EMAIL_ADDRESS','OUTLET_OTIS_CODE','ORG_ID','ORG_NAME','ORG_CODE','STATE','IS_ACTIVE','IS_ASSORMENT','LAST_CALL_TIME','LONGITUDE','LATITUDE','CREATED','UPDATED','CORET_2','EXCLUSIVED','PRIORITY','FOCUS_CHANNEL','BATTLE_GROUND','KEY_DEALER_FLAG','KEY_CUSTOMER_NAME','SALES_PROD_FLAG','WHOLESALER_FLAG','CORE_STORE_SCAN_CHANNEL','CORE_T2_DISTRIBUTION_CHANNEL','IS_NEED_VISIT','TYPE_ID','TYPE_NAME','LEVEL_ID','LEVEL_NAME')

df_upsert = df_upsert.select('STORE_KEY','STORE_ID','STORE_NAME','STORE_CODE','STORE_TYPE','STORE_LEVEL','ASSORTMENT_CODE','ASSORTMENT_NAME','DISTRIBUTION_SEGEMENT_LCODE','DISTRIBUTION_SEGEMENT_LNAME','SUB_CHANNEL_CODE','SUB_CHANNEL_NAME','CHANNEL_CODE','CHANNEL_NAME','CITY_CODE','CITY','PROVINCE_CODE','PROVINCE',  'REGION_SUB_REGION_KEY','SUB_REGION_CODE','SUB_REGION','REGION_CODE','REGION','EMP_ID','EMP_CODE','EMP_NAME','DISTRIBUTOR_NAME','IS_CONTRACTED','PRICING_GROUP','CONTACT_PERSON_NAME','ADDRESS_1','FAX','PHONE_NUMBER','EMAIL_ADDRESS','OUTLET_OTIS_CODE','ORG_ID','ORG_NAME','ORG_CODE','STATE','IS_ACTIVE','IS_ASSORMENT','LAST_CALL_TIME','LONGITUDE','LATITUDE','CREATED','UPDATED','CORET_2','EXCLUSIVED','PRIORITY','FOCUS_CHANNEL','BATTLE_GROUND','KEY_DEALER_FLAG','KEY_CUSTOMER_NAME','SALES_PROD_FLAG','WHOLESALER_FLAG','CORE_STORE_SCAN_CHANNEL','CORE_T2_DISTRIBUTION_CHANNEL','IS_NEED_VISIT','TYPE_ID','TYPE_NAME','LEVEL_ID','LEVEL_NAME','OLD_SALES_REP_START_DATE','SALES_REP_CHANGE')

df_insert = df_insert.withColumnRenamed('REGION_SUB_REGION_KEY','SUB_REGION_KEY')
df_upsert = df_upsert.withColumnRenamed('REGION_SUB_REGION_KEY','SUB_REGION_KEY')

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.functions import lit

df_insert = df_insert.withColumn('DW_CREATED_DATE',F.current_timestamp()).withColumn('DW_UPDATED_DATE',F.lit('1900-01-01')).withColumn('ACTIVE_FLAG',F.lit(1)).withColumn('SALES_REP_START_DATE',F.current_timestamp()).withColumn('SALES_REP_END_DATE',F.lit('1900-01-01')).withColumn('DW_CREATED_BY',F.lit(dbutils.notebook.entry_point.getDbutils().notebook().getContext().notebookPath().get())).withColumn('DW_UPDATED_BY',F.lit(dbutils.notebook.entry_point.getDbutils().notebook().getContext().notebookPath().get()))

df_upsert = df_upsert.withColumn('DW_CREATED_DATE',F.current_timestamp()).withColumn('DW_UPDATED_DATE',F.lit('1900-01-01')).withColumn('ACTIVE_FLAG',F.lit(1)).withColumn('SALES_REP_END_DATE',F.lit('1900-01-01')).withColumn('DW_CREATED_BY',F.lit(dbutils.notebook.entry_point.getDbutils().notebook().getContext().notebookPath().get())).withColumn('DW_UPDATED_BY',F.lit(dbutils.notebook.entry_point.getDbutils().notebook().getContext().notebookPath().get()))


df_upsert = df_upsert.withColumn('SALES_REP_START_DATE',F.when(df_upsert.SALES_REP_CHANGE == 'UPDATE', F.current_timestamp()).otherwise(df_upsert.OLD_SALES_REP_START_DATE))

from pyspark.sql.functions import unix_timestamp, from_unixtime

df_upsert = df_upsert.withColumn('DW_CREATED_DATE',from_unixtime(unix_timestamp('DW_CREATED_DATE', 'yyyy-MM-dd'))).withColumn('DW_UPDATED_DATE',from_unixtime(unix_timestamp('DW_UPDATED_DATE', 'yyyy-MM-dd')))

df_insert = df_insert.withColumn('DW_CREATED_DATE',from_unixtime(unix_timestamp('DW_CREATED_DATE', 'yyyy-MM-dd'))).withColumn('DW_UPDATED_DATE',from_unixtime(unix_timestamp('DW_UPDATED_DATE', 'yyyy-MM-dd')))

# COMMAND ----------

df_dist_insert = df_tgt_a.select('TGT_STORE_KEY','TGT_STORE_CODE')
df_dist_upsert = df_tgt_a.select('TGT_STORE_KEY','TGT_STORE_CODE')

df_dist_insert = df_dist_insert.withColumnRenamed('TGT_STORE_KEY','DIST_STORE_KEY').withColumnRenamed('TGT_STORE_CODE','DIST_STORE_CODE')
df_dist_upsert = df_dist_upsert.withColumnRenamed('TGT_STORE_KEY','DIST_STORE_KEY').withColumnRenamed('TGT_STORE_CODE','DIST_STORE_CODE')

# COMMAND ----------

df_insert = df_insert.join(df_dist_insert, df_insert.DISTRIBUTOR_NAME == df_dist_insert.DIST_STORE_CODE, how = 'left' )
df_upsert = df_upsert.join(df_dist_upsert, df_upsert.DISTRIBUTOR_NAME == df_dist_upsert.DIST_STORE_CODE, how = 'left' )

# COMMAND ----------

df_insert = df_insert.select('STORE_KEY','STORE_ID','STORE_NAME','STORE_CODE','STORE_TYPE','STORE_LEVEL','ASSORTMENT_CODE','ASSORTMENT_NAME','DISTRIBUTION_SEGEMENT_LCODE','DISTRIBUTION_SEGEMENT_LNAME','SUB_CHANNEL_CODE','SUB_CHANNEL_NAME','CHANNEL_CODE','CHANNEL_NAME','CITY_CODE','CITY','PROVINCE_CODE','PROVINCE',  'SUB_REGION_KEY','SUB_REGION_CODE','SUB_REGION','REGION_CODE','REGION','EMP_ID','EMP_CODE','EMP_NAME','SALES_REP_START_DATE','SALES_REP_END_DATE','DIST_STORE_KEY','DISTRIBUTOR_NAME','IS_CONTRACTED','PRICING_GROUP','CONTACT_PERSON_NAME','ADDRESS_1','FAX','PHONE_NUMBER','EMAIL_ADDRESS','OUTLET_OTIS_CODE','ORG_ID','ORG_NAME','ORG_CODE','STATE','IS_ACTIVE','IS_ASSORMENT','LAST_CALL_TIME','LONGITUDE','LATITUDE','CREATED','UPDATED','CORET_2','EXCLUSIVED','PRIORITY','FOCUS_CHANNEL','BATTLE_GROUND','KEY_DEALER_FLAG','KEY_CUSTOMER_NAME','SALES_PROD_FLAG','WHOLESALER_FLAG','CORE_STORE_SCAN_CHANNEL','CORE_T2_DISTRIBUTION_CHANNEL','IS_NEED_VISIT','TYPE_ID','TYPE_NAME','LEVEL_ID','LEVEL_NAME','ACTIVE_FLAG','DW_CREATED_DATE','DW_UPDATED_DATE','DW_CREATED_BY','DW_UPDATED_BY')


df_upsert = df_upsert.select('STORE_KEY','STORE_ID','STORE_NAME','STORE_CODE','STORE_TYPE','STORE_LEVEL','ASSORTMENT_CODE','ASSORTMENT_NAME','DISTRIBUTION_SEGEMENT_LCODE','DISTRIBUTION_SEGEMENT_LNAME','SUB_CHANNEL_CODE','SUB_CHANNEL_NAME','CHANNEL_CODE','CHANNEL_NAME','CITY_CODE','CITY','PROVINCE_CODE','PROVINCE',  'SUB_REGION_KEY','SUB_REGION_CODE','SUB_REGION','REGION_CODE','REGION','EMP_ID','EMP_CODE','EMP_NAME','SALES_REP_START_DATE','SALES_REP_END_DATE','DIST_STORE_KEY','DISTRIBUTOR_NAME','IS_CONTRACTED','PRICING_GROUP','CONTACT_PERSON_NAME','ADDRESS_1','FAX','PHONE_NUMBER','EMAIL_ADDRESS','OUTLET_OTIS_CODE','ORG_ID','ORG_NAME','ORG_CODE','STATE','IS_ACTIVE','IS_ASSORMENT','LAST_CALL_TIME','LONGITUDE','LATITUDE','CREATED','UPDATED','CORET_2','EXCLUSIVED','PRIORITY','FOCUS_CHANNEL','BATTLE_GROUND','KEY_DEALER_FLAG','KEY_CUSTOMER_NAME','SALES_PROD_FLAG','WHOLESALER_FLAG','CORE_STORE_SCAN_CHANNEL','CORE_T2_DISTRIBUTION_CHANNEL','IS_NEED_VISIT','TYPE_ID','TYPE_NAME','LEVEL_ID','LEVEL_NAME','ACTIVE_FLAG','DW_CREATED_DATE','DW_UPDATED_DATE','DW_CREATED_BY','DW_UPDATED_BY')

# COMMAND ----------

#Load the latest saved CSV in a dataframe
file_location = "/mnt/APAC/EXTERNAL_TABLES/DIMENSION/DIM_SFA_EMPLOYEE/*.csv"
file_type = "csv"

# CSV options
infer_schema = "true"
first_row_is_header = "true"
delimiter = ","

# The applied options are for CSV files. For other file types, these will be ignored.
df_sales_rep = spark.read.format(file_type) \
  .option("inferSchema", infer_schema) \
  .option("header", first_row_is_header) \
  .option("sep", delimiter) \
  .option("encoding", "UTF-8") \
  .load(file_location)

df_sales_rep = df_sales_rep.filter(df_sales_rep.ACTIVE_FLAG == 1)
df_sales_rep = df_sales_rep.select('EMP_KEY','EMP_CODE')
df_sales_rep = df_sales_rep.withColumnRenamed('EMP_CODE','DIM_EMP_CODE')

# COMMAND ----------

df_insert = df_insert.join(df_sales_rep, df_insert.EMP_CODE == df_sales_rep.DIM_EMP_CODE, how = 'left')
df_upsert = df_upsert.join(df_sales_rep, df_upsert.EMP_CODE == df_sales_rep.DIM_EMP_CODE, how = 'left')

# COMMAND ----------

df_insert = df_insert.select('STORE_KEY','STORE_ID','STORE_NAME','STORE_CODE','STORE_TYPE','STORE_LEVEL','ASSORTMENT_CODE','ASSORTMENT_NAME','DISTRIBUTION_SEGEMENT_LCODE','DISTRIBUTION_SEGEMENT_LNAME','SUB_CHANNEL_CODE','SUB_CHANNEL_NAME','CHANNEL_CODE','CHANNEL_NAME','CITY_CODE','CITY','PROVINCE_CODE','PROVINCE',  'SUB_REGION_KEY','SUB_REGION_CODE','SUB_REGION','REGION_CODE','REGION','EMP_KEY','EMP_ID','EMP_CODE','EMP_NAME','SALES_REP_START_DATE','SALES_REP_END_DATE','DIST_STORE_KEY','DISTRIBUTOR_NAME','IS_CONTRACTED','PRICING_GROUP','CONTACT_PERSON_NAME','ADDRESS_1','FAX','PHONE_NUMBER','EMAIL_ADDRESS','OUTLET_OTIS_CODE','ORG_ID','ORG_NAME','ORG_CODE','STATE','IS_ACTIVE','IS_ASSORMENT','LAST_CALL_TIME','LONGITUDE','LATITUDE','CREATED','UPDATED','CORET_2','EXCLUSIVED','PRIORITY','FOCUS_CHANNEL','BATTLE_GROUND','KEY_DEALER_FLAG','KEY_CUSTOMER_NAME','SALES_PROD_FLAG','WHOLESALER_FLAG','CORE_STORE_SCAN_CHANNEL','CORE_T2_DISTRIBUTION_CHANNEL','IS_NEED_VISIT','TYPE_ID','TYPE_NAME','LEVEL_ID','LEVEL_NAME','ACTIVE_FLAG','DW_CREATED_DATE','DW_UPDATED_DATE','DW_CREATED_BY','DW_UPDATED_BY')


df_upsert = df_upsert.select('STORE_KEY','STORE_ID','STORE_NAME','STORE_CODE','STORE_TYPE','STORE_LEVEL','ASSORTMENT_CODE','ASSORTMENT_NAME','DISTRIBUTION_SEGEMENT_LCODE','DISTRIBUTION_SEGEMENT_LNAME','SUB_CHANNEL_CODE','SUB_CHANNEL_NAME','CHANNEL_CODE','CHANNEL_NAME','CITY_CODE','CITY','PROVINCE_CODE','PROVINCE',  'SUB_REGION_KEY','SUB_REGION_CODE','SUB_REGION','REGION_CODE','REGION','EMP_KEY','EMP_ID','EMP_CODE','EMP_NAME','SALES_REP_START_DATE','SALES_REP_END_DATE','DIST_STORE_KEY','DISTRIBUTOR_NAME','IS_CONTRACTED','PRICING_GROUP','CONTACT_PERSON_NAME','ADDRESS_1','FAX','PHONE_NUMBER','EMAIL_ADDRESS','OUTLET_OTIS_CODE','ORG_ID','ORG_NAME','ORG_CODE','STATE','IS_ACTIVE','IS_ASSORMENT','LAST_CALL_TIME','LONGITUDE','LATITUDE','CREATED','UPDATED','CORET_2','EXCLUSIVED','PRIORITY','FOCUS_CHANNEL','BATTLE_GROUND','KEY_DEALER_FLAG','KEY_CUSTOMER_NAME','SALES_PROD_FLAG','WHOLESALER_FLAG','CORE_STORE_SCAN_CHANNEL','CORE_T2_DISTRIBUTION_CHANNEL','IS_NEED_VISIT','TYPE_ID','TYPE_NAME','LEVEL_ID','LEVEL_NAME','ACTIVE_FLAG','DW_CREATED_DATE','DW_UPDATED_DATE','DW_CREATED_BY','DW_UPDATED_BY')

# COMMAND ----------

#union the Target dataframe with upsert and insert dataframes
df_final = df_final.union(df_upsert)
df_final = df_final.union(df_insert)

# COMMAND ----------

#display(df_final.filter(df_final.STORE_CODE == '20002324'))

# COMMAND ----------

df_final.createOrReplaceTempView('df_outlet')

df_final = sql('SELECT COALESCE(STORE_KEY, ROW_NUMBER() OVER(ORDER BY STORE_KEY)+(SELECT COALESCE(MAX(STORE_KEY), 0) FROM df_outlet)) AS STORE_KEY,STORE_ID,STORE_NAME,STORE_CODE,STORE_TYPE,STORE_LEVEL,ASSORTMENT_CODE,ASSORTMENT_NAME,DISTRIBUTION_SEGEMENT_LCODE,DISTRIBUTION_SEGEMENT_LNAME,SUB_CHANNEL_CODE,SUB_CHANNEL_NAME,CHANNEL_CODE,CHANNEL_NAME,CITY_CODE,CITY,PROVINCE_CODE,PROVINCE, SUB_REGION_KEY,SUB_REGION_CODE,SUB_REGION,REGION_CODE,REGION,EMP_KEY,EMP_ID,EMP_CODE,EMP_NAME,SALES_REP_START_DATE,SALES_REP_END_DATE,DIST_STORE_KEY,DISTRIBUTOR_NAME,IS_CONTRACTED,PRICING_GROUP,CONTACT_PERSON_NAME,ADDRESS_1,FAX,PHONE_NUMBER,EMAIL_ADDRESS,OUTLET_OTIS_CODE,ORG_ID,ORG_NAME,ORG_CODE,STATE,IS_ACTIVE,IS_ASSORMENT,LAST_CALL_TIME,LONGITUDE,LATITUDE,CREATED,UPDATED,CORET_2,EXCLUSIVED,PRIORITY,FOCUS_CHANNEL,BATTLE_GROUND,KEY_DEALER_FLAG,KEY_CUSTOMER_NAME,SALES_PROD_FLAG,WHOLESALER_FLAG,CORE_STORE_SCAN_CHANNEL,CORE_T2_DISTRIBUTION_CHANNEL,IS_NEED_VISIT,TYPE_ID,TYPE_NAME,LEVEL_ID,LEVEL_NAME,ACTIVE_FLAG,DW_CREATED_DATE,DW_UPDATED_DATE,DW_CREATED_BY,DW_UPDATED_BY FROM df_outlet')

# COMMAND ----------

from pyspark.sql.functions import *

df_final = df_final.withColumn('DW_CREATED_DATE',from_unixtime(unix_timestamp('DW_CREATED_DATE', 'yyyy-MM-dd'))).withColumn('DW_UPDATED_DATE',from_unixtime(unix_timestamp('DW_UPDATED_DATE', 'yyyy-MM-dd'))).withColumn('SALES_REP_START_DATE',from_unixtime(unix_timestamp('SALES_REP_START_DATE', 'yyyy-MM-dd'))).withColumn('SALES_REP_END_DATE',from_unixtime(unix_timestamp('SALES_REP_END_DATE', 'yyyy-MM-dd'))).withColumn('CREATED',from_unixtime(unix_timestamp('CREATED', 'yyyy-MM-dd'))).withColumn('UPDATED',from_unixtime(unix_timestamp('UPDATED', 'yyyy-MM-dd'))).withColumn('LAST_CALL_TIME',from_unixtime(unix_timestamp('LAST_CALL_TIME', 'yyyy-MM-dd')))

df_final = df_final.orderBy(df_final.STORE_KEY)

# COMMAND ----------

df_final = df_final.fillna({'SUB_REGION_KEY' : -1, 'EMP_KEY' : -1,'DIST_STORE_KEY' : -1})

# COMMAND ----------

df_final.coalesce(1).write.mode("overwrite").format("com.databricks.spark.csv").option("header", "true").csv("mnt/APAC/EXTERNAL_TABLES/DIMENSION/DIM_OUTLET")