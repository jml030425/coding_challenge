# Databricks notebook source
# MAGIC %md
# MAGIC #DATA LOAD

# COMMAND ----------

from pyspark.sql.functions import *

# COMMAND ----------

#reading the json file
receipt_df1 = spark.read.json('/FileStore/tables/receipts_json.gz')


#exploding the item list array
receipt_df2 = receipt_df1.select("*",explode("rewardsReceiptItemList").alias('ITEM_LIST'))


#extracting values, flattening ITEM_LIST, converting the date fields from milliseconds to timestamps, casting text fields to upper case
receipt_df3 = receipt_df2.select(upper("_id.$oid").alias('RECEIPT_ID')
                , col("bonusPointsEarned").cast("smallint").alias('TTL_BONUS_PTS_EARN_QTY')
                , upper("bonusPointsEarnedReason").alias('TTL_BONUS_PTS_EARN_RSN')
                , to_timestamp(col("createDate.$date")/1000).alias('EVENT_CREATE_UTC_TMS')
                , to_timestamp(col("dateScanned.$date")/1000).alias('SCAN_UTC_TMS')
                , to_timestamp(col("finishedDate.$date")/1000).alias('FINISH_UTC_TMS')
                , to_timestamp(col("modifyDate.$date")/1000).alias('MODIFY_UTC_TMS')
                , to_timestamp(col("pointsAwardedDate.$date")/1000).alias('PTS_AWD_UTC_TMS')
                , col("pointsEarned").cast("decimal(18,2)").alias('TTL_PTS_EARN_QTY')
                , to_timestamp(col("purchaseDate.$date")/1000).alias('PURCH_UTC_TMS')
                , col("purchasedItemCount").cast("smallint").alias('TTL_PURCH_ITEM_QTY')                
                , upper("rewardsReceiptStatus").alias('RECEIPT_STATUS')
                , col("totalSpent").cast("decimal(18,2)").alias('TTL_SPENT_AMT')
                , upper("userId").alias('USER_ID')
                , upper(col("ITEM_LIST.barcode")).alias('ITEM_BARCODE')
                , upper("ITEM_LIST.brandCode").alias('BRAND_CD')
                , upper("ITEM_LIST.competitiveProduct").alias('COMPETITIVE_PROD_IND')
                , upper("ITEM_LIST.competitorRewardsGroup").alias('COMPETITOR_REWARD_GRP')
                , upper("ITEM_LIST.deleted").alias('DELETED_IND')
                , upper("ITEM_LIST.description").alias('ITEM_DESC')
                , upper("ITEM_LIST.discountedItemPrice").cast("decimal(18,2)").alias('ITEM_DISCNT_PRICE')
                , upper("ITEM_LIST.finalPrice").cast("decimal(18,2)").alias('ITEM_FINAL_PRICE')
                , upper("ITEM_LIST.itemNumber").alias('ITEM_NBR_TXT')
                , upper("ITEM_LIST.itemPrice").cast("decimal(18,2)").alias('ITEM_PRICE')
                , upper("ITEM_LIST.metabriteCampaignId").alias('METABRITE_CAMPAIGN_ID')
                , upper("ITEM_LIST.needsFetchReview").alias('FETCH_REVIEW_NEED_IND')
                , upper("ITEM_LIST.needsFetchReviewReason").alias('FETCH_REVIEW_NEED_RSN')
                , col("ITEM_LIST.originalFinalPrice").cast("decimal(18,2)").alias('ITEM_ORGNL_FINAL_PRICE')
                , upper("ITEM_LIST.originalMetaBriteBarcode").alias('ORGNL_METABRITE_BARCODE')
                , upper("ITEM_LIST.originalMetaBriteDescription").alias('ORGNL_METABRITE_DESC')
                , col("ITEM_LIST.originalMetaBriteItemPrice").cast("decimal(18,2)").alias('ORGNL_METABRITE_ITEM_PRICE')
                , col("ITEM_LIST.originalMetaBriteQuantityPurchased").cast("smallint").alias('ORGNL_METABRITE_PURCH_QTY')
                , upper("ITEM_LIST.originalReceiptItemText").alias('ORGNL_RECEIPT_ITEM_TXT')
                , upper("ITEM_LIST.partnerItemId").alias('PARTNER_ITEM_ID')
                , col("ITEM_LIST.pointsEarned").cast("decimal(18,2)").alias('ITEM_PTS_EARN_QTY')
                , upper("ITEM_LIST.pointsNotAwardedReason").alias('PTS_NOT_AWD_RSN')
                , upper("ITEM_LIST.pointsPayerId").alias('PTS_PAYER_ID')
                , upper("ITEM_LIST.preventTargetGapPoints").alias('PREVENT_TARGET_GAP_PTS_IND')
                , col("ITEM_LIST.priceAfterCoupon").cast("decimal(18,2)").alias('PRICE_AFTER_COUPON')
                , col("ITEM_LIST.quantityPurchased").cast("smallint").alias('PURCH_QTY')
                , upper("ITEM_LIST.rewardsGroup").alias('REWARD_GRP')
                , upper("ITEM_LIST.rewardsProductPartnerId").alias('REWARD_PROD_PARTNER_ID')
                , col("ITEM_LIST.targetPrice").cast("decimal(18,2)").alias('TARGET_PRICE')
                , upper("ITEM_LIST.userFlaggedBarcode").alias('USER_FLAGGED_BARCODE')
                , upper("ITEM_LIST.userFlaggedDescription").alias('USER_FLAGGED_DESC')
                , upper("ITEM_LIST.userFlaggedNewItem").alias('USER_FLAGGED_NEW_ITEM_IND')
                , upper("ITEM_LIST.userFlaggedPrice").cast("decimal(18,2)").alias('USER_FLAGGED_PRICE')
                , upper("ITEM_LIST.userFlaggedQuantity").cast("smallint").alias('USER_FLAGGED_QTY')
)

#updating boolean columns to Y/N
receipt_df4 = (receipt_df3
               .withColumn('COMPETITIVE_PROD_IND', when(col('COMPETITIVE_PROD_IND') == 'TRUE', 'Y').when(col('COMPETITIVE_PROD_IND') == 'FALSE', 'N'))
               .withColumn('DELETED_IND', when(col('DELETED_IND') == 'TRUE', 'Y').when(col('DELETED_IND') == 'FALSE', 'N'))
               .withColumn('FETCH_REVIEW_NEED_IND', when(col('FETCH_REVIEW_NEED_IND') == 'TRUE', 'Y').when(col('FETCH_REVIEW_NEED_IND') == 'FALSE', 'N'))
               .withColumn('PREVENT_TARGET_GAP_PTS_IND', when(col('PREVENT_TARGET_GAP_PTS_IND') == 'TRUE', 'Y').when(col('PREVENT_TARGET_GAP_PTS_IND') == 'FALSE', 'N'))
               .withColumn('USER_FLAGGED_NEW_ITEM_IND', when(col('USER_FLAGGED_NEW_ITEM_IND') == 'TRUE', 'Y').when(col('USER_FLAGGED_NEW_ITEM_IND') == 'FALSE', 'N'))
)
                                                                                                      
receipt_df4.createOrReplaceTempView('RECEIPTS')

# COMMAND ----------

#reading json file
user_df1 = spark.read.json('/FileStore/tables/users.json')

#extracting values, converting the date fields from milliseconds to timestamps, casting text fields to upper case, removing duplicates, changing boolean to Y/N
user_df2 = user_df1.select(upper(col("_id.$oid")).alias('USER_ID')
                 , upper(col("active")).cast('string').alias('ACTIVE_IND')
                 , upper(col("role")).alias('ROLE')
                 , upper(col("state")).alias('STATE_CD')
                 , upper(col("signUpSource")).alias('SIGN_UP_SRC')
                 , to_timestamp(col("createdDate.$date")/1000).alias('CREATE_UTC_TMS')
                 , to_timestamp(col("lastLogin.$date")/1000).alias('LAST_LOGIN_UTC_TMS')  
).withColumn('ACTIVE_IND', when(col('ACTIVE_IND') == 'TRUE', 'Y').when(col('ACTIVE_IND') == 'FALSE', 'N')).distinct()

user_df2.createOrReplaceTempView('USERS')

# COMMAND ----------

#reading json file
brand_df1 = spark.read.json('/FileStore/tables/brands.json')

#extracting values, casting text fields to upper case, changing boolean to Y/N
brand_df2 = brand_df1.select(upper(col("_id.$oid")).alias('BRAND_ID')
                 , upper(col("barcode")).alias('ITEM_BARCODE')
                 , upper(col("cpg.$id.$oid")).alias('CPG')
                 , upper(col("brandCode")).alias('BRAND_CD')
                 , upper(col("name")).alias('BRAND_NM')
                 , upper(col("categoryCode")).alias('CATEGORY_CD')
                 , upper(col("category")).alias('CATEGORY_NM')
                 , upper(col("topBrand")).alias('TOP_BRAND_IND')
).withColumn('TOP_BRAND_IND', when(col('TOP_BRAND_IND') == 'TRUE', 'Y').when(col('TOP_BRAND_IND') == 'FALSE', 'N'))

brand_df2.createOrReplaceTempView('BRAND')

# COMMAND ----------

# MAGIC %sql
# MAGIC create or replace temp view RECEIPTS_SUMMARY as 
# MAGIC select distinct RECEIPT_ID
# MAGIC   , USER_ID
# MAGIC   , RECEIPT_STATUS
# MAGIC   , TTL_PURCH_ITEM_QTY
# MAGIC   , TTL_SPENT_AMT
# MAGIC   , TTL_PTS_EARN_QTY
# MAGIC   , TTL_BONUS_PTS_EARN_QTY
# MAGIC   , TTL_BONUS_PTS_EARN_RSN
# MAGIC   , PURCH_UTC_TMS
# MAGIC   , EVENT_CREATE_UTC_TMS
# MAGIC   , SCAN_UTC_TMS
# MAGIC   , MODIFY_UTC_TMS
# MAGIC   , FINISH_UTC_TMS
# MAGIC   , PTS_AWD_UTC_TMS
# MAGIC from RECEIPTS

# COMMAND ----------

# MAGIC %sql
# MAGIC create or replace temp view RECEIPTS_DTL as 
# MAGIC select r.RECEIPT_ID || row_number() over(partition by r.RECEIPT_ID order by r.ITEM_BARCODE) as RECEIPT_DTL_ID
# MAGIC   , r.RECEIPT_ID
# MAGIC   , r.USER_ID
# MAGIC   , r.RECEIPT_STATUS 
# MAGIC   , r.ITEM_BARCODE
# MAGIC   , b.BRAND_NM
# MAGIC   , b.CATEGORY_NM
# MAGIC   , r.ITEM_DESC
# MAGIC   , r.ITEM_NBR_TXT
# MAGIC   , r.PURCH_QTY
# MAGIC   , r.ITEM_PRICE
# MAGIC   , r.ITEM_DISCNT_PRICE
# MAGIC   , r.PRICE_AFTER_COUPON
# MAGIC   , r.ITEM_FINAL_PRICE
# MAGIC   , r.TARGET_PRICE
# MAGIC   , r.PTS_PAYER_ID
# MAGIC   , r.ITEM_PTS_EARN_QTY
# MAGIC   , r.PTS_NOT_AWD_RSN
# MAGIC   , r.REWARD_GRP
# MAGIC   , r.REWARD_PROD_PARTNER_ID
# MAGIC   , r.PARTNER_ITEM_ID
# MAGIC   , r.COMPETITIVE_PROD_IND
# MAGIC   , r.COMPETITOR_REWARD_GRP
# MAGIC   , r.METABRITE_CAMPAIGN_ID
# MAGIC   , r.ORGNL_METABRITE_BARCODE
# MAGIC   , r.ORGNL_METABRITE_DESC
# MAGIC   , r.ORGNL_METABRITE_ITEM_PRICE
# MAGIC   , r.ORGNL_METABRITE_PURCH_QTY
# MAGIC   , r.ORGNL_RECEIPT_ITEM_TXT
# MAGIC   , r.ITEM_ORGNL_FINAL_PRICE
# MAGIC   , r.PREVENT_TARGET_GAP_PTS_IND
# MAGIC   , r.DELETED_IND
# MAGIC   , r.USER_FLAGGED_BARCODE
# MAGIC   , r.USER_FLAGGED_DESC
# MAGIC   , r.USER_FLAGGED_NEW_ITEM_IND
# MAGIC   , r.USER_FLAGGED_PRICE
# MAGIC   , r.USER_FLAGGED_QTY
# MAGIC   , r.FETCH_REVIEW_NEED_IND
# MAGIC   , r.FETCH_REVIEW_NEED_RSN
# MAGIC   , r.PURCH_UTC_TMS
# MAGIC   , r.EVENT_CREATE_UTC_TMS
# MAGIC   , r.SCAN_UTC_TMS
# MAGIC   , r.MODIFY_UTC_TMS
# MAGIC   , r.FINISH_UTC_TMS
# MAGIC   , r.PTS_AWD_UTC_TMS
# MAGIC from RECEIPTS r
# MAGIC left join brand b
# MAGIC on r.ITEM_BARCODE = b.ITEM_BARCODE
# MAGIC   and (b.ITEM_BARCODE <> '511111704140' and b.BRAND_NM <> 'DIET CHRIS COLA') 
# MAGIC   --barcode is duplicated in brand causing duplicate records here; removing the specific brand record above as it is not used in the receipts data at all

# COMMAND ----------

# MAGIC %md
# MAGIC #QUERIES TO ANSWER QUESTIONS

# COMMAND ----------

# MAGIC %md
# MAGIC #### What are the top 5 brands by receipts scanned for most recent month?
# MAGIC #### How does the ranking of the top 5 brands by receipts scanned for the recent month compare to the ranking for the previous month
# MAGIC ###### Both of these can be interpreted by top brands by volume or by spend; also ignoring nulls since we don't know brand name for these
# MAGIC ###### Due to missing barcodes in the brand table, unable to identify brands from the most recent records without parsing from ITEM_DESC

# COMMAND ----------

# MAGIC %sql
# MAGIC --Only Jan 2021 data could match barcodes to the brand table, none of the other months had barcodes that matched; further cleansing/understanding woud be needed to fill the gap
# MAGIC select trunc(SCAN_UTC_TMS,'month') MNTH_DT
# MAGIC   , BRAND_NM
# MAGIC   , sum(ITEM_FINAL_PRICE*PURCH_QTY) TTL_SPEND
# MAGIC from RECEIPTS_DTL
# MAGIC where BRAND_NM is not null
# MAGIC group by 1,2
# MAGIC qualify row_number() over(partition by MNTH_DT order by TTL_SPEND desc) <= 5
# MAGIC order by 1 desc, 3 desc
# MAGIC
# MAGIC
# MAGIC -- MNTH_DT	  BRAND_NM	            TTL_SPEND
# MAGIC -- 2021-01-01	CRACKER BARREL CHEESE	2251.20
# MAGIC -- 2021-01-01	CHEETOS	              132.00
# MAGIC -- 2021-01-01	SWANSON	              122.76
# MAGIC -- 2021-01-01	TOSTITOS	            80.66
# MAGIC -- 2021-01-01	MOUNTAIN DEW	        71.92

# COMMAND ----------

# MAGIC %md
# MAGIC #### When considering average spend from receipts with 'rewardsReceiptStatus’ of ‘Accepted’ or ‘Rejected’, which is greater?
# MAGIC ##### Accepted is not an option, but FINISHED is greater
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC select RECEIPT_STATUS
# MAGIC   , avg(TTL_SPENT_AMT) AVG_SPEND
# MAGIC from RECEIPTS_SUMMARY 
# MAGIC where RECEIPT_STATUS in ('FINISHED','REJECTED') --there is no accepted, so assuming finished is what you're looking for
# MAGIC group by 1
# MAGIC
# MAGIC -- RECEIPT_STATUS	AVG_SPEND
# MAGIC -- FINISHED	      81.167694
# MAGIC -- REJECTED	      24.355147

# COMMAND ----------

# MAGIC %md
# MAGIC #### When considering total number of items purchased from receipts with 'rewardsReceiptStatus’ of ‘Accepted’ or ‘Rejected’, which is greater?
# MAGIC ###### FINISHED is greater
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC select RECEIPT_STATUS
# MAGIC   , sum(TTL_PURCH_ITEM_QTY) TTL_ITEMS_PURCH
# MAGIC   , avg(TTL_PURCH_ITEM_QTY) AVG_ITEMS_PURCH
# MAGIC from RECEIPTS_SUMMARY 
# MAGIC where RECEIPT_STATUS in ('FINISHED','REJECTED') --there is no accepted, so assuming finished is what you're looking for
# MAGIC group by 1
# MAGIC
# MAGIC -- RECEIPT_STATUS	  TTL_ITEMS_PURCH	  AVG_ITEMS_PURCH
# MAGIC -- FINISHED	        8184	            15.86046511627907
# MAGIC -- REJECTED	        173	              2.5441176470588234

# COMMAND ----------

# MAGIC %md
# MAGIC #### Which brand has the most spend among users who were created within the past 6 months?
# MAGIC #### Which brand has the most transactions among users who were created within the past 6 months?
# MAGIC
# MAGIC ###### Given the data is from 2021, I can only mimick what I would do if I actually had the data, so for the sake of this exercise, it's April 1, 2021
# MAGIC
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC select BRAND_NM
# MAGIC   , sum(ITEM_FINAL_PRICE*PURCH_QTY) TTL_SPEND
# MAGIC   -- , count(distinct receipt_id) TRANS_CNT --assuming 1 receipt = transaction
# MAGIC from RECEIPTS_DTL r
# MAGIC join users u
# MAGIC on r.USER_ID = u.USER_ID
# MAGIC   and cast(u.CREATE_UTC_TMS as date) > dateadd(month,-6, '2021-04-01')  --if it had current data, this is what I would use DATEADD(MONTH, -6, CURRENT_DATE())
# MAGIC where BRAND_NM is not null
# MAGIC group by 1
# MAGIC order by 2 desc 
# MAGIC
# MAGIC
# MAGIC -- BRAND_NM	              TTL_SPEND
# MAGIC -- CRACKER BARREL CHEESE	1041.18
# MAGIC
# MAGIC -- BRAND_NM	TRANS_CNT
# MAGIC -- TOSTITOS	11
# MAGIC -- SWANSON	11
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC #DATA QUALITY

# COMMAND ----------



BRANDS:
  1.The source is incomplete with many missing barcodes not being included in the table.
  2.Several barcodes are tied to 2 different brands, and from my understanding, every barcode should be unique.
  3.Almost all fields have nulls and/or blanks "   " and we would need to understand where the gap is coming from.
  4.There seems to be a lot of test data that would need to be excluded from PROD.
  5.BRAND_CD seems to be the same or slight variation of BRAND_NM. It would be more beenficial to have an actual code, or switch to using BRAND_ID. This is also showing nulls.
  6.There didn't seem to be a rollup for brands, i.e. ABSOLUT MANDARIN, LEMON, etc. can tie to ABSOLUT. It would be beneficial to see the analytics behind the rollup.
  7.The nulls in TOP_BRAND_IND should be changed to N, unless there's some other reason for keeping it null
  8.A lot of missing categories in general. Some CATEGORY_CDs are null when the same CATEGORY_NM is being used. I.E. a record has BABY for both CATERGORY & CATEGORY_CD while another record only shows BABY for CATEGORY_NM. 
  9.BRAND_CD was sometimes the same as ITEM_BARCODE.


USERS:
  10.The source is incomplete with USER_IDs showing up in the RECEIPTS_SUMMARY / RECEIPTS_DTL tables, but not in the USERS table.
  11.The ROLE column also includes FETCH-STAFF; if this is for testing purposes, we would want to remove transactions tied to this.
  12.Also missing data under SIGN_UP_SRC, STATE_CD, and LAST_LOGIN_UTC_TMS.
  13.For LAST_LOGIN_UTC_TMS, if they dont login after creating the account, should account creation timestamp be the last login?
  14.Duplicate USER_IDs found in the table.


RECEIPTS:
  15.A lot of nulls throughout the data.
  16.The receipt items don't match the receipt summary totals when rolled up. This may be not enough information on my end and/or lack of understanding.
  17.Quite a lot of ITEM NOT FOUND descriptions. Are these additional gaps we need to fill in or potential errors?
  18.The indicator columns should change nulls to N, unless the nulls serve a specific purpose.
  19.I don't see anything regarding tax. Not sure if this is captured elsewhere.
  20.If there was an amount in ITEM_DISCNT_PRICE or PRICE_AFTER_COUPON, it seemed to match ITEM_PRICE as well. It would be beneficial to see the before and after pricing.
  21.ITEM_NBR_TXT seemed to be the same as ITEM_BARCODE. Is this column necessary? 



# COMMAND ----------

# MAGIC %sql
# MAGIC --8.Some CATEGORY_CDs are null when the same CATEGORY_NM is being used. I.E. a records has BABY for both CATERGORY_NM & CATEGORY_CD while another only shows BABY for CATEGORY.
# MAGIC
# MAGIC select distinct category_nm, category_cd
# MAGIC from brand

# COMMAND ----------

# MAGIC %sql
# MAGIC --10.The source is incomplete with USER_IDs showing up in the RECEIPTS_SUMMARY / RECEIPTS_DTL tables, but not in the USERS table.
# MAGIC
# MAGIC select count(distinct a.user_id) user_cnt
# MAGIC   , count(distinct b.user_id) user_mtch_cnt
# MAGIC   , 1 - (user_mtch_cnt/user_cnt) pct_missing
# MAGIC from receipts_summary a
# MAGIC left join users b 
# MAGIC on a.user_id = b.user_id
# MAGIC
# MAGIC -- user_cnt	user_mtch_cnt	pct_missing
# MAGIC -- 246	    133	          0.45934959349593496

# COMMAND ----------

# MAGIC %sql
# MAGIC -- 16.The receipt items don't match the receipt summary totals when rolled up. This may be not enough information on my end and lack of understanding.
# MAGIC
# MAGIC with DTL_ROLLUP as  
# MAGIC   (
# MAGIC     select RECEIPT_ID
# MAGIC       , sum(PURCH_QTY*ITEM_FINAL_PRICE) TTL_PAID
# MAGIC       , sum(PURCH_QTY) PURCH_QTY
# MAGIC       , sum(ITEM_PTS_EARN_QTY) PTS_EARN
# MAGIC     from RECEIPTS_DTL
# MAGIC     group by 1
# MAGIC   )
# MAGIC select a.RECEIPT_ID
# MAGIC   , a.TTL_PURCH_ITEM_QTY
# MAGIC   , b.PURCH_QTY
# MAGIC   , a.TTL_SPENT_AMT
# MAGIC   , b.TTL_PAID
# MAGIC   , a.TTL_PTS_EARN_QTY
# MAGIC   , b.PTS_EARN
# MAGIC   , a.RECEIPT_STATUS
# MAGIC from RECEIPTS_SUMMARY a 
# MAGIC left join DTL_ROLLUP b 
# MAGIC on a.RECEIPT_ID = b.RECEIPT_ID
# MAGIC where  a.TTL_PURCH_ITEM_QTY <> b.PURCH_QTY
# MAGIC   or a.TTL_SPENT_AMT <> b.TTL_PAID
# MAGIC   or a.TTL_PTS_EARN_QTY <> b.PTS_EARN
# MAGIC
# MAGIC --289 records of 679; roughly 43% don't align - further details & understanding needed
# MAGIC --40 of 679 purchase qty misalign
# MAGIC --165 of 678 total spend misalign
# MAGIC --154 of 679 points earned misalign

# COMMAND ----------

# MAGIC %sql
# MAGIC select *
# MAGIC from receipts 
# MAGIC where receipt_id = '5FF29BE20A7214ADA1000571'
# MAGIC
# MAGIC -- item description needs to be cleaned up... i.e. "-cheese" as shown for receipt_id = '5FF29BE20A7214ADA1000571'

# COMMAND ----------

# MAGIC %sql
# MAGIC --14.Duplicate USER_IDs found in the table.
# MAGIC --Used distinct function to take care of this when creating the USERS table.
# MAGIC
# MAGIC select user_id
# MAGIC   , count(*)
# MAGIC from users
# MAGIC group by 1
# MAGIC having count(*) > 1

# COMMAND ----------

# MAGIC %sql
# MAGIC --4.There seems to be a lot of test data that would need to be excluded from PROD.
# MAGIC
# MAGIC select  *
# MAGIC from brand
# MAGIC where brand_cd like '%TEST%'
# MAGIC or brand_nm like '%TEST%'

# COMMAND ----------

# MAGIC %sql
# MAGIC --verified none of these test brands are showing up in the receipt data
# MAGIC
# MAGIC select *
# MAGIC from receipts 
# MAGIC where item_barcode in (select item_barcode from brand where brand_cd like '%TEST%' or brand_nm like '%TEST%')

# COMMAND ----------

# MAGIC %sql
# MAGIC --2.Several barcodes are tied to 2 different brands, and from my understanding, a barcode can only be tied to one brand.
# MAGIC
# MAGIC select item_barcode 
# MAGIC   , count(distinct brand_id)
# MAGIC from brand 
# MAGIC group by 1
# MAGIC having count(distinct brand_id) > 1
# MAGIC
# MAGIC -- item_barcode
# MAGIC -- 511111204923
# MAGIC -- 511111504788
# MAGIC -- 511111004790
# MAGIC -- 511111605058
# MAGIC -- 511111305125
# MAGIC -- 511111704140
# MAGIC -- 511111504139

# COMMAND ----------

# MAGIC %sql
# MAGIC --only a handful of records are being affected by duplicate barcodes - all from PREGO
# MAGIC
# MAGIC with dupes as  
# MAGIC   (
# MAGIC     select item_barcode 
# MAGIC       , count(distinct brand_id) brand_cnt
# MAGIC     from brand 
# MAGIC     group by 1
# MAGIC     having count(distinct brand_id) > 1 
# MAGIC   )
# MAGIC select *
# MAGIC from receipts 
# MAGIC where item_barcode in (select item_barcode from dupes)  

# COMMAND ----------

# MAGIC %sql
# MAGIC --3.Almost all fields have nulls and/or blanks "   " and we would need to understand where the gap is coming from.
# MAGIC
# MAGIC select distinct brand_cd, brand_nm 
# MAGIC from brand
# MAGIC where brand_cd is null
# MAGIC or trim(brand_cd) = ''
# MAGIC order by 1 desc 