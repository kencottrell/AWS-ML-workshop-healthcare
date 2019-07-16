# Databricks notebook source
import dbutils
import boto
import boto.s3
from boto.s3.key import Key
import StringIO
import pandas as pd
import pyspark.sql.functions as F
from pyspark.sql.functions import udf
import pyspark.sql.types as T

import calendar;
import time;
myTimeStamp = str(calendar.timegm(time.gmtime()))


keyId ="abc"
sKeyId="abc"
writeKeyID = "def"
writeSecKey = "def"


inputbucketName="kjcdiabetesdata"
#outBucketName = keyId.lower() + '-cleaned'
outBucketName = 'diabetes-data-cleaned-' + myTimeStamp

diabdata="diabetic_data.csv"
admitTypeLookupCodes = "admission_type.csv"
admitSourceLookupCodes = "admission_source.csv"
dischargeLookupCodes = "discharge_disposition.csv"

inconn = boto.connect_s3(keyId,sKeyId)
inbucket = inconn.get_bucket(inputbucketName)

k1 = Key(inbucket,diabdata)
k2 = Key(inbucket,admitTypeLookupCodes)
k3 = Key(inbucket,admitSourceLookupCodes)
k4 = Key(inbucket,dischargeLookupCodes)

outconn = boto.connect_s3(writeKeyID, writeSecKey)
outbucket = outconn.create_bucket(outBucketName,
    location=boto.s3.connection.Location.DEFAULT)
keyout = Key(outbucket)







# COMMAND ----------

diabContent = k1.get_contents_as_string()
admitTypeCodes = k2.get_contents_as_string()
admitSourceCodes = k3.get_contents_as_string()
dischargeCodes = k4.get_contents_as_string()

diabDF = pd.read_csv(StringIO.StringIO(diabContent))
testdf = pd.read_csv(StringIO.StringIO(diabContent))

adtTypeCodesDF = pd.read_csv(StringIO.StringIO(admitTypeCodes))
adtSourceCodesDF = pd.read_csv(StringIO.StringIO(admitSourceCodes))
dischargeCodesDF = pd.read_csv(StringIO.StringIO(dischargeCodes))



# COMMAND ----------

diabDF.count

# COMMAND ----------

def AgeBucketize(colval):
    # do stuff to column here, read decade ranges as [0-10], [10-20] etc
    
    newval = colval
    agedecade = colval[1]
    # print(colval)
    if(agedecade == '0' or agedecade == '1' ):
      newval = "Young"
    elif (agedecade > '1' and agedecade < '5'):
      newval = "Adult"
    elif (agedecade >= '5'):
      newval = "Old"
    else:
       newval = "na"
    return newval
  

# ages = diabDF.ix[:, 'age']  # N = ages.size      NN = ages.shape   print("length = ", NN)

#for i in ages:
#  diabDF.ix[i, 'age'] = AgeBucketize(i)
#  print(diabDF.ix[i, 'encounter_id'] , '--', diabDF.ix[i, 'age'])

def ReadmitCategorize(admitval):
  newval =  "Unknown"
  level = admitval[0]
  if level == 'N':  # NO
      newval = "NO"
  elif level == '<':   #  <30
    newval = "YES"
  elif level  == '>':    #  >30
    newval = "NO"
 
  return newval


# COMMAND ----------

spark_df = sqlContext.createDataFrame(diabDF)

# COMMAND ----------

spark_df.select('age').show()

# COMMAND ----------

UdfAgeCategorize = udf(AgeBucketize, T.StringType())
dfstage1 = spark_df.withColumn('age', UdfAgeCategorize(spark_df['age']))




# COMMAND ----------

dfstage1.select('age').show()   # show ages binned into 3 levels

# COMMAND ----------

dfstage2 = dfstage1.select([c for c in spark_df.columns if c not in {'weight', 'payer_code','medical_specialty', }])

# COMMAND ----------

dfstage2.printSchema()

# COMMAND ----------

dfstage2.select('readmitted').show(n=20)        # before making target into binary buckets

# COMMAND ----------

UdfReadmit = udf(ReadmitCategorize, T.StringType())
dfstage3 = dfstage2.withColumn('readmitted', UdfReadmit(dfstage2['readmitted']))


# COMMAND ----------


dfstage3.select('readmitted').show(n=20)


# COMMAND ----------

dfstage4 = dfstage3.toPandas()
cleanedFile = myTimeStamp + '_kjcout.csv'
dfstage4.to_csv(cleanedFile, encoding='utf-8', index=False)
keyout.set_contents_from_filename(cleanedFile) 


# COMMAND ----------

# MAGIC %sh pwd ; ls -al 

# COMMAND ----------


