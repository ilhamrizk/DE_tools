from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window as W 

import pandas as pd
pd.options.display.max_columns = 999
pd.options.display.html.table_schema = True
spark = SparkSession \
    .builder \
    .appName("Ilhamr - plygrnd") \
    .config('spark.dynamicAllocation.enabled', 'false') \
    .config('spark.executor.instances', '8') \
    .config('spark.executor.cores', '2') \
    .config('spark.executor.memory', '16g') \
    .config('spark.yarn.executor.memoryOverhead', '8g') \
    .enableHiveSupport() \
    .getOrCreate()
    
class myInternalFunc:
  '''My internal function, for the sake of maintenance and table analizing 
  - by Ilham Rizky'''
  def __init__(self, tablename):
    self.source=spark.read.table(tablename)
    
  def enumerateDist(self, threshold=20):
    ''' enumerate all distinct element in the table. required tablename and 
    threshold (or the number of distinct needed) 20 by default.
    this function applying Devide and Conquer in partition file
    '''
    ## Internal function to get ditinct element more simple
    def distinctElement(source, columns):
      listoftuple = [(x+1,) for x in range(threshold)]
      df = spark.createDataFrame(listoftuple, schema = ['index'])
      for column in columns:
        print("Reading column: ", column)
        temp = source\
          .select(column)\
          .distinct()\
          .orderBy(column)\
          .withColumn("temp_rownum", F.row_number().over(W.orderBy(column)))\
          .limit(threshold)
        df = df.join(temp, df.index==temp.temp_rownum, 'left').drop('temp_rownum')
      return df.orderBy('index').drop('index')

    ## function to read all partition file in spesific column
    def readFromTables(column, row_name='temp_rownum'):
      df = spark.read.table("temp.sample_{}_{}".format(table, 0))
      df = df_column.select(column)
      for i in range(len(partitions)-1):
        df_temp = spark.read.table("temp.sample_{}_{}".format(table, i+1))
        df = df.union(df_temp)
      df = df\
          .orderBy(column)\
          .withColumn(row_name, F.row_number().over(W.orderBy(column)))\
          .limit(threshold)
      return df

    ## Set parameters
    source = self.source
    db, table = tablename.split(sep='.')
    
    print("Counting ...")
    columns = [ column for column in source.columns if source.select(column).distinct().count() < threshold ]
    print("only enumerating: ", columns)

    ## is the table have partition? if yes, DEVIDE AT IMPERA!!
    try:
      partitions = spark.sql("""SHOW PARTITIONS {}""".format(tablename)).limit(1).collect()
      print("ALERT!! Partition File, it'll takes longer time")

      ### iterate all partition file then yield some distinct element of each part
      for i, partition_str in enumerate(partitions):
        filter_condition = partition_str[0].split(sep='/')
        filter_condition = ' AND '.join(filter_condition)

        print("Executing {}".format(filter_condition))
        df_part = distinctElement(source.filter(filter_condition), columns)

        print("Saving into temp.sample_{}_{}".format(table,i))
        df_part.write\
          .format("parquet")\
          .mode("overwrite")\
          .saveAsTable("temp.sample_{}_{}".format(table, i))

      ### iterate read file, and yield global distinct elements of all partitions
      print("Re-reading")
      listoftuple = [(x+1,) for x in range(threshold)]
      df_column = spark.createDataFrame(listoftuple, schema = ['index'])
      for column in columns:
        df_temp = readFromTables(column)
        df_column = df_column.join(df_temp, df_temp.temp_rownum==df_column.column_rownum, 'left')
      df_all = df_column.orderBy('index').drop('index')

      ### Drop all sample table
      print("Drop all sample table")
      for i, partition_str in enumerate(partitions):
        spark.sql(""" DROP TABLE temp.sample_{}_{}""".format(table, i))
      return df_all
    
    ## if only the table not partitioned
    except:
      return distinctElement(self.source, columns)
  

#tablename = 'temp.adhoc_credit_card_kkd_concatenate'
tablename = 'datamart.master_dly_customer_info_brinets'
x = myInternalFunc(tablename).enumerateDist(10000)
db, table = tablename.split('.')
finaltable = 'temp.enumerate_table_'+table
x.write\
    .format("parquet")\
    .mode("overwrite")\
    .saveAsTable(finaltable)
spark.read.table(finaltable).toPandas()

#==========================================================================================================================
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window as W 

import pandas as pd
pd.options.display.max_columns = 999
pd.options.display.html.table_schema = True
spark = SparkSession \
    .builder \
    .appName("Ilhamr - test-test doang") \
    .config('spark.dynamicAllocation.enabled', 'false') \
    .config('spark.executor.instances', '2') \
    .config('spark.executor.cores', '2') \
    .config('spark.executor.memory', '4g') \
    .config('spark.yarn.executor.memoryOverhead', '2g') \
    .enableHiveSupport() \
    .getOrCreate()

## Set Variables
tablename="temp.adhoc_credit_card_kkd_concatenate"
x = 5

## Get Minimum and Maximum values of the data 
source=spark.read.table(tablename)
columns=source.columns
def getSelectScript(list):
  script=''
  for column in list:
    script= script + "MAX({0}) AS max_{0}, \n MIN({0}) AS min_{0},".format(column)
  return script[0:-3]
df = spark.sql("""
  SELECT {}
  FROM {}
""".format(getSelectScript(columns), tablename))
df.toPandas()

## Get Table Dictionary
def getDictionary():
  script=''
  for column in columns:
    script= script + "({0} IS NOT NULL AND TRIM({0}) NOT IN ('', 'null')) AND ".format(column)
  script = script[0:-4]
  source = spark.sql("""SELECT * FROM {} WHERE {}""".format(tablename, getConditionScript(tablename)))
  df = source.limit(100).toPandas().tail().iloc[0]
  types = [source.dtypes[x][1] for x in range(len(columns))]
  examples = [df[column] for column in columns]
  data = {'Attribute': columns, 
          'Tipe Data': types, 
          'Contoh Data': examples}
  pd_df = pd.DataFrame.from_dict(data, orient='columns')
  

## Get x Maximum and Minimum Columns from table
def scriptGenerator():
  script = "(SELECT {0} FROM {1} ORDER BY {0}) AS min_{0}".format(column, tablename)
  for column in columns:
    subquery = "(SELECT {0} FROM {1} ORDER BY {0}) AS min_{0}".format(column, tablename)
    
df=spark.sql("""SELECT * FROM """)


## Get x-maximum and minimum row from table 
source=spark.read.table(tablename)
columns=source.columns
c = iter(source.columns)
column = next(c)
df = source\
  .select(column)\
  .orderBy(column, ascending=0)\
  .limit(x)\
  .withColumn("row_num", F.row_number().over(W.orderBy(column)))
for i in range(len(columns)-1):
  column = next(c)
  jointmp  = source\
      .select(column)\
      .orderBy(column, ascending=0)\
      .limit(x)\
      .withColumn("row", F.row_number().over(W.orderBy(column)))
  df = df.join(jointmp, df.row_num==jointmp.row, "full").drop('row')
  
    
    
    
class myInternalFunc:
  '''My internal function, for the sake of maintenance and table analizing 
  - by Ilham Rizky'''
  def __init__(self, tablename):
    self.source=spark.read.table(tablename)
  
  def getExtremValues(self, x, asc_desc):
    source= self.source
    columns = source.columns
    c = iter(columns)
    column = next(c)
    df = source\
      .select(column)\
      .orderBy(column, ascending=asc_desc)\
      .limit(x)\
      .withColumn("row_num", F.row_number().over(W.orderBy(column)))
    for i in range(len(columns)-1):
      column = next(c)
      jointmp  = source\
          .select(column)\
          .orderBy(column, ascending=asc_desc)\
          .limit(x)\
          .withColumn("row", F.row_number().over(W.orderBy(column)))
      df = df.join(jointmp, df.row_num==jointmp.row, "full").drop('row')
    return df

  def minValues(self, x):
    '''Get x-minimum rows from each columns of the tables
    PS: it required number of minimum values each row INT'''
    return self.getExtremValues(x, 1).orderBy('row_num', ascending=1).drop('row_num')

  def maxValues(self, x):
    '''Get x-maximum rows from each columns of the tables
    PS: it required number of maximum values each row INT'''
    return self.getExtremValues(x, 0).orderBy('row_num', ascending=0).drop('row_num')
  
  def countDistinct(self):
    ''' Count the number of distinct values of each columns'''
    source = self.source
    columns = source.columns
    for column in columns:
      distint_values = source.select(column).distinct().count()
      print("{} has {} unique values".format(column,distinct_values))
  
  def comprehentCount(self):
    ''' Comprehensive count, yield the number of null/blank values and the percentages '''
    source = self.source
    columns = source.columns
    total_rows=source.count()
    print("{} has {} total rows".format(tablename, total_rows))
    for column in columns:
      null_values = source.select(column).filter("{0} IS NULL OR TRIM({0}) IN ('', 'null', 'NULL') OR CAST({0} AS BIGINT)=0".format(column)).count()
      percentage = null_values*100/total_rows
      print('{} has {} null/blank rows or {}% of total rows'.format(column, null_values, percentage))

  def colSummary(self, col):
    ''' Summary of the column yield elements to summarize the values just like pandas' describe/summary 
    PS: it required column name
    '''
    source = self.source
    col='usia'
    schema_summary = ['summary',col]
    describe = source.select(col).describe().collect()
    summaries_key = ['mean', 'std', 'min', '25%', '50%', '75%', '95%', 'max']
    summaries_val = [describe[1][1],
                    describe[2][1],
                    describe[3][1],
                     source.selectExpr('percentile(usia, 0.25)').collect()[0][0],
                     source.selectExpr('percentile(usia, 0.5)').collect()[0][0],
                     source.selectExpr('percentile(usia, 0.75)').collect()[0][0],
                     source.selectExpr('percentile(usia, 0.95)').collect()[0][0],
                    describe[4][1]]
    df = spark.createDataFrame([('count', describe[0][1]) ], schema = schema_summary)
    for i in range(len(summaries_key)):
      temp = spark.createDataFrame([(summaries_key[i], summaries_val[i]) ])
      df = df.union(temp)
    return df
  
  def tableDictionary(self):
    

  
x = myInternalFunc(tablename)
x.compCount()
x.MinVal(5).show()
print(x.minValues(5).__doc__)

    
  def getDictionary():
    script=''
    for column in columns:
      script= script + "({0} IS NOT NULL AND TRIM({0}) NOT IN ('', 'null')) AND ".format(column)
    script = script[0:-4]
    source = spark.sql("""SELECT * FROM {} WHERE {}""".format(tablename, getConditionScript(tablename)))
    df = source.limit(100).toPandas().tail().iloc[0]
    types = [source.dtypes[x][1] for x in range(len(columns))]
    examples = [df[column] for column in columns]
    data = {'Attribute': columns, 
            'Tipe Data': types, 
            'Contoh Data': examples}
    return pd.DataFrame.from_dict(data, orient='columns')    
  
## Desc Generator (by Andri)
def descGenerator(schema, table): # inisiasi function
  first = True # variable untuk status data pertama
  df = spark.read.table("{}.{}".format(schema, table)) # read input tabel
  for c in df.columns: # loop sebanyak kolom pada tabel
    data_type = str(type(c))[8:-2] # memngambil data type dari kolom
    cntAll = df.select(c).count() # count all row pada input kolom
    cntDist = df.select(c).distinct().count() # count distinct pada input kolom
    cntNull = df.select(c).where(F.col(c).isNull()).count() # count null pada input kolom
    percNull = (cntNull/cntAll)*100 # percentage null terhadap keseluruhan row
    if first: # kondisi ketika data pertama
      first = False # mengubah status data menjadi bukan pertama
      columns = ['attributes','data_type','count_all','count_distinct','count_null','null_percentage'] # membuat schema awal
      wrap = spark.createDataFrame([(c,data_type,cntAll,cntDist,cntNull,percNull)], schema=columns) # membentuk tabel
    else: # kondisi ketika data bukan pertama
      temp = spark.createDataFrame([(c,data_type,cntAll,cntDist,cntNull,percNull)]) # membentuk tabel
      wrap = wrap.union(temp) # melakukan append data
    wrap \
      .write.format("parquet") \
      .mode("overwrite") \
      .saveAsTable("{}.{}".format("temp", "babi_desc_{0}".format(table))) # write as table
  
  
def enumGenerator(schema, table): # inisiasi function
  first = True # variable untuk status data pertama
  df = spark.read.table("{}.{}".format(schema, table)) # read input tabel
  columns = df.columns # perolehan list kolom dari df
  for i, c in enumerate(columns): # loop sebanyak kolom pada tabel
    num = (str(i)).zfill(len(str(len(columns)))) # menambah 0 di depan untuk angka 1 digit
    if df.select(c).distinct().count() <= 10000: # threshold count distict
      tmp = df.select(c).groupBy(c).count().sort(c) # grouping untuk mendapat distinct dari row dan count dari distinct
      col = "{0}_{1}".format(c, num) # formatting nama kolom {nama_kolom}_{num}
      tmp = tmp.withColumnRenamed(c, col) # mengubah nama kolom {nama_kolom}_{num}
      tmp = tmp.withColumnRenamed("count", "count_{0}".format(c)) # mengubah nama kolom count menjadi count_{nama_kolom}
      tmp = tmp.withColumn("row", F.row_number().over(W.orderBy(col))) # menabahkan kolom row yaitu row number
      if first: # kondisi ketika data pertama
        first = False # mengubah status data menjadi bukan pertama
        wrap = tmp.withColumnRenamed("row", "idx") # mengubah nama kolom row menjadi idx
      else: # kondisi ketika data bukan pertama
        if wrap.count() < tmp.count(): # kondisi jumlah data induk lebih kecil
          wrap = wrap.join(tmp, wrap.idx==tmp.row, "full").drop("idx") # joid dan hapus kolom idx
          wrap = wrap.withColumnRenamed("row", "idx") # mengubah nama kolom row menjadi idx
        else: # kondisi jumlah data induk lebih besar
          wrap = wrap.join(tmp, wrap.idx==tmp.row, "full").drop("row") # joid dan hapus kolom row
    else:
      tmp = spark.createDataFrame([(1,df.select(c).distinct().count())], schema=["row","count_distinct_{0}_{1}".format(c, num)])
      if first: # kondisi ketika data pertama
        first = False # mengubah status data menjadi bukan pertama
        wrap = tmp.withColumnRenamed("row", "idx") # mengubah nama kolom row menjadi idx
      else: # kondisi ketika data bukan pertama
        if wrap.count() < tmp.count(): # kondisi jumlah data induk lebih kecil
          wrap = wrap.join(tmp, wrap.idx==tmp.row, "full").drop("idx") # joid dan hapus kolom idx
          wrap = wrap.withColumnRenamed("row", "idx") # mengubah nama kolom row menjadi idx
        else: # kondisi jumlah data induk lebih besar
          wrap = wrap.join(tmp, wrap.idx==tmp.row, "full").drop("row") # joid dan hapus kolom row
    writeTable = wrap.select(F.col('idx').alias('no'), "*").drop("idx").sort("no") # formatting kolom dengan order berdasarkan no
    writeTable \
      .write.format("parquet") \
      .mode("overwrite") \
      .saveAsTable("{}.{}".format("temp", "babi_enum_{0}".format(table))) # write as table
      


#======================================================================#
#Whitelist rekening pinjaman
#======================================================================#
#from pyspark.sql import SparkSession
#from pyspark.context import SparkContext
#from pyspark.conf import SparkConf
#from pyspark.sql import SQLContext
#from pyspark.sql.functions import col
#from pyspark.sql import functions as F
#from pyspark.sql.functions import expr
#from pyspark.sql import Window
#from pyspark.sql.types import *
#from datetime import date, timedelta, datetime, time
#
#import logging
#import json
#import pandas as pd
#import re
#import csv
#from datetime import datetime
#import math
#import calendar
#import os, time
#
#os.environ['TZ']='Asia/Jakarta'
#time.tzset()
#time.strftime('%X %x %Z')
#


#whitelist_rek = spark.read.csv("hdfs:///user/cdh-etl1/data_whitelist_pinajaman/whitelist_pinjaman.csv", header='True', sep=';')
#whitelist_rek.createOrReplaceTempView("tbl_rekening")
#
#
#
#
##=======Total Mutasi Debet========= 
#df=spark.sql("""
#select 
#  a.cifno,
#  a.no_rekening,
#  c.sname as nama,
#  c.type as kode_tipe_produk,
#  d.loandescr as tipe_produk,
#  c.status,
#  c.curtyp as valuta,
#  c.orgamt_base as plafon,
#  c.cbal_base as outstanding,
#  sum(b.lhamt) as volume
#from tbl_rekening a
#left join (select * from datalake.`8000_rc_trx_as4_lnhist`
#            where year='2019' and month='09' 
#                and lhdorc='D' and lhtran in (13,10,15,110))b
#  on trim(a.no_rekening)=trim(cast(b.lhacct as string))
#left join (select * from datalake.`8989_bisdwh_fact_loanmaster`
#            where ds=201909)c
#  on trim(a.no_rekening)=trim(cast(c.acctno as string))
#left join (select * from datalake.8989_bisdwh_dim_par_loantype
#            where dttm_ingest='2019-09-05 03:16:51.246185' and trim(currency)='IDR') d
#      on trim(c.type)=trim(d.loantype)
#group by a.cifno,a.no_rekening,c.sname,c.type,c.status,c.orgamt_base,
#          c.cbal_base,d.loandescr,c.curtyp
#""")
#df.limit(100).toPandas()
#
#
##SAVE TO HIVE
##============
#"""hiveDB = "temp"
#hiveTable = "total_mutasi_pinjaman"
#df\
#  .write.format("parquet") \
#  .mode("overwrite") \
#  .saveAsTable("{}.{}".format(hiveDB, hiveTable))"""
#
##=======Interest Income Debet========= 
#df_interest=spark.sql("""
#select 
#  a.cifno,
#  a.no_rekening,
#  c.sname as nama,
#  c.orgamt_base as plafon,
#  c.cbal_base as outstanding,
#  sum(b.lhamt) as interest
#from tbl_rekening a
#left join (select * from datalake.`8000_rc_trx_as4_lnhist`
#            where year='2019' and month='09' 
#                and lhdorc='D' and lhtran in (496,302,499,29))b
#  on trim(a.no_rekening)=trim(cast(b.lhacct as string))
#left join (select * from datalake.`8989_bisdwh_fact_loanmaster`
#            where ds=201909)c
#  on trim(a.no_rekening)=trim(cast(c.acctno as string))
#left join (select * from datalake.8989_bisdwh_dim_par_loantype
#            where dttm_ingest='2019-09-05 03:16:51.246185' and trim(currency)='IDR') d
#      on trim(c.type)=trim(d.loantype)
#group by a.cifno,a.no_rekening,c.sname,c.orgamt_base,
#          c.cbal_base
#""")
#
##SAVE TO HIVE
##============
#"""hiveDB = "temp"
#hiveTable = "interest_income_debet"
#df_interest\
#  .write.format("parquet") \
#  .mode("overwrite") \
#  .saveAsTable("{}.{}".format(hiveDB, hiveTable))"""
#
#df_interest.limit(100).toPandas()
#
#spark.stop()
#
#
#bafa=spark.read.table("temp.bafa_rekening_pinjaman_list_tabel_hasil_prediksi_201911")
#
#bafa=spark.sql("""
#SELECT 
#  acctno,
#  cfaref,
#  fraud,
#  prediksi
#FROM(
#  SELECT 
#    ROW_NUMBER() 
#      OVER(PARTITION BY acctno ORDER BY COUNT(acctno) DESC) AS row_num,
#    CAST(acctno AS DECIMAL(19,0)) AS acctno,
#    COUNT(*) co,
#    cfaref, 
#    fraud, 
#    prediksi
#  FROM temp.`bafa_rekening_pinjaman_list_tabel_hasil_prediksi_201911`
#  GROUP BY acctno, cfaref, fraud, prediksi
#  --HAVING row_num=1
#  ORDER BY acctno, row_num 
#) WHERE row_num = 1
#""")
#bafa.createOrReplaceTempView("bafa")
#bafa_1 = bafa.filter('row>1')
#bafa_1.createOrReplaceTempView("bafa_1")
#
#df_try = spark.sql("""
#  SELECT prediksi, min(fraud) FROM bafa WHERE prediksi='fraud' GROUP BY prediksi
#""")
#bafa.limit(250).toPandas()
#hiveDB = "temp"
#hiveTable = "bafa_distinct_convacctno_cfaref_fraud_prediksi"
#bafa\
#  .write.format("parquet") \
#  .mode("overwrite") \
#  .saveAsTable("{}.{}".format(hiveDB, hiveTable))
  
  

