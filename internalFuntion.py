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
    '''     # MY INTERNAL FUNCTION
    for the sake of maintenance and table analizing. this function need table name as a parameter.
    it has saveral callable method to allow you get to know better to your dataframe 

    1. enumerateDistinct
        to enumerate all distinct values in the table. need threshold values, 
        but not compulsary. output of this function is spark's dataframe so you can
        show, toPandas, or even write to anything else. 
    2. minValues
        to get a number of minimum values of each column. required how many rows you need.
    3. maxValues
        same as 'minValues', only this maximum values
    4. countDistinct
        get the number of unique values
    5. comprehentCount
        count comprehensively, yeild the number of nul/blank and the percetages
    6. colSummary
        to summerize the column. take one argument (column) and it will show you count, max, min, percentile. typical with pandas'
    7. getSimpleDictionary
        cheap version of 'getDictionary'
    8. getDictionary
        get all dictionary needs, such as 'attributes','data_type', 'examples','count_all','count_distinct','count_null', and 'null_percentage'
        takes one boolean argument but not compulsory, since the defaulf value of description is False

    - by Ilham Rizky'''
    def __init__(self, tablename):
        self.source=spark.read.table(tablename)

    def enumerateDistinct(self, threshold=10000):
        ''' enumerate all distinct element in the table. required tablename and 
        threshold (or the number of distinct needed) 10.000 by default.
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

        ## is the table have partition? if yes devide by the partitions!!
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

    def minValues(self, rows):
        '''Get x-minimum rows from each columns of the tables
        PS: it required number of minimum values each row INT'''
        return self.getExtremValues(rows, 1).orderBy('row_num', ascending=1).drop('row_num')

    def maxValues(self, rows):
        '''Get x-maximum rows from each columns of the tables
        PS: it required number of maximum values each row INT'''
        return self.getExtremValues(rows, 0).orderBy('row_num', ascending=0).drop('row_num')
    
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
        schema_summary = ['summary',col]
        describe = source.select(col).describe().collect()
        summaries_key = ['mean', 'std', 'min', '25%', '50%', '75%', '95%', 'max']
        summaries_val = [describe[1][1],
                        describe[2][1],
                        describe[3][1],
                        source.selectExpr('percentile({}, 0.25)'.format(col)).collect()[0][0],
                        source.selectExpr('percentile({}, 0.5)'.format(col)).collect()[0][0],
                        source.selectExpr('percentile({}, 0.75)'.format(col)).collect()[0][0],
                        source.selectExpr('percentile({}, 0.95)'.format(col)).collect()[0][0],
                        describe[4][1]]
        df = spark.createDataFrame([('count', describe[0][1]) ], schema = schema_summary)
        for i in range(len(summaries_key)):
            temp = spark.createDataFrame([(summaries_key[i], summaries_val[i]) ])
            df = df.union(temp)
        return df

    def getSimpleDictionary(self):
        ''' Simple dictionary: column_name, data_type, and examples '''
        source = self.source
        labels = ['attributes','data_type', 'examples',]
        isfirst = True
        for column in source.columns:
            data_type = str(type(column))[8:-2]
            example = source.selectExpr('percentile({}, 0.5)'.format(column)).collect()[0][0]
            table_values = [(column, data_type, examples)]
            if isfirst:
                first = False
                dictionary = spark.createDataFrame(table_values, schema=labels)
            else:
                temp = spark.createDataFrame(table_values)
                simple_dictionary = simple_dictionary.union(temp)
        return simple_dictionary        

    def getDictionary(self, description=False):
        ''''''
        source = self.source
        labels = ['attributes','data_type', 'examples','count_all','count_distinct','count_null','null_percentage', 'description']
        isfirst = True
        for column in source.columns:
            data_type = str(type(column))[8:-2]
            examples = source.selectExpr('percentile({}, 0.5)'.format(column)).collect()[0][0]
            count_all = source.select(column).count()
            count_distinct = source.select(column).distinct().count()
            cntNull = source.select(c).where(F.col(c).isNull()).count()
            percNull = (cntNull/cntAll)*100
            description_txt = input("Please enter description of {} column :".format(column)) if description else ''
            table_values = [(
                column,
                data_type,
                examples
                count_all,
                count_distinct,
                count_null,
                percentage_null,
                description_txt
            )]
            if isfirst:
                first = False
                dictionary = spark.createDataFrame(table_values, schema=labels)
            else:
                temp = spark.createDataFrame(table_values)
                dictionary = dictionary.union(temp)
        return dictionary
