# MY INTERNAL FUNCTION
for the sake of maintenance and table analizing. this function need table name as a parameter. call this function by

```
from TableAnalyzing import myInternalFunc
x = myInternalFunc(table_name)
```

it has saveral callable method to allow you get to know better to your dataframe 
1. **enumerateDistinct**
to enumerate all distinct values in the table. need threshold values, but not compulsary. output of this function is spark's dataframe so you can show, toPandas, or even write to anything else. 
2. **minValues**
to get a number of minimum values of each column. required how many rows you need.
3. **maxValues**
same as 'minValues', only this maximum values
4. **countDistinct**
get the number of unique values
5. **comprehentCount**
count comprehensively, yeild the number of nul/blank and the percetages
6. **colSummary**
to summerize the column. take one argument (column) and it will show you count, max, min, percentile. typical with pandas'
7. **getSimpleDictionary**
cheap version of 'getDictionary'
8. **getDictionary**
get all dictionary needs, such as 'attributes','data_type', 'examples','count_all','count_distinct','count_null', and 'null_percentage'. takes one boolean argument but not compulsory, since the defaulf value of description is False

```
df_distinct = x.enumerateDistinct(threshold)
df_min = x.minValues(rows)
df_max = x.maxValues(rows)
x.countDistinct()
x.comprehentCount()
x.colSummary(column_name)
df_simpledict = x.getSimpleDictionary()
df_Distionary = x.getDictionary()
```
#### - by Ilham Rizky
