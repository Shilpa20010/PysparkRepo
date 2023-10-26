from pyspark.sql import SparkSession
from pyspark.sql.functions import *

#11.	Find total amount exported to each country of each product.

def pivot(df,colname,colName2,colName3):
    pivotDF = df.groupBy(colname).pivot(colName2).sum(colName3)
    return pivotDF

#12.	Perform unpivot function on output of question 2.

def unpivot(colname,pivotDF):
    unpivotExpr = "stack(3, 'Canada', Canada, 'China', China, 'Mexico', Mexico) as (Country,Total)"
    unPivotDF = pivotDF.select(colname, expr(unpivotExpr)).where("Total is not null")
    return unPivotDF

