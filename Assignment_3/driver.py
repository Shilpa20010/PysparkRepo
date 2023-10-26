from Pyspark_Assignment.Assignment_1.utils import start_Session,create_df
from Pyspark_Assignment.Assignment_3.utils import *

data = [("Banana",1000,"USA"), ("Carrots",1500,"USA"), ("Beans",1600,"USA"),
      ("Orange",2000,"USA"),("Orange",2000,"USA"),("Banana",400,"China"),
      ("Carrots",1200,"China"),("Beans",1500,"China"),("Orange",4000,"China"),
      ("Banana",2000,"Canada"),("Carrots",2000,"Canada"),("Beans",2000,"Mexico")]

columns = ["Product","Amount","Country"]

#Start The session

sc = start_Session()

#create df

df = create_df(sc,data,columns)
df.show()

col_name3= "amount"
col_name2= "country"
col_name1= "product"

Pivot_DF = pivot(df,col_name1,col_name2,col_name3)
Pivot_DF.show()

Un_PivotDF = unpivot(col_name1,Pivot_DF)
Un_PivotDF.show()