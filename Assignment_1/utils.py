from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *

#Creating session
def start_Session():
    return SparkSession.builder.appName("Assignment1").getOrCreate()

#Question_1

#Creating DF
def create_df(sc,data,schema):
    return sc.createDataFrame(data,schema)

#Function for date & time

def format_time(df,column_name1):
    return df.withColumn(column_name1,from_unixtime(df[column_name1]/1000).cast("timestamp"))

#Function to change data type

def format_type(df,column_name1):
    return df.withColumn(column_name1,to_date(df[column_name1]))

#Function for extra spaces

def trim_space(df,column_name3):
    return df.withColumn(column_name3,trim(df[column_name3]))

#Function for null values

def null_value(df,column_name2):
    return df.na.fill("",subset=[column_name2])

#Question_2

#Functionname to rename

def rename_column(df,existingname,newname):
    return df.withColumnRenamed(existingname,newname)

#b)	Add another column as start_time_ms and convert the values of StartTime to milliseconds.

def format_unix(df,column_name1):
    return df.withColumn("start_time_ms",(unix_timestamp(df[column_name1],"yyyy-MM-dd'T'HH:mm:ss.SSSXXX") * 1000).cast("long"))

#df1 = df.withColumn("start_time_ms", (unix_timestamp(col("StartTime"), "yyyy-MM-dd'T'HH:mm:ss.SSSXXX") * 1000).cast("long"))
# df1.show()

def join_df(df,df1,column_name2,column_name1):
    return df.join(df1,df[column_name2]==df1[column_name1],"outer")

