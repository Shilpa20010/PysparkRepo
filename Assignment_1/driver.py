from Pyspark_Assignment.Assignment_1.utils import *
from pyspark.sql.types import *

schema = ["Product Name","Issue Date","Price","Brand","Country","Product number"]
datastruct = [('Washing Machine',1648770933000,20000,'Samsung','India',1),
              ('Refrigerator',1648770999000,35000,' LG',None,2),
              ('Air Cooler',1648770948000,45000,' Voltas',None,3)]

schema_1 = StructType([
            StructField("SourceId",IntegerType(),True),
            StructField("TransactionNumber",IntegerType(),True),
            StructField("Language",StringType(),True),
            StructField("ModelNumber",StringType(),True),
            StructField("StartTime",StringType(),True),
            StructField("ProductNumber",IntegerType(),True)
            ])

datastruct_1 = [
    (150711, 123456, 'EN', 456789, '2021-12-27T08:20:29.842+0000', 1),
    (150439, 234567, 'UK', 345678, '2021-12-27T08:21:14.645+0000', 2),
    (150647, 345678, 'ES', 234567, '2021-12-27T08:22:42.445+0000', 3)
]

column_name1 = "Issue Date"
column_name2 = "Country"
column_name3 = "Brand"

#Start session

sc = start_Session()

#Question_1

#Create DF

df_product_1 = create_df(sc,datastruct,schema)
df_product_1.show()
df_product_1.printSchema()

#Convert the Issue Date with the timestamp format

df_formated_timetsamp = format_time(df_product_1,column_name1)
df_formated_timetsamp.show()
df_formated_timetsamp.printSchema()

# Convert Issue Date with dateformat

df_formated_date = format_type(df_formated_timetsamp,column_name1)
df_formated_date.show()

#To trim the extra spaces

df_trimed_data = trim_space(df_formated_date,column_name3)
df_trimed_data.show()

#To null values

df_null_data = null_value(df_trimed_data,column_name2)
df_null_data.show()

#Question2
#creating df

df_product_2 = create_df(sc,datastruct_1,schema_1)
df_product_2.show()

#Rename the column name

df_rename = rename_column(df_product_2,'SourceId','source_id')
df_rename = rename_column(df_rename,'TransactionNumber','transaction_number')
df_rename = rename_column(df_rename,'ModelNumber','model_number')
df_rename = rename_column(df_rename,'ProductNumber','product_number')
df_rename.show()

#b)	Add another column as start_time_ms and convert the values of StartTime to milliseconds.

df_format_unix = format_unix(df_rename,'StartTime')
df_format_unix.show()

#Joining tables

df_joindf = join_df(df_null_data,df_product_2,"Product number","ProductNumber")
df_joindf.show()