from Pyspark_Assignment.Assignment_1.utils import start_Session,create_df
from Pyspark_Assignment.Assignment_2.utilis import *

#Data and schema for DataFrame


schema = StructType([
       StructField('name',StructType([
            StructField('firstname',StringType(),True),
            StructField('middlename',StringType(),True),
            StructField('lastname',StringType(),True)
       ])),
       StructField('dob',StringType(),True),
       StructField('gender',StringType(),True),
       StructField('salary',LongType(),True)
])

dataDF = [(('James','','Smith'),'03011998','M',3000),
  (('Michael','Rose',''),'10111998','M',20000),
  (('Robert','','Williams'),'02012000','M',3000),
  (('Maria','Anne','Jones'),'03011998','F',1100),
  (('Jen','Mary','Brown'),'04101998','F',10000)
]

#Starting the session
sc = start_Session()

#Creating dataframe
df = create_df(sc,dataDF,schema)
df.printSchema()

#Selecting particular column

df_selected,df_selected1,df_selected3 = select_columnname(df,'name.firstname'),select_columnname(df,'name.lastname'),select_columnname(df,'salary')
df_selected.show()
df_selected1.show()
df_selected3.show()

#Adding new column

df = add_column(df,'Country')
df = add_column(df,'department')
df = add_column(df,'age')
df.show()

#creating new salary column

df = salary_value(df,'salary')
df.show()

#Change the data types of DOB and salary to String
df = change_data_type(df,'salary','string')
df = change_data_type(df,'dob','string')

df.show()

#5.	Derive new column from salary column.

df = add_newcolumn(df,'salary','salary_new')
df.show()

# 6.Rename nested column( Firstname -> firstposition, middlename -> secondposition, lastname -> lastposition)

schema2 = StructType([
    StructField("firstposition",StringType()),
    StructField("secondposition",StringType()),
    StructField("lastposition",StringType())])

df2 = change_nested_name(df,'name',schema2)
df2.printSchema()


# 7.Filter the name column whose salary in maximum.
filter_value(df,'salary','name').show()

#8.	Drop the department and age column

df = drop_column(df,'department')
df = drop_column(df,'age')
df.show()

#9.	List out distinct value of dob and salary

distinct_value(df,'salary').show()

#Stop Session

sc.stop()
