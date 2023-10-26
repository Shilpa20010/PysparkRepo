from pyspark.sql.functions import *
from pyspark.sql.types import *


# 1.Select firstname, lastname and salary from Dataframe.

def select_columnname(df, columnname):
    df_select_name = df.select(df[columnname])
    return df_select_name


# 2.	Add Country, department, and age column in the dataframe.

def add_column(df, columnname):
    df_add_column = df.withColumn(columnname, lit(''))
    return df_add_column


# 3.	Change the value of salary column.

def salary_value(df, columnname):
    df_salary_change = df.withColumn('bonusaddes', col(columnname) * 10)
    return df_salary_change


# 4.	Change the data types of DOB and salary to String

def change_data_type(df, columnname, newname):
    df_change_dob = df.withColumn(newname, col(columnname))
    return df_change_dob


# Derive new column from salary column.

def add_newcolumn(df, columnname, new):
    df_add_Column = df.withColumn(new, col(columnname))
    return df_add_Column


# Rename nested column( Firstname -> firstposition, middlename -> secondposition, lastname -> lastposition)

def change_nested_name(df, colname, schema):
    df = df.withColumn(colname, col(colname).cast(schema))
    return df


# 7.	Filter the name column whose salary in maximum

def filter_value(df, colname, colname2):
    max_value = df.agg({colname: "max"}).collect()[0][0]
    result = df.filter(col(colname) == max_value).select(colname2)
    return result

#8.	Drop the department and age column

def drop_column(df,colname):
    df1 = df.drop(colname)
    return df1

#9.	List out distinct value of dob and salary

def distinct_value(df,columnname):
    df = df.select(columnname).distinct()
    return df
