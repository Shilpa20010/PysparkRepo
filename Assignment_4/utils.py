from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql import Window

def department_grp(employee_df):
    partitioned_dept = Window.partitionBy("department").orderBy("employee_name")
    row_number_added = employee_df.withColumn("row_number", row_number().over(partitioned_dept))
    first_row = row_number_added.filter(row_number_added.row_number == 1).drop("row_number")
    return first_row

def row_data(spark):
    schema_emp = StructType([StructField("name", StringType(), True),
                             StructField("age", IntegerType(), True),
                             StructField("Job", StringType(), True)
                             ])
    row = ("HARI", 20, "IT")
    employee_data = [("RAVI", 24, "Admin"),
                     ("RAGU", 28, "HR")]

    added_data = [row] + employee_data

    new_df = spark.createDataFrame(data=added_data, schema=schema_emp)
    return new_df


def max_salary(employee_df):
    highest_Salary = employee_df.groupBy("department").agg(max("salary").alias("max_salary"))
    #highest_salary_employees = employee_df.join(highest_Salary, on=["department", "salary"], how="inner").select("name")
    return highest_Salary

def low_salary(employee_df):
    lowest_salary = employee_df.groupBy("department").agg(min("salary").alias("low_salary"))
    return lowest_salary

def avg_salary(employee_df):
    avg_salary = employee_df.groupBy("department").agg(avg("salary").alias("avg_salary"))
    return avg_salary

def total_salary(employee_df):
    tot_Sal = employee_df.groupBy("department").agg(sum("salary").alias("total_Salary"))
    return tot_Sal
