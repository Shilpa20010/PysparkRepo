from Pyspark_Assignment.Assignment_4.utils import *
from Pyspark_Assignment.Assignment_1.utils import *

employee_schema = StructType([StructField("employee_name", StringType(), True),
                              StructField("department", StringType(), True),
                              StructField("salary", IntegerType(), True)
                              ])
employee_data = [("James", "Sales", 3000),
                 ("Michael", "Sales", 4600),
                 ("Robert", "Sales", 4100),
                 ("Maria", "Finance", 3000),
                 ("Raman", "Finance", 3000),
                 ("Scott", "Finance", 3300),
                 ("Jen", "Finance", 3900),
                 ("Jeff", "Marketing", 3000),
                 ("Kumar", "Marketing", 2000)]

#Starting session

spark = start_Session()

#creating df

df = create_df(spark,employee_data,employee_schema)
df.show()

#	Select first row from each department group.

df1 = department_grp(df)
df1.show()

#	Create a Dataframe from Row and List of tuples.

df2 = row_data(spark)
df2.show()

# Retrieve Employees who earns the highest salary.

df3 = max_salary(df)
df3.show()

# Retrieve employees who ears the lowest salary

df4 = low_salary(df)
df4.show()

# Retrive employees who earns the avg salary

df5 = avg_salary(df)
df5.show()

# Retrive employees who earns the total salary

df6 = total_salary(df)
df6.show()