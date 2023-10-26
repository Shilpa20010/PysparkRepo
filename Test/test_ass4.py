import unittest
from Pyspark_Assignment.Assignment_4.utils import *
from Pyspark_Assignment.Assignment_1.utils import *


class MyTestCase(unittest.TestCase):
    def setUp(self):
        self.spark = SparkSession.builder.appName("unit_test").getOrCreate()
        self.employee_schema = StructType([StructField("employee_name", StringType(), True),
                                           StructField("department", StringType(), True),
                                           StructField("salary", IntegerType(), True)
                                           ])
        self.employee_data = [("James", "Sales", 3000),
                              ("Michael", "Sales", 4600),
                              ("Robert", "Sales", 4100),
                              ("Maria", "Finance", 3000),
                              ("Raman", "Finance", 3000),
                              ("Scott", "Finance", 3300),
                              ("Jen", "Finance", 3900),
                              ("Jeff", "Marketing", 3000),
                              ("Kumar", "Marketing", 2000)]
        self.employee_df = self.spark.createDataFrame(self.employee_data, self.employee_schema)

    def tearDown(self):
        return self.spark.stop()

    def test_dept_grp(self):
        expected_result = department_grp(self.employee_df)
        schema = StructType([
            StructField("employee_name", StringType()),
            StructField("department", StringType()),
            StructField("salary", IntegerType())
        ])

        # Create a list of rows with the employee data
        data = [
            ("Jen", "Finance", 3900),
            ("Jeff", "Marketing", 3000),
            ("James", "Sales", 3000)
        ]
        actual_df = self.spark.createDataFrame(data=data, schema=schema)
        self.assertEqual(sorted(actual_df.collect()), sorted(expected_result.collect()))

    def test_row_data(self):
        expected_result = row_data(spark)
        schema = StructType([
            StructField("name", StringType()),
            StructField("age", IntegerType()),
            StructField("Job", StringType())
        ])

        # Create a list of rows with the employee data
        data = [
            ("HARI", 20, "IT"),
            ("RAVI", 24, "Admin"),
            ("RAGU", 28, "HR")
        ]
        actual_df = self.spark.createDataFrame(data=data, schema=schema)
        self.assertEqual(sorted(actual_df.collect()), sorted(expected_result.collect()))

    def test_highest_salary(self):
        expected_result = max_salary(self.employee_df)
        data = [
            Row(department="Sales", max_salary=4600),
            Row(department="Finance", max_salary=3900),
            Row(department="Marketing", max_salary=3000)
        ]

        actual_df = self.spark.createDataFrame(data=data, schema=schema)
        self.assertEqual(sorted(actual_df.collect()), sorted(expected_result.collect()))

    def test_low_salary(self):
        expected_result = low_salary(self.employee_df)
        data = [
            Row(department="Sales", min_salary=3000),
            Row(department="Finance", min_salary=3000),
            Row(department="Marketing", min_salary=2000)
        ]

        actual_df = self.spark.createDataFrame(data=data)
        self.assertEqual(sorted(actual_df.collect()), sorted(expected_result.collect()))

    def test_avg_salary(self):
        expected_result = avg_salary(self.employee_df)
        data = [
            Row(department="Sales", avg_salary=3900.0),
            Row(department="Finance", avg_salary=3300.0),
            Row(department="Marketing", avg_salary=2500.0)
        ]
        actual_df = self.spark.createDataFrame(data=data)
        self.assertEqual(sorted(actual_df.collect()), sorted(expected_result.collect()))

    def test_total_salary(self):
        expected_result = total_salary(self.employee_df)
        data = [
            ("Jen", "Finance", 3900),
            ("Jeff", "Marketing", 3000),
            ("Michael", "Sales", 4600)
        ]

        columns = ["employee_name", "department", "salary"]
        actual_df = self.spark.createDataFrame(data=data, schema=columns)
        self.assertEqual(sorted(actual_df.collect()), sorted(expected_result.collect()))


if __name__ == '__main__':
    unittest.main()
