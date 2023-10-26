import unittest

from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import *

from Pyspark_Assignment.Assignment_2.utilis import *
from Pyspark_Assignment.Assignment_3.utils import *


class MyTestCase(unittest.TestCase):
    def setUp(self):
        self.spark = SparkSession.builder.appName("unit_test").getOrCreate()
        self.schema = StructType([
            StructField('name', StructType([
                StructField('firstname', StringType(), True),
                StructField('middlename', StringType(), True),
                StructField('lastname', StringType(), True)
            ])),
            StructField('dob', StringType(), True),
            StructField('gender', StringType(), True),
            StructField('salary', LongType(), True)
        ])
        self.dataDF = [(('James', '', 'Smith'), '03011998', 'M', 3000),
                       (('Michael', 'Rose', ''), '10111998', 'M', 20000),
                       (('Robert', '', 'Williams'), '02012000', 'M', 3000),
                       (('Maria', 'Anne', 'Jones'), '03011998', 'F', 1100),
                       (('Jen', 'Mary', 'Brown'), '04101998', 'F', 10000)
                       ]
        self.df = self.spark.createDataFrame(self.dataDF, self.schema)

    def tearDown(self):
        return self.spark.stop()

    def test_select_column(self):
        # Function to Select columns from Dataframe.
        colName = "country"
        self.expected_result = select_columnname(self.df, colName)
        schema = StructType([
            StructField("firstname", StringType())
        ])

        # Create a list of rows with the data
        data = [
            ("James",),
            ("Michael",),
            ("Robert",),
            ("Maria",),
            ("Jen",)
        ]
        # Create a DataFrame
        self.actual_df = self.spark.createDataFrame(data, schema)
        self.assertEqual(sorted(self.actual_df.collect()), sorted(self.expected_result.collect()))

    def test_add_value_col(self):
        expected_result = add_column(self.df, 'salary')
        schema = StructType([
            StructField("name", StringType()),
            StructField("dob", StringType()),
            StructField("gender", StringType()),
            StructField("salary", IntegerType()),
            StructField("Country", StringType()),
            StructField("department", StringType()),
            StructField("age", StringType()),
            StructField("added_colname", IntegerType())
        ])

        # Create a list of rows with the data
        data = [
            (["James", "", "Smith"], "03011998", "M", 3000, "", "", "", 6000),
            (["Michael", "Rose", ""], "10111998", "M", 20000, "", "", "", 40000),
            (["Robert", "", "Williams"], "02012000", "M", 3000, "", "", "", 6000),
            (["Maria", "Anne", "Jones"], "03011998", "F", 1100, "", "", "", 2200),
            (["Jen", "Mary", "Brown"], "04101998", "F", 10000, "", "", "", 20000)
        ]
        actual_df = self.spark.createDataFrame(data=data, schema=schema)
        self.assertEqual(sorted(actual_df.collect()), sorted(expected_result.collect()))

    def test_change_data_type(self):
        expected_result = change_data_type(self.df, 'salary', 'string')
        schema = StructType([
            StructField("name", StringType()),
            StructField("dob", StringType()),
            StructField("gender", StringType()),
            StructField("salary", IntegerType()),
            StructField("Country", StringType()),
            StructField("department", StringType()),
            StructField("age", StringType()),
            StructField("New_salary", StringType())
        ])

        # Create a list of rows with the data
        data = [
            (["James", "", "Smith"], "03011998", "M", 3000, "", "", "", "03011998"),
            (["Michael", "Rose", ""], "10111998", "M", 20000, "", "", "", "10111998"),
            (["Robert", "", "Williams"], "02012000", "M", 3000, "", "", "", "02012000"),
            (["Maria", "Anne", "Jones"], "03011998", "F", 1100, "", "", "", "03011998"),
            (["Jen", "Mary", "Brown"], "04101998", "F", 10000, "", "", "", "04101998")
        ]
        actual_df = self.spark.createDataFrame(data=data, schema=schema)
        self.assertEqual(sorted(actual_df.collect()), sorted(expected_result.collect()))

    def test_filter_value(self):
        expected_result = filter_value(self.df, 'salary', 'name')
        schema = StructType([
            StructField("name", StringType())
        ])
        data = [
            (["Michael", "Rose", ""],),
        ]
        actual_df = self.spark.createDataFrame(data=data, schema=schema)
        self.assertEqual(sorted(actual_df.collect()), sorted(expected_result.collect()))

    def test_distinct_values(self):
        expected_result = distinct_value(self.df, 'salary')
        schema = StructType([
            StructField("salary", IntegerType())
        ])

        # Create a list of rows with the salary data
        data = [
            (3000,),
            (20000,),
            (1100,),
            (10000,)
        ]
        actual_df = self.spark.createDataFrame(data=data, schema=schema)
        self.assertEqual(sorted(actual_df.collect()), sorted(expected_result.collect()))



if __name__ == '__main__':
    unittest.main()
