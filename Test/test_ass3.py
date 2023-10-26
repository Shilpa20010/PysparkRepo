import unittest

from Pyspark_Assignment.Assignment_3.utils import *
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *

class MyTestCase(unittest.TestCase):
    def setUp(self):
        self.spark = SparkSession.builder.appName("unit_test").getOrCreate()
        self.data = [("Banana", 1000, "USA"), ("Carrots", 1500, "USA"), ("Beans", 1600, "USA"),
                     ("Orange", 2000, "USA"), ("Orange", 2000, "USA"), ("Banana", 400, "China"),
                     ("Carrots", 1200, "China"), ("Beans", 1500, "China"), ("Orange", 4000, "China"),
                     ("Banana", 2000, "Canada"), ("Carrots", 2000, "Canada"), ("Beans", 2000, "Mexico")]

        self.columns = ["Product", "Amount", "Country"]
        self.df = self.spark.createDataFrame(self.data, self.columns)

    def tearDown(self):
        return self.spark.stop()

    def test_pivot(self):
        colname3 = "amount"
        colName2 = "country"
        colname = "product"
        self.expected_result = pivot(self.df, colname, colName2, colname3)
        schema = StructType([
            StructField("product", StringType()),
            StructField("Canada", IntegerType()),
            StructField("China", IntegerType()),
            StructField("Mexico", IntegerType()),
            StructField("USA", IntegerType())
        ])

        # Create a list of rows with the data
        data = [
            ("Orange", None, 4000, None, 4000),
            ("Beans", None, 1500, 2000, 1600),
            ("Banana", 2000, 400, None, 1000),
            ("Carrots", 2000, 1200, None, 1500)
        ]

        # Create a DataFrame
        self.actual_df = self.spark.createDataFrame(data, schema)
        self.assertEqual(sorted(self.actual_df.collect()), sorted(self.expected_result.collect()))

    def test_unpivot(self):
        colname = "product"
        expected_result = unpivot(colname, self.expected_result)
        schema = StructType([
            StructField("product", StringType()),
            StructField("Country", StringType()),
            StructField("Total", IntegerType())
        ])

        # Create a list of rows with the data
        data = [
            ("Orange", "China", 4000),
            ("Beans", "China", 1500),
            ("Beans", "Mexico", 2000),
            ("Banana", "Canada", 2000),
            ("Banana", "China", 400),
            ("Carrots", "Canada", 2000),
            ("Carrots", "China", 1200)
        ]

        actual_df = self.spark.createDataFrame(data=data, schema=schema)
        self.assertEqual(sorted(actual_df.collect()), sorted(expected_result.collect()))




if __name__ == '__main__':
    unittest.main()
