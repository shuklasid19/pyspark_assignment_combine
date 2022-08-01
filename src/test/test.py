import unittest
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql import SparkSession
from src.etl.utils import *
from pyspark.sql.functions import regexp_replace

class Assignment2(unittest.TestCase):

    def setUp(self):
        self.spark = (SparkSession
                     .builder
                     .master("local[3]")
                     .appName("PySpark-unit-test")
                     .getOrCreate())

        
        self.employee1_data =  self.spark.read.option('inferSchema' , 'True').\
        option('header', 'True').csv('data\Employee_info_1.csv')

        self.employee2_data = self.spark.read.option('inferSchema' , 'True').\
        option('header', 'True').csv('data\Employee_info.csv')

        self.combined_df = self.employee1_data.join(self.employee2_data, on=['Id'], how='inner')

        self.removed_null_df = self.combined_df.withColumn('Department', regexp_replace('Department', 'Null', 'DavOps'))\
        .withColumn('City', regexp_replace('City', 'Null', 'Banglore'))



    def test__employee1(self):
        emp1_count = self.employee1_data.count()
        exepected_output = 10
        self.assertTrue(emp1_count , exepected_output)

        
    
    def test_removed_data(self):
        combined_df_count = self.removed_null_df.count()
        expected_output = 10 
        self.assertEqual(combined_df_count , expected_output, "not equal should be 10" )

        self.removed_null_df.show()


    
        #Department

if __name__ =="__main__":
    unittest.main()

