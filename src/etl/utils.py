from pyspark.sql import *
from pyspark import *
from pyspark.sql.functions import regexp_replace


#dataframe location
data = r'\data'


#starting the session or driver
def spark_driver():
    spark = SparkSession.builder.master('local[3]').appName('assignment3').getOrCreate()
    return spark
    

#reading the dataframe or creating 
def read_employee1(spark):
    return spark.read.option('inferSchema' , 'True').\
        option('header', 'True').csv('data\Employee_info_1.csv')


#reading the dataframe or creating 
def read_employee2(spark):
    return spark.read.option('inferSchema' , 'True').\
    option('header', 'True').csv('data\Employee_info.csv')

#combine 2 dataframe and return it 
def combined_employee(employee1, employee2):
    return employee1.join(employee2, on=['Id'], how='inner')


#remove and replace most occuring values 
def removed_missing_val(combined_employee):
    return combined_employee.withColumn('Department', regexp_replace('Department', 'Null', 'DavOps'))\
        .withColumn('City', regexp_replace('City', 'Null', 'Banglore'))


