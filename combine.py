from pyspark.sql import *
from pyspark import *
from pyspark.sql.functions import regexp_replace

#starting the session or driver
spark = SparkSession.builder.master('local[3]').appName('assignment3').getOrCreate()

#dataframe location
data = r'\data'

#reading the dataframe or creating 
employee1 = spark.read.option('inferSchema' , 'True').\
    option('header', 'True').csv('data\Employee_info_1.csv')

#reading the dataframe or creating 
employee2 = spark.read.option('inferSchema' , 'True').\
    option('header', 'True').csv('data\Employee_info.csv')



print("first dataframe employee1")
employee1.show()

print("first dataframe employee2")
employee2.show()


combined_employee = employee1.join(employee2, on=['Id'], how='inner')


print("after combining two dataframe employee1 and employee2 inner join ")
combined_employee.show()

print("count of rows after combined dataframe")
print(combined_employee.count())

#missing_val = combined_employee.na.fill({'Department' : 'DavOps', 'City':'Banglore'})

#missing_val = combined_employee.with({'Department' : 'DavOps', 'City':'Banglore'})

combined_df = combined_employee.withColumn('Department', regexp_replace('Department', 'Null', 'DavOps'))\
.withColumn('City', regexp_replace('City', 'Null', 'Banglore'))

#after filling missing values most occured one in both columns
combined_df.show()