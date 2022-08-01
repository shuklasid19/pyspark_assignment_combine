from pyspark.sql import *
from pyspark import *
from pyspark.sql.functions import regexp_replace
from src.etl.utils import *

#starting the session or driver

#dataframe location
data = r'\data'

spark =  spark_driver()

#reading the dataframe or creating 
employee1 = read_employee1(spark)

#reading the dataframe or creating 
employee2 = read_employee2(spark)

#combined 2 dataframe
combined_df = combined_employee(employee1, employee2)

print("after combining two dataframe employee1 and employee2 inner join ")
combined_df.show()

print("count of rows after combined dataframe")
print(combined_df.count())

removed_null = removed_missing_val(combined_df)

#after filling missing values most occured one in both columns
removed_null.show()
