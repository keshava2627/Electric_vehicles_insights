from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
spark=SparkSession.builder.appName("vehicle_sales_dim_date").master('local[*]').getOrCreate()
# spark.read.format('csv').\
#     option('header',True).\
#     load("C:/Users/kesha/Downloads/RPC12_project/RPC12_problem_statement_datasets/datasets/dim_date.csv").show()

bucket='temp3-bucket'
spark.conf.set('temporaryGcsBucket',bucket)

df=spark.read.format('csv').\
    option('header',True).\
    load("gs://electric_automative/dim_date.csv")
#
# df.printSchema()
#
# df.withColumn('date',trim(col('date')))\
#   .withColumn('fiscal_year',trim(col('fiscal_year')))\
#   .withColumn('quarter',trim(col('quarter'))).show(vertical=True)



df1=df.withColumn('date',trim(col('date')))\
  .withColumn('fiscal_year',trim(col('fiscal_year')))\
  .withColumn('quarter',trim(col('quarter')))
#
# df1.withColumn('date',to_date(col('date'),'dd-MMM-yy')).show(vertical=True)


df2=df1.withColumn('date',to_date(col('date'),'dd-MMM-yy'))
# df2.select(col('date').cast(DateType()),\
#            col('fiscal_year').cast(IntegerType()),\
#            col('quarter').cast(StringType())).show(vertical=True)



df3=df2.select(col('date').cast(DateType()),\
           col('fiscal_year').cast(IntegerType()),\
           col('quarter').cast(StringType()))

# df3.printSchema()

# df3.show()
df3.write.format('bigquery').\
    option('table','electric_vehicle_sales_by_state.electric_vehicle_sales_dim_date_stage').\
    option('createDisposition','CREATE_IF_NEEDED').\
    mode('overwrite').\
    save()