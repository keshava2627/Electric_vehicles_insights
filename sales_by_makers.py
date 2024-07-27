from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
spark=SparkSession.builder.appName('vehicle_sales_by_makers').master('local[*]').getOrCreate()
bucket='temp3-bucket'
spark.conf.set('temporaryGcsBucket',bucket)
# spark.read.format('csv').\
#     option('header',True).\
#     load("C:/Users/kesha/Downloads/RPC12_project/RPC12_problem_statement_datasets/datasets/electric_vehicle_sales_by_makers.csv").\
#     show()


df=spark.read.format('csv').\
    option('header',True).\
    load("gs://electric_automative/electric_vehicle_sales_by_makers.csv")
#
# df.withColumn('date',trim(col('date')))\
#   .withColumn('vehicle_category',trim(col('vehicle_category')))\
#   .withColumn('maker',trim(col('maker')))\
#   .withColumn('electric_vehicles_sold',trim(col('electric_vehicles_sold'))).show()



df1=df.withColumn('date',trim(col('date')))\
  .withColumn('vehicle_category',trim(col('vehicle_category')))\
  .withColumn('maker',trim(col('maker')))\
  .withColumn('electric_vehicles_sold',trim(col('electric_vehicles_sold')))

#
# df1.withColumn('date',to_date(col('date'),'dd-MMM-yy')).show()


df2=df1.withColumn('date',to_date(col('date'),'dd-MMM-yy'))
#
# df2.select(col('date').cast(DateType()),\
#            col('vehicle_category').cast(StringType()),\
#            col('maker').cast(StringType()),\
#            col('electric_vehicles_sold').cast(IntegerType())).show()


#
# df2.select(col('date').cast(DateType()),\
#            col('vehicle_category').cast(StringType()),\
#            col('maker').cast(StringType()),\
#            col('electric_vehicles_sold').cast(IntegerType())).printSchema()


df3=df2.select(col('date').cast(DateType()),\
           col('vehicle_category').cast(StringType()),\
           col('maker').cast(StringType()),\
           col('electric_vehicles_sold').cast(IntegerType()))


df3.write.format('bigquery').\
    option('table','electric_vehicle_sales_by_state.electric_vehicle_sales_by_makers_stage').\
    option('createDisposition','CREATE_IF_NEEDED').\
        mode('overwrite').\
    save()