from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *

spark=SparkSession.builder.appName("reading_vehicles_by_state").master('local[*]').getOrCreate()
bucket='temp3-bucket'
spark.conf.set('temporaryGcsBucket',bucket)

df=spark.read.format('csv').\
    option('header',True).\
    load("gs://electric_automative/electric_vehicle_sales_by_state.csv")

# df.show(3,vertical=True,truncate=False)

# df.printSchema()
#
# df.withColumn('date',trim(col('date')))\
#   .withColumn('state',trim(col('state')))\
#   .withColumn('vehicle_category',trim(col('vehicle_category')))\
#   .withColumn('electric_vehicles_sold',trim(col('electric_vehicles_sold')))\
#   .withColumn('total_vehicles_sold',trim(col('total_vehicles_sold'))).show(vertical=True,truncate=False)



df1=df.withColumn('date',trim(col('date')))\
  .withColumn('state',trim(col('state')))\
  .withColumn('vehicle_category',trim(col('vehicle_category')))\
  .withColumn('electric_vehicles_sold',trim(col('electric_vehicles_sold')))\
  .withColumn('total_vehicles_sold',trim(col('total_vehicles_sold')))

#
# df1.withColumn('date',to_date(col('date'),'dd-MMM-yy')).show()


df2=df1.withColumn('date',to_date(col('date'),'dd-MMM-yy'))
# df2.select(col('date')).show(1000)
#
# df2.printSchema()

# df2.select(col('date').cast(DateType()),\
#            col('state').cast(StringType()),\
#            col('vehicle_category').cast(StringType()),\
#            col('electric_vehicles_sold').cast(IntegerType()),\
#            col('total_vehicles_sold').cast(IntegerType())).show()

df3=df2.select(col('date').cast(DateType()),\
           col('state').cast(StringType()),\
           col('vehicle_category').cast(StringType()),\
           col('electric_vehicles_sold').cast(IntegerType()),\
           col('total_vehicles_sold').cast(IntegerType()))

# df3.printSchema()

df3.write.format('bigquery').\
    option('table','electric_vehicle_sales_by_state.electric_vehicle_sales_by_state_stage').\
    option('createDisposition','CREATE_IF_NEEDED').\
        mode('overwrite').\
            save()



