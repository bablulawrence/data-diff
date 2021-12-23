from pyspark.sql import SparkSession

spark = SparkSession.builder.appName('datadiff').getOrCreate()

def displayOutput(df, matched=True): 
    df.printSchema
    expr = [
        'L_vendorId', 
        'L_tpep_pickup_datetime', 
        'L_tpep_dropoff_datetime', 
        'L_fare_amount', 
        'L_total_amount', 
        'R_vendorId', 
        'R_tpep_pickup_datetime', 
        'R_tpep_dropoff_datetime', 
        'R_fare_amount', 
        'R_total_amount', 
        'matched'
    ]
    df.selectExpr(expr).filter('matched').show()


path = '/mnt/data/data_diff/files/matched/yellow_taxi'
df = spark.read.format('delta').load(path)
displayOutput(df)