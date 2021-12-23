from pyspark.sql import SparkSession

spark = SparkSession.builder.appName('datadiff').getOrCreate()

def displayOutput(df, matched=True): 
    df.printSchema
    expr = [
        'left.vendorId', 
        'left.tpep_pickup_datetime', 
        'left.tpep_dropoff_datetime', 
        'left.fare_amount', 
        'left.total_amount', 
        'right.vendorId', 
        'right.tpep_pickup_datetime', 
        'right.tpep_dropoff_datetime', 
        'right.fare_amount', 
        'right.total_amount', 
        'matched'
    ]
    df.selectExpr(expr).filter(col('matched') == matched).show()


path = '/mnt/data/data_diff/files/matched/yellow_taxi'
df = spark.read.format('delta').load(path)
displayOutput(df)