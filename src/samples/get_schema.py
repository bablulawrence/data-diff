from pyspark.sql import SparkSession

spark = SparkSession.builder.appName('datadiff').getOrCreate()

folderPath = '/mnt/data/data_diff/'

yellow_taxi = spark.read.format('csv') \
                    .option("header", True) \
                    .option("inferSchema", True) \
                    .load(f"{folderPath}/files/yellow_taxi")


green_taxi = spark.read.format('csv') \
                    .option("header", True) \
                    .option("inferSchema", True) \
                    .load(f"{folderPath}/files/green_taxi")

print('Yellow Taxi Schema : ')
print(yellow_taxi.schema)
print('=============================')
print('Green Taxi Schema : ')
print(green_taxi.schema)