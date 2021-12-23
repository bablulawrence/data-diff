from dataDiff.schemaStore import addSchemaRow
from pyspark.sql.types import *

leftFileSchema = StructType([
    StructField('VendorID', IntegerType(), True),
    StructField('lpep_pickup_datetime', StringType(), True),
    StructField('lpep_dropoff_datetime', StringType(), True),
    StructField('store_and_fwd_flag', StringType(), True),
    StructField('RatecodeID', IntegerType(), True),
    StructField('PULocationID', IntegerType(), True),
    StructField('DOLocationID', IntegerType(), True),
    StructField('passenger_count', IntegerType(), True),
    StructField('trip_distance', DoubleType(), True),
    StructField('fare_amount', DoubleType(), True),
    StructField('extra', DoubleType(), True),
    StructField('mta_tax', DoubleType(), True),
    StructField('tip_amount', DoubleType(), True),
    StructField('tolls_amount', DoubleType(), True),
    StructField('ehail_fee', StringType(), True),
    StructField('improvement_surcharge', DoubleType(), True),
    StructField('total_amount', DoubleType(), True),
    StructField('payment_type', IntegerType(), True),
    StructField('trip_type', IntegerType(), True),
    StructField('congestion_surcharge', DoubleType(), True),
]).json()

rightFileSchema = StructType([
    StructField('VendorID', IntegerType(), True),
    StructField('lpep_pickup_datetime', StringType(), True),
    StructField('lpep_dropoff_datetime', StringType(), True),
    StructField('store_and_fwd_flag', StringType(), True),
    StructField('RatecodeID', IntegerType(), True),
    StructField('PULocationID', IntegerType(), True),
    StructField('DOLocationID', IntegerType(), True),
    StructField('passenger_count', IntegerType(), True),
    StructField('trip_distance', DoubleType(), True),
    StructField('fare_amount', DoubleType(), True),
    StructField('extra', DoubleType(), True),
    StructField('mta_tax', DoubleType(), True),
    StructField('tip_amount', DoubleType(), True),
    StructField('tolls_amount', DoubleType(), True),
    StructField('ehail_fee', StringType(), True),
    StructField('improvement_surcharge', DoubleType(), True),
    StructField('total_amount', DoubleType(), True),
    StructField('payment_type', IntegerType(), True),
    StructField('trip_type', IntegerType(), True),
    StructField('congestion_surcharge', DoubleType(), True),
]).json()

keyCols = {
    'VendorId' : 'VendorId',
    'lpep_pickup_datetime' : 'lpep_pickup_datetime',
    'lpep_dropoff_datetime' : 'lpep_dropoff_datetime',
}

# matchCols = MapType(StringType(), StringType(), True) 
matchCols = {
    'fare_amount' : 'fare_amount',
    'total_amount' : 'total_amount',
    'payment_type' : 'payment_type'
}

leftFilePath = '/mnt/data/data_diff/files/green_taxi'
leftFileFormat = 'csv'
leftFileHeader = True
leftFileDelimiter = ','
rightFilePath = '/mnt/data/data_diff/files/green_taxi'
rightFileFormat = 'csv'
rightFileHeader = True
rightFileDelimiter = ','
outputFilePath = '/mnt/data/data_diff/files/matched/green_taxi'

data = [
        'green-taxi-2021-01',
        leftFilePath,
        leftFileFormat,
        leftFileHeader,
        leftFileDelimiter,
        leftFileSchema,
        rightFilePath,
        rightFileFormat,
        rightFileHeader,
        rightFileDelimiter,
        rightFileSchema,
        outputFilePath,
        keyCols,
        matchCols
]

def addSchema(spark, storeName): 
    addSchemaRow(spark, data, storeName)
