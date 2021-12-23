from dataDiff.schemaStore import addSchemaRow
from pyspark.sql.types import *

leftFileSchema = StructType([
    StructField('VendorID', IntegerType(), True),
    StructField('tpep_pickup_datetime', TimestampType(), True),
    StructField('tpep_dropoff_datetime', TimestampType(), True),
    StructField('passenger_count', IntegerType(), True),
    StructField('trip_distance', DoubleType(), True),
    StructField('RatecodeID', IntegerType(), True),
    StructField('store_and_fwd_flag', StringType(), True),
    StructField('PULocationID', IntegerType(), True),
    StructField('DOLocationID', IntegerType(), True),
    StructField('payment_type', IntegerType(), True),
    StructField('fare_amount', DoubleType(), True),
    StructField('extra', DoubleType(), True),
    StructField('mta_tax', DoubleType(), True),
    StructField('tip_amount', DoubleType(), True),
    StructField('tolls_amount', DoubleType(), True),
    StructField('improvement_surcharge', DoubleType(), True),
    StructField('total_amount', DoubleType(), True),
    StructField('congestion_surcharge', DoubleType(), True)
    ]).json()

rightFileSchema = StructType([
    StructField('VendorID', IntegerType(), True),
    StructField('tpep_pickup_datetime', TimestampType(), True),
    StructField('tpep_dropoff_datetime', TimestampType(), True),
    StructField('passenger_count', IntegerType(), True),
    StructField('trip_distance', DoubleType(), True),
    StructField('RatecodeID', IntegerType(), True),
    StructField('store_and_fwd_flag', StringType(), True),
    StructField('PULocationID', IntegerType(), True),
    StructField('DOLocationID', IntegerType(), True),
    StructField('payment_type', IntegerType(), True),
    StructField('fare_amount', DoubleType(), True),
    StructField('extra', DoubleType(), True),
    StructField('mta_tax', DoubleType(), True),
    StructField('tip_amount', DoubleType(), True),
    StructField('improvement_surcharge', DoubleType(), True),
    StructField('total_amount', DoubleType(), True),
    StructField('congestion_surcharge', DoubleType(), True)
    ]).json()

keyCols = {
    'VendorId' : 'VendorId',
    'tpep_pickup_datetime' : 'tpep_pickup_datetime',
    'tpep_dropoff_datetime' : 'tpep_dropoff_datetime',
}

# matchCols = MapType(StringType(), StringType(), True) 
matchCols = {
    'fare_amount' : 'fare_amount',
    'total_amount' : 'total_amount'
}

leftFilePath = '/mnt/data/data_diff/files/yellow_taxi'
leftFileFormat = 'csv'
leftFileHeader = True
leftFileDelimiter = ','
rightFilePath = '/mnt/data/data_diff/files/yellow_taxi'
rightFileFormat = 'csv'
rightFileHeader = True
rightFileDelimiter = ','
outputFilePath = '/mnt/data/data_diff/files/matched/yellow_taxi'

data = [
        'yellow-taxi-2021-07-01',
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
