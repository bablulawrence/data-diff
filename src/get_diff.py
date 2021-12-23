import argparse
from pyspark.sql import SparkSession
from dataDiff.diff import getDiff

parser = argparse.ArgumentParser(description='Comparare data between files in a Schema Store',
                                epilog="python get_diff.py --storeName 'nyctaxi_schema'")
parser.add_argument('--storeName', dest='storeName', type=str, help='Name of the store')
args = parser.parse_args()

spark = SparkSession.builder.appName('datadiff').getOrCreate()

getDiff(spark, args.storeName)

