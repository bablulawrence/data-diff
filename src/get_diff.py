import argparse
from pyspark.sql import SparkSession
from dataDiff.diff import getDiff

parser = argparse.ArgumentParser(description='Comparare data between files in a Schema Store',
    epilog="python get_diff.py --databaseName 'default' --storeName 'nyctaxi_schema'"
)

parser.add_argument('--databaseName', dest='databaseName', type=str, help='Name of the database where the store is present')
parser.add_argument('--storeName', dest='storeName', type=str, help='Name of the store')
args = parser.parse_args()

spark = SparkSession.builder.appName('datadiff').getOrCreate()

spark.sql(f"USE {args.databaseName}")
getDiff(spark, args.storeName)

