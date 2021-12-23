import argparse
from pyspark.sql import SparkSession
from dataDiff.schemaStore import deleteStore

parser = argparse.ArgumentParser(description='Delete Schema Store',
    epilog="python delete_store.py --databaseName 'default' --storeName 'nyctaxi_schema' --storePath '/mnt/data/data_diff/nyctaxi_schema'"
)

parser.add_argument('--databaseName', dest='databaseName', type=str, help='Name of the database to delete the store from')
parser.add_argument('--storeName', dest='storeName', type=str, help='Name of the store')
parser.add_argument('--storePath', dest='storePath', type=str, help='HDFS path of the store')
args = parser.parse_args()

spark = SparkSession.builder.appName('datadiff').getOrCreate()

spark.sql(f"USE {args.databaseName}")

deleteStore(spark, args.storeName, args.storePath)