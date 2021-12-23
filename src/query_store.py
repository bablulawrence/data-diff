import argparse    
from pyspark.sql import SparkSession

parser = argparse.ArgumentParser(description='Query Schema Store',
       epilog="python query_store.py --databaseName 'default' --storeName 'nyctaxi_schema'"
)

parser.add_argument('--databaseName', dest='databaseName', type=str, help='Name of the database where the store is present')
parser.add_argument('--storeName', dest='storeName', type=str, help='Name of the store')
args = parser.parse_args()

spark = SparkSession.builder.appName('datadiff').getOrCreate()

spark.sql(f"USE {args.databaseName}")
spark.sql(f"SELECT * FROM {args.storeName}").show()
