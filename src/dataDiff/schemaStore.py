from pyspark.sql.types import StructField, StructType, StringType, MapType, BooleanType
from pyspark.sql import SparkSession

schema = StructType([
    StructField('id', StringType(), False),
    StructField('left_file_path', StringType(), False),
    StructField('left_file_format', StringType(), False),
    StructField('left_file_header', BooleanType(), False),
    StructField('left_file_delimiter', StringType(), False),
    StructField('left_file_schema', StringType(), False),    
    StructField('right_file_path', StringType(), False),
    StructField('right_file_format', StringType(), False),
    StructField('right_file_header', BooleanType(), False),
    StructField('right_file_delimiter', StringType(), False),
    StructField('right_file_schema', StringType(), False),
    StructField('output_file_path', StringType(), False),
    StructField('key_cols', MapType(StringType(),StringType(), False)),
    StructField('match_cols', MapType(StringType(),StringType(), False)),
])

def deleteStore(spark, storeName, storePath): 
    spark.sql(f"DROP TABLE IF EXISTS {storeName}")
    deleteStoreFolderPath(spark, storePath)

def createSchemaStore(spark, storeName, storePath): 
    df = spark.createDataFrame([], schema)
    df.write.format('delta') \
        .mode('overwrite')  \
        .save(storePath)

    spark.sql(f"""CREATE TABLE IF NOT EXISTS {storeName}
                USING DELTA
                LOCATION '{storePath}'""")

def addSchemaRow(spark, data, storeName):     
    df = spark.createDataFrame([ data ], schema)
    tableDf = spark.table(storeName)
    tableDf.union(df) \
        .dropDuplicates(['id']) \
        .write \
        .insertInto(storeName, overwrite = True)  

def getStoreDF(spark, storeName): 
    return spark.table(storeName)

def storeExists(spark, storeName):
    return spark._jsparkSession.catalog().tableExists(storeName)

def deleteStoreFolderPath(spark, storePath):
    sc = spark.sparkContext
    fs = (sc._jvm.org
          .apache.hadoop
          .fs.FileSystem
          .get(sc._jsc.hadoopConfiguration())
          )
    fs.delete(sc._jvm.org.apache.hadoop.fs.Path(storePath), True)
