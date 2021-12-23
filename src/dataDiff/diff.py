from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
import json

from dataDiff.schemaStore import getStoreDF

spark = SparkSession.builder.appName('datadiff').getOrCreate()

def getJoinExpression(keyCols):
    expr = ""
    for leftCol in keyCols:
        rightCol = keyCols[leftCol]
        expr += f"LEFT.{leftCol} == RIGHT.{rightCol} AND "
    return expr[:-5] #Remove the extra 'AND' at the end of expression

def getSelectExpression(leftCols, rightCols):
    expr = "SELECT "    
    for col in leftCols:
        expr += f"LEFT.{col} AS L_{col}, "
    for col in rightCols:
        expr += f"RIGHT.{col} AS R_{col}, "           
    expr = expr[:-2] #Remove the extra ',' at the end of expression
    expr += " FROM LEFT FULL OUTER JOIN RIGHT ON "    
    return expr

def getMatchExpression(matchCols):     
    expr = ""
    for leftCol in matchCols:
        rightCol = matchCols[leftCol]
        expr += f"L_{leftCol} == R_{rightCol} AND "
    return expr[:-5] #Remove the extra 'AND' at the end of expression


def matchFile(row):
    leftFileSchema = StructType.fromJson(json.loads(row['left_file_schema']))
    rightFileSchema = StructType.fromJson(json.loads(row['right_file_schema']))

    leftDF = spark.read \
        .format(row['left_file_format']) \
        .option('header', row['left_file_header'] ) \
        .option('delimiter', row['left_file_delimiter'] ) \
        .schema(leftFileSchema) \
        .load(row['left_file_path']) \

    rightDF = spark.read \
        .format(row['left_file_format']) \
        .option('header', row['left_file_header'] ) \
        .option('delimiter', row['left_file_delimiter'] ) \
        .schema(rightFileSchema) \
        .load(row['right_file_path']) \
        
    leftDF.createOrReplaceTempView('LEFT')
    rightDF.createOrReplaceTempView('RIGHT')
    
    df = spark.sql(getSelectExpression(leftDF.columns, rightDF.columns) + getJoinExpression(row['key_cols']))
    # print(joinDF.schema)

    matchCols = row['match_cols']
    def unmatchedCols(row): 
        d = {}
        for leftCol in matchCols:
            leftColName = f"L_{leftCol}"
            rightColName = f"R_{matchCols[leftCol]}"
            try: 
                if row[leftColName] != row[rightColName]:
                    d[leftColName] = rightColName
            except Exception as e:
                    d[leftColName] = rightColName
        return d

    unmatchedColsUDF = udf(unmatchedCols, MapType(StringType(), StringType()))
    matchDF = df.withColumn('matched', expr(getMatchExpression(matchCols))) \
                .withColumn('unmatched_cols', unmatchedColsUDF(struct([df[x] for x in df.columns])))

    return matchDF

def getDiff(spark, storeName) : 
    schemaStorItr = getStoreDF(spark, storeName).rdd.toLocalIterator()
    for row in schemaStorItr:        
        # matchFile(row).show()
        matchFile(row).write \
            .format('delta') \
            .mode('overwrite') \
            .save(row['output_file_path'])
