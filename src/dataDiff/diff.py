import json
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, expr, struct
from pyspark.sql.types import StructType, MapType, StringType
from dataDiff.schemaStore import getStoreDF

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


def matchFile(spark, row):
    leftFileSchema = StructType.fromJson(json.loads(row['left_file_schema']))
    rightFileSchema = StructType.fromJson(json.loads(row['right_file_schema']))

    leftFileFormat = row['left_file_format'] 
    if (leftFileFormat == 'csv'):
        leftDF = spark.read \
            .format(leftFileFormat) \
            .option('header', row['left_file_header'] ) \
            .option('delimiter', row['left_file_delimiter'] ) \
            .schema(leftFileSchema) \
            .load(row['left_file_path'])
    else: #Skip header and delimiter for text and delta formats 
        leftDF = spark.read \
            .format(leftFileFormat) \
            .schema(leftFileSchema) \
            .load(row['left_file_path'])

    rightFileFormat = row['right_file_format']
    if (rightFileFormat == 'csv'):
        rightDF = spark.read \
            .format(rightFileFormat) \
            .option('header', row['right_file_header'] ) \
            .option('delimiter', row['right_file_delimiter'] ) \
            .schema(rightFileSchema) \
            .load(row['right_file_path'])
    else: 
        rightDF = spark.read \
            .format(rightFileFormat) \
            .schema(rightFileSchema) \
            .load(row['right_file_path'])

    leftDF.createOrReplaceTempView('LEFT')
    rightDF.createOrReplaceTempView('RIGHT')
    
    df = spark.sql(getSelectExpression(leftDF.columns, rightDF.columns) + getJoinExpression(row['key_cols']))

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
        matchFile(spark, row).write \
            .format('delta') \
            .mode('overwrite') \
            .save(row['output_file_path'])
