__author__ = 'BANDEEP SINGH'

from pyspark import SparkConf, SparkContext
from pyspark.sql import SQLContext
from pyspark.sql.types import *
#from pyspark.sql.types import StructType, StructField, StringType, IntegerType

import sys, operator

output = sys.argv[1]

conf = SparkConf().setAppName('Reading Parquet')
sc = SparkContext(conf=conf)
assert sc.version >= '1.5.1'

sqlContext = SQLContext(sc)

df = sqlContext.read.parquet("/user/bandeeps/parquet")

df.registerTempTable('SongData')

data_table = sqlContext.sql("""
SELECT ArtistName,Duration,Year,ArtistFamiliarity,SongHotttnesss,Loudness,KeySignature,StartOfFadeOut,EndOfFadeIn,ModeConfidence
FROM SongData where SongHotttnesss>0
LIMIT 1000
"""
)

rdd = data_table.coalesce(1)

final = rdd.map(tuple)
#print final.collect()
final.saveAsTextFile(output)