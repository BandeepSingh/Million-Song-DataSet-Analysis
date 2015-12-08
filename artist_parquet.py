from pyspark import SparkConf, SparkContext,SQLContext
from pyspark.sql.types import StructType, StructField, StringType, IntegerType
from pyspark.sql.functions import *
import sys, operator

def main():
	conf = SparkConf().setAppName('artist_career')
	sc = SparkContext(conf=conf)
	assert sc.version >= '1.5.1'
	sqlContext=SQLContext(sc)
	inputs = sys.argv[1]
	output = sys.argv[2]
	customSchema = StructType([StructField('SongNumber', StringType(),False),StructField('SongID', StringType(),False),StructField('AlbumID', StringType(),False),StructField('AlbumName', StringType(),False),StructField('ArtistID', StringType(),False),StructField('ArtistLatitude', StringType(),False),StructField('ArtistLocation', StringType(),False),StructField('ArtistLongitude', StringType(),False),StructField('ArtistName', StringType(),False),StructField('Danceability', StringType(),False),StructField('Duration', StringType(),False),StructField('KeySignature', StringType(),False),StructField('KeySignatureConfidence', StringType(),False),StructField('Tempo', StringType(),False),StructField('TimeSignature', StringType(),False),StructField('TimeSignatureConfidence', StringType(),False),StructField('Title', StringType(),False),StructField('Year', StringType(),False),StructField('Energy', StringType(),False),StructField('ArtistFamiliarity', StringType(),False),StructField('ArtistMbid', StringType(),False),StructField('SongHotttnesss', StringType(),False),StructField('Loudness', StringType(),False),StructField('StartOfFadeOut', StringType(),False),StructField('EndOfFadeIn', StringType(),False),StructField('ModeConfidence', StringType(),False)])

	df= sqlContext.read.format('com.databricks.spark.csv').options(header='true').load(inputs,schema = customSchema)

	df.registerTempTable('artist_data')

	million_song=sqlContext.sql("SELECT SongNumber,SongID,AlbumID,AlbumName,ArtistID,ArtistLatitude,ArtistLocation,ArtistLongitude,ArtistName,Danceability,Duration,KeySignature,KeySignatureConfidence,Tempo,TimeSignature,TimeSignatureConfidence,Title,Year,Energy,ArtistFamiliarity,ArtistMbid,SongHotttnesss,Loudness,StartOfFadeOut,EndOfFadeIn,ModeConfidence from artist_data where Year!=0 AND ArtistFamiliarity!='nan'")
	million_song.write.format('parquet').save(output)
	
if __name__ == "__main__":
    main()
