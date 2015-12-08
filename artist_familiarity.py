from pyspark import SparkConf, SparkContext,SQLContext
from pyspark.sql.types import StructType, StructField, StringType, IntegerType
from pyspark.sql.functions import *
import sys

inputs = sys.argv[1]
output = sys.argv[2]

reload(sys)
sys.setdefaultencoding("utf8")

def main():
     conf = SparkConf().setAppName('Scenario')
     sc = SparkContext(conf=conf)
     assert sc.version >= '1.5.1'
     sqlContext = SQLContext(sc)
     read_parq=sqlContext.read.parquet(inputs).cache()

     read_parq.registerTempTable('data')
     scenario_1=sqlContext.sql("""
     SELECT ArtistName , MAX(ArtistFamiliarity) AS MaxFamiliarity
     FROM data
     where ArtistFamiliarity!='nan' and ArtistLocation is NOT NULL
     GROUP BY ArtistName
     ORDER BY MAX(ArtistFamiliarity) DESC
     LIMIT 100
     """)
     #scenario_1.show()
     scenario_1.registerTempTable('scenario1')

     joined_artist=sqlContext.sql("""
     SELECT A.ArtistName , B.ArtistFamiliarity,B.SongID,B.SongHotttnesss,B.ArtistLocation,B.ArtistLatitude,B.ArtistLongitude
     FROM scenario1 A
     INNER JOIN data B ON A.ArtistName=B.ArtistName
     """)

     #joined_artist.show()
     joined_artist.registerTempTable('joined_artist')

     AvgFamiliarity=sqlContext.sql("""
     SELECT first(ArtistName) AS Artist_Name, MIN(ArtistFamiliarity) AS MinFamiliarity,MAX(ArtistFamiliarity) AS MaxFamiliarity,ArtistLocation AS Location, first(ArtistLatitude) AS Latitude, first(ArtistLongitude) AS Longitude
     FROM joined_artist
     where ArtistFamiliarity!='nan' and ArtistLatitude <> '' and ArtistLatitude is not NULL
     GROUP BY ArtistLocation
     ORDER BY MAX(ArtistFamiliarity) DESC
     """)


     #AvgFamiliarity.show()

     AvgFamiliarity.rdd.map(tuple).coalesce(1).saveAsTextFile(output+'/scenario1')


     scenario_2=sqlContext.sql("""
     SELECT ArtistName , MAX(SongHotttnesss) as MaxHotttness
     FROM data
     where SongHotttnesss!='nan'
     GROUP BY ArtistName
     ORDER BY MAX(SongHotttnesss) DESC
     LIMIT 10
     """)
     scenario_2.registerTempTable('scenario2')

     joined_artist_hotness=sqlContext.sql("""
     SELECT B.Title, A.ArtistName , B.SongHotttnesss,B.ArtistFamiliarity
     FROM scenario2 A
     INNER JOIN data B ON A.ArtistName=B.ArtistName and A.MaxHotttness=B.SongHotttnesss
     """)
     #joined_artist_hotness.show()
     joined_artist_hotness.rdd.map(tuple).coalesce(1).saveAsTextFile(output+'/scenario2')

     #Of a particular artist
     scenario_3=sqlContext.sql("""
     SELECT ArtistName , Year, AVG(ArtistFamiliarity) AS AvgFamiliarity,COUNT(SongID) AS Total_Songs
     FROM data
     where ArtistName='Britney Spears' and Year!=0
     GROUP BY Year,ArtistName
     ORDER BY Year
     """)


     scenario_3.rdd.map(tuple).coalesce(1).saveAsTextFile(output+'/scenario3')

     scenario_4=sqlContext.sql("""
     SELECT ArtistName , MAX(ArtistFamiliarity)-MIN(ArtistFamiliarity) AS Difference
     FROM data
     GROUP BY ArtistName
     """)
     scenario_4.registerTempTable('scenario4')

     top_diff=sqlContext.sql("""
     SELECT ArtistName , MAX(Difference) as MAXDiff
     FROM scenario4
     GROUP BY ArtistName
     ORDER BY MAXDiff DESC
     LIMIT 10
     """)
     #top_diff.show()



if __name__ == "__main__":
    main()

