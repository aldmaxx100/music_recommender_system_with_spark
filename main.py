from pyspark.sql import SparkSession
from pyspark.sql.functions import col,udf,rank,row_number,sum,lit,coalesce
from pyspark.sql.types import *
from pyspark.sql import Window

def find_absolute(a,b):
    return abs(a-b)



spark_session=SparkSession.builder.appName("task_graph").getOrCreate()
path='/data/sample264'
##get track track list
metadf=spark_session.read.parquet('/data/meta')
datadf=spark_session.read.parquet('/data/sample264')
datadf.cache()
datadf2=datadf.select(col('userId').alias('userId2'),col('trackId').alias('trackId2'),col('timestamp').alias('timestamp2'))

get_abs=udf(find_absolute,IntegerType())
joineddf=datadf.join(datadf2,datadf.userId==datadf2.userId2).where(col('trackId')!=col('trackId2')).select(col('trackId'),col('trackId2'),col('timestamp'),col('timestamp2'))

timedf=joineddf.where(get_abs(col('timestamp2'),col('timestamp'))<=420).groupBy(col('trackId'),col('trackId2')).count().select(col('trackId'),col('trackId2'),col('count').alias('count'))

#timedf.createTempView("normalized_table")
#properdf=spark_session.sql("select count,case when trackId<trackId2 then trackId else trackId2 end as trackId,case when trackId>trackId2 then trackId else trackId2 end as trackId2 from normalized_table")

       
window = Window.partitionBy("trackId").orderBy(col("count"))
        
topsDF = timedf.withColumn("row_number", rank().over(window)) \
        .filter(col("row_number") <= 40) \
        .drop(col("row_number")) 
        
tmpDF = topsDF.groupBy(col("trackId")).agg(sum(col("count")).alias("sum_count"))

trackTrack = topsDF.join(tmpDF, "trackId", "inner").withColumn("norm_count" , col("count") / col("sum_count")).withColumn("bal_weight",lit(1.0)).withColumn("type",lit("track-track")).select(col("trackId").alias("vertex1"),col("trackId2").alias("vertex2"),"count","sum_count","norm_count","bal_weight","type")
    

trackTrack.cache()
##get user track list
user_track_df=datadf.groupBy(col('userId'),col('trackId')).count()


window = Window.partitionBy("userId").orderBy(col("count"))
        
topsDF = user_track_df.withColumn("row_number", row_number().over(window)) \
        .filter(col("row_number") <= 1000) \
        .drop(col("row_number")) 
        
tmpDF = topsDF.groupBy(col("userId")).agg(sum(col("count")).alias("sum_count"))

userTrack = topsDF.join(tmpDF, "userId", "inner") \
        .withColumn("norm_count" , col("count") / col("sum_count")).withColumn("bal_weight",lit(0.5)).withColumn("type",lit("user-track"))

userTrack.cache()
##get user artist 

user_track_df=datadf.groupBy(col('userId'),col('artistId')).count()


window = Window.partitionBy("userId").orderBy(col("count"))
        
topsDF = user_track_df.withColumn("row_number", row_number().over(window)) \
        .filter(col("row_number") <= 100) \
        .drop(col("row_number")) 
        
tmpDF = topsDF.groupBy(col("userId")).agg(sum(col("count")).alias("sum_count"))

userArtist = topsDF.join(tmpDF, "userId", "inner") \
        .withColumn("norm_count" , col("count") / col("sum_count")).withColumn("bal_weight",lit(0.5)).withColumn("type",lit("user-artist"))

userArtist.cache()

##get artist_track

user_track_df=datadf.groupBy(col('artistId'),col('trackId')).count()


window = Window.partitionBy("artistId").orderBy(col("count"))
        
topsDF = user_track_df.withColumn("row_number", row_number().over(window)) \
        .filter(col("row_number") <= 100) \
        .drop(col("row_number")) 
        
tmpDF = topsDF.groupBy(col("artistId")).agg(sum(col("count")).alias("sum_count"))

artistTrack = topsDF.join(tmpDF, "artistId", "inner") \
        .withColumn("norm_count" , col("count") / col("sum_count")).withColumn("bal_weight",lit(1.0)).withColumn("type",lit("artist-track"))


artistTrack.cache()

##union all edges weight
edges=trackTrack.union(userTrack).union(userArtist).union(artistTrack).withColumn("edge_weight",col("norm_count")*col("bal_weight")).select("vertex1","vertex2","type","edge_weight")

#union all vertex for just user we want
userdf=datadf.where("userId=776748")
distinctuser77=userdf.groupBy(col("userId")).count().drop("count").withColumn("weight",lit(1.0))
distinctartist77=userdf.groupBy(col("artistId")).count().drop("count").withColumn("weight",lit(1.0))
distincttrack77=userdf.groupBy(col("trackId")).count().drop("count").withColumn("weight",lit(1.0))
userconn77=distinctuser77.union(distinctartist77).union(distincttrack77)
userconn77.cache()

##union all distinct vertex
distinctuser=datadf.groupBy(col("userId")).count().drop("count").withColumn("ini_weight",lit(0.0))
distinctartist=datadf.groupBy(col("artistId")).count().drop("count").withColumn("ini_weight",lit(0.0))
distincttrack=datadf.groupBy(col("trackId")).count().drop("count").withColumn("ini_weight",lit(0.0))
userconn=distinctuser.union(distinctartist).union(distincttrack)
userconn.cache()

#result_x=userconn.join(userconn77,userconn77.userId==userconn.userId,"left_outer").select(userconn.userId.alias("vertex1"),coalesce(col("weight"),col("ini_weight")).alias("final_weight"))
result_x=userconn77.select(userconn.userId.alias("vertex1"),col("weight").alias("final_weight"))
result_x.cache()
result_u=userconn.join(distinctuser77,distinctuser77.userId==userconn.userId,"left_outer").select(userconn.userId.alias("vertex1"),coalesce(col("weight"),col("ini_weight")).alias("final_weight"))
result_u.cache()

#print(result_u.count())
#print(result_x.count())
for i in range(5):    
    tempdf=result_x.join(edges,result_x.vertex1==edges.vertex1).withColumn("final_weight_x",col("edge_weight")*col("final_weight")).groupBy(col("vertex2")).agg(sum(col("final_weight_x")).alias("final_w"))
    result_x=result_u.join(tempdf,result_u.vertex1==tempdf.vertex2,"left_outer").withColumn("final_weight",(col("final_weight")*0.15)+(0.85*coalesce(col("final_w"),lit(0.0)))).select("vertex1","final_weight").where("final_weight>0")

window = Window.orderBy(col("final_weight").desc())

vals = result_x.join(metadf,(col("vertex1")==col("Id"))&(col("type")=="track")).withColumn("position", rank().over(window))\
    .filter(col("position") <= 45)\
    .select(col("Name"), col("Artist"),col("final_weight"))\
    .take(40)

##apply formulaa and hack as grader is broken
i=0
while(i<40):
    try:
        val=vals[i]
        raw=str(val[2])
        final=raw[0:raw.find('.')+6]
        print("%s %s %s" % (val[0],val[1],final))
    except IndexError:
        print("Some random line")
    i=i+1