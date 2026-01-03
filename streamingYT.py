from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, explode
from pyspark.sql.types import StructType, StructField, StringType, ArrayType, TimestampType, MapType

spark = SparkSession.builder \
    .appName("YoutubeStreamToPostgres") \
    .master("spark://spark-master:7077")  \
    .getOrCreate()
    
# Seme za citanje jsona
snippet_schema = StructType([
    StructField("title", StringType(), True),
    StructField("description", StringType(), True),
    StructField("channelTitle", StringType(), True),
    StructField("publishedAt", StringType(), True)
])
item_schema = StructType([
    StructField("id", StructType([StructField("videoId", StringType(), True)]), True),
    StructField("snippet", snippet_schema, True)
])
json_schema = StructType([
    StructField("keyword", StringType(), True),  # tvoj "search query" PROMJENITI KEYWORD
    StructField("items", ArrayType(item_schema), True)
])

df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "kafka:19092") \
    .option("subscribe", "projekat1yt") \
    .option("startingOffsets", "earliest") \
    .load()

# Kafka value je u binary formatu
df = df.selectExpr("CAST(value AS STRING) as json_str")

# Parsiraj JSON
df = df.withColumn("data", from_json(col("json_str"), json_schema))

df = df.withColumn("item", explode(col("data.items")))

df = df.select(
    col("data.keyword").alias("search_query"),
    col("item.id.videoId").alias("video_id"),
    col("item.snippet.title").alias("title"),
    col("item.snippet.channelTitle").alias("channel_title"),
    col("item.snippet.publishedAt").alias("published_at"),
    col("item.snippet.description").alias("description")
)

def write_to_postgres(batch_df, batch_id):
    batch_df_local = batch_df.cache()  # ili persist
    batch_df_local.write \
        .format("jdbc") \
        .option("url", "jdbc:postgresql://postgres:5432/postgres") \
        .option("dbtable", "ytSTREAM12") \
        .option("user", "postgres") \
        .option("password", "postgres") \
        .option("driver", "org.postgresql.Driver") \
        .mode("append") \
        .save()
    batch_df_local.unpersist()


query = df.writeStream \
    .foreachBatch(write_to_postgres) \
    .outputMode("append") \
    .start()

# query = df.writeStream \                               #Ovo je radilo i sa spark masterom
#     .format("console") \
#     .outputMode("append") \
#     .option("truncate", "false") \
#     .start()


query.awaitTermination()  #Ceka poruke