from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, explode
from pyspark.sql.types import StructType, StructField, StringType, ArrayType

spark = SparkSession.builder \
    .appName("YoutubeStreamToPostgres") \
    .master("spark://spark-master-stream:7077")  \
    .getOrCreate()

# ----- Shema komentara -----
author_schema = StructType([
    StructField("value", StringType(), True)
])

top_comment_snippet_schema = StructType([
    StructField("textOriginal", StringType(), True),
    StructField("authorDisplayName", StringType(), True),
    StructField("authorChannelId", author_schema, True),
    StructField("likeCount", StringType(), True),
    StructField("publishedAt", StringType(), True)
])

top_level_comment_schema = StructType([
    StructField("id", StringType(), True),
    StructField("snippet", top_comment_snippet_schema, True)
])

snippet_schema = StructType([
    StructField("videoId", StringType(), True),
    StructField("topLevelComment", top_level_comment_schema, True)
])

item_schema = StructType([
    StructField("id", StringType(), True),  # commentId
    StructField("snippet", snippet_schema, True)
])

json_schema = StructType([
    StructField("items", ArrayType(item_schema), True)
])

# ----- Spark streaming iz Kafka -----
df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "kafka:19092") \
    .option("subscribe", "projekat1yt") \
    .option("startingOffsets", "earliest") \
    .load()

# Kafka value je u binary formatu
df = df.selectExpr("CAST(value AS STRING) as json_str")

# Parsiranje JSON-a
df = df.withColumn("data", from_json(col("json_str"), json_schema))

# Explodiraj items
df = df.withColumn("item", explode(col("data.items")))

# ----- SELECT sa ispravnim kolonama -----
df = df.select(
    col("item.snippet.videoId").alias("video_id"),
    col("item.id").alias("comment_id"),
    col("item.snippet.topLevelComment.id").alias("top_comment_id"),
    col("item.snippet.topLevelComment.snippet.textOriginal").alias("comment_text"),
    col("item.snippet.topLevelComment.snippet.authorDisplayName").alias("author_name"),
    col("item.snippet.topLevelComment.snippet.authorChannelId.value").alias("author_channel_id"),
    col("item.snippet.topLevelComment.snippet.likeCount").alias("like_count"),
    col("item.snippet.topLevelComment.snippet.publishedAt").alias("comment_published_at")
)

# ----- Funkcija za upis u Postgres -----
def write_to_postgres(batch_df, batch_id):
    batch_df_local = batch_df.cache()
    batch_df_local.write \
        .format("jdbc") \
        .option("url", "jdbc:postgresql://postgres:5432/postgres") \
        .option("dbtable", "CommentsStream") \
        .option("user", "postgres") \
        .option("password", "postgres") \
        .option("driver", "org.postgresql.Driver") \
        .mode("append") \
        .save()
    batch_df_local.unpersist()

# ----- Stream upis -----
query = df.writeStream \
    .foreachBatch(write_to_postgres) \
    .outputMode("append") \
    .start()

query.awaitTermination()
