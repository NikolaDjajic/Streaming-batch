from pyspark.sql import SparkSession
from pyspark.sql.functions import col, explode
import boto3

spark = SparkSession.builder \
    .appName("JSON from MinIO to DataFrame") \
    .config("spark.jars", "/app/postgresql-42.7.3.jar,/app/hadoop-aws-3.3.1.jar,/app/aws-java-sdk-bundle-1.12.262.jar") \
    .master("spark://spark-master-batch:7077") \
    .getOrCreate()


# Konfiguracija za MinIO
spark._jsc.hadoopConfiguration().set("fs.s3a.access.key", "nikola")
spark._jsc.hadoopConfiguration().set("fs.s3a.secret.key", "nikolanikola")
spark._jsc.hadoopConfiguration().set("fs.s3a.endpoint", "http://minio:9000")
spark._jsc.hadoopConfiguration().set("fs.s3a.path.style.access", "true")
spark._jsc.hadoopConfiguration().set("fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")

# Čitanje JSON fajlova direktno iz MinIO bucket-a
df = spark.read.option("multiLine", True).json("s3a://projekat1yt/*.json")
df.show(truncate=False)

# Eksplodiramo listu 'items' tako da svaki komentar postane jedan red
df_flat = (
    df.withColumn("item", explode("items"))
      .select(
                col("item.kind").alias("comment_kind"),
                col("item.etag").alias("comment_etag"),
                col("item.id").alias("comment_id"),

                col("item.snippet.channelId").alias("channel_id"),
                col("item.snippet.videoId").alias("video_id"),
                col("item.snippet.canReply").alias("can_reply"),
                col("item.snippet.totalReplyCount").alias("total_reply_count"),
                col("item.snippet.isPublic").alias("is_public"),

                col("item.snippet.topLevelComment.kind").alias("top_comment_kind"),
                col("item.snippet.topLevelComment.etag").alias("top_comment_etag"),
                col("item.snippet.topLevelComment.id").alias("top_comment_id"),

                col("item.snippet.topLevelComment.snippet.channelId").alias("snippet_channel_id"),
                col("item.snippet.topLevelComment.snippet.videoId").alias("snippet_video_id"),
                col("item.snippet.topLevelComment.snippet.textOriginal").alias("comment_text_original"),
                col("item.snippet.topLevelComment.snippet.textDisplay").alias("comment_text_display"),
                col("item.snippet.topLevelComment.snippet.authorDisplayName").alias("author_name"),
                col("item.snippet.topLevelComment.snippet.authorProfileImageUrl").alias("author_profile_image_url"),
                col("item.snippet.topLevelComment.snippet.authorChannelUrl").alias("author_channel_url"),
                
                col("item.snippet.topLevelComment.snippet.canRate").alias("can_rate"),
                col("item.snippet.topLevelComment.snippet.viewerRating").alias("viewer_rating"),
                col("item.snippet.topLevelComment.snippet.likeCount").alias("like_count"),
                col("item.snippet.topLevelComment.snippet.publishedAt").alias("published_at"),
                col("item.snippet.topLevelComment.snippet.updatedAt").alias("updated_at"),

                col("item.snippet.topLevelComment.snippet.authorChannelId.value").alias("author_channel_id")
      )
)

df_fixed = df_flat \
    .withColumn("can_reply", col("can_reply").cast("boolean")) \
    .withColumn("can_rate", col("can_rate").cast("boolean")) \
    .withColumn("is_public", col("is_public").cast("boolean"))

df_fixed.write \
  .format("jdbc") \
  .option("url", "jdbc:postgresql://postgres:5432/postgres") \
  .option("dbtable", "CommentsBatch12") \
  .option("user", "postgres") \
  .option("password", "postgres") \
  .option("driver", "org.postgresql.Driver") \
  .mode("append") \
  .save()


# # --- 7. Brisanje fajlova iz MinIO koristeći boto3 ---
# s3 = boto3.client(
#     's3',
#     endpoint_url='http://minio:9000',  # isto kao u tvom Spark setup-u
#     aws_access_key_id='nikola',
#     aws_secret_access_key='nikolanikola',
#     region_name='us-east-1'
# )

# bucket = 'projekat1yt'

# # Nabavi sve fajlove u bucketu
# objects = s3.list_objects_v2(Bucket=bucket)

# for obj in objects.get('Contents', []):
#     key = obj['Key']
#     if key.endswith('.json'):
#         s3.delete_object(Bucket=bucket, Key=key)
#         print(f"Obrisan fajl: {key}")

# print("✅ Svi JSON fajlovi iz MinIO su obrisani nakon obrade")
# spark.stop()
