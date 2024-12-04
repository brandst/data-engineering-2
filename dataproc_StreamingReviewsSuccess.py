from pyspark.sql import SparkSession
from pyspark import SparkConf
from pyspark.sql.types import StructType, StructField, LongType, StringType, DoubleType, IntegerType

from pyspark.sql.functions import *
from time import sleep

# Stream File in .py format for dataproc usage

sparkConf = SparkConf()
#sparkConf.setMaster("spark://spark-master:7077")
sparkConf.setAppName("StreamingReviews")
sparkConf.set("spark.driver.memory", "2g")
sparkConf.set("spark.executor.cores", "1")
sparkConf.set("spark.driver.cores", "1")

#Review_ID,Movie_ID,Reviewer_Name,Review_Rating,Reviewer_Nationality,Reviewer_Age,Review_Date,Sex
dataSchema = StructType(
    [StructField("Review_ID", StringType(), True),
     StructField("Movie_ID", StringType(), True),
     StructField("Reviewer_Name", StringType(), True),
     StructField("Review_Rating", LongType(), True),
     StructField("Reviewer_Nationality", StringType(), True),
     StructField("Reviewer_Age", LongType(), True),
     StructField("Review_Date", StringType(), True),
     StructField("Sex", StringType(), True)
     ])

# create the spark session, which is the entry point to Spark SQL engine.
spark = SparkSession.builder.config(conf=sparkConf).getOrCreate()

# We need to set the following configuration whenever we need to use GCS.
# Setup hadoop fs configuration for schema gs://
conf = spark.sparkContext._jsc.hadoopConfiguration()
conf.set("fs.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem")
conf.set(
    "fs.AbstractFileSystem.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFS"
)

# Use the Cloud Storage bucket for temporary BigQuery export data used by the connector.
bucket = "imdb-spark-stream"
spark.conf.set("temporaryGcsBucket", bucket)

# Read the whole dataset as a batch
kafkaStream = spark \
    .readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "kafka1:9093") \
    .option("subscribe", "movie_reviews") \
    .option("startingOffsets", "earliest") \
    .load()

df = kafkaStream.selectExpr("CAST(value AS STRING)")

df1 = df.select(from_json(df.value, dataSchema.simpleString()))

df1.printSchema()

sdf = df1.select(col("from_json(value).*"))

# Converting releaseDateTheaters to datetime
sdf = sdf.withColumn(
    "Review_Date", to_date(col("Review_Date"), "M/d/yyyy")
)

sdf.printSchema()

# Step 1: Calculate the daily average rating for each movie
daily_movie_ratings = (
    sdf
    .groupBy("Movie_ID", "Review_Date")
    .agg(avg("Review_Rating").alias("Daily_Average_Rating"))
)

# Step 2: Filter movies with very low average ratings
low_rated_movies = (
    daily_movie_ratings
    .filter(col("Daily_Average_Rating") < 6.0)  # Threshold for low ratings
)

def my_foreach_batch_function(df, batch_id):
   # Saving the data to BigQuery as batch processing sink -see, use write(), save(), etc.
    df.write.format('bigquery') \
      .option('table', 'dataengineering-439112.labdataset.low_rated_movies') \
      .mode("overwrite") \
      .save()

# Output to the console or sink
query = (
    low_rated_movies
    .writeStream
    .outputMode("complete") 
    .trigger(processingTime="2 seconds")
    .foreachBatch(my_foreach_batch_function)
    .start()
    
)

try:
    query.awaitTermination()
except KeyboardInterrupt:
    query.stop()
    # Stop the spark context
    spark.stop()
    print("Stoped the streaming query and the spark context")