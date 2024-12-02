# Imports
from pyspark.sql import SparkSession
from pyspark import SparkConf
from pyspark.sql.functions import col, avg, dense_rank
from pyspark.sql.window import Window

# Original IMDB_analysis_batch file to .py for Dataproc usage

# Spark Configuration
sparkConf = SparkConf()
#We don't have to set the master, Spark automatically finds it
#sparkConf.setMaster("spark://spark-master:7077") 
sparkConf.setAppName("IMDB_Analysis")
sparkConf.set("spark.driver.memory", "2g")
sparkConf.set("spark.executor.cores", "1")
sparkConf.set("spark.driver.cores", "1")

# Create Spark Session
spark = SparkSession.builder.config(conf=sparkConf).getOrCreate()

# Set up Hadoop configuration for Google Cloud Storage
conf = spark.sparkContext._jsc.hadoopConfiguration()
conf.set("fs.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem")
conf.set("fs.AbstractFileSystem.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFS")

# Define BigQuery dataset and table and bucketname
project_id = "dataengineering-439112"
dataset_table = "labdataset.imdb_top_1000"
output_table = "labdataset.imdb_analysis"
bucket_name = "imdb-output-bucket"
# Read data from BigQuery table
imdb_df = spark.read.format("bigquery") \
    .option("project", project_id) \
    .option("table", dataset_table) \
    .load()

# schema
imdb_df.printSchema()
imdb_df.show(5)

# average IMDB rating by genre
avg_rating_by_genre = imdb_df.groupBy("Genre") \
    .agg(avg("IMDB_Rating").alias("Avg_IMDB_Rating")) \
    .orderBy(col("Avg_IMDB_Rating").desc())

# rank movies by IMDB rating within each genre
window_spec = Window.partitionBy("Genre").orderBy(col("IMDB_Rating").desc())
ranked_movies = imdb_df.withColumn("Rank", dense_rank().over(window_spec))

# extract top-rated movies for each genre
top_movies_by_genre = ranked_movies.filter(col("Rank") == 1) \
    .select("Genre", "Series_Title", "IMDB_Rating", "Director", "Released_Year")

# Results to big query and first temporary bucket to store it
spark.conf.set("temporaryGcsBucket", bucket_name)

# Write average ratings by genre to BigQuery
avg_rating_by_genre.write.format("bigquery") \
    .option("table", f"{project_id}.{output_table}_avg_rating_by_genre") \
    .mode("overwrite") \
    .save()

# Write top-rated movies by genre to BigQuery
top_movies_by_genre.write.format("bigquery") \
    .option("table", f"{project_id}.{output_table}_top_movies_by_genre") \
    .mode("overwrite") \
    .save()
# Stop Spark session
spark.stop()