from pyspark.sql import SparkSession
from pyspark import SparkConf
from pyspark.sql.functions import col, avg, explode, split, dense_rank, row_number, countDistinct, sum, stddev, rank, regexp_replace, floor, trim, udf
from pyspark.sql.types import ArrayType, StringType, IntegerType
from pyspark.sql.window import Window
from pyspark.ml.feature import VectorAssembler, StringIndexer, OneHotEncoder, Tokenizer, StopWordsRemover, CountVectorizer
from pyspark.ml.regression import RandomForestRegressor
from pyspark.ml.clustering import LDA
from pyspark.ml import Pipeline
from pyspark.ml.evaluation import RegressionEvaluator
import numpy as np
# Original imdb_analysis_batch_additional file to .py for Dataproc usage

# Spark Configuration
sparkConf = SparkConf()
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
### Define Variables and Load data
# Define BigQuery dataset and table
project_id = "dataengineering-439112"
dataset_table = "labdataset.imdb_top_1000"
output_table_decade_trends = "labdataset.imdb_decade_trends"
output_table_genre_ratings = "labdataset.imdb_genre_ratings"
bucket_name = "imdb-output-bucket"

# Create Spark session
spark = SparkSession.builder \
    .appName("IMDB_Analysis") \
    .config("temporaryGcsBucket", bucket_name) \
    .getOrCreate()

# Load data
imdb_df = spark.read.format("bigquery") \
    .option("project", project_id) \
    .option("table", dataset_table) \
    .load()

### Analysis on Dataset based on Decades and on Genres
# Clean and preprocess data
imdb_df = imdb_df.withColumn("Runtime", regexp_replace(col("Runtime"), " min", "").cast("int"))
imdb_df = imdb_df.withColumn("Gross", col("Gross").cast("double"))

# Fill missing values
imdb_df = imdb_df.fillna({
    "Runtime": 0,
    "IMDB_Rating": 0,
    "No_of_Votes": 0,
    "Gross": 0,
    "Director": "unknown",
    "Genre": "unknown",
    "Overview": "unknown"
})

# Add Decade column
imdb_df = imdb_df.withColumn("Decade", (floor(col("Released_Year").cast("int") / 10) * 10).cast("int"))

# Split genres into individual rows and remove duplicates
split_genres_df = imdb_df.withColumn("Individual_Genre", explode(split(trim(col("Genre")), ", "))).dropDuplicates(["Individual_Genre", "Series_Title"])

# Decade-Based Trends
decade_trends = imdb_df.groupBy("Decade") \
    .agg(
        avg("IMDB_Rating").alias("Avg_IMDB_Rating"),
        stddev("IMDB_Rating").alias("StdDev_IMDB_Rating"),
        countDistinct("Series_Title").alias("Movies_Count"),
        avg("Runtime").alias("Avg_Runtime"),
        stddev("Runtime").alias("StdDev_Runtime")
    ) \
    .orderBy("Decade")

# Display an example of Decade Trends DataFrame
print("Example of Decade-Based Trends:")
decade_trends.show(5, truncate=False)

# Write decade trends to BigQuery
decade_trends.write.format("bigquery") \
    .option("table", f"{project_id}.{output_table_decade_trends}") \
    .mode("overwrite") \
    .save()

# Ratings, Runtime, and Movies Count by Genre 
avg_rating_by_genre = split_genres_df.groupBy("Individual_Genre") \
    .agg(
        avg("IMDB_Rating").alias("Avg_IMDB_Rating"),
        stddev("IMDB_Rating").alias("StdDev_IMDB_Rating"),
        countDistinct("Series_Title").alias("Movies_Count"),
        avg("Runtime").alias("Avg_Runtime"),
        avg("Gross").alias("Avg_Box_Office")
    )

# Display an example of Genre Ratings DataFrame
print("Example of Ratings by Genre:")
avg_rating_by_genre.show(5, truncate=False)

# Write genre ratings to BigQuery
avg_rating_by_genre.write.format("bigquery") \
    .option("table", f"{project_id}.{output_table_genre_ratings}") \
    .mode("overwrite") \
    .save()

print(imdb_df.head())
### Topic Modeling + Additional Dataset
# Create Spark session
spark = SparkSession.builder \
    .appName("IMDB_Analysis") \
    .config("temporaryGcsBucket", "imdb-output-bucket") \
    .getOrCreate()

# Load original data from BigQuery
imdb_df = spark.read.format("bigquery") \
    .option("project", "dataengineering-439112") \
    .option("table", "labdataset.imdb_top_1000") \
    .load()

# Load the additional dataset from Cloud Storage
additional_df = spark.read.csv("gs://imdb-bucket-data/imdb_additional_data.csv", header=True, inferSchema=True)

# Deduplicate additional_df before the join
additional_df = additional_df.dropDuplicates(["Series_Title"])

# Perform the join
imdb_df = imdb_df.join(additional_df, on="Series_Title", how="left")

# Remove duplicates after the join
imdb_df = imdb_df.dropDuplicates()

# Drop rows with missing critical data
imdb_df = imdb_df.dropna(subset=["IMDB_Rating", "Gross", "Runtime", "Director", "Genre", "Overview"])

# Check row count
print(f"Final row count: {imdb_df.count()}")

# Clean and preprocess data
imdb_df = imdb_df.withColumn("Runtime", regexp_replace(col("Runtime"), " min", "").cast("int"))
imdb_df = imdb_df.withColumn("Gross", col("Gross").cast("double"))

# Remove rows with missing values 
imdb_df = imdb_df.dropna(subset=["IMDB_Rating", "Gross", "Runtime", "Director", "Genre", "Overview"])

# Print the count of remaining rows
print(f"Number of rows after dropping missing values: {imdb_df.count()}")

# Add Decade column
imdb_df = imdb_df.withColumn("Decade", (floor(col("Released_Year").cast("int") / 10) * 10).cast("int"))

# Preprocess data
# Tokenize Overview text
tokenizer = Tokenizer(inputCol="Overview", outputCol="Tokenized_Overview")
imdb_df = tokenizer.transform(imdb_df)

# Remove stopwords
remover = StopWordsRemover(inputCol="Tokenized_Overview", outputCol="Filtered_Overview")
imdb_df = remover.transform(imdb_df)

# Vectorize text
vectorizer = CountVectorizer(inputCol="Filtered_Overview", outputCol="Overview_Features")
vectorizer_model = vectorizer.fit(imdb_df)
imdb_df = vectorizer_model.transform(imdb_df)

# Train LDA Model
lda = LDA(k=5, maxIter=10, featuresCol="Overview_Features", seed=42)
lda_model = lda.fit(imdb_df)
imdb_df = lda_model.transform(imdb_df)

# Features
# Index and encode categorical features
genre_indexer = StringIndexer(inputCol="Genre", outputCol="Genre_Indexed", handleInvalid="keep")
genre_encoder = OneHotEncoder(inputCol="Genre_Indexed", outputCol="Genre_Encoded")

director_indexer = StringIndexer(inputCol="Director", outputCol="Director_Indexed", handleInvalid="keep")
director_encoder = OneHotEncoder(inputCol="Director_Indexed", outputCol="Director_Encoded")

location_indexer = StringIndexer(inputCol="Filming_Locations", outputCol="Location_Indexed", handleInvalid="keep")
location_encoder = OneHotEncoder(inputCol="Location_Indexed", outputCol="Location_Encoded")

# Assemble all features, including topics and additional data
assembler = VectorAssembler(
    inputCols=[
        "IMDB_Rating", "Runtime", "Awards_Won", "Oscars_Nominated", 
        "Production_Budget", "Audience_Score", "Genre_Encoded", 
        "Director_Encoded", "Location_Encoded", "topicDistribution"
    ],
    outputCol="features",
    handleInvalid="skip"
)

# Train Model
rf = RandomForestRegressor(featuresCol="features", labelCol="Gross", numTrees=50, maxDepth=10, seed=42)

pipeline = Pipeline(stages=[
    genre_indexer, genre_encoder, director_indexer, director_encoder, 
    location_indexer, location_encoder, assembler, rf
])

# Train-test split
train_df, test_df = imdb_df.randomSplit([0.8, 0.2], seed=42)

# Train the model
model = pipeline.fit(train_df)

# Make predictions
predictions = model.transform(test_df)

# Calculate R²
evaluator = RegressionEvaluator(labelCol="Gross", predictionCol="prediction", metricName="r2")
r2 = evaluator.evaluate(predictions)
print(f"R²: {r2}")

# Feature importances
rf_model = model.stages[-1]  # RandomForestRegressor
importances = rf_model.featureImportances.toArray()

# Map feature importances
assembler_inputs = assembler.getInputCols()
feature_importance_map = dict(zip(assembler_inputs, importances))

# Sort and print feature importances
sorted_importance = sorted(feature_importance_map.items(), key=lambda x: x[1], reverse=True)
print("Feature Importances:")
for feature, score in sorted_importance:
    print(f"{feature}: {score}")

# Convert feature importances to a DataFrame
feature_importances_df = spark.createDataFrame(
    [(k, float(v)) for k, v in feature_importance_map.items()],
    ["Feature", "Importance"]
)

# Save to BigQuery
project_id = "dataengineering-439112"
output_table_feature_importances = "labdataset.imdb_feature_importances"

feature_importances_df.write.format("bigquery") \
    .option("table", f"{project_id}.{output_table_feature_importances}") \
    .mode("overwrite") \
    .save()

print("Feature importances successfully saved to BigQuery.")

# Stop Spark session
spark.stop()