from pyspark.ml.classification import LogisticRegression
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, rand
import os

spark = SparkSession.builder \
    .appName("Get data to training model") \
    .config("spark.hadoop.fs.s3a.access.key", os.getenv("MINIO_USERNAME")) \
    .config("spark.hadoop.fs.s3a.secret.key", os.getenv("MINIO_PASSWORD")) \
    .config("spark.hadoop.fs.s3a.endpoint", "http://localhost:9000")\
    .config("spark.jars", "/home/drissdo/Desktop/Scalable-Distributed-Systems/src/jars/aws-java-sdk-bundle-1.11.901.jar, /home/drissdo/Desktop/Scalable-Distributed-Systems/src/jars/hadoop-aws-3.3.1.jar")\
    .config("spark.hadoop.fs.s3a.path.style.access", "true")\
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")\
    .getOrCreate()


silver_bucket_path = "s3a://sliver/preprocessed_loan_data"

loan_data_scaled = spark.read.parquet(silver_bucket_path)

default_data = loan_data_scaled.filter(col("Default") == 1)
non_default_data = loan_data_scaled.filter(col("Default") == 0)

train_default, test_default = default_data.randomSplit([0.8, 0.2], seed=42)
train_non_default, test_non_default = non_default_data.randomSplit([0.8, 0.2], seed=42)

train_data = train_default.union(train_non_default)
test_data = test_default.union(test_non_default)

# Shuffle the data
train_data = train_data.orderBy(rand(seed=42))
test_data = test_data.orderBy(rand(seed=42))

# Drop unnecessary columns
train_data = train_data.select(["LoanID", "Default","scaled_features"])
test_data = test_data.select(["LoanID", "scaled_features"])



# Training model
lr = LogisticRegression(featuresCol="scaled_features", labelCol="Default", maxIter=10)
lr_model = lr.fit(train_data)

# save model
model_path = "/home/drissdo/Desktop/Scalable-Distributed-Systems/ML/model"
import os
if os.path.exists(model_path):
    print(f"Path {model_path} already exists. Consider removing it or choosing a new path.")

# Save the model
lr_model.write().overwrite().save(model_path)
  