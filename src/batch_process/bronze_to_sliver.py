from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, mean
from minio import Minio
import os
from pyspark.ml import Pipeline
from pyspark.ml.feature import Imputer, StringIndexer, OneHotEncoder, VectorAssembler, StandardScaler

# Run full cores
spark = SparkSession.builder \
    .appName("LoanDefaultPrediction") \
    .config("spark.hadoop.fs.s3a.access.key", os.getenv("MINIO_USERNAME")) \
    .config("spark.hadoop.fs.s3a.secret.key", os.getenv("MINIO_PASSWORD")) \
    .config("spark.hadoop.fs.s3a.endpoint", "http://localhost:9000")\
    .config("spark.jars", "/home/drissdo/Desktop/Scalable-Distributed-Systems/src/jars/aws-java-sdk-bundle-1.11.901.jar, /home/drissdo/Desktop/Scalable-Distributed-Systems/src/jars/hadoop-aws-3.3.1.jar")\
    .config("spark.hadoop.fs.s3a.path.style.access", "true")\
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")\
    .getOrCreate()

bronze_path = "s3a://bronze/Loan_default.csv" 
# Read data from bronze
loan_data = spark.read.csv(bronze_path, inferSchema=True, header=True)


# Preprocessing

imputer = Imputer(
    inputCols=["Income", "MonthsEmployed", "NumCreditLines", "InterestRate", "LoanTerm", "DTIRatio"],
    outputCols=["Income_filled", "MonthsEmployed_filled", "NumCreditLines_filled", "InterestRate_filled", "LoanTerm_filled", "DTIRatio_filled"]
)
loan_data_imputed = imputer.fit(loan_data).transform(loan_data)

string_indexers = [
    StringIndexer(inputCol="Education", outputCol="Education_index"),
    StringIndexer(inputCol="EmploymentType", outputCol="EmploymentType_index"),
    StringIndexer(inputCol="MaritalStatus", outputCol="MaritalStatus_index"),
    StringIndexer(inputCol="HasMortgage", outputCol="HasMortgage_index"),
    StringIndexer(inputCol="HasDependents", outputCol="HasDependents_index"),
    StringIndexer(inputCol="LoanPurpose", outputCol="LoanPurpose_index"),
    StringIndexer(inputCol="HasCoSigner", outputCol="HasCoSigner_index")
]

pipeline_indexers = Pipeline(stages=string_indexers)
loan_data_indexed = pipeline_indexers.fit(loan_data_imputed).transform(loan_data_imputed)

# One-hot encode indexed columns
one_hot_encoders = [
    OneHotEncoder(inputCol="Education_index", outputCol="Education_vec"),
    OneHotEncoder(inputCol="EmploymentType_index", outputCol="EmploymentType_vec"),
    OneHotEncoder(inputCol="MaritalStatus_index", outputCol="MaritalStatus_vec"),
    OneHotEncoder(inputCol="HasMortgage_index", outputCol="HasMortgage_vec"),
    OneHotEncoder(inputCol="HasDependents_index", outputCol="HasDependents_vec"),
    OneHotEncoder(inputCol="LoanPurpose_index", outputCol="LoanPurpose_vec"),
    OneHotEncoder(inputCol="HasCoSigner_index", outputCol="HasCoSigner_vec")
]

pipeline_encoders = Pipeline(stages=one_hot_encoders)
loan_data_encoded = pipeline_encoders.fit(loan_data_indexed).transform(loan_data_indexed)

# Normalize numerical features
numerical_cols = [
    "Age", "Income_filled", "LoanAmount", "CreditScore", "MonthsEmployed_filled",
    "NumCreditLines_filled", "InterestRate_filled", "LoanTerm_filled", "DTIRatio_filled"
]

assembler = VectorAssembler(inputCols=numerical_cols, outputCol="features")
loan_data_assembled = assembler.transform(loan_data_encoded)

scaler = StandardScaler(inputCol="features", outputCol="scaled_features", withStd=True, withMean=True)
scaler_model = scaler.fit(loan_data_assembled)
loan_data_scaled = scaler_model.transform(loan_data_assembled)

silver_bucket_path = "s3a://sliver/preprocessed_loan_data"
loan_data_scaled.repartition(1).write.parquet(silver_bucket_path,  mode="overwrite")

spark.stop()

