from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import *
from pyspark.ml.feature import Imputer, StringIndexer, OneHotEncoder, VectorAssembler, StandardScaler
from pyspark.ml import Pipeline, PipelineModel
from pyspark.ml.classification import LogisticRegressionModel

# Initialize Spark Session with streaming configurations
spark = SparkSession.builder \
    .appName("RealTimeLoanPrediction") \
    .config("spark.streaming.stopGracefullyOnShutdown", "true") \
    .config("spark.sql.streaming.schemaInference", "true") \
    .getOrCreate()

# Define schema matching your Kafka messages
schema = StructType([
    StructField("LoanID", StringType(), True),
    StructField("Age", IntegerType(), True),
    StructField("Income", IntegerType(), True),
    StructField("LoanAmount", IntegerType(), True),
    StructField("CreditScore", IntegerType(), True),
    StructField("MonthsEmployed", IntegerType(), True),
    StructField("NumCreditLines", IntegerType(), True),
    StructField("InterestRate", DoubleType(), True),
    StructField("LoanTerm", IntegerType(), True),
    StructField("DTIRatio", DoubleType(), True),
    StructField("Education", StringType(), True),
    StructField("EmploymentType", StringType(), True),
    StructField("MaritalStatus", StringType(), True),
    StructField("HasMortgage", StringType(), True),
    StructField("HasDependents", StringType(), True),
    StructField("LoanPurpose", StringType(), True),
    StructField("HasCoSigner", StringType(), True)
])

# Load the pre-trained model
model_path = "/home/drissdo/Desktop/Scalable-Distributed-Systems/ML/model"
lr_model = LogisticRegressionModel.load(model_path)

def preprocess_streaming_data(df):
    """Preprocess streaming data using the same pipeline as training"""
    # Handle missing values
    imputer = Imputer(
        inputCols=["Income", "MonthsEmployed", "NumCreditLines", "InterestRate", "LoanTerm", "DTIRatio"],
        outputCols=["Income_filled", "MonthsEmployed_filled", "NumCreditLines_filled", 
                   "InterestRate_filled", "LoanTerm_filled", "DTIRatio_filled"]
    )
    
    # Convert categorical variables to numeric
    categorical_columns = ["Education", "EmploymentType", "MaritalStatus", 
                         "HasMortgage", "HasDependents", "LoanPurpose", "HasCoSigner"]
    indexers = [StringIndexer(inputCol=col, outputCol=f"{col}_index", handleInvalid="keep") 
               for col in categorical_columns]
    
    # One-hot encode indexed categorical variables
    encoders = [OneHotEncoder(inputCol=f"{col}_index", outputCol=f"{col}_vec", dropLast=True) 
               for col in categorical_columns]
    
    # Combine all numeric features
    numeric_cols = ["Age", "Income_filled", "LoanAmount", "CreditScore", 
                   "MonthsEmployed_filled", "NumCreditLines_filled", 
                   "InterestRate_filled", "LoanTerm_filled", "DTIRatio_filled"]
    
    assembler = VectorAssembler(inputCols=numeric_cols, outputCol="numeric_features")
    
    # Scale numeric features
    scaler = StandardScaler(inputCol="numeric_features", outputCol="scaled_features",
                           withStd=True, withMean=True)
    
    # Create and apply the preprocessing pipeline
    pipeline = Pipeline(stages=[imputer] + indexers + encoders + [assembler, scaler])
    preprocessor = pipeline.fit(df)
    processed_df = preprocessor.transform(df)
    
    # Select relevant columns for prediction
    return processed_df.select("LoanID", "scaled_features")

# Read from Kafka stream
streaming_df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "loan_applications") \
    .option("startingOffsets", "latest") \
    .load()

# Parse JSON from Kafka
parsed_df = streaming_df \
    .select(from_json(col("value").cast("string"), schema).alias("data")) \
    .select("data.*")

# Process the streaming data and make predictions
def process_batch(df, epoch_id):
    try:
        # Preprocess the batch
        processed_df = preprocess_streaming_data(df)
        
        # Make predictions
        predictions = lr_model.transform(processed_df)
        
        # Format results
        results = predictions.select(
            "LoanID",
            when(col("prediction") == 1.0, "High Risk")
            .otherwise("Low Risk").alias("risk_assessment"),
            col("probability").getItem(1).alias("default_probability")
        )
        
        # Write results
        results.write \
            .format("console") \
            .mode("append") \
            .save()
            
        # Optional: Write to another Kafka topic
        results.selectExpr("LoanID as key", "to_json(struct(*)) as value") \
            .write \
            .format("kafka") \
            .option("kafka.bootstrap.servers", "localhost:9092") \
            .option("topic", "loan_predictions") \
            .save()
            
    except Exception as e:
        print(f"Error processing batch: {str(e)}")

# Start the streaming query
query = parsed_df.writeStream \
    .foreachBatch(process_batch) \
    .outputMode("update") \
    .trigger(processingTime="5 seconds") \
    .start()

# Wait for the streaming to finish
try:
    query.awaitTermination()
except KeyboardInterrupt:
    print("Stopping the streaming query...")
    query.stop()
    spark.stop()