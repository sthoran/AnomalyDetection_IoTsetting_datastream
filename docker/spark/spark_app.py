from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, mean
from pyspark.ml.feature import VectorAssembler, MinMaxScaler
import joblib
import setuptools

# Create Spark session
spark = SparkSession.builder \
    .appName("KafkaSparkStreamingWithCassandra") \
    .config("spark.cassandra.connection.host", "cassandra") \
    .config("spark.cassandra.connection.port", "9042") \
    .getOrCreate()

# Read Kafka stream
df = spark.readStream.format("kafka") \
    .option("kafka.bootstrap.servers", "kafka:9092") \
    .option("subscribe", "fictive_sensor_data") \
    .load()

# Preprocessing
schema = "temperature DOUBLE, humidity DOUBLE, sound_volume DOUBLE, timestamp DOUBLE"#, label_if_outlier BOOLEAN"

#Extract and parse JSON data
df = df.selectExpr("CAST(value AS STRING)") \
    .select(from_json(col("value"), schema).alias("data")) \
    .select("data.*")

# Fill missing values with the column mean
for column in ["temperature", "humidity", "sound_volume"]:
    mean_value = df.select(mean(col(column))).collect()[0][0]  # Calculate mean
    df = df.fillna({column: mean_value})  # Fill missing values with mean

# Assemble features for the model
assembler = VectorAssembler(inputCols=["temperature", "humidity", "sound_volume"], outputCol="features_raw")
df = assembler.transform(df)

# --- Min-Max Scaling ---
# Scale features to the range [0, 1]
scaler = MinMaxScaler(inputCol="features_raw", outputCol="features")
scaler_model = scaler.fit(df)  # Fit the scaler model on the data
df = scaler_model.transform(df)

# Load Isolation Forest model
model = joblib.load("/opt/spark-app/isolation_forest.pkl")

# Predict anomalies
def predict_anomaly(row):
    features = [row["temperature"], row["humidity"], row["sound_volume"]]
    return int(model.predict([features])[0] == -1)  # 1 = anomaly

from pyspark.sql.functions import udf
from pyspark.sql.types import IntegerType
predict_udf = udf(predict_anomaly, IntegerType())
df = df.withColumn("anomaly", predict_udf(col("features")))

# Write predictions to Cassandra
df.select(
    col("timestamp").alias("timestamp"),
    col("temperature").alias("temperature"),
    col("humidity").alias("humidity"),
    col("sound_volume").alias("sound_volume"),
    col("anomaly").alias("anomaly")  # Prediction from the model
).writeStream \
    .format("org.apache.spark.sql.cassandra") \
    .options(table="predictions", keyspace="sensor_data") \
    .outputMode("append") \
    .start() \
    .awaitTermination()
