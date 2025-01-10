import unittest
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, mean
from pyspark.ml.feature import VectorAssembler, MinMaxScaler
from pyspark.sql import Row



class SparkAppTests(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        # Set up a Spark session for testing
        cls.spark = SparkSession.builder \
            .appName("SparkAppTest") \
            .master("local[2]") \
            .getOrCreate()

    @classmethod
    def tearDownClass(cls):
        # Stop the Spark session after tests
        cls.spark.stop()

    def test_missing_data_handling(self):
        # Mock input data with missing values
        data = [
            Row(temperature=25.5, humidity=45.0, sound_volume=85.0),
            Row(temperature=None, humidity=50.0, sound_volume=90.0),
            Row(temperature=30.2, humidity=None, sound_volume=None),
        ]
        df = self.spark.createDataFrame(data)

        # Fill missing values with the column mean
        for column in ["temperature", "humidity", "sound_volume"]:
            mean_value = df.select(mean(col(column))).collect()[0][0]  # Calculate mean
            df = df.fillna({column: mean_value})  # Fill missing values

        # Collect the results for testing
        results = df.collect()
        expected = [
            Row(temperature=25.5, humidity=45.0, sound_volume=85.0),
            Row(temperature=27.85, humidity=50.0, sound_volume=90.0),
            Row(temperature=30.2, humidity=47.5, sound_volume=87.5),
        ]
        self.assertEqual(results, expected)

    def test_feature_scaling(self):
        # Mock input data
        data = [
            Row(temperature=25.5, humidity=45.0, sound_volume=85.0),
            Row(temperature=30.2, humidity=50.0, sound_volume=90.0),
        ]
        df = self.spark.createDataFrame(data)

        # Assemble features
        assembler = VectorAssembler(
            inputCols=["temperature", "humidity", "sound_volume"],
            outputCol="features_raw"
        )
        df = assembler.transform(df)

        # Scale features using Min-Max Scaling
        scaler = MinMaxScaler(inputCol="features_raw", outputCol="features")
        scaler_model = scaler.fit(df)
        df = scaler_model.transform(df)

        # Collect the scaled features for testing
        scaled_features = df.select("features").collect()
        self.assertEqual(len(scaled_features), 2)  # Ensure two rows exist

    def test_anomaly_detection_udf(self):
        # Mock input data
        data = [
            Row(features=[25.5, 45.0, 85.0]),
            Row(features=[30.2, 50.0, 90.0]),
        ]
        df = self.spark.createDataFrame(data)

        # Mock anomaly detection UDF
        def mock_predict_anomaly(row):
            # Mock logic: classify as anomaly if temperature (first feature) > 29
            return 1 if row[0] > 29 else 0

        from pyspark.sql.functions import udf
        from pyspark.sql.types import IntegerType

        predict_udf = udf(mock_predict_anomaly, IntegerType())
        df = df.withColumn("anomaly", predict_udf(col("features")))

        # Collect the predictions for testing
        results = df.select("anomaly").collect()
        expected = [Row(anomaly=0), Row(anomaly=1)]
        self.assertEqual(results, expected)

if __name__ == "__main__":
    unittest.main()
