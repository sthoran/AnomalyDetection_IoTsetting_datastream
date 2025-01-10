import unittest
from unittest.mock import MagicMock, patch
import json

class TestKafkaProducer(unittest.TestCase):
    # Mock Producer
    @patch('confluent_kafka.Producer')
    def test_kafka_producer(self, mock_producer_class):
        # Create a mock instance of Producer
        mock_producer = MagicMock()
        mock_producer_class.return_value = mock_producer

        # Import the script you want to test
        from kafka_producer import Producer

        # Initialize the producer
        producer = Producer({'bootstrap.servers': 'mock_broker:9092'})

        # Simulate sending data
        data = {"temperature": 25.3, "humidity": 60, "sound_volume": 45.7, "label_if_outlier": False}
        producer.send("test_topic", value=data)

        # Check that the mock's send method was called
        mock_producer.send.assert_called_once_with("test_topic", value=data)
        print("Test passed.")
