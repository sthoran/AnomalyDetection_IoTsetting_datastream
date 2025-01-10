-- Create the keyspace
CREATE KEYSPACE IF NOT EXISTS fictive_sensor_data 
WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1};

-- Create the predictions table
CREATE TABLE IF NOT EXISTS fictive_sensor_data.predictions (
    timestamp DOUBLE PRIMARY KEY,
    temperature DOUBLE,
    humidity DOUBLE,
    sound_volume DOUBLE,
    anomaly INT
);
