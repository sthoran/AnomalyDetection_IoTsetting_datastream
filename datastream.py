import random
import json
import pandas as pd
import time

def generate_sensor_data(batch_size=1000, num_outlier=20, start_timestamp=int(time.time())):
    batch = []
    used_timestamps = set()  # Keep track of used timestamps

    for i in range(batch_size):
        # Ensure unique timestamps by incrementing from the starting timestamp
        while start_timestamp in used_timestamps:
            start_timestamp += 1  # Increment until unique
        used_timestamps.add(start_timestamp)

        data = {
            'temperature': round(random.uniform(-30.0, 60.0), 2),
            'humidity': round(random.uniform(23, 97), 2),
            'sound_volume': round(random.uniform(80, 120)),
            'label_if_outlier': False,
            'timestamp': start_timestamp
        }
        batch.append(data)

    # Introduce outliers
    for _ in range(num_outlier):
        index = random.randint(0, batch_size - 1)  # Randomly select a row
        while batch[index]['label_if_outlier']:  # Ensure no duplicate outliers
            index = random.randint(0, batch_size - 1)
        batch[index] = {
            "temperature": round(random.uniform(-80.0, 150.0), 2),  # Extreme temperature
            "humidity": round(random.uniform(-20.0, 150.0), 2),     # Extreme humidity
            "sound_volume": round(random.uniform(0.0, 300.0), 2),   # Extreme sound
            'label_if_outlier': True,
            'timestamp': batch[index]['timestamp']  # Keep the same unique timestamp
        }

    return pd.DataFrame(batch)


# Generate batch of data with unique timestamps
batch = generate_sensor_data(batch_size=1000, num_outlier=20)

# Save batch to JSON
batch.to_json('sensor_data.json', orient='records', indent=4)

print('Test complete: Data generated and saved to JSON.')
