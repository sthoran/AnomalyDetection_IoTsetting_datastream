# generate fictive sensor data first in batches to check the outcome
import random 
import json
import pandas as pd

def generate_sensor_data(batch_size = 1000, num_outlier= 20):
    batch= []
    for _ in range(batch_size):
        data =  {
            # stqndard betriebstemeraturbereich -20 bis 45 grad celsius
            'temperature': round(random.uniform(-30.0, 60.0),2),
            'humidity': round(random.uniform(23,97),2),
            'sound_volume': round(random.uniform(80,120)),
            #'timestamp' : random.randint(1600000000, 1700000000)
        }
        batch.append(data)
    
    for _ in range(num_outlier):
        index = random.randint(0, batch_size - 1)  # Randomly select a row
        batch[index] = {  # Replace with outlier values
            "temperature": round(random.uniform(-80.0, 150.0), 2),  # Extreme temperature
            "humidity": round(random.uniform(-20.0, 150.0), 2),     # Extreme humidity
            "sound_volume": round(random.uniform(0.0, 300.0), 2),   # Extreme sound
            #"timestamp": random.randint(1600000000, 1700000000)     # Random Unix timestamp
        }
    
    
    return pd.DataFrame(batch)
    
    
batch = generate_sensor_data(1000, 20)

#save batch to json 
batch.to_json('sensor_data.json', orient = 'records', indent = 4)

print('test')
        