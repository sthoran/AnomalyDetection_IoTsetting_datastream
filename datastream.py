# generate fictive sensor data first in batches to check the outcome
import random 
import json

def generate_sensor_data(batch_size = 10):
    batch= []
    for _ in range(batch_size):
        data =  {
            # stqndard betriebstemeraturbereich -20 bis 45 grad celsius
            'temperature': round(random.uniform(-30.0, 60.0),2),
            'humidity': round(random.uniform(23,97),2),
            'sound_volume': round(random.uniform(80,120)),
            'timestamp' : random.randint(1600000000, 1700000000)
        }
        batch.append(data)
    return batch
    
    
batch = generate_sensor_data(batch_size=5)
#save batch to json 
with open("sensor_data.json", "w") as json_file:
    json.dump(batch,json_file, indent=4)

for data in batch:
    print(f"Testing Data: {json.dumps(data, indent=2)}")
print('test')
        