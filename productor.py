import json
import random
import time
from datetime import datetime, timezone
from kafka import KafkaProducer

producer = KafkaProducer(bootstrap_servers='127.0.0.1:9092', value_serializer=lambda v: json.dumps(v).encode('utf-8'))
print('Enviando sensores a sensor_data; Ctrl+C para detener.', flush=True)
try:
    while True:
        data = {'sensor_id': random.choice(['sensor_01', 'sensor_02', 'sensor_03']),
                'temperatura': round(random.uniform(18, 38), 2),
                'humedad': round(random.uniform(30, 85), 2),
                'timestamp': datetime.now(timezone.utc).isoformat()}
        producer.send('sensor_data', data).get(timeout=10)
        print(data, flush=True)
        time.sleep(2)
except KeyboardInterrupt:
    print('Productor detenido.')
finally:
    producer.flush()
    producer.close()
