from kafka import KafkaProducer
from datetime import datetime
import json
import random
import os
import string
import uuid

kafka_topic = os.getenv('KAFKA_TOPIC_NAME')
kafka_ips = os.getenv('KAFKA_BROKER_LIST').split(',')

date = datetime.now().strftime("%Y/%m/%d")

user_id = random.choice([1000, 2000, 3000])
word_id = random.randint(1,5)

producer = KafkaProducer(bootstrap_servers=kafka_ips, value_serializer=lambda m: json.dumps(m).encode('utf-8'))

kafka_msg = {'id': str(uuid.uuid4()), 'word': ''.join([random.choice(string.ascii_letters) for _ in range(10)])}
producer.send(kafka_topic, key=date.encode('utf-8'), value=kafka_msg).get(timeout=1)
