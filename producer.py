from kafka import KafkaProducer, KafkaConsumer

# Test Producer
producer = KafkaProducer(bootstrap_servers=['localhost:9092'])
producer.send('test-topic', b'Hello Kafka!')
producer.flush()
print("✅ Message envoyé!")

# Test Consumer
consumer = KafkaConsumer('test-topic', bootstrap_servers=['localhost:9092'])
for message in consumer:
    print(f"✅ Message reçu: {message.value}")
    break