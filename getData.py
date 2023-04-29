from kafka import KafkaConsumer

bootstrap_server = ['18.211.252.152:9092']
topic = 'de-capstone3'
consumer = KafkaConsumer(topic,bootstrap_servers=bootstrap_server,auto_offset_reset='earliest')

try:
    for msg in consumer:
        print(msg.topic,msg.partition,msg.offset,msg.key,msg.value)
except KeyboardInterrupt:
    exit()