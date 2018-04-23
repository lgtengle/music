from kafka import KafkaConsumer

consumer = KafkaConsumer('test',  bootstrap_servers=['192.168.1.63:9092','192.168.1.63:9093','192.168.1.63:9094'])

print(consumer.assignment())
print(consumer.fetch_messages())
# for message in consumer:
#     print ("%s:%d:%d: key=%s value=%s" % (message.topic, message.partition,
#                                           message.offset, message.key,
#                                           message.value))