from kafka import KafkaProducer
producer = KafkaProducer(bootstrap_servers=['192.168.1.63:9092','192.168.1.63:9093','192.168.1.63:9094'])

for i in range(10):
    #msg = "msg%d" % i
    producer.send('my-mutil-topics', b"1msg%d" % i, partition=0)

producer.close()