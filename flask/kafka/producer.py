from time import sleep
from json import dumps
from kafka import KafkaProducer

if __name__ == '__main__':
    producer = KafkaProducer(
        bootstrap_servers=['localhost:9092'],
        value_serializer=lambda x: dumps(x).encode('utf-8')
    )

    for j in range(9999):
        print("Iteration", j)
        data = {'lat': 45, 'lon':-4}
        producer.send('topic_test', value=data)
        sleep(0.5)