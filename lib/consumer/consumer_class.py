from confluent_kafka import Consumer
import json

class KafkaConsumer:
    def __init__(self, conf:dict):
        self.conf = conf
        self.consumer_client = Consumer(conf)
        # print("Kafka consumer created successfully.") # test

class KafkaConsumerPoll(KafkaConsumer):
    def __init__(self, consumer_client, topics:list):
        super().__init__(consumer_client)
        self.topics = topics
        self.consumer_client.subscribe(self.topics)
        # print("Kafka consumer demo created successfully.") # test
    
    def poll_messages(self, period:float=1.0):
        try:
            while True:
                # set poll
                msg = self.consumer_client.poll(period)
                if msg is None:
                    continue
                
                # read datas (topic, partition, key, message)
                topic = msg.topic()
                partition = msg.partition()
                key = msg.key().decode('utf-8') if msg.key() else None
                try:
                    value = json.loads(msg.value().decode('utf-8')) if msg.value() else None
                except json.JSONDecodeError:
                    value = msg.value().decode('utf-8')
                value_type = type(value)
                
                # print datas
                print(f"topic: {topic}, partition: {partition}, key: {key}, value: {value}, value type: {value_type}")

        except KeyboardInterrupt:
            pass
        
        finally:
            self.consumer_client.close()
        
if __name__ == "__main__":
    conf = {'bootstrap.servers': 'localhost:9092',         
            'group.id': 'TEST',
            'auto.offset.reset': 'earliest' }
    topics = ["test"]
    consumer = KafkaConsumerPoll(conf, topics)
    consumer.poll_messages()
    