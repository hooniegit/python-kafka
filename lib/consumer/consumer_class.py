from confluent_kafka import Consumer
import json

class kafka_consumer:
    def __init__(self, conf:dict):
        self.conf = conf
        self.consumer_client = Consumer(conf)
        print("Kafka consumer created successfully.") # test

class kafka_consumer_poll(kafka_consumer):
    def __init__(self, consumer_client, topic):
        super().__init__(consumer_client)
        self.topic = topic
        self.consumer_client.subscribe([self.topic])
        print("Kafka consumer demo created successfully.") # test
    
    def poll_messages(self):
        try:
            while True:
                # set poll
                msg = self.consumer_client.poll(1.0)
                if msg is None:
                    continue
                
                # read datas
                key = msg.key().decode('utf-8') if msg.key() else None
                try:
                    value = json.loads(msg.value().decode('utf-8')) if msg.value() else None
                except json.JSONDecodeError:
                    value = msg.value().decode('utf-8')
                value_type = type(value)
                
                # print datas
                print(f"key: {key}, value: {value}, value type: {value_type}")

        except KeyboardInterrupt:
            pass
        
        finally:
            self.consumer_client.close()
        
if __name__ == "__main__":
    conf = {'bootstrap.servers': 'localhost:9092',         
            'group.id': 'TEST',
            'auto.offset.reset': 'earliest' }
    topic = "test"
    consumer = kafka_consumer_poll(conf, topic)
    consumer.poll_messages()