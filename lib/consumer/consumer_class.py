from confluent_kafka import Consumer
import json

class kafka_consumer:
    def __init__(self, conf:dict, topic:str):
        self.conf = conf
        self.topic = topic
        self.consumer_client = Consumer(conf)
        self.consumer_client.subscribe([topic])
        # print("Kafka consumer created successfully.") # test
    
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
    consumer = kafka_consumer(conf, topic)
    consumer.poll_messages()