
def publish_message(conf, topic:str, key:str, message):
    from confluent_kafka import Producer
 
    try:
        producer = Producer(conf)
        producer.produce(topic=topic, key=key, value=message)
        producer.flush()
    except Exception as E:
        print(E)

def publish_message_json(conf, topic:str, key:str, message:dict):
    from confluent_kafka import Producer
    import json
    
    try:
        producer = Producer(conf)
        producer.produce(topic=topic, key=key, value=json.dumps(message).encode('utf-8'))
        producer.flush()
    except Exception as E:
        print(E)

if __name__ == "__main__":
    from datetime import datetime
    from time import sleep
    
    conf = {'bootstrap.servers': 'localhost:9092'}
    topic = "test"
    key = "2024-01-02"
    message = {'message':'hello, kafka!', 'date': datetime.now().strftime("%Y-%m-%d %H:%M:%S")} 
    
    publish_message(conf=conf, topic=topic, key=key, message=str(message)) #1
    sleep(3)
    publish_message_json(conf=conf, topic=topic, key=key, message=message) # 2
    
    