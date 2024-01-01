
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
    conf = {'bootstrap.servers': 'localhost:9092'}
    topic = "serializingProducer"
    key = "2024-01-02"
    message = {'intro':'hello, kafka!'}
    
    publish_message(conf=conf, topic=topic, key=key, message=str(message)) #1
    publish_message_json(conf=conf, topic=topic, key=key, message=message) #2
    
    