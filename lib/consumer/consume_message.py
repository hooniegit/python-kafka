
def consume_messages(conf, topic:str):
    import json
    from confluent_kafka import Consumer
    
    # set cunsumer
    consumer = Consumer(conf)
    consumer.subscribe([topic])

    try:
        while True:
            msg = consumer.poll(1.0)
            if msg is None:
                continue

            # read datas
            key = msg.key().decode('utf-8') if msg.key() else None
            try:
                value = json.loads(msg.value().decode('utf-8')) if msg.value() else None
            except json.JSONDecodeError:
                value = None
            value_type = type(value)
            
            # print datas
            print(f"key: {key}, value: {value}, value type: {value_type}")

    except KeyboardInterrupt:
        pass
    
    finally:
        consumer.close()

if __name__ == "__main__":
    conf = {
        'bootstrap.servers': 'localhost:9092',
        'group.id': 'TEST',
        'auto.offset.reset': 'earliest' 
    }
    topic = "serializingProducer"

    consume_messages(conf=conf, topic=topic)

