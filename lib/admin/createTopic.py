
def create_topics(conf, name_list:list, num_partitions:int=1, replication_factor:int=1):
    from confluent_kafka.admin import AdminClient, NewTopic
    
    # create admin
    admin_client = AdminClient(conf)
    
    # check topic existance
    topics_metadata = admin_client.list_topics().topics
    for name in name_list:
        if name in topics_metadata:
            print(f"Topic '{name}' Already Exists.")
        else:
            # create topic
            new_topic = NewTopic(name, num_partitions, replication_factor)
            admin_client.create_topics([new_topic])
            print(f"Topic '{name}' created successfully.")

def delete_topics(conf, name_list:list):
    from confluent_kafka.admin import AdminClient
    
    # create admin
    admin_client = AdminClient(conf)
    
    # check topic existance
    topics_metadata = admin_client.list_topics().topics
    for name in name_list:
        if name not in topics_metadata:
            print(f"Topic '{name}' Does Not Exists.")
        else:
            # delete topic
            admin_client.delete_topics([name])
            print(f"Topic '{name}' deleted successfully.")


if __name__ == "__main__":    
    conf = {'bootstrap.servers': 'localhost:9092'}
    create_name_list = ["serializingProducer"]
    create_topics(conf=conf, name_list=create_name_list)
    
    delete_name_list = []
    delete_topics(conf=conf, name_list=delete_name_list)