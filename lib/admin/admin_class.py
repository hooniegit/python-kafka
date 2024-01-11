from confluent_kafka.admin import AdminClient, NewTopic

class KafkaAdmin:
    def __init__(self, conf:dict):
        self.conf = conf
        self.admin_client = AdminClient(self.conf)
        # print("Kafka admin created successfully.") # test
    
    def create_topics(self, name_list:list, num_partitions:int=1, replication_factor:int=1):
        # check topic existance
        topics_metadata = self.admin_client.list_topics().topics
        for name in name_list:
            if name in topics_metadata:
                print(f"Topic '{name}' Already Exists.")
            else:
                # create topic
                new_topic = NewTopic(name, num_partitions, replication_factor)
                self.admin_client.create_topics([new_topic])
                print(f"Topic '{name}' created successfully.")
    
    def delete_topics(self, name_list:list):
        # check topic existance
        topics_metadata = self.admin_client.list_topics().topics
        for name in name_list:
            if name not in topics_metadata:
                print(f"Topic '{name}' Does Not Exists.")
            else:
                # delete topic
                self.admin_client.delete_topics([name])
                print(f"Topic '{name}' deleted successfully.")

if __name__ == "__main__":
    conf = {'bootstrap.servers': 'localhost:9092'}
    admin = KafkaAdmin(conf)
    admin.delete_topics(name_list=["test"])