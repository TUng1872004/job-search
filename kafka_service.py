from confluent_kafka.admin import AdminClient, NewTopic
from confluent_kafka import Producer

class KafkaService:
    def __init__(self, bootstrap_servers='localhost:9092'):
        self.bootstrap_servers = bootstrap_servers
        self.admin_client = AdminClient({'bootstrap.servers': self.bootstrap_servers})
        self.producer = Producer({'bootstrap.servers': self.bootstrap_servers})

    def create_topic(self, topic_name, num_partitions=1, replication_factor=1):
        if self.is_topic_exists(topic_name):
            print(f"Topic {topic_name} already exists")
            return
        topic = NewTopic(topic_name, num_partitions=num_partitions, replication_factor=replication_factor)
        fs = self.admin_client.create_topics([topic])
        for topic, f in fs.items():
            try:
                f.result()  # The result itself is None
                print(f"Topic {topic} created")
            except Exception as e:
                print(f"Failed to create topic {topic}: {e}")
        
    def delete_topic(self, topic_name):
        if not self.is_topic_exists(topic_name):
            print(f"Topic {topic_name} does not exist")
            return
        fs = self.admin_client.delete_topics([topic_name])
        for topic, f in fs.items():
            try:
                f.result()  # The result itself is None
                print(f"Topic {topic} deleted")
            except Exception as e:
                print(f"Failed to delete topic {topic}: {e}")
    
    def is_topic_exists(self, topic_name):
        metadata = self.admin_client.list_topics(timeout=10)
        return topic_name in metadata.topics

    def produce_message(self, topic_name, key, value):
        self.producer.produce(topic_name, key=key, value=value)
        self.producer.flush()