from confluent_kafka.admin import AdminClient, NewTopic
from confluent_kafka import Producer

class KafkaService:
    def __init__(self, bootstrap_servers='localhost:9092'):
        self.bootstrap_servers = bootstrap_servers
        # AdminClient is used for administrative tasks like creating/deleting topics
        self.admin_client = AdminClient({'bootstrap.servers': self.bootstrap_servers})
        # Producer is used to publish messages to the Kafka cluster
        self.producer = Producer({'bootstrap.servers': self.bootstrap_servers})

    def create_topic(self, topic_name, num_partitions=1, replication_factor=1):
        """Creates a new Kafka topic if it does not already exist."""
        
        # 1. Check if the topic exists to avoid redundant creation attempts
        if self.is_topic_exists(topic_name):
            print(f"Topic {topic_name} already exists")
            return

        # 2. Define the new topic specifications
        topic = NewTopic(topic_name, num_partitions=num_partitions, replication_factor=replication_factor)
        
        # 3. Request topic creation. This returns a dictionary of futures (async results).
        fs = self.admin_client.create_topics([topic])
        
        # 4. Iterate through results and wait for them to complete
        for topic, f in fs.items():
            try:
                f.result()  # This blocks until the operation is finished or fails
                print(f"Topic {topic} created")
            except Exception as e:
                print(f"Failed to create topic {topic}: {e}")
        
    def delete_topic(self, topic_name):
        """Deletes a Kafka topic."""
        
        # 1. Check existence first
        if not self.is_topic_exists(topic_name):
            print(f"Topic {topic_name} does not exist")
            return
        
        # 2. Request deletion
        fs = self.admin_client.delete_topics([topic_name])
        
        # 3. Wait for the result
        for topic, f in fs.items():
            try:
                f.result()  # Blocks until deletion is confirmed
                print(f"Topic {topic} deleted")
            except Exception as e:
                print(f"Failed to delete topic {topic}: {e}")
    
    def is_topic_exists(self, topic_name):
        """Checks if a topic exists in the cluster metadata."""
        # Fetch metadata for all topics (wait up to 10 seconds)
        metadata = self.admin_client.list_topics(timeout=10)
        # Check if the topic name is in the list of topics
        return topic_name in metadata.topics

    def produce_message(self, topic_name, key, value):
        """Sends a message to the specified topic."""
        # Async send
        self.producer.produce(topic_name, key=key, value=value)
        # Flush ensures the message is actually sent before moving on (synchronous wait)
        # Note: In high-throughput systems, you might not want to flush after every message.
        self.producer.flush()