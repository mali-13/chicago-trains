"""Producer base-class providing common utilites and functionality"""
import logging
import time


from confluent_kafka import avro
from confluent_kafka.admin import AdminClient, NewTopic
from confluent_kafka.avro import CachedSchemaRegistryClient, AvroProducer

logger = logging.getLogger(__name__)

SCHEMA_REGISTRY_URL = "http://localhost:8081"
BROKER_URL = ":9092"


class Producer:
    """Defines and provides common functionality amongst Producers"""

    # Tracks existing topics across all Producer instances
    existing_topics = set([])

    def init_existing_topics(self):
        client = AdminClient(self.broker_properties)
        topic_metadata = client.list_topics(timeout=5)
        self.existing_topics = set(
            t.topic for t in iter(topic_metadata.topics.values())
        )

    def __init__(
        self,
        topic_name,
        key_schema,
        value_schema=None,
        num_partitions=1,
        num_replicas=1,
    ):
        """Initializes a Producer object with basic settings"""
        self.topic_name = topic_name
        self.key_schema = key_schema
        self.value_schema = value_schema
        self.num_partitions = num_partitions
        self.num_replicas = num_replicas

        #
        #
        # Configure the broker properties below. Make sure to reference the project README
        # and use the Host URL for Kafka and Schema Registry!
        #
        #
        self.broker_properties = {"bootstrap.servers": BROKER_URL}

        # Get existing topics
        self.init_existing_topics()

        # If the topic does not already exist, try to create it
        if self.topic_name not in Producer.existing_topics:
            self.create_topic()
            Producer.existing_topics.add(self.topic_name)

        # Configure the AvroProducer

        schema_registry = CachedSchemaRegistryClient({"url": SCHEMA_REGISTRY_URL})

        self.producer = AvroProducer(
            config=self.broker_properties, schema_registry=schema_registry
        )

    def create_topic(self):
        """Creates the producer topic if it does not already exist"""
        #
        #
        # Write code that creates the topic for this producer if it does not already exist on
        # the Kafka Broker.
        #
        #
        client = AdminClient(self.broker_properties)
        futures = client.create_topics(
            [
                NewTopic(
                    topic=self.topic_name,
                    num_partitions=self.num_partitions,
                    replication_factor=self.num_replicas,
                )
            ]
        )
        for _, future in futures.items():
            try:
                future.result()
            except Exception as e:
                pass

    def time_millis(self):
        return int(round(time.time() * 1000))

    def close(self):
        """Prepares the producer for exit by cleaning up the producer"""
        #
        #
        # Write cleanup code for the Producer here
        #
        #
        self.producer.flush()
