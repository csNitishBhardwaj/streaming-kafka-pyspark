import os
from typing import Dict, List

from confluent_kafka import Consumer
from confluent_kafka.serialization import SerializationContext, MessageField
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroDeserializer
from confluent_kafka.admin import AdminClient

from config import BOOTSTRAP_SERVERS, SCHEMA_REGISTRY_URL, CONSUME_TOPIC_TRIP_STATS_WINDOW


class AvroConsumerClass:
    def __init__(self, props: Dict):
        schema_str = self.get_schema_from_schema_registry(props['schema_registry.url'], f"{CONSUME_TOPIC_TRIP_STATS_WINDOW}-value")
        schema_registry_client  = SchemaRegistryClient({'url': props['schema_registry.url']})
        self.avro_value_deserializer = AvroDeserializer(schema_registry_client=schema_registry_client,
                                                        schema_str=schema_str)

        consumer_props = {
            'bootstrap.servers': props['bootstrap.servers'],
            'group.id': props['group.id'],
            'auto.offset.reset': props['auto.offset.reset']
        }

        self.consumer = Consumer(consumer_props)

    @staticmethod
    def get_schema_from_schema_registry(schema_registry_url, schema_registry_subject):
        sr = SchemaRegistryClient({'url': schema_registry_url})
        latest_version = sr.get_latest_version(schema_registry_subject)
        return latest_version.schema.schema_str

    def consumer_from_kafka(self, topics: List[str]):
        self.consumer.subscribe(topics=topics)
        while True:
            try:
                msg = self.consumer.poll(1.0)
                if msg is None:
                    continue
                record = self.avro_value_deserializer(msg.value(), SerializationContext(msg.topic(), MessageField.VALUE))
                if record is not None:
                    print("{}".format(record))
            except KeyboardInterrupt:
                break

        self.consumer.close()


if __name__ == "__main__" :
    config = {
        'bootstrap.servers': BOOTSTRAP_SERVERS,
        'auto.offset.reset': 'earliest',
        'group.id': 'nblabs.taxirides.avro.consumer.v1',
        'schema_registry.url': SCHEMA_REGISTRY_URL,
    }

    avro_consumer = AvroConsumerClass(props=config)
    avro_consumer.consumer_from_kafka(topics=[CONSUME_TOPIC_TRIP_STATS_WINDOW])