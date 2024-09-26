# Kafka Source
BOOTSTRAP_SERVERS = "localhost:9092,broker:29092"
CONSUME_TOPIC = 'rides_avro'

# Kafka Sink
PRODUCE_TOPIC_TRIP_STATS = 'trip_stats'
PRODUCE_TOPIC_TRIP_PICK_UP_COUNT_WINDOW = 'trip_pick_up_count_window'
PRODUCE_TOPIC_TRIP_DROP_OFF_COUNT_WINDOW = 'trip_drop_off_count_window'
PRODUCE_TOPIC_TRIP_STATS_WINDOW = 'trip_stats_window'

# Schema Registry
SCHEMA_REGISTRY_URL = 'http://schema-registry:8081'
