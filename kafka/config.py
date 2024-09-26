INPUT_DATA_PATH = '../resources/data/rides.csv'

SCHEMA_REGISTRY_URL = 'http://localhost:8081'
BOOTSTRAP_SERVERS = 'localhost:9092'

# KAFKA PRODUCER
KAFKA_TOPIC = 'rides_avro'
RIDE_KEY_SCHEMA_PATH = '../resources/schemas/taxi_ride_key.avsc'
RIDE_VALUE_SCHEMA_PATH = '../resources/schemas/taxi_ride_value.avsc'

# KAFKA CONSUMER
CONSUME_TOPIC_TRIP_STATS = 'trip_stats'
CONSUME_TOPIC_TRIP_PICK_UP_COUNT_WINDOW = 'trip_pick_up_count_window'
CONSUME_TOPIC_TRIP_DROP_OFF_COUNT_WINDOW = 'trip_drop_off_count_window'
CONSUME_TOPIC_TRIP_STATS_WINDOW = 'trip_stats_window'