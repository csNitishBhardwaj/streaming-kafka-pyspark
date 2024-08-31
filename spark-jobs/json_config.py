import pyspark.sql.types as T

BOOTSTRAP_SERVERS = 'localhost:9092'
CONSUME_TOPIC = 'rides_json'

RIDE_SCHEMA = T.StructType([
T.StructField("vendor_id", T.StringType()),
T.StructField('tpep_pickup_datetime', T.ArrayType(T.TimestampType())),
T.StructField('tpep_dropoff_datetime', T.ArrayType(T.TimestampType())),
T.StructField("passenger_count", T.IntegerType()),
T.StructField("trip_distance", T.DecimalType()),
T.StructField("rate_code_id", T.IntegerType()),
T.StructField("store_and_fwd_flag", T.StringType()),
T.StructField("pu_location_id", T.IntegerType()),
T.StructField("do_location_id", T.IntegerType()),
T.StructField("payment_type", T.StringType()),
T.StructField("fare_amount", T.DecimalType()),
T.StructField("extra", T.DecimalType()),
T.StructField("mta_tax", T.DecimalType()),
T.StructField("tip_amount", T.DecimalType()),
T.StructField("tolls_amount", T.DecimalType()),
T.StructField("improvement_surcharge", T.DecimalType()),
T.StructField("total_amount", T.DecimalType()),
T.StructField("congestion_surcharge", T.DecimalType()),
])