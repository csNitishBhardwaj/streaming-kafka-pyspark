import pyspark.sql.functions as F
import pyspark.sql.types as T
from pyspark.sql import SparkSession
from pyspark.sql.avro.functions import from_avro, to_avro

from config import BOOTSTRAP_SERVERS,CONSUME_TOPIC, SCHEMA_REGISTRY_URL, \
    PRODUCE_TOPIC_TRIP_STATS, PRODUCE_TOPIC_TRIP_PICK_UP_COUNT_WINDOW, PRODUCE_TOPIC_TRIP_DROP_OFF_COUNT_WINDOW, PRODUCE_TOPIC_TRIP_STATS_WINDOW

from utils import get_avro_schema, get_schema_from_schema_registry, register_schema


def read_from_kafka(consume_topic):
    df = spark \
        .readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", BOOTSTRAP_SERVERS) \
        .option("subscribe", consume_topic) \
        .option("startingOffsets", "earliest") \
        .load()
    return df

def sink_kafka(df, sink_topic):
    write_query = df \
        .writeStream \
        .format("kafka") \
        .trigger(processingTime='1 second') \
        .outputMode("complete") \
        .option("kafka.bootstrap.servers", BOOTSTRAP_SERVERS) \
        .option("topic", sink_topic) \
        .option("checkpointLocation", "tmp/checkpoint") \
        .start() \

    return write_query

def sink_console(df, output_mode: str ="complete", processing_time: str ="1 seconds"):
    write_query = df \
        .writeStream \
        .trigger(processingTime=processing_time) \
        .outputMode(output_mode) \
        .format("console") \
        .option("truncate", False) \
        .start()
    return write_query

def deser_confluent_avro(df, topic_name: str):
    schema_registry_subject = f'{topic_name}-value'

    binary_to_string_udf = F.udf(lambda x: str(int.from_bytes(x, byteorder='big')), T.StringType())

    # remove first 5 bytes from value
    df = df.withColumn("fixedValue", F.expr("substring(value, 6, length(value)-5)"))

    # get schema if from value
    df = df.withColumn("valueSchemaId", binary_to_string_udf(F.expr("substring(value, 2, 4)")))

    # deserialize data
    _, latest_version_rides = get_schema_from_schema_registry(SCHEMA_REGISTRY_URL, schema_registry_subject)
    fromAvroOptions = {"mode":"PERMISSIVE"}
    df = df \
        .select(
            from_avro(
                F.col("fixedValue"),
                latest_version_rides.schema.schema_str,
                fromAvroOptions)
                .alias("data")
            ) \
        .select("data.*")
    return df

def ser_confluent_avro(df, topic_name: str):

    schema_registry_subject = f'{topic_name}-value'
    new_schema = get_avro_schema(df, "com.nblabs.taxi", "record", topic_name)
    schema_id = register_schema(SCHEMA_REGISTRY_URL, schema_registry_subject, new_schema)

    int_to_binary_udf = F.udf(lambda value, byte_size: (value).to_bytes(byte_size, byteorder='big'), T.BinaryType())

    # Fetch latest schema from Schema Reg
    _, latest_version_rides_agg = get_schema_from_schema_registry(SCHEMA_REGISTRY_URL, schema_registry_subject)

    # Convert df to binary data
    df = df.select(to_avro(F.struct(F.col("*")),
            latest_version_rides_agg.schema.schema_str)
            .alias("value"))
    
    # Add Conflunet Avro Magic Byte(1 byte) and Schema ID (4 byte)
    magicByteBinary = int_to_binary_udf(F.lit(0), F.lit(1))
    schemaIdBinary = int_to_binary_udf(F.lit(latest_version_rides_agg.schema_id), F.lit(4))
    df = df.withColumn("value", F.concat(magicByteBinary, schemaIdBinary, F.col("value")))
    return df


# Analytics Operations

def op_trip_stats(df):
    df = df \
        .withWatermark("tpep_dropoff_datetime", "60 seconds") \
        .withColumn( "trip_duration_in_sec" ,( F.col("tpep_dropoff_datetime") - F.col("tpep_pickup_datetime") ).cast("long") ) \
        .groupBy(
            df.pu_location_id,
            df.do_location_id
        ).agg(
            F.avg("trip_duration_in_sec").cast("float").alias("avg_trip_duration_in_sec"),
            F.avg("total_amount").cast("float").alias("avg_total_amount"),
            F.count("*").alias("trip_count")
        )
    return df

def op_window_groupby_pickup_count(df, window_duration: str, slide_duration: str | None = None):
    df = df \
        .withWatermark("tpep_dropoff_datetime", "60 seconds") \
        .groupBy(
            F.window(timeColumn=df.tpep_pickup_datetime, windowDuration=window_duration),
            df.pu_location_id
        ).agg(F.count("*").alias("trip_pickup_count"))
    return df

def op_window_groupby_dropoff_count(df, window_duration: str, slide_duration: str | None = None):
    df = df \
        .withWatermark("tpep_dropoff_datetime", "60 seconds") \
        .groupBy(
            F.window(timeColumn=df.tpep_dropoff_datetime, windowDuration=window_duration),
            df.do_location_id
    ).agg(F.count("*").alias("trip_dropoff_count"))
    return df

def op_window_groupby_pickup_dropoff(df, window_duration: str, slide_duration: str | None = None):
    df = df \
        .withWatermark("tpep_dropoff_datetime", "60 seconds") \
        .withColumn( "trip_duration_in_sec" ,( F.col("tpep_dropoff_datetime") - F.col("tpep_pickup_datetime") ).cast("long") ) \
        .groupBy(
            F.window(timeColumn=df.tpep_dropoff_datetime, windowDuration=window_duration),
            df.pu_location_id,
            df.do_location_id
    ).agg(
        F.avg("trip_duration_in_sec").cast("float").alias("avg_window_trip_duration_in_sec"),
        F.round(F.avg("total_amount"), 2).cast("float").alias("avg_window_total_amount"),
        F.count("*").alias("window_trip_count")
    )
    return df

if __name__ == "__main__":
    spark = SparkSession.builder.appName("avroRidesStream").getOrCreate()
    spark.conf.set("spark.sql.session.timeZone", "Asia/Kolkata")
    spark.sparkContext.setLogLevel("ERROR")

    #Read from kafka topic
    df_consume_stream = read_from_kafka(consume_topic = CONSUME_TOPIC)

    # DeSer CAvro to SparkDF
    df_rides = deser_confluent_avro(df_consume_stream, CONSUME_TOPIC)

    # Console Output of rides
    sink_console(df_rides, output_mode="append")

    # TRIP (DISTANCE, TOTAL_AMOUNT, COUNT) STATS
    df_trip_stats = op_trip_stats(df_rides)
    sink_console(df_trip_stats)

    # Pick up location count for each 30 minutes
    df_window_pickup_count = op_window_groupby_pickup_count(df_rides, "30 minutes", "15 minutes")
    sink_console(df_window_pickup_count)

    # # Drop off location count for each 30 minutes
    df_window_drop_off_count = op_window_groupby_dropoff_count(df_rides, "30 minutes", "15 minutes")
    sink_console(df_window_drop_off_count)
 
    # TRIP STATS(AVG_FAIR, AVG_DURATION, COUNT) FOR ROUTES SPLIT BY TIME
    df_window_trip_stats = op_window_groupby_pickup_dropoff(df_rides, "30 minutes")
    # sink_console(df_window_trip_stats)
    ser_df = ser_confluent_avro(df_window_trip_stats, topic_name = PRODUCE_TOPIC_TRIP_STATS_WINDOW)
    sink_kafka(ser_df, sink_topic = PRODUCE_TOPIC_TRIP_STATS_WINDOW)

    spark.streams.awaitAnyTermination()