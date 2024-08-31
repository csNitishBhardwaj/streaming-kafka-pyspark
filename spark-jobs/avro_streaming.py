import pyspark.sql.functions as F
import pyspark.sql.types as T
from pyspark.sql import SparkSession
from pyspark.sql.avro.functions import from_avro, to_avro

from confluent_kafka.schema_registry import SchemaRegistryClient

from avro_config import CONSUME_TOPIC, SCHEMA_REGISTRY_URL, SCHEMA_REGISTRY_RIDES_SUBJECT


def read_from_kafka(consume_topic):
    df = spark \
        .readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "localhost:9092,broker:29092") \
        .option("subscribe", consume_topic) \
        .option("startingOffsets", "earliest") \
        .load()
    return df

def get_schema_from_schema_registry(schema_registry_url, schema_registry_subject):
    sr = SchemaRegistryClient({'url': schema_registry_url})
    latest_version = sr.get_latest_version(schema_registry_subject)
    return sr, latest_version

def ser_confluent_avro(df):
    #UDF
    int_to_binary_udf = F.udf(lambda value, byte_size: (value).to_bytes(byte_size, byteorder='big'), T.BinaryType())

def deser_confluent_avro(df):
    assert df.isStreaming is True, "DF isnt streaming data"

    #UDF
    binary_to_string_udf = F.udf(lambda x: str(int.from_bytes(x, byteorder='big')), T.StringType())
    
    df = df.withColumn("fixedValue", F.expr("substring(value, 6, length(value)-5)"))
    df = df.withColumn("valueSchemaId", binary_to_string_udf(F.expr("substring(value, 2, 4)")))

    _, latest_version_rides = get_schema_from_schema_registry(SCHEMA_REGISTRY_URL, SCHEMA_REGISTRY_RIDES_SUBJECT)

    fromAvroOptions = {"mode":"PERMISSIVE"}
    
    df = df \
        .select(from_avro(F.col("fixedValue"), latest_version_rides.schema.schema_str, fromAvroOptions).alias("data")) \
        .select("data.*") 
    return df

def sink_console(df, output_mode: str ="complete", processing_time: str ="5 seconds"):
    write_query = df \
        .writeStream \
        .outputMode(output_mode) \
        .trigger(processingTime=processing_time) \
        .format("console") \
        .option("truncate", False) \
        .start()
    return write_query
        

if __name__ == "__main__":
    spark = SparkSession.builder.appName("avro-rides-stream").getOrCreate()
    spark.sparkContext.setLogLevel('WARN')

    #Read from kafka topic
    df_consume_stream = read_from_kafka(consume_topic = CONSUME_TOPIC)
    print(df_consume_stream.printSchema())

    #DeSer CAvro to SparkDF
    df_rides = deser_confluent_avro(df_consume_stream)
    print(df_rides.printSchema())

    #Console Output of rides
    sink_console(df_rides, output_mode="append")

    spark.streams.awaitAnyTermination()

