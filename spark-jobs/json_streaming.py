from pyspark.sql import SparkSession
import pyspark.sql.functions as F
import pyspark.sql.types as T

from json_config import BOOTSTRAP_SERVERS, CONSUME_TOPIC, RIDE_SCHEMA

def read_from_kafka(consume_topic: str):
    df_stream = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "localhost:9092,broker:29092") \
        .option("subscribe", consume_topic) \
        .option("startingOffsets", "earliest") \
        .load()
    return df_stream

def parse_json_ride_from_kafka(df, schema):
    assert df.isStreaming is True, "DataFrame isnt streaming data"

    df = df.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)") \
        .select(F.from_json(F.col("value"), schema).alias("data")) \
        .select("data.*") \
        .withColumn("tpep_pickup_datetime", F.element_at(F.col("tpep_pickup_datetime"), 1)) \
        .withColumn("tpep_dropoff_datetime", F.element_at(F.col("tpep_dropoff_datetime"), 1))
    return df

def sink_console(df, output_mode: str = 'complete', processing_time: str = '5 seconds'):
    write_query = df.writeStream \
        .outputMode(output_mode) \
        .trigger(processingTime=processing_time) \
        .format("console") \
        .option("truncate", False) \
        .start()
    return write_query

def op_windowed_groupby(df, window_duration, slide_duration):
    df_windowed_aggregation = df.groupBy(
        F.window(timeColumn=df.tpep_pickup_datetime, windowDuration=window_duration, slideDuration=slide_duration),
        df.vendor_id
    ).count()
    return df_windowed_aggregation

if __name__ == "__main__":
    spark = SparkSession.builder.appName('json-ride-stream').getOrCreate()
    spark.sparkContext.setLogLevel('WARN')

    #Read Stream
    df_consume_stream = read_from_kafka(consume_topic=CONSUME_TOPIC)
    print(df_consume_stream.printSchema())

    #Parse KJson Stream
    df_rides = parse_json_ride_from_kafka(df_consume_stream, schema=RIDE_SCHEMA)
    print(df_rides.printSchema())

    sink_console(df_rides, output_mode='append')

    windowed_df = op_windowed_groupby(df_rides, window_duration="2 minutes", slide_duration='1 minutes')
    sink_console(windowed_df)

    spark.streams.awaitAnyTermination()



    
