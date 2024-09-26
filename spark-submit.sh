docker exec -it spark-master spark-submit \
		--master spark://spark-master:7077 \
		--packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.2,org.apache.spark:spark-avro_2.12:3.5.2,org.apache.spark:spark-streaming-kafka-0-10_2.12:3.5.2 \
		spark-jobs/spark_streaming.py

		
