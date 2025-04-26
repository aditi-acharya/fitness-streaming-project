Systems such as smartwatches, fitness trackers, social media, and online applications produce continuous streams of data every second. 
To analyse this kind of data, we need powerful systems that can handle information as it arrives — this is known as real-time data streaming.

This project is based on the idea of building a real-time fitness tracking system using modern big data technologies. 
It simulates data from fitness devices (like smartwatches or fitness bands), such as heart rate, steps taken, and calories burned. 
The goal is to process this data as it is generated and gain insights in real time.

The project uses Apache Kafka and Apache Spark Streaming, two popular technologies used in real-time data processing

Technologies used:

Apache Kafka 3.7.1

Apache Zookeeper

Apache Spark 3.5.0

Python 3.10+

MySQL (on Windows)

Ubuntu (via WSL)

kafka-python, pyspark, mysql-connector-python, tabulate libraries


To run the project:

Start zookeeper on new terminal
cd ~/kafka_2.12-3.7.1/
bin/zookeeper-server-start.sh config/zookeeper.properties

start kafka on another terminal
cd ~/kafka_2.12-3.7.1/
bin/kafka-server-start.sh config/server.properties

create kafka topics on another terminal 
cd ~/kafka_2.12-3.7.1/
bin/kafka-topics.sh --create --topic fitness_raw --bootstrap-server localhost:9092 --partitions 1 --replication-factor 1
bin/kafka-topics.sh --create --topic user_aggregates --bootstrap-server localhost:9092 --partitions 1 --replication-factor 1
bin/kafka-topics.sh --create --topic alerts --bootstrap-server localhost:9092 --partitions 1 --replication-factor 1

run the spark streaming job
cd ~/fitness-streaming-project/spark
PYTHONPATH=$PYTHONPATH:$(pwd)/.. spark-submit \
  --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0 \
  --master local[*] \
  spark_streaming_processor.py

start kafka producer in new terminal
cd ~/fitness-streaming-project/kafka
python3 fitness_producer.py

view kafka topic data
cd ~/kafka_2.12-3.7.1/
bin/kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic fitness_raw --from-beginning
bin/kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic alerts --from-beginning
bin/kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic user_aggregates --from-beginning

view stored data
mysql -h 172.19.96.1 -P 3306 -u ubuntu -p
-- password: *****
USE fitness_data; //for this db

-- View streaming data:
SELECT * FROM user_aggregates;
SELECT * FROM alerts;

optionally you can run batch_processing to view the same results.
python3 batch_processing.py

This project successfully demonstrates real-time data ingestion, streaming analytics, batch processing, and storage using modern Big Data technologies.
By Aditi P Acharya and Abhigna D, PES University- DBT MINI PROJECT
