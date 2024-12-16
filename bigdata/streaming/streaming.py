import os
import traceback
import pyspark
import requests
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import explode
from pyspark.sql.functions import split
from pyspark.sql.functions import from_json, col, current_timestamp, window, expr, hour
from pyspark.sql.types import StructType, StructField, StringType, TimestampType, IntegerType

import threading
import pandas as pd
from cassandra.cluster import Cluster
from cassandra.query import SimpleStatement
from cassandra.policies import ReconnectionPolicy


os.environ["PYSPARK_PYTHON"] = "python3"
os.environ["SPARK_LOCAL_HOSTNAME"] = "localhost"

class CustomReconnectionPolicy(ReconnectionPolicy):
    def new_schedule(self):
        # Reconnect every 5 seconds
        return [5]

def streaming(time):

    # build Session
    spark = SparkSession.builder.appName("KafkaSparkStreaming").master("local[*]").getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")

    # Lay dong data -> df
    df = spark \
        .readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "kafka-1:9092") \
        .option("subscribe", "TrafficAccident") \
        .option("startingOffsets","earliest") \
        .load()
    
    schema = StructType([
    StructField("ID", StringType()),
    StructField("Severity", StringType()),
    StructField("Start_Time", TimestampType()),
    StructField("Weather_Condition", StringType())
    ])

    # Xu ly df
    df_parsed = df.select(from_json(col("value").cast("string"), schema).alias("data"))
    df_extracted = df_parsed.select(
        "data.*",
        current_timestamp().alias("time_received")
    )

    # Connect voi Cassandra
    cluster = Cluster(
        ["cassandra"],
        connect_timeout=10000000,
        reconnection_policy=CustomReconnectionPolicy(),
    )  # Thay '172.18.0.2' bằng địa chỉ IP của máy chủ Cassandra
    print(cluster.contact_points)
    session = cluster.connect("trafficaccidentshub")
    print('connected')

    ### Thuc hien Count theo severity
    # Group by theo Severity
    print(df_extracted)
    severity_count = df_extracted.groupBy(
        window(col("time_received"), f"{time} seconds"),
        col("Severity")
    ).count()

    # def ham
    def process_row_severity_count(df, epoch_id):
        if not df.rdd.isEmpty():
            print(f"Batch {epoch_id} - Dữ liệu:")

            # Drop bảng nếu tồn tại
            drop_table_query = "DROP TABLE IF EXISTS severity_count;"
            session.execute(drop_table_query)

            # Tạo bảng nếu chưa tồn tại
            create_table_query = """
                CREATE TABLE IF NOT EXISTS severity_count (
                    severity TEXT PRIMARY KEY,
                    count INT,
                    window TIMESTAMP
                );
            """
            session.execute(create_table_query)

            # Insert vao bang
            for row in df.toLocalIterator():
                print(row)
                # row = row.fillna("")
                insert_query = """
                    INSERT INTO severity_count (
                        severity, count, window
                    )
                    VALUES (%s, %s, %s);
                """
                session.execute(
                SimpleStatement(insert_query),
                (
                    row["Severity"],
                    int(row["count"]),
                    row['window']['start']
                ),
                )

        else:
            print(f"Batch {epoch_id} is empty")

    # Chay ham process_row
    query_severity_counts = severity_count.writeStream \
        .outputMode("update") \
        .foreachBatch(process_row_severity_count) \
        .trigger(processingTime=f"{time} seconds") \
        .start()
    # print('hao')

    # query_counts.awaitTermination()
    # print('hihihi')

    ### Thuc hien Count % theo Hour
    # Tao them cot Day of week, Group by theo Dayofweek, Tinh ra phan tram
    print(df_extracted)
    df_with_hour = df_extracted.withColumn(
        "Hour", hour(col("Start_Time"))
    )
    
    hour_count = df_with_hour.groupBy(
        window(col("time_received"), f"{time} seconds"),
        col("Hour")
    ).count()

    # def ham
    def process_row_hour_count(df, epoch_id):
        if not df.rdd.isEmpty():
            print(f"Batch {epoch_id} - Dữ liệu:")

            # Drop bảng nếu tồn tại
            drop_table_query = "DROP TABLE IF EXISTS hour_count;"
            session.execute(drop_table_query)

            # Tạo bảng nếu chưa tồn tại
            create_table_query = """
                CREATE TABLE IF NOT EXISTS hour_count (
                    hour INT PRIMARY KEY,
                    count INT,
                    window TIMESTAMP
                );
            """
            session.execute(create_table_query)

            # Insert vao bang
            for row in df.toLocalIterator():
                print(row)
                # row = row.fillna("")
                insert_query = """
                    INSERT INTO hour_count (
                        hour, count, window
                    )
                    VALUES (%s, %s, %s);
                """
                session.execute(
                SimpleStatement(insert_query),
                (
                    row["Hour"],
                    int(row["count"]),
                    row['window']['start']
                ),
                )

        else:
            print(f"Batch {epoch_id} is empty")
    
    # Chay ham process_row
    query_hour_count = hour_count.writeStream \
        .outputMode("update") \
        .foreachBatch(process_row_hour_count) \
        .trigger(processingTime=f"{time} seconds") \
        .start()
    # print('hao--hao')

    ### Count Samples by Time Window
    sample_count = df_extracted.groupBy(
        window(col("time_received"), f"{time} seconds")
    ).count()

    # Function to Process Each Batch
    def process_row_sample_count(df, epoch_id):
        if not df.rdd.isEmpty():
            print(f"Batch {epoch_id} - Sample Count Data:")

            # Drop bảng nếu tồn tại
            drop_table_query = "DROP TABLE IF EXISTS sample_count_over_time;"
            session.execute(drop_table_query)

            # Create Table if Not Exists
            create_table_query = """
                CREATE TABLE IF NOT EXISTS sample_count_over_time (
                    time_bucket TEXT PRIMARY KEY,
                    count INT
                );
            """
            session.execute(create_table_query)

            # Insert Data into Table
            for row in df.toLocalIterator():
                print(row)
                insert_query = """
                    INSERT INTO sample_count_over_time (
                        time_bucket, count
                    )
                    VALUES (%s, %s);
                """
                session.execute(
                    SimpleStatement(insert_query),
                    (
                        row['window']['start'].strftime("%Y-%m-%d %H:%M:%S"),
                        int(row['count'])
                    ),
                )
        else:
            print(f"Batch {epoch_id} is empty")

    # Write Stream to Cassandra
    query_sample_count = sample_count.writeStream \
        .outputMode("update") \
        .foreachBatch(process_row_sample_count) \
        .trigger(processingTime=f"{time} seconds") \
        .start()

    ### Thuc hien Count % theo Hour
    # Tao them cot Day of week, Group by theo Dayofweek, Tinh ra phan tram
    print(df_extracted)
    weather_count = df_extracted.groupBy(
        window(col("time_received"), f"{time} seconds"),
        col("Weather_Condition")
    ).count()

    # def ham
    def process_row_weather_count(df, epoch_id):
        if not df.rdd.isEmpty():
            print(f"Batch {epoch_id} - Dữ liệu:")

            # Drop bảng nếu tồn tại
            drop_table_query = "DROP TABLE IF EXISTS weather_sample_count;"
            session.execute(drop_table_query)

            # Tạo bảng nếu chưa tồn tại
            create_table_query = """
                CREATE TABLE IF NOT EXISTS weather_sample_count (
                    weather_condition TEXT PRIMARY KEY,
                    cumulative_count INT,
                    window TIMESTAMP
                );
            """
            session.execute(create_table_query)

            # Insert vao bang
            for row in df.toLocalIterator():
                print(row)
                # row = row.fillna("")
                insert_query = """
                    INSERT INTO weather_sample_count (
                        weather_condition, cumulative_count, window
                    )
                    VALUES (%s, %s, %s);
                """
                session.execute(
                SimpleStatement(insert_query),
                (
                    row["Weather_Condition"],
                    int(row["count"]),
                    row['window']['start']
                ),
                )

        else:
            print(f"Batch {epoch_id} is empty")
    
    # Chay ham process_row
    query_weather_count = weather_count.writeStream \
        .outputMode("update") \
        .foreachBatch(process_row_weather_count) \
        .trigger(processingTime=f"{time} seconds") \
        .start()

    # weather_count = df_extracted.groupBy(
    #     window(col("time_received"), f"{time} seconds"),
    #     col("Weather_Condition")
    # ).count()

    # # Function to Process Each Batch
    # def process_weather_count(df, epoch_id):
    #     if not df.rdd.isEmpty():
    #         print(f"Batch {epoch_id} - Weather Count Data:")

    #         # Create Table if Not Exists
    #         create_table_query = """
    #             CREATE TABLE IF NOT EXISTS weather_sample_count (
    #                 weather_condition TEXT,
    #                 time_bucket TEXT,
    #                 cumulative_count INT,
    #                 PRIMARY KEY (weather_condition, time_bucket)
    #             );
    #         """
    #         session.execute(create_table_query)

    #         # Retrieve Current Counts from Cassandra
    #         current_counts = {}
    #         rows = session.execute("SELECT weather_condition, time_bucket, cumulative_count FROM weather_sample_count")
    #         for row in rows:
    #             current_counts[(row.weather_condition, row.time_bucket)] = row.cumulative_count

    #         # Insert or Update Counts
    #         for row in df.toLocalIterator():
    #             weather_condition = row['Weather_Condition']
    #             time_bucket = row['window']['start'].strftime("%Y-%m-%d %H:%M:%S")
    #             current_count = current_counts.get((weather_condition, time_bucket), 0)
    #             new_count = current_count + int(row['count'])

    #             print(f"Inserting: {weather_condition}, {time_bucket}, {new_count}")

    #             insert_query = """
    #                 INSERT INTO weather_sample_count (
    #                     weather_condition, time_bucket, cumulative_count
    #                 )
    #                 VALUES (%s, %s, %s);
    #             """
    #             session.execute(
    #                 SimpleStatement(insert_query),
    #                 (weather_condition, time_bucket, new_count)
    #             )

    #     else:
    #         print(f"Batch {epoch_id} is empty")

    # # Write Stream to Cassandra
    # query_weather_count = weather_count.writeStream \
    #     .outputMode("update") \
    #     .foreachBatch(process_weather_count) \
    #     .trigger(processingTime=f"{time} seconds") \
    #     .start()

    query_weather_count.awaitTermination()
    # print('hihihi')


    
    return 

if __name__ == '__main__':
    try:
        # print('jett')
        streaming(50)
        # print('hieu2222')

    except BrokenPipeError:
        exit("Pipe Broken, Exiting...")
    except KeyboardInterrupt:
        exit("Keyboard Interrupt, Exiting..")
    except Exception as e:
        traceback.print_exc()
        exit("Error in Spark App")