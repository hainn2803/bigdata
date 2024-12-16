import pyspark
import hdfs

base_path = "/user/pdt/raw_rows/"

client = hdfs.Client("http://namenode:9870")
files_list = client.list(base_path)
print(files_list)
full_df = None

spark = pyspark.sql.SparkSession.builder.appName("collect-to-new-batch").getOrCreate()
spark.sparkContext.setLogLevel("WARN")

processed_list = []
try:
    processed_list = client.list("/user/pdt/processed")
except:
    pass


for file_name in files_list:
    print('hieu')
    try:
        if file_name in processed_list:
            continue
        if full_df is None:
            full_df = spark.read \
                .option("delimiter","|||") \
                .option("header", "true") \
                .csv("hdfs://namenode:9000" + base_path + file_name)
            full_df.write \
                .option("delimiter","|||") \
                .csv(f"hdfs://namenode:9000/user/pdt/processed/{file_name}", header=True)
        else:
            df = spark.read \
                .option("delimiter","|||") \
                .option("header", "true") \
                .csv("hdfs://namenode:9000" + base_path + file_name)
            full_df = full_df.union(df)
            full_df.write \
                .option("delimiter","|||") \
                .csv(f"hdfs://namenode:9000/user/pdt/processed/{file_name}", header=True)
    except:
        pass

try:
    full_df.write \
        .option("delimiter", "|||") \
        .csv("hdfs://namenode:9000/user/pdt/news/new_batch.csv", header=True)
except:
    pass
# for file_name in files_list:
#     client.delete(base_path + file_name, recursive=True)