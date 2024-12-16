sudo docker exec -it namenode hadoop dfs -rm -r /user
docker exec -it namenode hdfs dfs -mkdir /user
docker exec -it namenode hdfs dfs -mkdir /user/pdt
docker exec -it namenode hdfs dfs -mkdir /user/pdt/raw_rows
docker exec -it namenode hdfs dfs -mkdir /user/pdt/processed

sudo docker exec -it namenode hdfs dfs -ls /
