sudo docker cp streaming/streaming.py spark-master:/streaming.py
sudo docker exec -it spark-master /spark/bin/spark-submit --master spark://spark-master:7077 --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.1.1 streaming.py
