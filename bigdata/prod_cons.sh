gnome-terminal -- bash -c "source ~/miniconda3/etc/profile.d/conda.sh && conda activate bigdata && python producer/producer.py; exec bash"
sleep 10


# sudo docker cp consumer/consumer.py spark-master:consumer.py
# sudo docker exec -it spark-master /spark/bin/spark-submit --master spark://spark-master:7077 consumer.py
