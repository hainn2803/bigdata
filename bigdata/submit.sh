docker cp batch/submit.py spark-master:submit.py

docker exec spark-master /spark/bin/spark-submit --master spark://spark-master:7077 submit.py
