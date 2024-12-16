sudo docker cp database.cql cassandra:database.cql
echo "hao"
sudo docker exec cassandra cqlsh -f database.cql --request-timeout=6000
echo "hao2"
sudo docker exec kafka-1 /usr/bin/kafka-topics --create --zookeeper zookeeper:2181 --replication-factor 2 --partitions 3 --topic trafficaccident
# echo "hao3"