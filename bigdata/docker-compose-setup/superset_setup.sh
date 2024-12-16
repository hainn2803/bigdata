# !/bin/bash


### Superset

docker-compose -f ./docker-compose.yml exec superset superset fab create-admin \
    --username admin \
    --firstname Superset \
    --lastname Admin \
    --email wednesdayhao@gmail.com \
    --password admin

docker-compose -f ./docker-compose.yml exec superset pip install sqlalchemy-trino
docker-compose -f ./docker-compose.yml restart superset
docker-compose -f ./docker-compose.yml exec superset superset db upgrade

docker-compose -f ./docker-compose.yml exec superset superset init


# trino://'':@trino:8085/cassandra


