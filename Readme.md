# Kayak

Kayak is a tool intended to read message from kafka and send them to elasticsearch to help message debugging.

docker-compose exec kafka1 /usr/bin/kafka-console-consumer --zookeeper zookeeper:2181 --topic bots_events --from-beginning
