echo "Starting docker ."

sudo docker-compose up -d --build

echo "Waiting for the system t be ready"

sleep 200

sudo docker cp auth_users.js mongo:docker-entrypoint-initdb.d


echo "\nAdding MongoDB Kafka Sink Connector for  'urgent_data' and 'normal_data' Topics into the 'patient.sensorlogs' collection in mongodb"


curl -X POST -H "Content-Type: application/json" --data '
  {"name": "mongo-sinkSensorlogs",
   "config": {
     "connector.class":"com.mongodb.kafka.connect.MongoSinkConnector",
     "tasks.max":"1",
     "topics":"urgent_data, normal_data",
     "connection.uri":"mongodb://root:root@mongo:27017",
     "database":"patient",
     "collection":"sensorlogs",
     "key.converter":"org.apache.kafka.connect.storage.StringConverter",
     "key.converter.schemas.enable":false,
     "value.converter":"org.apache.kafka.connect.storage.StringConverter",
     "value.converter.schemas.enable":false
 }}' http://localhost:8083/connectors -w "\n"  
 
echo "\nmongod connector configured"

sleep 2

echo "\nKafka Connectors:"
curl -X GET "http://localhost:8083/connectors/" -w "\n"
sleep 2
echo '''

==============================================================================================================
The local MongoDB server and Kafka broker is ready to receive data.
==============================================================================================================
You can confirm the kafka producer is working by reading messages from the Kafka Topic "urgent_data" '''

echo "\nInstall kafkacat "
sudo apt-get install kafkacat
kafkacat -b localhost:9092 -t urgent_data -C 

