echo "Starting docker ."

sudo docker-compose up -d --build

echo "Waiting for the system t be ready"

sleep 1.7m

sudo docker cp auth_users.js mongo:docker-entrypoint-initdb.d


echo "\nAdding MongoDB Kafka Sink Connectors for  'urgent_data' and 'normal_data' Topics into the 'patient.sensorlogs' collection in mongodb"


curl -X POST -H "Content-Type: application/json" --data '
  {"name": "mongosinkUrgent_data",
   "config": {
     "connector.class":"com.mongodb.kafka.connect.MongoSinkConnector",
     "tasks.max":"1",
     "topics":"urgent_data",
     "connection.uri":"mongodb://root:root@mongo:27017",
     "database":"patient",
     "collection":"sensorlogs",
     "key.converter":"org.apache.kafka.connect.storage.StringConverter",
     "key.converter.schemas.enable":false,
     "value.converter":"org.apache.kafka.connect.storage.StringConverter",
     "value.converter.schemas.enable":false
 }}' http://localhost:8083/connectors -w "\n"
 
curl -X POST -H "Content-Type: application/json" --data '
  {"name": "mongosinkNormal_data",
   "config": {
     "connector.class":"com.mongodb.kafka.connect.MongoSinkConnector",
     "tasks.max":"1",
     "topics":"normal_data",
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

echo  "execute the alert_nurse python file"
python3 ../alert_nurse.py ## execute the alert_nurse python file
