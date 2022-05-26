
 FROM confluentinc/cp-server-connect:7.1.1
 
 ENV  CONNECT_PLUGIN_PATH:"/usr/share/java,/usr/share/confluent-hub-components" \
 && CLASSPATH: /usr/share/java/monitoring-interceptors/monitoring-interceptors-7.1.1.jar
 
 RUN confluent-hub install --no-prompt mongodb/kafka-connect-mongodb:latest
 

