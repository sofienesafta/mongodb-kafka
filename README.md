# Mongodb kafka demo


## Requirements:
  
   * Ubuntu 18.4
   * Docker 18.9+
   * Docker compsoe 1.24+
   * Anaconda 2.1.1+

## Running the demo
 
 ### 1. Download/Clone the docker files from the GitHub repository
 
 #### [Github repo](https://github.com/sofienesafta/mongodb-kafka/tree/feature-review )
 
 To run the demo issue ```sh run.sh``` which will:

  * Run ```docker-compose up```
  * Wait for MongoDB, Kafka, Kafka Connect to be ready
  * Register the Confluent Mongodb sink Connector
  * Create a kafka consumer to read data from kafka Topic 
  
  examine the topics in the Kafka control center [http://localhost:9021/](http://localhost:9021/)
### 2. Run the python data generator application
   run ```pip install -r requirements.txt```in a new shell. Then 
   run ```python3 kafka-mongodb.py``` to start generating fake data to mongodb collections in ```patient``` database and Kafka Topics.

### 3. Access Control in Mongodb
  
   IN ```auth_users.js```file 3 users are created : ```root``` ,```doctor``` and ```care_maker``` with access control for each user.
   
   Issue this command line ```sudo docker-compose exec mongo bash``` to execute commands inside the ```mongo``` container then type : ```mongo < docker-entrypoint-initdb.d/auth_users.js```
   
  To examine the access control of each user type inside mongo container : ```mongo -u <username> -p <password> --authenticationDatabase <database>```
  
  The password fo each user is his username. ***exp***: ```mongo -u doctor -p doctor --authenticationDatabase patient```  allows to login as the doctor user.
