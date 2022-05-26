# Mongodb kafka


## Requirements:
  
   * Ubuntu 18.4
   * Docker 18.9+
   * Docker compsoe 1.24+
   * Anaconda 2.1.1+

## Running the demo
 
 ### 1. Download/Clone the docker files from the GitHub repository
 
 #### [Github repo](https://github.com/sofienesafta/mongodb-kafka/tree/feature-review "gh")
 
 To run the demo issue ```sh run.sh``` which will:

  * Run ```docker-compose up```
  * Wait for MongoDB, Kafka, Kafka Connect to be ready
  * Register the Confluent Mongodb sink Connector
  * Create a kafka consumer to read data from kafka Topic
  
### 2. Run the python data generator application

   run python3 a```uth_users.py``` in a new shell to start generating fake data to mongodb collections in ```patient``` database.

### 3. Access Control in Mongodb
  
   IN ```auth_users.js``` file 3 users are created : ```root``` ,```doctor``` and ```care_maker``` with access contrl for eachone.
   
   Issue this command line in sudo ```docker-compose exec mongo bash``` to execute commands inside the ```mongo``` container then type : ```mongo < auth_users.js/docker-entrypoint-initdb.d```
   
  To examine the access control of each user type inside mongo container : ```mongo -u <username> -p <password> --authenticationDatabase <database>```
  
  The password fo each user is his username. ***exp***: ```mongo -u doctor -p doctor --authenticationDatabase patient```  allows to login as the doctor user.