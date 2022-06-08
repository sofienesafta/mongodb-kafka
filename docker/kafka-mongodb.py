
from kafka import KafkaProducer
import json
import requests
import random
import datetime
from time import sleep
from pymongo import MongoClient 


def patient_profiles():
    r=requests.get("https://raw.githubusercontent.com/animesh1012/machineLearning/main/Heart%20Failure%20Prediction/heart.csv")
    lines = r.text.split('\n')[1:11]
    header=["Age","Sex","ChestPainType","RestingBP","Cholesterol","FastingBS","RestingECG"
        ,"MaxHR","ExerciseAngina","Oldpeak","ST_Slope","HeartDisease"]
    profiles=[dict(zip(header,line.split(','))) for line in lines]
    list_range=range(1200,1210)
    for i, profile in enumerate(profiles) :
        profile["patient"]= list_range[i]
        
    return profiles


 
def generate_random_date():
    start_date = datetime.date(2021, 12, 1)
    end_date = datetime.date(2022, 5, 30)
    time_between_dates = end_date - start_date
    days_between_dates = time_between_dates.days
    random_number_of_days = random.randrange(days_between_dates)
    random_date = start_date + datetime.timedelta(days=random_number_of_days)
    return str(random_date)

def generate_medical_action(seed=None):
    random.seed(seed)
    
    patient = random.choice(range(1200,1210))
    return {
        "doctor/care_maker": random.choice([100,102,104,98]),
        "Patient": patient,
        "Date_of_action": generate_random_date(),
        "Time": random.choice(["12.22","22.54","23.15","00.20","9.25","6.00","8.30","19.05","14.00","17.10"]),
        "Action_ID": random.choice([4,12,5,18,28,15])
    }
#login to mongodb with root user
myclient = MongoClient('mongodb://%s:%s@127.0.0.1/admin' % ('root', 'root'))

#create patient database and collections
Patientdb = myclient['patient']
sensorlogs_col = Patientdb["sensorlogs"]
profile_col = Patientdb["profile"]
medical_action_col = Patientdb["medical_action"]


# list all databases
myclient.list_database_names()

def create_mongo_collections():
    profiles= patient_profiles()
    adoc = profile_col.insert_many(profiles)
    actions_doc = [generate_medical_action()  for i in range(10)]
    adoc = medical_action_col.insert_many(actions_doc)

create_mongo_collections()


def generate_sensors_data():
    date = datetime.datetime.now()
    
    message = random.choices([  [str(date.time())[:5],str(date.date()),random.choice(['Sensor1','Sensor2','Sensor3']),
                            random.choice(['Normal','Urgent']),
                           ]+ random.choice([['General data','Temperature',random.choice([40,36,35,33,41,42])],
                 ['Heart data','heart Rate',random.choice([60,63,55,58,52])] ,
                        ['Heart data','Oxygene level',random.choice(['60%','65%','55%','50%'])]])+
                         [random.choice(range(1200,1210))],
                             [str(date.time())[:5],str(date.date()),'PC-2','Normal','Medical data','Address','QFM',
      
                    random.choice(range(1200,1210))]],weights=(90,10))
    
    header = ['Time','Date','Id sensor','Category','Attribute','Data type','Value','Patient']
    
    final_message = dict(zip(header,message[-1]))
    
    return final_message 

#create kafka producer
producer = KafkaProducer(bootstrap_servers= 'localhost:9092',
                       value_serializer=lambda v: json.dumps(v).encode('utf-8'))


# send 10 events to either normal data or urgent_data every 2 seconds
for i in range(10):
    
    message = generate_sensors_data()
    
    topic_name = "normal_data"

    if message['Category'] == 'Urgent':
        topic_name = "urgent_data"
        
    producer.send(topic_name, message)
    
    sleep(2)
        
producer.flush()






