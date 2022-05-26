#!/usr/bin/env python
# coding: utf-8

# In[16]:




# In[1]:


from kafka import KafkaProducer
from json import loads 
import json
import random
import string
import datetime
from time import sleep
from faker import Faker
import pymongo
from pymongo import MongoClient 
from pprint import pprint


# In[2]:


#login to mongod with root user
myclient = MongoClient('mongodb://%s:%s@127.0.0.1/admin' % ('root', 'root'))


# In[22]:


# list all databases
myclient.list_database_names()


# In[23]:


Patientdb = myclient['patient']
sensorlogs_col = Patientdb["sensorlogs"]
profile_col = Patientdb["profile"]
medical_action_col = Patientdb["medical_action"]


# In[5]:


fake=Faker()

def get_registered_patient(seed=None):
    random.seed(seed)
    fake.seed_instance(seed)
    gender =random.choice(["Homme","Femme"])
    date = fake.date_between(start_date='-8M', end_date='now')
    patient = fake.unique.random_int()
    return ({
        "name": fake.name_male() if gender =="Homme" else fake.name_female(),
        "Gender": gender,
        "Adresse": fake.address(),
        "date_of_hospitalization": str(date),
        "date_of_birth":str(fake.date_of_birth()),
        "Patient": patient,
        "Job": fake.job(),
      },(patient,date)) 


# In[6]:


def generate_profiles ():
    patient_list=[]
    patient_with_date=[]
    for i in range(10):
        patient =get_registered_patient()
        patient_list.append(patient[0])
        patient_with_date.append(patient[1])
        
    patient_Id = [doc['Patient'] for doc in patient_list]

    return (patient_list, dict(patient_with_date) , patient_Id)


# In[7]:


Patients,patient_with_date,patient_Id = generate_profiles()


# In[8]:



def generate_medical_action(seed=None):
    random.seed(seed)
    fake.seed_instance(seed)
    
    patient = random.choice(patient_Id)
    return {
        "doctor/care_maker": fake.random_int(),
        "Patient": patient,
    "Date_of_action": str(fake.date_between(start_date=patient_with_date[patient], 
                        end_date='now')),
        "Time": fake.time(),
        "Action_ID": random.choice([4,12,5,18,28,15])
    }


# In[24]:


def create_fake_mongo_collections():

    adoc = profile_col.insert_many(Patients)
    actions_doc = [generate_medical_action()  for i in range(10)]
    adoc = medical_action_col.insert_many(actions_doc)
    
create_fake_mongo_collections ()


# In[20]:


def generate_sensors_data():
    date = datetime.datetime.now()
    
    message = random.choices([  [str(date.time())[:5],str(date.date()),random.choice(['Sensor1','Sensor2','Sensor3']),
                            random.choice(['Normal','Urgent']),
                           ]+ random.choice([['General data','Temperature',random.choice([40,36,35,33,41,42])],
                 ['Heart data','heart Rate',random.choice([60,63,55,58,52])] ,
                        ['Heart data','Oxygene level',random.choice(['60%','65%','55%','50%'])]])+
                         [random.choice(patient_Id)],
                             [str(date.time())[:5],str(date.date()),'PC-2','Normal','Medical data','Address','QFM',
      
                    random.choice(patient_Id) ]],weights=(90,10))
    
    header = ['Time','Date','Id sensor','Category','Attribute','Data type','Value','Patient']
    
    final_message = dict(zip(header,message[-1]))
    
    return final_message


# In[11]:


#create kafka producer
producer = KafkaProducer(bootstrap_servers= 'localhost:9092',
                       value_serializer=lambda v: json.dumps(v).encode('utf-8'))


# In[21]:


# send 10 events to either normal_data topic or urgent_data topic every 2 seconds
for i in range(10):
    
    message = generate_sensors_data()
    
    topic_name = "normal_data"

    if message['Category'] == 'Urgent':
        topic_name = "urgent_data"
        
    producer.send(topic_name, message)
    
    sleep(2)
    
    
producer.flush()


# In[ ]:




