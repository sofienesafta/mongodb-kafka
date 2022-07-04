
from kafka import KafkaProducer
import json
import requests
import random
import datetime
import pandas as pd
from time import sleep
import joblib
from pymongo import MongoClient 

 
def generate_random_date(y1,m1,d1,y2,m2,d2):
    start_date = datetime.date(y1, m1, d1)
    end_date = datetime.date(y2, m2, d2)
    time_between_dates = end_date - start_date
    days_between_dates = time_between_dates.days
    random_number_of_days = random.randrange(days_between_dates)
    random_date = start_date + datetime.timedelta(days=random_number_of_days)
    return str(random_date)
    
def patient_profiles(nb_patient):
    patient_list= []
    name_male =['John Coffey','Justin Marshall','Michael Hensley','Samuel Parker',
 'Brian Mendez','Jermaine Garrison','Kevin Brock','Albert White','Mr. Stephen Crosby', 'Adam Robbins','Michael Klein','Richard Baker',
 'Ralph Zimmerman','Jason Clark','Robert Austin DDS','John Miller','Adam Escobar','Brian Burke',
 'Frank Zavala', 'Jonathan Cummings']
    
    name_female= ['Dana Walker','Tammy Murray','Victoria Reynolds','Faith Weaver',
 'Christine Rubio','Desiree Bean','Jessica Gaines','Breanna Davies','Alicia Thompson','Jenny Sloan','Cynthia Tucker',
 'Angela Riley','Shannon Guzman','Tammy Anderson','Christine Torres','Virginia Lopez','Lori SinghStephanie Reyes',
 'Stephanie Edwards','Katherine Osborne']
    
    
    
    address= ['1893 Lynn Underpass\nNew Brittany, ME 93981','0105 Bobby Bridge Suite 025\nOscarfurt, NY 28942',
 '17247 Rocha Place Apt. 581\nEast Kelly, AZ 59359','26144 Walker Course Apt. 615\nWaynefort, ID 84757',
'783 Jennifer Ports\nPort Amanda, MA 16533','3581 Thornton Unions\nWest Samantha, KY 84758',
 '989 Lam Mountains Apt. 286\nLake Jane, NV 27732','PSC 4037, Box 7456\nAPO AE 67545',
 'USNS Vaughan\nFPO AP 46086','7701 Middleton Ranch Apt. 805\nLake Toddhaven, CA 94474',
 '5306 Christopher Avenue\nLake Sarahland, VT 17929','3824 Joyce Vista\nLake Breanna, RI 13721',
 '49244 Armstrong Shoals\nNew Johnborough, OR 61986','00081 Kimberly Way Suite 660\nEast Elizabeth, PA 77404',
 '6873 Jennifer Ferry Apt. 579\nThomasfort, OH 08793','360 Pearson Pass\nNew Lisamouth, NY 87005',
 '523 Davis Pike Apt. 605\nSouth Nathanview, MO 51307','564 Veronica Inlet\nLaurachester, NH 73220',
 '712 Todd Locks\nWest Paulchester, MS 28908','38388 Christina Freeway Suite 310\nSouth Cristina, LA 29557']
   
    job= ['Paramedic','Automotive engineer','Financial risk analyst','Aid worker',
 'Science writer','Scientist, research (maths)','Radio producer','Sub','Production assistant, radio',
 'Scientist, research (physical sciences)','Production assistant, television','Housing manager/officer',
    'Historic buildings inspector/conservation officer',  'Theme park manager', 'Arboriculturist',
    'Ranger/warden', 'Designer, television/film set', 'Dramatherapist', 'Surveyor, mining', 'Leisure centre manager']
    for i in range(nb_patient):
        gender =random.choice(["Homme","Femme"])
      
        patient_list.append({"name":  name_male.pop() if gender =="Homme" else name_female.pop(),
        "Gender": gender,
        "Address": address.pop(),
        "date_of_hospitalization": str(generate_random_date(2020,12,1,2022,4,2)),

        "date_of_birth":str(generate_random_date(1950,8,1,2000,4,2)),
        "Patient": i+1,
        "Job":job.pop()}
       )
    return patient_list

def generate_medical_action(nb_patient,seed=None):
    random.seed(seed)
    
    patient = random.choice(range(1,nb_patient+1))
    return {
        "doctor/care_maker": random.choice([100,102,104,98]),
        "Patient": patient,
        "Date_of_action": generate_random_date(2022,4,1,2022,8,1),
        "Time": random.choice(["12.22","22.54","23.15","00.20","9.25","6.00","8.30","19.05","14.00","17.10",20.12,22.00]),
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

def create_mongo_collections(nb_patient=10):
    profiles= patient_profiles(nb_patient)
    adoc = profile_col.insert_many(profiles)
    actions_doc = [generate_medical_action(nb_patient)  for i in range(nb_patient)]
    adoc = medical_action_col.insert_many(actions_doc)

create_mongo_collections(15)

def df_init_version():
    df= pd.read_csv("heart.csv")
    return df
   

def patient_classified_with_ML_model():
    r=requests.get("https://raw.githubusercontent.com/rashida048/Datasets/master/Heart.csv")
    lines = r.text.split()
    header = lines[0].split(',')
    df=pd.DataFrame(lines,columns=['line'])
    df= df['line'].str.split(',',expand=True)
    df.columns=header
    df.rename(columns={'':'patient'},inplace=True)
    df= df.iloc[1:,:-1]  
    df['ChestPain'] = df['ChestPain'].replace({"typical":3,"nontypical":1,"asymptomatic":0,"nonanginal":2})
    df['Thal'] = df['Thal'].replace({"fixed":1,"normal":2,"reversable":3})
    header.remove("Oldpeak")
    df[header[1:-1]] =df[header[1:-1]].astype('int')
    df['patient']=df['patient'].astype('int')
    df['Oldpeak']=df['Oldpeak'].astype('float')
    
    df['target'] = loaded_model.predict(df.drop('patient',axis=1))
    return df

  

def data_to_kafka(nb_patient=10,n=2,ML_predict=False,model=None):

	#create kafka producer
    producer = KafkaProducer(bootstrap_servers= 'localhost:9092',
                       value_serializer=lambda v: json.dumps(v).encode('utf-8'))
  
        	
    df= df_init_version().iloc[:nb_patient,:]
	
    for _ , row in df.iterrows() :
	
	if ML_predict:
		
		X= row.drop('target').to_frame().T
           	type_record =  model.predict(X)
        else:
                type_record= row['target']
        
        if type_record==1:
                topic_name = "urgent_data"
        else:

                topic_name= "normal_data"
		
	
        doc= row.drop('target').to_dict()
        
        producer.send(topic_name,doc)
    
        sleep(n)
        
    producer.flush()

loaded_model = joblib.load("./model.joblib") ## You can use the ML model saved in this directory in case of the booleen parameter ML_predict is set True.
                                                
	
data_to_kafka(nb_patient=15,ML_predict=False)




