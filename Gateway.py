
from kafka import KafkaProducer
import json
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

  

def data_to_kafka(nb_patient=10,n=2,ML_predict=False,model=None): ##If ML_predict is set to False the current labels of initial dataset 'heart.csv'
	                                                          ## will be taken in account rather then The ML classifier prediction.

	#create kafka producer
    producer = KafkaProducer(bootstrap_servers= 'localhost:9092',
                       value_serializer=lambda v: json.dumps(v).encode('utf-8'))
  
    if ML_predict:    ## A condition to Use a classification Ml algorithm to predict the target .    	
    	
	data = pd.read_csv('sensors_data.csv').iloc[:nb_patient,:] ## sensors_data.csv is X_test obtained after splitting in X_test and X_train.                                                            
	
    else:
	data= pd.read_csv('heart.csv').iloc[:nb_patient,:]
	

    for  i in data.index:
	    X= data.iloc[[i],:]
	
	    if 'target' in X.columns:              
		type_record= X.loc[i,'target']
	        X.drop('target',axis=1)
                                             ## type_record is a variable that identify the label of each record read.
        else:
            	type_record= model.predict(X)
        
        if type_record==1:
		
                topic_name = "urgent_data"  
        else:                              ## The kafka Topic is specified based on the type_record value(1 or 0)

                topic_name= "normal_data"
		
	
        doc= X.drop(['sex','age'],axis=1).to_dict('records')[0]
        doc['date']=str(datetime.datetime.now().date())
        doc['time']=str(datetime.datetime.now().time())[:5]
        doc['Patient_ID'] = i+1
        producer.send(topic_name,doc)
        print("this data is urgent !!!" if type_record==1 else "this data is normal")
        sleep(n)
        
    producer.flush()



## You can use the ML model saved in this directory in case of the booleen parameter ML_predict is set True.
model = joblib.load("./model.joblib") 
                                                	
data_to_kafka(nb_patient=15,ML_predict=False) ## reading initial version of data already classified .




