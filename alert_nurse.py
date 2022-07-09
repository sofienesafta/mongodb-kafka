from kafka import KafkaConsumer
from json import loads 
from email.message import EmailMessage
import smtplib

def email_alert(subject,body,to,user,password):
    msg= EmailMessage()
    msg.set_content(body)
    msg['subject']= subject
    msg['to'] = to
    msg['from'] = user
    server = smtplib.SMTP("smtp.gmail.com",587)
    server.starttls()
    server.login(user, password)
    server.send_message(msg)
    server.quit()

# create the Kafka Consumer  
consumer = KafkaConsumer('urgent_data',
         bootstrap_servers = ['localhost : 9092'], 
         group_id = 'Alert_staff'
                        ) 

staff_email = input("to : " )   ## The user gives a valid email to test Alert email message.
user_address= input(" user : ")
passw= input("password : " ) ##   The password given is not the one you put to login to your gmail accunt.It
                            ##  is an application password provided by google.
for msg in consumer:
    print("ALERT Emergency")    
    email_alert("ALERT emergency","A patient need urgent intervention" ,staff_email,user_address,passw)
