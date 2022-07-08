from kafka import KafkaConsumer
from json import loads 
from email.message import EmailMessage
import smtplib

def email_alert(subject,body,to):
    msg= EmailMessage()
    msg.set_content(body)
    msg['subject']= subject
    msg['to'] = to
    user = "sofiene.safta19@gmail.com"
    msg['from'] = user
    password= "jtrcanuaytrfxtjr"
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

staff_email = input("Email : " )   ## The user gives a valid email to test Alert message.
for msg in consumer:
    print("ALERT Emergency")    
    email_alert("ALERT emergency","A patient need urgent intervention" ,staff_email)
