from kafka import KafkaConsumer
from json import loads 
from email.message import EmailMessage
import smtplib

def email_alert(subject,body,to):
    msg= EmailMessage()
    msg.set_content(body)
    msg['subject']= subject
    msg['to'] = to
    user = "sofiene.safta@horizon-tech.tn"
    msg['from'] = user
    password= "lbdpyemcsksjrsgk"
    server = smtplib.SMTP("smtp.gmail.com",587)
    server.starttls()
    server.login(user, password)
    server.send_message(msg)
    server.quit()

# create the Kafka Consumer  
consumer = KafkaConsumer('urgent_data',
         bootstrap_servers = ['localhost : 9092'], 
         group_id = 'care_makers',  
         value_deserializer = lambda x : loads(x.decode('utf-8'))  
        ) 
nurse_email = input("Email : " )
for msg in consumer:
      
    print("ALERT: Emergency")    
    email_alert("ALERT","emergency" ,nurse_email)
