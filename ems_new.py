#!/usr/bin/env python
# coding: utf-8

# # Version 2
# 

# In[1]:


from paho.mqtt import client as mqtt_client
from influxdb_client.client.write_api import SYNCHRONOUS
from influxdb_client import InfluxDBClient, Point, WriteOptions
import random 
import json
import time


Broker = "3.7.85.13"
Port = 1883
TopicEM = "JBMGroup/em3phase/neel5"
TopicMch = "JBMGroup/em3phase/#"


def connect_mqtt() -> mqtt_client:
    def on_connect(client, userdata, flags, rc):
        
        if rc == 0:
            pass
            
        else:
            print("Failed to connect:", rc)
            
    try:
        client_id = str(random.randint(0,9))
        client = mqtt_client.Client(client_id)
        client.on_connect = on_connect
        client.connect(Broker, Port)
#         print(client)
        return client
              
    except Exception as e:
        print(e)
                

def send_email():
    import smtplib
    message = "Device disconnected from the internet and checking the time interval"

    server = smtplib.SMTP('smtp.gmail.com', 587)
    server.starttls()
    server.login('utkarshjbm@gmail.com', "hgreexexnbdwuabl")
    server.sendmail('utkarshjbm@gmail.com', "utkarshgaur2101995@gmail.com", message)

    print("mail sent")             

                
def subscribe(client: mqtt_client):
    def on_message(client, userdata, msg):
        rawdata = str(msg.payload.decode("utf-8"))
        data = json.loads(rawdata)
        print(data)
        
        try:
            if data["LPS_B_LINE_12_13-ACTIVE_CURRENT"] == 0:
                send_email()                
        except Exception as e:
            print(e)           
               
            
        
        
        
        
    try:
        client.subscribe("JBMGroup/em3phase")
        client.on_message = on_message
        
    except Exception as e:
        print(e)
        
    
    
    
    
def run():
    client = connect_mqtt()
    
    if client == None:
        print("client is None")
    else:
        
        subscribe(client)
        client.loop_forever()
         
    
while True:
    run()
    time.sleep(2)
    print("Connecting...")





