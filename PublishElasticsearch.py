# -*- coding: utf-8 -*-

from datetime import datetime
from elasticsearch import Elasticsearch

import paho.mqtt as mqtt
import paho.mqtt.client as mqtt
import json


MQTT_ADDRESS = '127.0.0.1'
MQTT_PORT = 1883
MQTT_TIMEOUT = 60
MQTT_TOPIC_SUBSCRIBE = 'sensor/cliente'
MQTT_PWD =''
MQTT_LGN =''
MQTT_ID ='sensor'

es = Elasticsearch(['http://127.0.0.1:9200'])

def on_connect(client, userdata, flags, rc):
    #print('Conectado. Resultado: %s' % str(rc))
    client.subscribe(MQTT_TOPIC_SUBSCRIBE) # a chamada de subscrição e feita de dentro do metodo de conexao.
    
def on_message(client, userdata, msg):
    #print('topico: %s' % msg.topic)   
    #print('Mensagem : %s' % msg.payload.decode('utf-8'))

    data = (str(msg.payload.decode('utf-8')))

    post_data(data)

def startMqtt():
    client = mqtt.Client(MQTT_ID)
    client.on_connect = on_connect
    client.on_message = on_message
    client.username_pw_set(MQTT_LGN, MQTT_PWD)
    client.connect(MQTT_ADDRESS, MQTT_PORT, MQTT_TIMEOUT)
    client.loop_forever()

def post_data(data):

    try:
        res = es.index(index="cliente", doc_type="measure",body=data)
        print(res['created'])
        print('posted')
    
    except Exception as MotivoErro:
        print('ferror:')
        print (MotivoErro.args)

if __name__ == '__main__':

    print ('Iniciando...')
    startMqtt()
