# -*- coding: utf-8 -*-

'''
Created on 16/11/2016

@author: mateus bispo
@email: math.l.bispo@gmail.com

'''

import paho.mqtt as mqtt                 # bibliotecas mqtt
import paho.mqtt.client as mqtt
import json
import logging

# dados para inicializar o cliente mqtt
MQTT_ADDRESS = '127.0.0.1'
MQTT_PORT = 1883
MQTT_TIMEOUT = 60

MQTT_TOPIC_SUBSCRIBE = 'nomeTopico1'
MQTT_TOPIC_SUBSCRIBE2 = 'nomeTopico2'
MQTT_TOPIC_PUBLISH_CLIENTE1 = "iot/cliente1"
MQTT_TOPIC_PUBLISH_CLIENTE2 = "iot/cliente2"
MQTT_TOPIC_PUBLISH_CLIETNE3 = "iot/cliente3"
MQTT_LGN = 'iot'
MQTT_PSW = ''
BOX_ID = 'sensor_iot'

kronIDsCliente1 = ["001906075", "001906076", "001906077", "001906078", "001906079", "001906080", "001906081", "001906082", "001906083", "001906084", "001906085", "001906176"]
kronIDsCliente2 = ["1913751", "138465", "1256897"]
kronIDsCliente3 = ["0000101", "10010011"]

client = mqtt.Client(BOX_ID) # criacao do cliente mqtt

def on_connect(client, userdata, flags, rc): # callback executado ao conectar
    print('Conectado. Resultado: %s' % str(rc))
    client.subscribe(MQTT_TOPIC_SUBSCRIBE) # a chamada de subscricao e feita dentro do metodo de conexao.
    client.subscribe(MQTT_TOPIC_SUBSCRIBE2)
    
def on_subscribe(client, userdata, mid, granted_qos): # callback que informa quando foi feito um subscribe
    print('Inscrito no topico: %d' % mid)
    
def on_message(client, userdata, msg): # callback que mostra as menssagem publicadas nos topicos que estiver inscrito
    print('topico: %s' % msg.topic)
    print('Mensagem: %s' % msg.payload.decode('utf-8'))
    
    
    try:
        strText = str(msg.payload.decode('utf-8')) # variavel que recebe o texto da menssagem do mqtt
        jsonText = json.loads(strText) #converte a string da menssagem recebida do mqtt em um objeto json
		
        dicReceived = convertJson(jsonText) # metodo que ira manipular a estrutura do json para adequar ao formato que deve ser postado
        
        if dicReceived["cliente"] != ' ': # verifica se o valor da key cliente nao esta vazia 
            publishJson(dicReceived) # encaminha o dicionario com os dados para o metodo que ira publicar os dados
            
    except Exception as failed:
        print (failed.args)
		logging.exception("Reason for failure: ")
        
def on_publish(mosq, obj, mid):
    print("publicado: "+str(mid))
    
def startMqtt(): # metodo que inicializa o cliente mqtt
    
    client.on_connect = on_connect
    client.on_subscribe = on_subscribe
    client.on_message = on_message
    client.username_pw_set(MQTT_LGN, MQTT_PSW)
    client.connect(MQTT_ADDRESS, MQTT_PORT, MQTT_TIMEOUT)
    client.loop_forever()

'''
acimas estao os callbacks e metodos proprios da biblioteca de mqtt
'''

def convertJson(dataReceived):
    
    thingKey = '' # variavel para armazenar o id do sensor
    package = {"cliente":" "} # dicionario para retornar os dados para publicar no mqtt
    datas = "" # variavel para receber os dados que seram selecionados do json inicial
	jsonPost = "" # variavel que recebe o novo json que sera publicado
	
    try:  
        listKeys = list(dataReceived) # gerando uma lista com as keys do dicionario dataReceived / esse dicionario é necessario pois o primeira key do msg recebida é gerada dinamicamente, dessa forma o primeiro valor é acessado pela posiçao.
        thingKey = dataReceived[listKeys[0]]["params"]["thingKey"] # atribui o id do cliente que publicou a msg a uma variavel

        if thingKey in kronIDsCliente1: # verifica se o id é de um sensor do cliente1
            package["cliente"] = "cliente" # adiciona o cliente ao dicionario que sera publicado no mqtt
            
            datas = dataReceived[listKeys[0]]["params"]["data"] # separa em uma variavel os dados coletados pelo sensor
            jsonPost = json.dumps({'node': thingKey,'reply': 'sensorIot/'+thingKey+'/set','vendor': 'sensor','payload': {'medidor':datas}}) # monta o json que sera publicado com o id do cliente e os dados do sensor
            
        elif thingKey in kronIDsCliente2:
            package["cliente"] = "cliente2"
            
            datas = dataReceived[listKeys[0]]["params"]["data"]
            jsonPost = json.dumps({'node': thingKey,'reply': 'sensorIot/'+thingKey+'/set','vendor': 'sensor','payload': {'medidor':datas}})
            
        elif thingKey in kronIDsCliente3:
            package["cliente"] = "cliente3"
           
            datas = dataReceived[listKeys[0]]["params"]["data"]
            jsonPost = json.dumps({'node': thingKey,'reply': 'sensorIot/'+thingKey+'/set','vendor': 'sensor','payload': {'medidor':datas}})

        package.update({"json":jsonPost}) # adicona o json que deve ser publicado ao dicionario que sera retornado pelo metodo
        
    except Exception as erro:
        print(erro.args)    
		logging.exception("Reason for failure: ")
		
    return package # retornando o dicionario
   
def publishJson(newJson): #metodo que publica os dados no mqtt

    try:
        if newJson["cliente"] == 'cliente1': # verifica se o valor da chave cliente do dicionario é igual a string definida
            client.publish(MQTT_TOPIC_PUBLISH_CLIENTE1, newJson["json"]) #publica o valor da chave json no topico definido

        elif newJson["cliente"] == 'cliente2':
            client.publish(MQTT_TOPIC_PUBLISH_CLIENTE2, newJson["json"])
            
        elif newJson["cliente"] == 'cliente3':
            client.publish(MQTT_TOPIC_PUBLISH_CLIETNE3, newJson["json"])

        print ('publicou')
    
    except Exception as motivoErro:
        print(motivoErro.args) 
		logging.exception("Reason for failure: ")
		
if __name__ == '__main__': # metodo principal do scrip

    print ('Iniciando...')
    startMqtt() # chamando o metodo que inicializa o mqtt
