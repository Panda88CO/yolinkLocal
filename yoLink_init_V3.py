#!/usr/bin/env python3
import requests
import time
import json
import psutil
import sys
import math

from  datetime import datetime
try:
    import udi_interface
    logging = udi_interface.LOGGER
    #logging = getlogger('yolink_init_V2')
    Custom = udi_interface.Custom
except ImportError:
    import logging
    logging.basicConfig(level=logging.DEBUG)
    #root = logging.getLogger()
    #root.setLevel(logging.DEBUG)
    #handler = logging.StreamHandler(sys.stdout)
    #handler.setLevel(logging.DEBUG)
    #formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    #handler.setFormatter(formatter)
    #root.addHandler(handler)

countdownTimerUpdateInterval_G = 10


import paho.mqtt.client as mqtt
from queue import Queue
from threading import Thread, Event, Lock
DEBUG = False


class YoLinkInitPAC(object):
    def __init__(self, ID, secret, tokenURL='https://api.yosmart.com/open/yolink/token' , apiURL='https://api.yosmart.com/open/yolink/v2/api', mqttURL= 'api.yosmart.com', mqttPort = 8003, home_id = None):
        self.homeID  = home_id
        self.disconnect_occured = False 
        self.tokenLock = Lock()
        self.fileLock = Lock()
        self.TimeTableLock = Lock()
        self.publishQueue = Queue()
        #self.delayQueue = Queue()
        self.messageQueue = Queue()
        self.fileQueue = Queue()
        #self.timeQueue = Queue()
        self.MAX_MESSAGES = 100  # number of messages per self.MAX_TIME
        self.MAX_TIME = 30      # Time Window
        self.time_tracking_dict = {} # structure to track time so we do not violate yolink publishing requirements
        self.debug = False
        #self.pendingDict = {}
        self.pending_messages = 0
        self.time_since_last_message_RX = 0
        self.tokenURL = tokenURL
        self.apiv2URL = apiURL
        self.mqttURL = mqttURL
        self.mqttPort = mqttPort

        self.connectedToBroker = False
        self.loopRunning = False
        self.uaID = ID
        self.secID = secret
        self.apiType = 'UAC'
        self.tokenExpTime = 0
        self.timeExpMarging = 3600 # 1 hour - most devices report once per hour
        self.lastTransferTime = int(time.time())

        self.local_URL = ''
        #self.local_port = ':1080'
        self.local_client_id = ID
        self.local_client_secret = secret

        #self.timeExpMarging = 7170 #min for testing 
        self.tmpData = {}
        self.lastDataPacket = {}
        self.mqttList = {}
        self.TtsMessages = {}
        self.nbrTTS = 0
        self.temp_unit = 0
        self.online = False
        self.deviceList = []
        self.token = None
        self.mqtt_str = ''
        self.QoS = 1
        self.keepAlive = 60


        self.unassigned_nodes = []
        try:
            #while not self.request_new_token( ):
            #    time.sleep(60)
            #    logging.info('Waiting to acquire access token')
           
            #self.retrieve_device_list()
            #self.retrieve_homeID()

            self.retryNbr = 0
            self.disconnect = False
            self.STOP = Event()

            #self.messageThread = Thread(target = self.process_message )
            #self.publishThread = Thread(target = self.transfer_data )
            #self.fileThread =  Thread(target = self.save_packet_info )
            #self.connectionMonitorThread = Thread(target = self.connection_monitor)

            #self.messageThread.start()
            #self.publishThread.start()
            #self.fileThread.start()
            

            logging.info('Connecting to YoLink MQTT server')
            while not self.refresh_token():
                time.sleep(35) # Wait 35 sec and try again - 35 sec ensures less than 10 attemps in 5min - API restriction
                logging.info('Trying to obtain new Token - Network/YoLink connection may be down')
            logging.info('Retrieving YoLink API info')
            time.sleep(1)
            if self.homeID is None:
                self.mode = 'cloud'
                if self.token != None:
                    self.retrieve_homeID()
                    self.mqtt_str = 'yl-home/'
                    self.retrieve_device_list()
                else:
                    if self.token != None:
                       self.mqtt_str = 'ylsubnet/'
            else:
                self.mode = 'local'
                self.mqtt_str = 'ylsubnet/'    

            #if self.client == None:    
      
            #logging.debug('initialize MQTT' )
            try:
                self.client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION1, self.homeID,  clean_session=True, userdata=None,  protocol=mqtt.MQTTv311, transport="tcp")
            except Exception as e:
                logging.error(f'Using non pG3x code {e}')
                self.client = mqtt.Client(self.homeID,  clean_session=True, userdata=None,  protocol=mqtt.MQTTv311, transport="tcp")

            self.client.on_connect = self.on_connect
            self.client.on_message = self.on_message
            self.client.on_subscribe = self.on_subscribe
            self.client.on_disconnect = self.on_disconnect
            self.client.on_publish = self.on_publish
            #logging.debug('finish subscribing ')

            while not self.connect_to_broker():
                time.sleep(2)
            
            #self.connectionMonitorThread.start()
            
            #logging.debug('Connectoin Established: {}'.format(self.check_connection( self.mqttPort )))
            
        except Exception as E:
            logging.error('Exception - init- MQTT: {}'.format(E))

        self.messagePending = False


    ############################
    #  Local ACCESS FUNCTIONS

    def initializeLocalAccess(self, client_id, client_secret, local_ip):
        logging.debug(f'initializeLocalAccess {client_id} {client_secret} {local_ip}')
        self.local_client_id = client_id
        self.local_client_secret = client_secret
        self.local_URL = 'http://'+local_ip+self.local_port
        response = requests.post(self.local_URL+'/open/yolink/token', 
                                 data={ "grant_type": "client_credentials",
                                        "client_id" :  self.local_client_id ,
                                        "client_secret":self.local_client_secret }, timeout= 5)
        if response.ok:
            temp = response.json()
            logging.debug('Local yoAccess Token : {}'.format(temp))
        
        if 'state' not in temp:
            self.local_token = temp
            self.local_token['expirationTime'] = int(self.token['expires_in'] + int(time.time()) )
            logging.debug('Local yoAccess Token : {}'.format(self.token ))
        else:
            if temp['state'] != 'error':
                logging.error('Authentication error')

    def local_refresh_token(self):
        
        try:
            logging.info('Refreshing Token ')
            now = int(time.time())
            if self.token != None:
                if now < self.token['expirationTime']:
                    response = requests.post( self.tokenURL,
                        data={"grant_type": "refresh_token",
                            "client_id" :  self.uaID,
                            "refresh_token":self.token['refresh_token'], 
                            }, timeout= 5
                    )
                else:
                    response = requests.post( self.tokenURL,
                        data={"grant_type": "client_credentials",
                            "client_id" : self.uaID,
                            "client_secret" : self.secID }, timeout= 5
                )
                if response.ok:
                    self.token =  response.json()
                    self.token['expirationTime'] = int(self.token['expires_in']) + now
                    return(True)
                else:
                    logging.error('Was not able to refresh token')
                    return(False)
            else:
                response = requests.post( self.tokenURL,
                    data={"grant_type": "client_credentials",
                        "client_id" : self.uaID,
                        "client_secret" : self.secID }, timeout= 5
                )
                if response.ok:
                    self.token =  response.json()
                    self.token['expirationTime'] = int(self.token['expires_in']) + now
                    return(True)
                else:
                    logging.error('Was not able to refresh token')
                    return(False)       



        except Exception as e:
            logging.debug(f'Exeption occcured during refresh {e}')





    '''
    def local_refresh_token(self):
        
        try:
            logging.info('Refreshing Local Token ')
            now = int(time.time())
            if self.token != None:
                if now < self.local_token['expirationTime']:
                    response = requests.post( self.local_URL+'/open/yolink/token',
                        data={"grant_type": "refresh_token",
                            "client_id" :  self.local_client_id,
                            "refresh_token":self.local_token['refresh_token'], 
                            }, timeout= 5
                    )
                else:
                    response = requests.post( self.local_URL+'/open/yolink/token',
                                 data={ "grant_type": "client_credentials",
                                        "client_id" :  self.local_client_id ,
                                        "client_secret":self.local_client_secret }, timeout= 5)
            if response.ok:
                if response.ok:
                    self.local_token =  response.json()
                    self.local_token['expirationTime'] = int(self.local_token['expires_in']) + now
                    return(True)
                else:
                    logging.error('Was not able to refresh token')
                    return(False)
            else:
                response = requests.post( self.local_URL+'/open/yolink/token',
                        data={"grant_type": "refresh_token",
                            "client_id" :  self.local_client_id,
                            "refresh_token":self.local_token['refresh_token'], 
                            }, timeout= 5
                    )
                if response.ok:
                    self.local_token =  response.json()
                    self.local_token['expirationTime'] = int(self.local_token['expires_in']) + now
                    return(True)
                else:
                    logging.error('Was not able to refresh token')
                    return(False)       

        except Exception as e:
            logging.debug('Exeption occcured during refresh_token : {}'.format(e))
            #return(self.request_new_token())

    '''

    #######################################
    #check if connected to YoLink Cloud server
    def measure_time(func):                                                                                                   
                                                                                                                          
        def wrapper(*arg):                                                                                                      
            t = time.time()                                                                                                     
            res = func(*arg)                                                                                                    
            logging.debug ("Function took " + str(time.time()-t) + " seconds to run")                                                    
            return res                                                                                                          
        return wrapper                                                                                                                

    #@measure_time
    def check_connection(self, port):
        logging.debug( 'check_connection: port {}'.format(port))
        connectons = psutil.net_connections()
        #logging.debug('connections: {}'.format(connectons))
        for netid in connectons:
            raddr = netid.raddr
            if len (raddr) > 0:
                if  port == raddr.port:
                    logging.debug('found : {} {}'.format(raddr.port, netid.status ))
                    return(netid)
        else:

            return(None)


    #####################################
    #@measure_time
    def getDeviceList(self):
        logging.debug(f'Device list: {self.deviceList}')
        return(self.deviceList)

    '''
    #@measure_time
    def request_new_token(self):
        logging.debug('yoAccess Token exists : {}'.format(self.token != None))
        now = int(time.time())
        if self.token == None:
            try:
                now = int(time.time())
                response = requests.post( self.tokenURL,
                        data={"grant_type": "client_credentials",
                            "client_id" : self.uaID,
                            "client_secret" : self.secID },
                    )
                if response.ok:
                    temp = response.json()
                    logging.debug('yoAccess Token : {}'.format(temp))
                else:
                    logging.error('Error occured obtaining token - check credentials')
                    return(False)
                if 'state' not in temp:
                    self.token = temp
                    self.token['expirationTime'] = int(self.token['expires_in'] + now )
                    #logging.debug('yoAccess Token : {}'.format(self.token ))
                else:
                    if temp['state'] != 'error':
                        logging.error('Authentication error')
                return(True)

            except Exception as e:
                logging.error('Exeption occcured during request_new_token : {}'.format(e))
                return(False)
        else:
            self.refresh_token()  
            return(True) # use existing Token 
    '''

    #@measure_time
    def refresh_token(self):
        
        try:
            logging.info('Refreshing Token ')
            now = int(time.time())
            if self.token != None:
                if now < self.token['expirationTime']:
                    response = requests.post( self.tokenURL,
                        data={"grant_type": "refresh_token",
                            "client_id" :  self.uaID,
                            "refresh_token":self.token['refresh_token'], 
                            }, timeout= 5
                    )
                else:
                    response = requests.post( self.tokenURL,
                        data={"grant_type": "client_credentials",
                            "client_id" : self.uaID,
                            "client_secret" : self.secID }, timeout= 5
                )
                if response.ok:
                    self.token =  response.json()
                    self.token['expirationTime'] = int(self.token['expires_in']) + now
                    return(True)
                else:
                    logging.error('Was not able to refresh token')
                    return(False)
            else:
                response = requests.post( self.tokenURL,
                    data={"grant_type": "client_credentials",
                        "client_id" : self.uaID,
                        "client_secret" : self.secID }, timeout= 5
                )
                if response.ok:
                    self.token =  response.json()
                    self.token['expirationTime'] = int(self.token['expires_in']) + now
                    return(True)
                else:
                    logging.error('Was not able to refresh token')
                    return(False)       



        except Exception as e:
            logging.debug('Exeption occcured during refresh_token : {}'.format(e))
            #return(self.request_new_token())

    #@measure_time
    def get_access_token(self):
        self.tokenLock.acquire()
        #now = int(time.time())
        if self.token == None:
            self.refresh_token()
        #if now > self.token['expirationTime']  - self.timeExpMarging :
        #    self.refresh_token()
        #    if now > self.token['expirationTime']: #we lost the token
        #        self.request_new_token()
        #    else:
        self.tokenLock.release() 

    #@measure_time                
    def is_token_expired (self, accessToken):
        return(accessToken == self.token['access_token'])
        
    #@measure_time
    def retrieve_device_list(self):
        try:
            logging.debug('retrieve_device_list')
            data= {}
            data['method'] = 'Home.getDeviceList'
            data['time'] = str(int(time.time_ns()/1e6))
            headers1 = {}
            headers1['Content-type'] = 'application/json'
            headers1['Authorization'] = 'Bearer '+ self.token['access_token']
            r = requests.post(self.apiv2URL, data=json.dumps(data), headers=headers1, timeout=5) 
            info = r.json()
            self.deviceList = info['data']['devices']
            logging.debug('self.deviceList : {}'.format(self.deviceList))
        except Exception as e:
            logging.error('Exception  -  retrieve_device_list : {}'.format(e))             

    #@measure_time
    def retrieve_homeID(self):
        try:
            data= {}
            data['method'] = 'Home.getGeneralInfo'
            data['time'] = str(int(time.time_ns()/1e6))
            headers1 = {}
            headers1['Content-type'] = 'application/json'
            headers1['Authorization'] = 'Bearer '+ self.token['access_token']

            r = requests.post(self.apiv2URL, data=json.dumps(data), headers=headers1, timeout=5) 
            logging.debug('Obtaining  homeID : {}'.format(r.ok))
            if r.ok:
                homeId = r.json()
                self.homeID = homeId['data']['id']
            else:
                self.homeID = None
                logging.error('Failed ot obtain HomeID')
        except Exception as e:
            logging.error('Exception  - retrieve_homeID: {}'.format(e))    

    #@measure_time
    #def getDeviceList (self):
    #    return(self.deviceList)

    #@measure_time
    def shut_down(self):
        try:
            self.disconnect = True
            if self.client:
                self.STOP.set()
                self.client.disconnect()
                self.client.loop_stop()
        except Exception as E:
            logging.error('Shut down exception {}'.format(E))
            
    ########################################
    # MQTT stuff
    ########################################

    #@measure_time
    def connect_to_broker(self):
        """
        Connect to MQTT broker
        """
        try: 
            logging.info("Connecting to broker...")
            #if self.mode in ['cloud']:
                #self.mqtt_str = 'yl-home/'
            while not self.refresh_token():
                time.sleep(35) # Wait 35 sec and try again (35sec ensure less than 10 attempts in 5 min)
                logging.info('Trying to obtain new Token - Network/YoLink connection may be down')
            logging.info('Retrieving YoLink API info')

            #self.retrieve_device_list()
            #self.retrieve_homeID()
            time.sleep(1)
            logging.debug(f'info: {self.mqttURL} {self.mqttPort} {self.keepAlive} {self.token}')
            if self.mode == 'cloud': 
                logging.debug('cloud : {}'.format(self.token['access_token']))
                self.client.username_pw_set(username=self.token['access_token'], password=None)
            elif self.mode == 'local':
                logging.debug(f'local ; {self.local_client_id} {self.local_client_secret}')
                self.client.username_pw_set(username=self.local_client_id, password=self.local_client_secret)

            logging.debug(f'info: {self.mqttURL} {self.mqttPort} {self.keepAlive} {self.token}')
            temp = self.client.connect('https://'+self.mqttURL, self.mqttPort, keepalive= self.keepAlive)
            logging.debug(f'self.client.connect: {temp}' )               
            self.client.loop_start()
            time.sleep(2)
            self.connectedToBroker = True   
            self.loopRunning = True 
            logging.debug(self.mqttList)
            for deviceId in self.mqttList:
                self.update_mqtt_subscription(deviceId)
                logging.debug('Updating {} in mqttList'.format(deviceId))
            #self.client.will_set()
            #if self.mode in ['local']:

            return(True)

        except Exception as e:
            logging.error('Exception  - connect_to_broker: {}'.format(e))
            #if self.token == None:
            #    self.request_new_token()
            #else:
            #    self.refresh_token()
            return(False)

    #@measure_time
    def subscribe_mqtt(self, deviceId, callback):
        logging.info('Subscribing deviceId {} to MQTT'.format(deviceId))
        topicReq = self.mqtt_str +self.homeID+'/'+ deviceId +'/request'
        topicResp = self.mqtt_str +self.homeID+'/'+ deviceId +'/response'
        topicReport = self.mqtt_str+ self.homeID+'/'+ deviceId +'/report'
        #topicReportAll = 'yl-home/'+self.homeID+'/+/report'
        
        if not deviceId in self.mqttList :
            self.client.subscribe(topicReq, self.QoS)
            self.client.subscribe(topicResp, self.QoS)
            self.client.subscribe(topicReport,  self.QoS)

            self.mqttList[deviceId] = { 'callback': callback, 
                                            'request': topicReq,
                                            'response': topicResp,
                                            'report': topicReport,
                                            'subscribed': True
                                            }
            time.sleep(1)

    #@measure_time
    def update_mqtt_subscription (self, deviceId):
        logging.info('update_mqtt_subscription {} '.format(deviceId))
        topicReq = self.mqtt_str +self.homeID+'/'+ deviceId +'/request'
        topicResp = self.mqtt_str +self.homeID+'/'+ deviceId +'/response'
        topicReport = self.mqtt_str +self.homeID+'/'+ deviceId +'/report'
        #topicReportAll = self.mqtt_str +self.homeID+'/+/report'
        
        if  deviceId in self.mqttList :
            logging.debug('unsubscribe {}'.format(deviceId))
            self.client.unsubscribe(self.mqttList[deviceId]['request'] )
            self.client.unsubscribe(self.mqttList[deviceId]['response'] )
            self.client.unsubscribe(self.mqttList[deviceId]['report'] )
            
            logging.debug('re-subscribe {}'.format(deviceId))
            self.client.subscribe(topicReq, self.QoS)
            self.client.subscribe(topicResp, self.QoS)
            self.client.subscribe(topicReport, self.QoS)
            self.mqttList[deviceId]['request'] =  topicReq
            self.mqttList[deviceId]['response'] = topicResp
            self.mqttList[deviceId]['report'] = topicReport
        #logging.debug('mqtt.list:{}.'.format(self.mqttList))

    #@measure_time
    def process_message(self):
        try:
            #self.messageLock.acquire()
            msg = self.messageQueue.get(timeout = 10) 
            logging.debug('Received message - Q size={}'.format(self.messageQueue.qsize()))
            payload = json.loads(msg.payload.decode("utf-8"))
            logging.debug('process_message : {}'.format(payload))
            
            deviceId = 'unknown'
            if 'targetDevice' in payload:
                deviceId = payload['targetDevice']
            elif 'deviceId' in payload:
                deviceId = payload['deviceId']
            else:
                logging.debug('Unknow device in payload : {}'.format(payload))

            logging.debug('process_message for {}: {} {}'.format(deviceId, payload, msg.topic))
            

            if deviceId in self.mqttList:

                tempCallback = self.mqttList[deviceId]['callback']
                
                #if payload['msgid'] in self.pendingDict:
                #    self.pendingDict.pop(payload['msgid'] )
                if  msg.topic == self.mqttList[deviceId]['report']: 
                    logging.debug('porcessing report: {}'.format(payload))                   
                    tempCallback(payload)
                    if self.debug:
                            fileData= {}
                            fileData['type'] = 'EVENT'
                            fileData['data'] = payload 
                            self.fileQueue.put(fileData)
                            event_fileThread = Thread(target = self.save_packet_info )
                            event_fileThread.start()
                            logging.debug('event_fileThread - starting')


                elif msg.topic == self.mqttList[deviceId]['response']:
                    logging.debug('porcessing response: {}'.format(payload))                   

                    if payload['code'] == '000000':
                        tempCallback(payload)
                    else:
                        logging.error('Non-000000 code {} : {}'.format(payload['desc'], str(json.dumps(payload))))
                        tempCallback(payload)
                    if self.debug:
                        fileData= {}
                        fileData['type'] = 'RESP'
                        fileData['data'] = payload 
                        self.fileQueue.put(fileData)
                        resp_fileThread = Thread(target = self.save_packet_info )
                        resp_fileThread.start()
                        logging.debug('resp_fileThread - starting')
                        
                elif msg.topic == self.mqttList[deviceId]['request']:
                    logging.debug('porcessing request - no action: {}'.format(payload))                   
                    #transmitted message
                    if self.debug:
                        fileData= {}
                        fileData['type'] = 'REQ'
                        fileData['data'] = payload
                        self.fileQueue.put(fileData)
                        req_fileThread = Thread(target = self.save_packet_info )
                        req_fileThread.start()
                        logging.debug('req_fileThread - starting')

                else:
                    logging.error('Topic not mathing:' + msg.topic + '  ' + str(json.dumps(payload)))
                    if self.debug:
                        fileData= {}
                        fileData['type'] = 'MISC'
                        fileData['data'] = payload
                        self.fileQueue.put(fileData)   
                        misc_fileThread = Thread(target = self.save_packet_info )
                        misc_fileThread.start()
                        logging.debug('misc_fileThread - starting')                                     
            else:
                logging.error('Unsupported device: {}'.format(deviceId))
            #self.messageLock.release()

        except Exception as e:
            logging.debug('message processing timeout - no new commands') 
            pass
            #self.messageLock.release()

    #@measure_time
    def on_message(self, client, userdata, msg):
        """
        Callback for broker published events
        """
        logging.debug('on_message: {}'.format(json.loads(msg.payload.decode("utf-8"))) )
        self.messageQueue.put(msg)
        qsize = self.messageQueue.qsize()
        logging.debug('Message received and put in queue (size : {})'.format(qsize))
        logging.debug('Creating threads to handle the received messages')
        threads = []
        for idx in range(0, qsize):
            threads.append(Thread(target = self.process_message ))
        [t.start() for t in threads]
        #[t.join() for t in threads]
        logging.debug('{} on_message threads starting'.format(qsize))

    #def obtain_connection (self):
    #    if not self.connectedToBroker:    
    #        self.client.disconnect()          
    #        logging.debug('Waiting to (re)establish connection to broker')
    #        self.client.connect(self.mqttURL, self.mqttPort, keepalive= 30) # ping server every 30 sec                    
    #        time.sleep(5)

    #@measure_time
    def on_connect(self, client, userdata, flags, rc):
        """
        Callback for connection to broker
        """
        netstate = []
        try:
            logging.debug('on_connect - Connected with result code {}'.format(rc))
            #// Possible values for client.state()
            #define MQTT_CONNECTION_TIMEOUT     -4
            #define MQTT_CONNECTION_LOST        -3
            #define MQTT_CONNECT_FAILED         -2
            #define MQTT_DISCONNECTED           -1
            #define MQTT_CONNECTED               0
            #define MQTT_CONNECT_BAD_PROTOCOL    1
            #define MQTT_CONNECT_BAD_CLIENT_ID   2
            #define MQTT_CONNECT_UNAVAILABLE     3
            #define MQTT_CONNECT_BAD_CREDENTIALS 4
            #define MQTT_CONNECT_UNAUTHORIZED    5

            if (rc == 0):
                self.online = True
                logging.info('Successfully connected to broker {} '.format(self.mqttURL))
                logging.debug('Re-subscribing devices after after disconnect')
                for deviceId in self.mqttList:
                    self.update_mqtt_subscription(deviceId)
                self.connectedToBroker = True

            elif (rc == 2):
                if self.connectedToBroker: # Already connected - need to disconnect before reconnecting
                    logging.error('Authentication error 2 - Token no longer valid - Need to reconnect ')
                    #netid = self.check_connection(self.mqttPort)
                    #logging.debug('netid = {}'.format(netid))
                    self.connectedToBroker = False
                    self.disconnect = True
                    self.client.disconnect()
                    self.refresh_token()
                    time.sleep(2)
                    self.connect_to_broker()

            elif (rc >= 4):
                if self.connectedToBroker: # Already connected - need to disconnect before reconnecting
                    logging.error('Authentication error {rc} - Token no longer valid - Need to reconnect ')
                    netid = self.check_connection(self.mqttPort)
                    logging.debug('netid = {}'.format(netid))

                    if None == netid: # no communication to brooker possible 
                        self.connectedToBroker = False
                        self.disconnect = True
                        self.client.disconnect()
                        time.sleep(2)
                        self.token = None
                        self.connect_to_broker()
                    else: # still connected - needs new token - disconnect should automatically reconnect
                        self.connectedToBroker = False
                        self.client.disconnect()

                else:
                    logging.error('Authentication error {rc}} - check credentials and try again  ')




            else:
                logging.error('Broker connection failed with result code {}'.format(rc))
                self.client.disconnect()
                self.connectedToBroker = False
                self.online = False
         
        except Exception as e:
            logging.error('Exception  -  on_connect: ' + str(e))       

    #@measure_time
    def on_disconnect(self, client, userdata,rc=0):
        logging.debug('Disconnect - stop loop')
        #self.connectedToBroker = False
        self.disconnect_occured = True
        if self.disconnect:
            logging.debug('Disconnect - stop loop')
            self.client.loop_stop()
            
        else:
            logging.error('Unintentional disconnect - Reacquiring connection')

            try:
                netid = self.check_connection(self.mqttPort)
                if None == netid:      
                    self.connectedToBroker = False
                elif netid.status.__contains__('ESTABLISHED'):
                    self.connectedToBroker = True
                else:
                    self.connectedToBroker = False
                logging.debug('on_disconnect - connectedToBroker = {}'.format(self.connectedToBroker))
                if not self.connectedToBroker:

                    logging.debug('on_disconnect - restarting broker')
                    #self.client.loop_stop() 
                    self.client.disconnect()  # seems it is needed to disconnect to not have API add up connections
                    time.sleep(2) 
                    self.token = None
                    self.connect_to_broker()
                    self.connectedToBroker = True
                self.online = False
                #time.sleep(3)   


            except Exception as e:
                logging.error('Exeption occcured during on_ disconnect : {}'.format(e))
                if self:
                    self.refresh_token()
                else:
                    logging.error('Lost credential info - need to restart node server')

    #@measure_time
    def on_subscribe(self, client, userdata, mID, granted_QoS):        
        logging.debug('on_subscribe')
        #logging.debug('client = ' + str(client))
        #logging.debug('userdata = ' + str(userdata))
        #logging.debug('mID = '+str(mID))
        #logging.debug('Granted QoS: ' +  str(granted_QoS))
        #logging.debug('\n')

    #@measure_time
    def on_publish(self, client, userdata, mID):
        logging.debug('on_publish')
        #logging.debug('client = ' + str(client))
        #logging.debug('userdata = ' + str(userdata))
        #logging.debug('mID = '+str(mID))
        #logging.debug('\n')


    #@measure_time
    def publish_data(self, data):
        logging.debug( 'Publish Data to Queue: {}'.format(data))
        while not self.connectedToBroker:
            logging.debug('Connection to Broker not established - waiting')
            time.sleep(1)       

        self.publishQueue.put(data, timeout = 5)
        
        publishThread = Thread(target = self.transfer_data )
        publishThread.start()
        logging.debug('publishThread - starting')
        return(True)

    #@measure_time
    def set_api_limits(self, api_calls, api_dev_calls):
        ''''''
        self.nbr_api_calls = api_calls
        self.nbr_api_dev_calls = api_dev_calls


    #@measure_time
    def time_tracking(self, dev_id):
        '''time_track_publish'''
        ''' make 100 overall calls per 5 min and 6 per dev per min and 200ms between calls'''
        try:
            self.TimeTableLock.acquire()
            if dev_id not in self.time_tracking_dict:
                self.time_tracking_dict[dev_id] = []
                #logging.debug('Adding timetrack for {}'.format(dev_id))            
            t_now = int(time.time_ns()/1e6)
            logging.debug('time_track_going in: {}, {}, {}'.format(t_now, dev_id, self.time_tracking_dict))
            max_dev_id = 5 # commands per dev_time_limit to same dev (add margin)
            max_dev_all = 99 # commands per call_time_limit to same dev (add margin)
            dev_time_limit = 60000 # 1 min =  60 sec = 60000 ms
            call_time_limit = 300000 # 5min = 300 sec = 300000 ms
            dev_to_dev_limit = 200 # min 200ms between calls to same dev
            total_dev_calls = 0
            total_dev_id_calls = 0
            t_oldest = t_now
            t_oldest_dev = t_now
            t_previous_dev = 0
            t_dev_2_dev = 0
            t_call = t_now

            #t_now = int(time.time_ns()/1e6)
            #logging.debug('time_tracking 0 - {}'.format(self.time_tracking_dict))
            discard_list = {}
            #remove data older than time_limit
            for dev in self.time_tracking_dict:
                for call_nbr  in range(0,len(self.time_tracking_dict[dev])):
                    t_call = self.time_tracking_dict[dev][call_nbr]
                    if t_call  < (t_now - call_time_limit): # more than 1 min ago
                        discard_list[t_call] = dev
            for tim in discard_list:
                self.time_tracking_dict[discard_list[tim]].remove(tim)
            # find oldest data in dict and for devices of the dev
            #logging.debug('time_track AFTER >1MIN REMOVAL: {}'.format(self.time_tracking_dict))
            for dev in self.time_tracking_dict:
                #logging.debug('time_tracking 1 - {} - {}'.format(dev, len(self.time_tracking_dict[dev])))
                for call_nbr  in range(0,len(self.time_tracking_dict[dev])):
                    #logging.debug('time_tracking 1.5 - {}'.format(t_call))
                    total_dev_calls = total_dev_calls + 1
                    t_call = self.time_tracking_dict[dev][call_nbr]
                    #logging.debug('Loop info : {} - {} - {} '.format(dev, call_nbr, (t_now - t_call)))  
                    if t_call < t_oldest:
                        t_oldest = t_call
                    #logging.debug('After cleanup {} {} {} - {}'.format(t_call, t_oldest, t_old_dev_tmp, self.time_tracking_dict ))
                    #logging.debug('devs {} {} {}'.format(dev==dev_id, dev, dev_id))
                    if dev == dev_id: # check if max_dev_id is in play
                        if t_call >= (t_now - dev_time_limit): # call is less than 1 min old
                            total_dev_id_calls = total_dev_id_calls + 1
                            #logging.debug('time_tracking2 - dev found')
                            #self.time_tracking_dict[dev].append(t_now)
                            if t_call < t_oldest_dev: # only test for selected dev_id
                                t_oldest_dev = t_call
                            if t_call > t_previous_dev:
                                t_previous_dev = t_call

            if total_dev_calls <= max_dev_all:
                t_all_delay = 0
            else:
                t_all_delay = call_time_limit - (t_now - t_oldest )
            
            if (t_now - t_previous_dev) <= dev_to_dev_limit:
                #time.sleep((dev_to_dev_limit + 10 -(t_now - t_previous_dev))/1000) # calls to same device must be min dev_to_dev_limit (200ms) apart
                #t_dev_2_dev = dev_to_dev_limit) # sleep 200ms + 100 ms margin - Seems calculating the limit is not accurate enough
                t_dev_2_dev = (dev_to_dev_limit + 100 -(t_now - t_previous_dev))
                #logging.debug('Sleeping {}ms due to too close dev calls '.format(t_now - t_previous_dev))
                #logging.debug('Sleeping {}s due to too close dev calls '.format(dev_to_dev_limit/1000))
            if total_dev_id_calls <= max_dev_id:
                t_dev_delay = 0
            else:
                t_dev_delay = dev_time_limit  - (t_now- t_oldest_dev)
            #logging.debug('total_calls = {}, total_dev_calls = {}'.format(total_dev_calls, total_dev_id_calls))
            t_delay = max(t_all_delay,t_dev_delay, t_dev_2_dev, 0 )
            logging.debug('Adding {} delay to t_now {}  =  {} to TimeTrack - dev delay={}, all_delay={}, dev2dev={}'.format(t_delay, t_now, t_now + t_delay, t_dev_delay, t_all_delay, t_dev_2_dev))
            self.time_tracking_dict[dev_id].append(t_now + t_delay)
            self.TimeTableLock.release()
            logging.debug('TimeTrack after: time {} dev: {} delay: {} -  {}'.format(t_now, dev_id, int(math.ceil(t_delay/1000)), self.time_tracking_dict))
            return(int(math.ceil(t_delay/1000)))
            #return(int(math.ceil(t_delay/1000)), int(math.ceil(t_all_delay)), int(math.ceil(t_all_delay)))
        except Exception as e:
            logging.debug(' Exception Timetrack : {}'.format(e))
            self.TimeTableLock.release()
        #self.time_tracking_dict[dev_id].append(time)

    #@measure_time
    def transfer_data(self):
        '''transfer_data'''
        self.lastTransferTime = int(time.time())
        
        try:
            data = self.publishQueue.get(timeout = 10)

            deviceId = data['targetDevice']

            #logging.debug('mqttList : {}'.format(self.mqttList))
            if deviceId in self.mqttList:
                logging.debug( 'Starting publish_data:')
                ### check if publish list is full
                
                #all_delay, dev_delay =  self.time_tracking(timeNow_ms, deviceId)
                delay_s =  self.time_tracking(deviceId)
                #logging.debug( 'Needed delay: {} - {}'.format(delay, timeNow_s))
                if delay_s > 0: # some delay needed
                    logging.info('Delaying call by {}sec due to too many calls'.format(delay_s))
                    time.sleep(delay_s)
                    # As this is multi threaded we can just sleep  - if another call is ready and can go though is will so in a differnt thread    
                data['time'] = str(int(time.time_ns()/1e6))  # update time to actual packet time (to include delays)
                dataStr = str(json.dumps(data))
                self.tmpData[deviceId] = dataStr
                self.lastDataPacket[deviceId] = data

                logging.debug( 'publish_data: {} - {}'.format(self.mqttList[deviceId]['request'], dataStr))
                result = self.client.publish(self.mqttList[deviceId]['request'], dataStr, self.QoS)
            else:
                logging.error('device {} not in mqtt list'.format(deviceId))
                return (False)
            
            if result.rc != 0:
                logging.error('Error {} during publishing {}'.format(result.rc, data))
                #errorCount = errorCount + 1
                if result.rc == 3: #off line
                    logging.debug('rc = {}'.format(result.rc))
                    self.online = False
                if result.rc == 4 : #off line
                    logging.debug('rc = {}'.format(result.rc))
                    self.online = False
                    self.client.reconnect() # is this the right strategy 
            else:
                self.lastTransferTime = int(time.time())
                self.online = True
        except Exception as e:
            pass # go wait again unless stop is called

    #@measure_time
    def save_packet_info(self):
        self.fileLock.acquire()
        try:
            data = self.fileQueue.get(timeout = 10)
            if 'targetDevice' in data['data']:
                deviceId = data['data']['targetDevice']
            elif 'deviceId' in data['data']:
                deviceId = data['data']['deviceId']
            if data['type'].upper() == 'REQ':
                f = open('TXpackets.txt', 'a')
            elif data['type'].upper() == 'RESP':
                f = open('RXpackets.txt', 'a')
            elif data['type'].upper() == 'EVENT':  
                f = open('EVENTpackets.txt', 'a')
            else:
                f = open('MISCpackets.txt', 'a')
            #jsonStr  = json.dumps(dataTemp, sort_keys=True, indent=4, separators=(',', ': '))
            f.write('{} - {}:  '.format( datetime.now(),deviceId))
            f.write(str(json.dumps(data['data'])))
            f.write('\n\n')
            #json.dump(jsonStr, f)
            f.close()
            time.sleep(0.2)
        
        except Exception as e:
            # logging.debug('File Queue looping {}'.format(e))
            pass # go wait again unless stop is called
        self.fileLock.release()

    #@measure_time
    def system_online(self):
        return(self.online)


################
#   Misc stuff
###############
    #@measure_time
    def set_temp_unit(self, unit):
        self.temp_unit = unit

    def get_temp_unit(self):
        return(self.temp_unit)

    def set_debug(self, debug):
        self.debug = debug


class YoLinkInitCSID(object):
    def __init__(self,  csName, csid, csSeckey, yoAccess_URL ='https://api.yosmart.com/openApi' , mqtt_URL= 'api.yosmart.com', mqtt_port = 8003 ):
        self.csName = csName
        self.csid = csid
        self.cssSeckey = csSeckey
        self.apiType = 'CSID'