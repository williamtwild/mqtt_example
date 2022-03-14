#!/usr/bin/python
# -*- coding: utf-8 -*-
import sys
import uuid
# set to your paho path
sys.path.append('../sprinkler')
import paho.mqtt.client as mqttClient
import json
import re
import os
import ConfigParser
import httplib
import urllib
import argparse
import requests
import csv
from time import time
from time import sleep
from time import strftime
from datetime import datetime


class MQTTWorker:

    subscribe_data_handler = None
    data_stream_subscription_topic = 'stub/#'

    broker_address= None
    broker_port = 0
    broker_user = None
    broker_password = None

    def __init__(self, worker_version = 'not-set', worker_name = 'not-set', admin_email_api_url = '127.0.0.1:5000'):

        self.library_version = '22.03.02.001' 
        
        self.log_to_file_enabled    = False
        self.supress_email          = True
        self.test_mode              = False
        #
        #
        #

        self.admin_email_api_url = admin_email_api_url
    
        self.status_interval_seconds = 900
        self.status_time_reference = time()

        if worker_name == 'not-set':
            self.core_worker_name = sys.argv[0][:-3] 
        else:
            self.core_worker_name = worker_name
        if self.test_mode == True:
            self.core_worker_name = '%s_test' % self.core_worker_name

        self.worker_token = str(uuid.uuid4().hex) #tr(uuid.uuid1())

        self.worker_version = worker_version
    
        self.kill_signal = False
        self.death_announced = False
        self._kill_kill_file()
        self.kill_interval_seconds = 10
        self.kill_time_reference = time()

        # this is the only place where explicit setting of this var should be used
        # use the _update_role_status function everywhere else to make sure that the 
        # worker information dictionary gets updated
        self.role_status  = 'undetermined'

        self.worker_exists_reply_received = False


        self.worker_information_dictionary = {
            "core_worker_name" : self.core_worker_name,
            "worker_token" : self.worker_token,
            "library_version" : self.library_version,
            "test_mode": self.test_mode,
            "admin_email_api_url":self.admin_email_api_url,
            "birthday" : int(time()),
            "kill_interval_seconds" : self.kill_interval_seconds,
            "status_interval_seconds" : self.status_interval_seconds,
            "role_status" : self.role_status,
            "worker_description" : "none",
            "worker_version" : self.worker_version,
            "last_error_hint" : 'none',
            "last_error_description" : 'none',
            "last_error_time_stamp": 'na',
            "error_count": 0 ,
            "perpetual_error_count": 0 
        }

        self._banner()

        
    #----------------------------------------------------------------------------------
    #                                                                                 -
    #                                                                                 -
    # setters                                                                         -
    #                                                                                 -
    #                                                                                 -
    #----------------------------------------------------------------------------------

    def set_worker_description(self, set_to_this):
        self.worker_information_dictionary['worker_description']  = set_to_this

    #----------------------------------------------------------------------------------
    #                                                                                 -
    #                                                                                 -
    # methods that must be called when instantiating                                                                         -
    #                                                                                 -
    #                                                                                 -
    #----------------------------------------------------------------------------------

    def start_connections(self):
        self.worker_stream_client = None
        self.worker_stream_connected = False
        self._worker_stream_client()
        self.publish_client = None
        self.publish_client_connected = False   
        self._publish_client()
        self.subscribe_client = None
        self.subscribe_client_connected = False   
        self._subscribe_client()
        self._log_class_events('Waiting for connections to settle')
        sleep(2)
        self._send_birth()

    def tick(self):
        # must be called in the main loop of the worker
        current_time = time()

        if current_time - self.status_time_reference >= self.status_interval_seconds:
            self.status_time_reference = time()
            if self.role_status  != 'active':
                self._log_class_events('Sending of status skipped. Worker not in active role')
            else:
                self._send_status()
           
        if current_time - self.kill_time_reference >= self.kill_interval_seconds:
            self.kill_time_reference = time()
            self._check_for_kill_file()
         
    #----------------------------------------------------------------------------------
    #                                                                                 -
    #                                                                                 -
    # file methods                                                          -
    #                                                                                 -
    #                                                                                 -
    #----------------------------------------------------------------------------------

    def _check_for_kill_file(self):
        kill_file_name = '%s.kill' % self.core_worker_name
        if os.path.isfile('./%s' % kill_file_name):
            self._log_class_events('Kill file found. Setting kill signal')
            self.kill_signal = True

    def _kill_kill_file(self):
        kill_file_name = '%s.kill' % self.core_worker_name
        if os.path.isfile('./%s' % kill_file_name):
            os.remove('./%s' % kill_file_name)


    #----------------------------------------------------------------------------------
    #                                                                                 -
    #                                                                                 -
    # logging type functipons                                                         -
    #                                                                                 -
    #                                                                                 -
    #----------------------------------------------------------------------------------

    def log_error(self,data_to_log):
        log_this = '%s - (%s:%s) [E+] %s' % (strftime("%Y-%m-%d %H:%M:%S"), self.core_worker_name,  self.worker_token[:3], data_to_log)
        print log_this
        hlog_file = open('%s_error.log' % self.core_worker_name, 'a')
        print >>hlog_file, log_this
        hlog_file.close()
    
    def log_events(self,data_to_log, same_line=0, log_to_file_only=0, log_to_screen_only=0):
        if self.log_to_file_enabled == 1:
            log_to_file_marker = '+'
        else:
            log_to_file_marker = ''
        log_this = '%s - (%s:%s) [W%s] %s' % (strftime("%Y-%m-%d %H:%M:%S"), self.core_worker_name,  self.worker_token[:3], log_to_file_marker, data_to_log)
        if log_to_file_only == 0:
            if same_line == 0:
                print log_this #[0:140]
            else:
                print '%s\r' % log_this[0:140],
        if log_to_screen_only == 0:
            self._write_log(log_this)

    def send_admin_email(self, email_subject ):

        if self.supress_email:
            self._log_class_events('Email supressed - %s' % email_subject)
            return
        if self.test_mode:
            self._log_class_events('Email disabled in test mode - %s' % email_subject)
            return
        self._log_class_events('Sending email: %s' % email_subject)
        email_body = 'Worker Information:\n'
        for key, value in self.worker_information_dictionary.iteritems():
            email_body = '%s%s : %s <br> \n' % (email_body, key, value)
        try:
            url = 'http://%s/send_admin_email?subject=%s&body=%s' % ( self.admin_email_api_url, email_subject, email_body )
            send_admin_response = requests.get(url)
            self._log_class_events( send_admin_response)
        except Exception, email_error_description:
            self.log_error( email_error_description)


    def _banner(self):
        self._log_class_events(' ')
        self._log_class_events('~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~')
        self._log_class_events('         %s' % self.core_worker_name)
        self._log_class_events('Version                  : %s' % self.worker_version)
        self._log_class_events('~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~')
        self._log_class_events('Sprinkler Version        : %s' % self.library_version)
        self._log_class_events('Kill interval seconds    : %s' % self.kill_interval_seconds)
        self._log_class_events('Status interval seconds  : %s' % self.status_interval_seconds)
        self._log_class_events('Log to file enabled      : %s' % self.log_to_file_enabled )
        self._log_class_events('Supress email            : %s' % self.supress_email )
        self._log_class_events('Test mode                : %s' % self.test_mode )
        self._log_class_events('EMail API                : %s' % self.admin_email_api_url )

        self._log_class_events('~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~')
        self._log_class_events(' ')

    def _log_class_events(self, data_to_log, same_line=0, log_to_file_only=0, log_to_screen_only=0):
        if self.log_to_file_enabled == 1:
            log_to_file_marker = '+'
        else:
            log_to_file_marker = ''
        log_this = '%s - (%s:%s) [C%s] %s' % (strftime("%Y-%m-%d %H:%M:%S"), self.core_worker_name, self.worker_token[:3], log_to_file_marker, data_to_log)
        if log_to_file_only == 0:
            if same_line == 0:
                print log_this #[0:140]
            else:
                print '%s\r' % log_this[0:140],
        if log_to_screen_only == 0:
            self._write_log(log_this)
        
    def _write_log(self, log_this):
        if self.log_to_file_enabled == 1:
            hlog_file = open('%s_%s_event.log' % (self.core_worker_name, self.worker_token), 'a')
            print >>hlog_file, log_this
            hlog_file.close()
                
    #----------------------------------------------------------------------------------
    #                                                                                 -
    #                                                                                 -
    # misc functipons                                                                 -
    #                                                                                 -
    #                                                                                 -
    #----------------------------------------------------------------------------------

    def _update_role_status(self, new_role_status):
        self.role_status = new_role_status
        self.worker_information_dictionary['role_status'] = new_role_status

    #----------------------------------------------------------------------------------
    #                                                                                 -
    #                                                                                 -
    # mqtt worker broadcasts                                                          -
    #                                                                                 -
    #                                                                                 -
    #----------------------------------------------------------------------------------

    def send_death(self, death_type):
        self.send_admin_email('Death')
        self.publish_client.publish("workers/%s/%s/%s/death/%s" % ( self.core_worker_name, self.worker_token, self.role_status, death_type ),json.dumps( self.worker_information_dictionary ), qos=1)
        while self.death_announced == False:
            sleep(1)
        self._log_class_events('Death announced')

    def send_error(self, error_hint, error_description) :
        last_error_hint = self.worker_information_dictionary['last_error_hint']
        error_count = int(self.worker_information_dictionary['error_count'])
        perpetual_error_count = int(self.worker_information_dictionary['perpetual_error_count'])
        error_count +=1
        perpetual_error_count +=1
        self.worker_information_dictionary['perpetual_error_count'] = perpetual_error_count

        error_hint  = re.sub('[^0-9a-zA-Z]+', '_', str(error_hint))
        error_hint  = error_hint.lower()
        error_description = re.sub('[^0-9a-zA-Z]+', '_', str(error_description))
        error_description = error_description.lower()

        # if this is the same error as the previous error then 
        # do not send every time
        if error_hint == last_error_hint:
            self.worker_information_dictionary['error_count'] = error_count
            self.worker_information_dictionary['last_error_time_stamp'] = time()
            # seems silly to check since if it was the same as the last 
            # is would HAVE to be greater than 1 
            if error_count > 1:
                if self.worker_information_dictionary['error_count'] % 10 !=0:
                    return
        else:
            self.worker_information_dictionary['error_count'] = 1
    
        self.log_error('Error %s:%s' % (error_hint ,error_description))
        self.worker_information_dictionary['last_error_description'] = error_description
        self.worker_information_dictionary['last_error_hint'] = error_hint
        self.worker_information_dictionary['last_error_time_stamp'] = time()
        self.publish_client.publish("workers/%s/%s/%s/error/%s" % ( self.core_worker_name, self.worker_token, self.role_status, error_hint ),json.dumps( self.worker_information_dictionary ), qos=1)
        self.send_admin_email('Worker Error')


    def _send_status(self, comment = '', suppress_post_to_web = True ):
        self.publish_client.publish("workers/%s/%s/%s/status/ok" % ( self.core_worker_name, self.worker_token, self.role_status ),json.dumps( self.worker_information_dictionary ), qos=1)
        # post to web hook removed

    def _send_pong(self):
        self.publish_client.publish("workers/%s/%/%s/pong/ok" % ( self.core_worker_name, self.worker_token, self.role_status ),json.dumps( self.worker_information_dictionary ), qos=1)

    def _send_birth(self):
        self.send_admin_email('Birth') 
        self.publish_client.publish("workers/%s/%s/%s/birth/ok" % ( self.core_worker_name, self.worker_token, self.role_status ),json.dumps( self.worker_information_dictionary ), qos=1)
        start_time_for_role_status = time()
        # set to zero seconds to immedialley trigger
        role_status_interval_seconds = 0
        self._log_class_events('Role discovery') 
        # setting this to 100 so thjat the first check will always 
        # trigger a promotion
        no_active_workers_found_count = 100 
        while self.role_status  != 'active':
            
            sleep(1)
            self.tick()
            if self.kill_signal:
                break
            current_time_for_role_status = time()
            if current_time_for_role_status - start_time_for_role_status >= role_status_interval_seconds:
                start_time_for_role_status = current_time_for_role_status
                # set to the actual value
                role_status_interval_seconds = 180
                # reset the flag - if first run will already be false
                self.worker_exists_reply_received = False
                # broadcast the discovery twice
                self._log_class_events('Broadcasting discovery request')
                self.publish_client.publish("workers/%s/%s/%s/request/does_worker_exist" % ( self.core_worker_name, self.worker_token, self.role_status ),json.dumps( self.worker_information_dictionary), qos=1)
                sleep(6)
                self._log_class_events('Discovery : %s' % self.worker_exists_reply_received)
                if self.worker_exists_reply_received:
                    self._log_class_events('Worker in standby') 
                    self._update_role_status('standby')
                    no_active_workers_found_count = 0 
                    
                else:
                    no_active_workers_found_count += 1
                    if no_active_workers_found_count > 3:
                        self._log_class_events('Promoting worker to active') 
                        self.send_admin_email('Promoting to Active')
                        self.publish_client.publish("workers/%s/%s/%s/role_status_change/to_active" % ( self.core_worker_name, self.worker_token, self.role_status ),json.dumps( self.worker_information_dictionary), qos=1)
                        self._update_role_status('active')
                        self._send_status( 'promote to active')
                    else:
                        self._log_class_events('No active worker found. Current count %s.'% no_active_workers_found_count) 

  
    
    #----------------------------------------------------------------------------------
    #                                                                                 -
    #                                                                                 -
    # subscribe                                                                       -
    #                                                                                 -
    #                                                                                 -
    #----------------------------------------------------------------------------------
    def _subscribe_client(self):
        self.subscribe_broker_connected = False   
        self.subscribe_client = mqttClient.Client("%s_subscribe_%s" % ( self.core_worker_name, self.worker_token ), clean_session=True)           
        self.subscribe_client.username_pw_set( self.broker_user , password = self.broker_password )    
        self.subscribe_client.on_connect = self._on_connect_subscribe                      
        self.subscribe_client.on_message = self._subscribe_on_message
        self.subscribe_client.on_subscribe = self._subscribe_show_qos
        self.subscribe_client.connect(self.broker_address, port = self.broker_port)          
        self.subscribe_client.loop_start()        
        while self.subscribe_broker_connected != True:    
            self.tick()
            if self.kill_signal:
                break
            sleep(0.1)

    def _subscribe_show_qos(self, client, userdata, mid, granted_qos):
        self._log_class_events('QOS  %s' % granted_qos)

    def _subscribe_on_message(self, client, userdata, incoming_data):
        if self.role_status != 'active' or self.kill_signal:   
            return
        self.subscribe_data_handler(client, userdata, incoming_data)

    def _on_connect_subscribe(self, client, userdata, flags, rc):
        if rc == 0: 
            self._log_class_events("Connected to main data stream")
            for topic in self.data_stream_subscription_topic:
                self._log_class_events('Subscribing to %s' % topic)
                self.subscribe_client.subscribe(topic, 1)  
            self.subscribe_broker_connected = True               
        else:
            self._log_class_events(rc)
            self.log_error("Subscribe connection failed")
            exit()

    #----------------------------------------------------------------------------------
    #                                                                                 -
    #                                                                                 -
    # publish                                                                         -
    #                                                                                 -
    #                                                                                 -
    #----------------------------------------------------------------------------------
    def _publish_client(self):
        self.publish_client = mqttClient.Client("%s_publish_%s" % ( self.core_worker_name, self.worker_token ),  clean_session=True)               
        self.publish_client.username_pw_set( self.broker_user , password=  self.broker_password)    
        self.publish_client.on_connect= self._on_connect_publish                      
        self.publish_client.connect(self.broker_address, port = self.broker_port)          
        self.publish_client.loop_start()        
        while self.publish_client_connected != True:    
            self.tick()
            if self.kill_signal:
                break
            sleep(0.1)

    def _on_connect_publish(self, client, userdata, flags, rc):
        if rc == 0: 
            self._log_class_events("Connected publish stream")            
            self.publish_client_connected = True                
        else:
            self._log_class_events(rc)
            self.log_error("Publish stream connection failed")
            exit()

    #----------------------------------------------------------------------------------
    #                                                                                 -
    #                                                                                 -
    # worker stream                                                                   -
    #                                                                                 -
    #                                                                                 -
    #----------------------------------------------------------------------------------
    def _worker_stream_client(self):
        self.worker_stream_client = mqttClient.Client("%s_worker_%s" % ( self.core_worker_name, self.worker_token ),  clean_session=True)    
        self.worker_stream_client.username_pw_set( self.broker_user , password = self.broker_password)    
        self.worker_stream_client.on_connect= self._on_worker_stream_connect_subscribe                      
        self.worker_stream_client.on_message = self._process_worker_stream
        self.worker_stream_client.connect(self.broker_address, port=self.broker_port)          
        self.worker_stream_client.loop_start()        
        while self.worker_stream_connected != True:    
            self.tick()
            if self.kill_signal:
                break
            sleep(0.1)

    def _on_worker_stream_connect_subscribe(self,client, userdata, flags, rc):
        if rc == 0: 
            self._log_class_events("Connected to worker stream")
            self.worker_stream_client.subscribe('workers/#', 1)     
            self.worker_stream_connected = True               
        else:
            self._log_class_events(rc)
            self.log_error("Subscribe worker connection failed")
            exit()

    def _process_worker_stream(self, client, userdata, incoming_data):
        full_payload    = incoming_data.payload
        topic           = incoming_data.topic
        topic_section_array = topic.split("/")
        if len(topic_section_array) < 6:
            return
        topic_root              = topic_section_array[0]
        topic_core_worker_name  = topic_section_array[1]
        topic_token             = topic_section_array[2]
        topic_role_status       = topic_section_array[3]
        topic_type_of_broadcast = topic_section_array[4]
        topic_action_or_info    = topic_section_array[5]
        try:
            data = json.loads(full_payload)    
        except:
            return
        # ignore from self except a death notice so that 
        # the worker can use this to make sure that 
        # the death notice went out successfully. 
        # exiting the proces to quickly will sonetimes not 
        # allow the broadcast to go out
        incoming_worker_token = data['worker_token']
        if incoming_worker_token == self.worker_token:
            if topic_type_of_broadcast == 'death':
                self.death_announced = True
            return

        # any boradcasts that are generic and not state dependant
        # should go here 
        if topic_type_of_broadcast == 'ping':
            self._send_pong()
            return
        # catch any broadcasts here and process
        # any that are for me or about me
        if topic_core_worker_name == self.core_worker_name:
            # if it was worker with the same core name 
            # and the role status is active then it was 
            # a response to a discovery 
            if topic_role_status == 'active':
                self.worker_exists_reply_received = True
                return
            # if there is a worker of my type that has an 
            # undetermined role then see if it is checking
            # if the worker exists
            if topic_role_status == 'undetermined' or topic_role_status == 'standby':
                if topic_type_of_broadcast == 'request':
                    if topic_action_or_info == 'does_worker_exist':
                        if self.role_status == 'active':
                            self._send_status('', True)

#----------------------------------------------------------------------------------
#                                                                                 -
#                                                                                 -
#                                                                                 -
#                                                                                 -
#----------------------------------------------------------------------------------
