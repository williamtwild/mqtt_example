#!/usr/bin/python
# -*- coding: utf-8 -*-
#
import sys
import os
import mqtt_leaf
import json
from datetime import datetime
from time import time
from time import sleep


class Example:

    def __init__(self, config ):
        self.version = '22.03.14.003'
        self.config = config

        self.packet_count = 0

        self.worker = mqtt_leaf.MQTTWorker( self.version, config['worker-core']['name'], config['worker-core']['admin_email_api_url'] )
        self.worker.broker_address   = config['broker']['address']
        self.worker.broker_port      = config['broker']['port']
        self.worker.broker_user      = config['broker']['user']
        self.worker.broker_password  = config['broker']['password']
        #
        topic_array = []
        for description, topic in config['broker']['subscriptions'].items():
            self.worker.log_events('Subscribing to : %s - %s' % (description, topic ))
            topic_array.append( str(topic) )
        self.worker.data_stream_subscription_topic = topic_array
        
        self.worker.subscribe_data_handler = self.example_process_data
        self.worker.worker_information_dictionary['description'] = config['worker-core']['description']
        self.worker.start_connections()

        self.periodic_interval_seconds = 10
        self.worker_running = True
        self.main_loop()

    def main_loop(self):
        start_time = time()
        while self.worker_running:
            # optional sleep 
            sleep( 0.1 )
            self.worker.tick()
            self.worker_running = not self.worker.kill_signal
            current_time = time()
            if current_time - start_time >= self.periodic_interval_seconds:
                start_time = current_time
                self.worker.publish_client.publish('ion/worker_triggered_event/packet_count',  self.packet_count, qos=1  )
                self.worker.log_events('Packets processed %s' % self.packet_count )
                self.packet_count = 0 
        self.worker.log_events('Worker exiting')
        self.worker.send_death('loop_exit')

    def example_process_data( self, client, userdata, incoming_data):

        full_payload    = incoming_data.payload
        topic           = incoming_data.topic
        topic_section_array     = topic.split("/")
        topic_root              = topic_section_array[0]
        try:
            data = json.loads(full_payload)
        except:
            return
        self.packet_count  +=1
        dev_eui             = data['header']['common']['serial_number']
        gateway_hostname    = data['header']['common']['gateway_hostname']
        payload_type        = data['header']['common']['payload_type']
        self.worker.log_events( '%s %s' % (dev_eui, topic))



#----------------------------------------------------------------------------------
#                                                                                 -
#                                                                                 -
#----------------------------------------------------------------------------------
#
#

arguments = len(sys.argv) - 1
config_file_name = 'config.json'
if arguments > 0:
    config_file_name = sys.argv[1]    
else:
    config_file_name = 'config.json'

with open( config_file_name ) as json_data_file:
    config = json.load(json_data_file)


print ('')
print ('          ----------------------------------------------------------')
print ('')
print ('                         ION Example Starting')
print ('')
print ('                         %s' % config['worker-core']['name'] )
print ('')
print ('                         %s' % config_file_name )
print ('')
print ('          ----------------------------------------------------------')
print ('')
print ('                          Worker specific settings:')
print ('')
for k,v in config['worker-specific'].items():
    print('                   %s : %s ' % (str(k) ,str(v) ) )
print ('')
print ('          ----------------------------------------------------------')
print ('')
#
#
#----------------------------------------------------------------------------------
#                                                                                 -
#                                                                                 -
#----------------------------------------------------------------------------------
#
#

application_object = globals()[str( config['worker-core']['object-name'])](config)




