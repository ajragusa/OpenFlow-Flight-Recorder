#!/usr/bin/python

import pprint
import sys
import signal
import pika
import os
import pymongo
import json
import time
from collections import Iterable
import xml.etree.ElementTree as xml

DEFAULT_CONFIG_FILE = '/etc/openflow_flight_recorder/config.xml'

BASE_DOC_SIZE = 3600 * 2

class Receiver():
    
    def __init__( self, config_file = DEFAULT_CONFIG_FILE ):
        
        self.config_file = config_file
        
        #parse xml config file passed in
        xml_tree = xml.parse( self.config_file )
        self.config = xml_tree.getroot()
        self.mongo_database = self.config.find('mongo').attrib.get('database')
        self.mongo_host  = self.config.find('mongo').attrib.get('host')
        self.mongo_port  = int(self.config.find('mongo').attrib.get('port'))
        self.rabbit_port = int(self.config.find('rabbit').attrib.get( 'port' ))
        self.rabbit_host = self.config.find('rabbit').attrib.get('host')
        self.rabbit_queue = self.config.find('rabbit').attrib.get('queue')
        self.mongo = pymongo.MongoClient( self.mongo_host, self.mongo_port )
        
        creds = pika.PlainCredentials(self.config.find('rabbit').attrib.get('user'), self.config.find('rabbit').attrib.get('pass'))
        connection   = pika.BlockingConnection( pika.ConnectionParameters( host = self.rabbit_host,
                                                                           port = self.rabbit_port,
                                                                           credentials = creds
                                                                       ) )

        self.channel = connection.channel()

        self.channel.queue_declare(queue = self.rabbit_queue,
                                   durable = False)

        self.channel.basic_qos(prefetch_count = 10)

        #print " %s waiting for input" % self.id
        
        self.channel.basic_consume(self.data_callback,
                                   queue = self.rabbit_queue,
                                   no_ack = False)

        print "Ready to receive events!\n"
        self.channel.start_consuming() 

    def data_callback(self, ch, method, properties, body):

        # parse json message
        try:
            data = json.loads(body)
        except ValueError:
            print "Couldn't decode \"%s\" as JSON, skipping" % body
            ch.basic_ack(delivery_tag=method.delivery_tag)
            return

        self.cache_misses = 0

        # generate a set of updates and inserts from all the data points
        updates = self.process_data(data)

        # send ack back to rabbit to let it know we're done with this message
        ch.basic_ack(delivery_tag=method.delivery_tag)


    def process_data(self, data):        
        # get mongo database instances based upon data type
        mongodb                 = self.mongo[self.mongo_database]
        data_collection         = mongodb['of_messages']

        pp = pprint.PrettyPrinter(indent=2)
        cleaned_data = self.change_longs(data)

        #find an existing stream
        existing_doc = data_collection.find({'stream_id': cleaned_data.stream_id})
        if(existing_doc.length > 0):
            if(existing_doc.index(existing_doc.length)['messages'].length > 50):
                #need to start a new document once it is too large
                #end the current doc and create a new doc
                existing_doc.index(existing_doc.length)['end'] = System.currentTimeMillis()
                data_collection.update_one({'stream_id': cleaned_data['stream_id'],
                                            'start': existing_doc.index(existing_doc.length)['start']},
                                           existing_doc.index(existing_doc.length))
                #create the new obj
                new_obj = defaultdict(list)
                new_obj['messages'] = []
                new_obj['stream_id'] = cleaned_data.stream_id
                new_obj['start'] = System.currentTimeMillis()
                new_obj['end'] = None
                new_obj['tcp'] = defaultdict(list)
                new_obj['tcp']['src_ip'] = cleaned_data['src']
                new_obj['tcp']['dst_ip'] = cleaned_data['dst']
                new_obj['tcp']['src_port'] = cleaned_data['src_port']
                new_obj['tcp']['dst_port'] = cleaned_data['dst_port']
                new_obj['dpid'] = cleaned_data['dpid']
                new_obj['messages'].push(cleaned_data['message'])
                data_collection.insert(new_obj)
                print "Inserted new Doc for existing stream\n"
            else:
                #update the existing doc
                #if we don't have our dpid set on the doc but do in this message set it
                if(existing_doc.index(existing_doc.length)['dpid'] == None and cleaned_data['dpid'] is not None):
                    existing_doc.index(existing_doc.length)['dpid'] = cleaned_data['dpid']
                #append our current message
                existing_doc.index(existing_doc.length)['messages'].push({'ts': System.currentTimeMillis(), 'message': cleaned_data['message']})
                #update the doc
                data_collection.update_one({'stream_id': cleaned_data['stream_id'], 
                                            'start': existing_doc.index(existing_doc.length)['start']},
                                           existing_doc.index(existing_doc.length))
                print "Updated existing doc\n"

        else:
            #there is no existing doc
            #update it
            new_obj = defaultdict(list)
            new_obj['messages'] = []
            new_obj['stream_id'] = cleaned_data.stream_id
            new_obj['start'] = System.currentTimeMillis()
            new_obj['end'] = None
            new_obj['tcp'] = defaultdict(list)
            new_obj['tcp']['src_ip'] = cleaned_data['src']
            new_obj['tcp']['dst_ip'] = cleaned_data['dst']
            new_obj['tcp']['src_port'] = cleaned_data['src_port']
            new_obj['tcp']['dst_port'] = cleaned_data['dst_port']
            new_obj['dpid'] = cleaned_data['dpid']
            new_obj['messages'].push(cleaned_data['message'])
            data_collection.insert(new_obj)
            print "Inserted Data!!\n";

    def change_longs(self,data):
        
        if(isinstance(data, dict)):
            for element in data.keys():
                #print element
                if(isinstance(data[element], Iterable)):
                    data[element] = self.change_longs(data[element])
                elif(isinstance(data[element], long)):
                    data[element] = float(data[element])
        
        elif(isinstance(data, list)):
            for i in range(len(data)):
                if(isinstance(data[i], long)):
                    data[i] = float(data[i])
                elif(isinstance(data[i], Iterable)):
                    data[i] = self.change_longs(data[i])
            
        if(isinstance(data, long)):
            data = float(data)

        return data

if __name__ == '__main__':

    Receiver()
