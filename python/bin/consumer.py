#!/usr/bin/python

import sys
import signal
import pika
import os
import pymongo
import json
import time
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
        
        connection   = pika.BlockingConnection( pika.ConnectionParameters( host = self.rabbit_host,
                                                                           port = self.rabbit_port ) )

        self.channel = connection.channel()

        self.channel.queue_declare(queue = self.rabbit_queue,
                                   durable = False)

        self.channel.basic_qos(prefetch_count = 10)

        #print " %s waiting for input" % self.id

        self.channel.basic_consume(self.data_callback,
                                   queue = self.rabbit_queue,
                                   no_ack = False)

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

            data_collection.insert(data)
            print "INserted Data!!\n";

if __name__ == '__main__':

    Receiver()