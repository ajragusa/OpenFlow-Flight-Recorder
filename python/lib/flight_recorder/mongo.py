import pymongo
import time
import logging
import pprint
import xml.etree.ElementTree as xml

DEFAULT_CONFIG_FILE = '/etc/openflow_flight_recorder/config.xml'

class FlightRecorderFetcher:
    def __init__( self, config_file = DEFAULT_CONFIG_FILE ):
        
        self.config_file = config_file
        #parse xml config file passed in
        xml_tree = xml.parse( self.config_file )
        self.config = xml_tree.getroot()
        self.mongo_database = self.config.find('mongo').attrib.get('database')
        self.mongo_host  = self.config.find('mongo').attrib.get('host')
        self.mongo_port  = int(self.config.find('mongo').attrib.get('port'))
        self.mongo = pymongo.MongoClient( self.mongo_host, self.mongo_port )
        self.offr = self.mongo.offr 
        self.messages = self.offr.OF_Messages

        self.error = ""

    def _iterate_over_cursor(self, cur, doIdRemoval=False, doIdRename=False, rename=""):
        results = []
        for doc in cur:
            if doIdRemoval:
                del doc['_id']
            if doIdRename:
                doc[rename] = doc['_id']
                del doc['_id']
            results.append(doc)

        return results

    def get_streams( self, params):
        """get streams"""
        start = params.get('start')
        stop = params.get('end')
        addr = params.get('addr')
        port = params.get('port')
        dpid = params.get('dpid')
        limit = params.get('limit')
        page = params.get('page')
        
        query = []
        if start is None or start <= 0:
            #this is required if they didn't specify set to 5min ago
            start = int(time.time()) - 300
        else:
            start = int(start)

        query.append({'$or': [{'start': {'$gt': start}}, {'start': {'$gt': str(start)}}]})

        if stop is None:
            #presume no end time
            pass
        else:
            if(int(stop) == -1):
                #specified no end time
                pass
            else:                
                #find streams between this time
                query.append({'$or': [{'end': {'$lt': int(stop)}}, {'end': {'$lt': str(stop)}}]})
            
        if port is not None:
            query.append({'$or': [{'tcp.src_port': int(port)}, {'tcp.dst_port': int(port)}]})

        if addr is not None:
            query.append({'$or': [{'tcp.src_ip': addr}, {'tcp.dst_ip': addr}]})

        if dpid is not None:
            query.append({'dpid': dpid})

        query = {'$and': query }

        if limit is None:
            limit = 100

        print query

        cur = self.messages.find( query ).limit(limit)
        print cur.count()
        results = self._iterate_over_cursor(cur)

        streams = {}
        for res in results:
            if streams.has_key(res.get("stream_id")):
                if streams[res.get("stream_id")]['dpid'] is None and res.get("dpid") is not None:
                    streams[res.get("stream_id")]['dpid'] = res.get("dpid")

                #process the end param
                if res.get('end') is not None:
                    if not streams[res.get("stream_id")].has_key('end'):
                        streams[res.get("stream_id")]['end'] = float(res.get("end"))
                    elif streams[res.get("stream_id")]['end'] < float(res.get("end")) and streams[res.get("stream_id")]['end'] != -1:
                        streams[res.get("stream_id")]['end'] = float(res.get("end"))
                    elif res.get("end") == -1:
                        streams[res.get("stream_id")]['end'] = -1

                #process the start param
                if res.get('start') is not None:
                    if not streams[res.get("stream_id")].has_key('start'):
                        streams[res.get("stream_id")]['start'] = float(res.get("start"))
                    elif streams[res.get("stream_id")]['start'] > float(res.get("start")):
                        streams[res.get("stream_id")]['start'] = float(res.get("start"))
            else:
                obj = {}
                streams[res.get("stream_id")] = obj

                obj['stream_id']= res.get("stream_id")
                obj["dpid"] = res.get("dpid")
                obj["tcp"] = res.get("tcp")

                if res.get("start") is not None:
                    obj = float(res.get("start"))
                if res.get("end") is not None:
                    obj = float(res.get("end"))
 
        s = []
        for(key, value) in streams.iteritems():
            s.append(value)
        print s
        return s

    def get_messages(self, params):
        """get messages super flexible call"""
        start = params.get('start')
        stop = params.get('end')
        addr = params.get('addr')
        port = params.get('port')
        dpid = params.get('dpid')
        limit = params.get('limit')
        page = params.get('page')
        stream_id = params.get('stream_id')
        query = []
        if start is None or start <= 0:
            #this is required if they didn't specify set to 5min ago
            start = int(time.time()) - 300
        else:
            start = int(start)

        query.append({'$or': [{'messages.ts': {'$gt': start}}, {'messages.ts': {'$gt': str(start)}}]})

        if stop is None:
            #presume no end time
            pass
        else:
            if(int(stop) == -1):
                #specified no end time
                pass
            else:
                #find streams between this time
                query.append({'$or': [{'messages.ts': {'$lt': int(stop)}}, {'messages.ts': {'$lt': str(stop)}}]})


        if stream_id is not None:
            query.append({'$and': [{'stream_id': stream_id}]})

        query = {'$and': query }

        if limit is None:
            limit = 100

        print query
        #cur = self.messages.aggregate( [{'$match': query},{ '$group' : { '_id': '$stream_id', 'messages': {'$push': '$messages.message'}}}, {'$sort': {'ts':-1}}, {'$limit': limit}], allowDiskUse=True)
        cur = self.messages.find( query ).sort('messages.ts', pymongo.ASCENDING).limit(limit)
        results = []
        for doc in cur:
            results += doc['messages']

        return results

    def get_error(self):
        return self.error

    def _set_error(self, error):
        self.error = error
