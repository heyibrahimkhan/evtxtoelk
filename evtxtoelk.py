import contextlib
import mmap
import traceback
import json
import argparse
from collections import OrderedDict
from datetime import datetime

from Evtx.Evtx import FileHeader
from pprint import pprint
from Evtx.Views import evtx_file_xml_view
from elasticsearch import Elasticsearch, helpers
import xmltodict
import sys


###GlobalVariables###
events_metadata_config = {}
count_logs_sent = 0
#####################


def load_events_metadata_from_config(config_file_path, win_events_meta_data_key='win_events_meta_data'):
    global events_metadata_config
    print('Path to config file: ' + config_file_path)
    events_metadata_config = json.load(open(config_file_path))[win_events_meta_data_key]
    print('Win events metadata loaded from ' + config_file_path)


class EvtxToElk:
    @staticmethod
    def bulk_to_elasticsearch(es, bulk_queue, testing):
        global count_logs_sent
        ret = True
        if testing == "1":
            count_logs_sent += len(bulk_queue)
            print('Total logs sent = {0}'.format(count_logs_sent))
            return True
        try:
            helpers.bulk(es, bulk_queue)
            count_logs_sent += len(bulk_queue)
            print('Total logs sent = {0}'.format(count_logs_sent))
            ret = True
        except:
            print(traceback.print_exc())
            ret = False
        return ret

    @staticmethod
    def add_meta_data(log_dict):
        global events_metadata_config
        log_dict = dict(log_dict)
        
        log_channel = ''
        log_provider_name = ''
        log_event_id = ''
        
        if 'Event' in log_dict:
            log_channel = log_dict['Event']['System']['Channel']
            log_provider_name = log_dict['Event']['System']['Provider']['@Name']
            log_event_id = log_dict['Event']['System']['EventID']['#text']
        else:
            temp = {'Event': log_dict, '@timestamp': log_dict['System']['TimeCreated']['@SystemTime']}
            log_dict = temp
            log_channel = log_dict['Event']['System']['Channel']
            log_provider_name = log_dict['Event']['System']['Provider']['@Name']
            log_event_id = log_dict['Event']['System']['EventID']['#text']

        event_meta_description = ''
        event_meta_extra_info = '###!!!Completely Unknown'

        if log_channel in events_metadata_config:
            if log_event_id in events_metadata_config[log_channel]:
                event_meta_description = events_metadata_config[log_channel][log_event_id]
            else:
                event_meta_extra_info = '###!!!Event ID under Channel is unknown'
                if log_event_id in events_metadata_config:
                    event_meta_description = events_metadata_config[log_event_id]
                else: event_meta_extra_info = '###!!!Completely Unknown'

        log_dict['event_meta_description'] = event_meta_description
        log_dict['event_meta_extra_info'] = event_meta_extra_info

        #print('Printing log')
        #pprint(log_dict)
        #print('Log has been printed')
        return log_dict


    @staticmethod
    def get_es_object(elk_ip, elk_port, es_timeout, es_user, es_pass, testing):
        if testing == "1": return None
        es_auth = ""
        es = ""
        if es_user != "" and es_pass != "":
            es_auth = "{0}:{1}@".format(es_user, es_pass)
        return Elasticsearch(hosts=['{0}{1}:{2}'.format(es_auth, elk_ip, elk_port)], timeout=es_timeout)


    @staticmethod
    def evtx_to_elk(filename, elk_ip, es_user, es_pass, testing, elk_port='9200', elk_index="hostlogs", bulk_queue_len_threshold=500, metadata={}, es_timeout=100):
        global count_logs_sent
        bulk_queue = []
        es = EvtxToElk.get_es_object(elk_ip, elk_port, es_timeout, es_user, es_pass, testing)
        with open(filename) as infile:
            with contextlib.closing(mmap.mmap(infile.fileno(), 0, access=mmap.ACCESS_READ)) as buf:
                fh = FileHeader(buf, 0x0)
                data = ""
                for xml, record in evtx_file_xml_view(fh):
                    try:
                        contains_event_data = False
                        log_line = xmltodict.parse(xml)

                        # Format the date field
                        date = log_line.get("Event").get("System").get("TimeCreated").get("@SystemTime")
                        if "." not in str(date):
                            date = datetime.strptime(date, "%Y-%m-%d %H:%M:%S")
                        else:
                            date = datetime.strptime(date, "%Y-%m-%d %H:%M:%S.%f")
                        log_line['@timestamp'] = str(date.isoformat())
                        log_line["Event"]["System"]["TimeCreated"]["@SystemTime"] = str(date.isoformat())
                        log_line["FileName"] = str(filename)

                        # Process the data field to be searchable
                        data = ""
                        if log_line.get("Event") is not None and testing != "1":
                            data = log_line.get("Event")
                            if log_line.get("Event").get("EventData") is not None:
                                data = log_line.get("Event").get("EventData")
                                if log_line.get("Event").get("EventData").get("Data") is not None:
                                    data = log_line.get("Event").get("EventData").get("Data")
                                    if isinstance(data, list):
                                        contains_event_data = True
                                        data_vals = {}
                                        for dataitem in data:
                                            try:
                                                if dataitem.get("@Name") is not None:
                                                    data_vals[str(dataitem.get("@Name"))] = str(
                                                        str(dataitem.get("#text")))
                                            except:
                                                pass
                                        log_line["Event"]["EventData"]["Data"] = data_vals
                                    else:
                                        if isinstance(data, OrderedDict):
                                            log_line["Event"]["EventData"]["RawData"] = json.dumps(data)
                                        else:
                                            log_line["Event"]["EventData"]["RawData"] = str(data)
                                        del log_line["Event"]["EventData"]["Data"]
                                else:
                                    if isinstance(data, OrderedDict):
                                        log_line["Event"]["RawData"] = json.dumps(data)
                                    else:
                                        log_line["Event"]["RawData"] = str(data)
                                    del log_line["Event"]["EventData"]
                            else:
                                if isinstance(data, OrderedDict):
                                    log_line = dict(data)
                                else:
                                    log_line["RawData"] = str(data)
                                    del log_line["Event"]
                        else:
                            pass

                        # Insert data into queue
                        #event_record = json.loads(json.dumps(log_line))
                        #event_record.update({
                        #    "_index": elk_index,
                        #    "_type": elk_index,
                        #    "metadata": metadata
                        #})
                        #bulk_queue.append(event_record)

                        if testing == "1": bulk_queue.append({})
                        else:
                            bulk_queue.append({
                                "_index": elk_index,
                                "_type": elk_index,
                                "body": json.loads(json.dumps(EvtxToElk.add_meta_data(log_line))),
                                "metadata": metadata
                            })

                        if len(bulk_queue) == bulk_queue_len_threshold:
                            print('Bulkingrecords to ES: ' + filename + ':' + str(len(bulk_queue)))
                            # start parallel bulking to ElasticSearch, default 500 chunks;
                            if EvtxToElk.bulk_to_elasticsearch(es, bulk_queue, testing):
                                bulk_queue = []
                            else:
                                print('Failed to bulk data to Elasticsearch')
                                sys.exit(1)

                    except:
                        print("***********")
                        print("Parsing Exception")
                        print(traceback.print_exc())
                        print(json.dumps(log_line, indent=2))
                        print("***********")

                # Check for any remaining records in the bulk queue
                if len(bulk_queue) > 0:
                    print('Bulking final set of records to ES: ' + filename + ':' + str(len(bulk_queue)))
                    if EvtxToElk.bulk_to_elasticsearch(es, bulk_queue, testing):
                        bulk_queue = []
                    else:
                        print('Failed to bulk data to Elasticsearch')
                        sys.exit(1)


if __name__ == "__main__":
    # Create argument parser
    parser = argparse.ArgumentParser()
    # Add arguments
    parser.add_argument('evtxfile', help="Evtx file to parse")
    parser.add_argument('elk_ip', default="localhost", help="IP of ELK instance")
    parser.add_argument('-ep', '--elk_port', default="9200", help="Port of ELK instance")
    parser.add_argument('-u', '--es_user', default="", required=None, help="Username of ES instance")
    parser.add_argument('-p', '--es_pass', default="", required=None, help="Password to ES instance")
    parser.add_argument('-i', default="hostlogs", help="ELK index to load data into")
    parser.add_argument('-s', default=500, help="Size of queue")
    parser.add_argument('-et', '--es_timeout', default=100, help="Elasticsearch timeout")
    parser.add_argument('-meta', default={}, type=json.loads, help="Metadata to add to records")
    parser.add_argument('-c', '--config_file_path', default='config.json', help='Path to config file')
    parser.add_argument('-t', '--testing', default="", required=None, help="empty means no testing. 1 means that this is a test and do not forward logs to ES")
    # Parse arguments and call evtx to elk class
    args = parser.parse_args()
    load_events_metadata_from_config(args.config_file_path)
    EvtxToElk.evtx_to_elk(args.evtxfile, args.elk_ip, args.es_user, args.es_pass, args.testing, elk_port=args.elk_port, elk_index=args.i, bulk_queue_len_threshold=int(args.s), metadata=args.meta, es_timeout=args.es_timeout)
