import os
import redis
import time
import pprint
from collections import defaultdict
from elasticsearch import Elasticsearch
import arrow
import Queue
from multiprocessing.pool import ThreadPool
import math
import sys

pp = pprint.PrettyPrinter(indent=4)

SHOME = os.path.abspath(os.path.join(os.path.dirname(__file__)))

def load_conf():
    import yaml
    with open('{}/config.yml'.format(SHOME), 'r') as f:
        doc = yaml.load(f)
    return doc

conf = load_conf()
poll_interval = conf['script_args'].get('poll_interval', 1)

es = Elasticsearch(conf['elasticsearch']['hosts'], **conf['elasticsearch']['args'])


def indexit(d):
    def get_index():
        now = arrow.utcnow().format('YYYY.MM.DD')
        return 'redis-stats-{}'.format(now)        
    es = Elasticsearch(conf['elasticsearch']['hosts'], **conf['elasticsearch']['args'])   
    return es.index(index=get_index(), doc_type='redis-stats', body=d)

def ddiff(d1,d2):
    d = {}

    d['@timestamp'] = arrow.utcnow().datetime
    d['hits_per_second'] = d2['keyspace_hits'] - d1['keyspace_hits']
    d['misses_per_second'] = float(d2['keyspace_misses'] - d1['keyspace_misses'])
    d['evictions_per_second'] = d2['evicted_keys'] - d1['evicted_keys']
    d['expired_per_second'] = d2['expired_keys'] - d1['expired_keys']
    d['commands_per_second'] = d2['total_commands_processed'] - d1['total_commands_processed']
    d['connections_per_second'] = d2['total_connections_received'] - d1['total_connections_received']
    d['key_count'] = d2['db0']['keys']
    d['connected_clients'] = d2['connected_clients']
    d['used_memory_rss'] = d2['used_memory_rss']
    d['used_memory'] = d2['used_memory']
    d['hit_percentage'] = d['hits_per_second'] / d['commands_per_second'] * 100
    d['last_save_time_epoch'] = d2['rdb_last_save_time']
    d['last_save_time'] = arrow.get(d['last_save_time_epoch']).datetime
    d['cpu_usage'] = { 'user': math.ceil(d2['used_cpu_user'] - d1['used_cpu_user']), 'system': math.ceil(d2['used_cpu_sys'] - d1['used_cpu_sys']) }
    d['cluster_info'] = [ 
                        {'redis_version': d2['redis_version']},
                        {'uptime_in_seconds': d2['uptime_in_seconds']},
                        {'uptime_in_days': d2['uptime_in_days']}
                    ]
    return d


def worker(conf):
    conn_args = conf
    red = redis.Redis(**conn_args)
    d1 = red.info()
    time.sleep(1)
    d2 = red.info()
    diffed = ddiff(d1,d2)
    diffed['host'] = conf['host']
    #pp.pprint(diffed)
    print indexit(diffed)

def main():
    conf = load_conf()
    pool = ThreadPool(processes=5)
    pool.map(worker, conf['redis'])
    pool.close()
    pool.join()



while True:
    main()
    time.sleep(poll_interval)
