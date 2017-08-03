import commands
import httplib
import json
import sys
import time
import urllib2
#from multiprocessing import Pool

if len(sys.argv) != 7:
    sys.stderr.write('Not enough (or too many) arguments\n')
    sys.exit(1)

hostname = sys.argv[1].partition('.')[0]
environment = sys.argv[2]
application = sys.argv[3]
prefix = '{0}.{1}.{2}'.format(environment, hostname, application)

host = sys.argv[4]
port = sys.argv[5]
omit_jvm_stats = sys.argv[6].lower() == 'true'

timestamp_millis = int(round(time.time() * 1000))
timestamp = timestamp_millis / 1000

def escape_topic(str):
    return str.replace('.','_')

def dispatch_value(topic_name, field, value, ts, q=None):
    str = '{0}.{1}.{2} {3} {4}'.format(prefix, escape_topic(topic_name), field, value, ts)
    if q is not None:
        q.append(str)
    else:
        print str

def request_and_response_or_bail(method, url, message):
    try:
        return urllib2.urlopen('http://{0}:{1}{2}'.format(host,port,url)).read()
    except:
        sys.stderr.write('{0}\n'.format(message))
        sys.exit(1)

def system_stats(core_name, omit_jvm_stats):
    system_content = request_and_response_or_bail('GET', '/solr/{0}/admin/system?wt=json&_={1}'.format(core_name, timestamp_millis), 'Error while retrieving system stats')
    system_json = json.loads(system_content)
    if not omit_jvm_stats:
        print '{0}.{1} {2} {3}'.format(prefix, 'jvm.uptimeMillis', system_json['jvm']['jmx']['upTimeMS'], timestamp)
        print '{0}.{1} {2} {3}'.format(prefix, 'jvm.memory.free', system_json['jvm']['memory']['raw']['free'], timestamp)
        print '{0}.{1} {2} {3}'.format(prefix, 'jvm.memory.max', system_json['jvm']['memory']['raw']['max'], timestamp)
        print '{0}.{1} {2} {3}'.format(prefix, 'jvm.memory.total', system_json['jvm']['memory']['raw']['total'], timestamp)
        print '{0}.{1} {2} {3}'.format(prefix, 'jvm.memory.used', system_json['jvm']['memory']['raw']['used'], timestamp)
        print '{0}.{1} {2} {3}'.format(prefix, 'jvm.processors', system_json['jvm']['processors'], timestamp)
    print '{0}.{1} {2} {3}'.format(prefix, 'system.committedVirtualMemorySize', system_json['system']['committedVirtualMemorySize'], timestamp)
    print '{0}.{1} {2} {3}'.format(prefix, 'system.freePhysicalMemorySize', system_json['system']['freePhysicalMemorySize'], timestamp)
    print '{0}.{1} {2} {3}'.format(prefix, 'system.freeSwapSpaceSize', system_json['system']['freeSwapSpaceSize'], timestamp)
    print '{0}.{1} {2} {3}'.format(prefix, 'system.maxFileDescriptorCount', system_json['system']['maxFileDescriptorCount'], timestamp)
    print '{0}.{1} {2} {3}'.format(prefix, 'system.openFileDescriptorCount', system_json['system']['openFileDescriptorCount'], timestamp)
    print '{0}.{1} {2} {3}'.format(prefix, 'system.processCpuTime', system_json['system']['processCpuTime'], timestamp)
    print '{0}.{1} {2} {3}'.format(prefix, 'system.systemLoadAverage', system_json['system']['systemLoadAverage'], timestamp)
    print '{0}.{1} {2} {3}'.format(prefix, 'system.totalPhysicalMemorySize', system_json['system']['totalPhysicalMemorySize'], timestamp)
    print '{0}.{1} {2} {3}'.format(prefix, 'system.totalSwapSpaceSize', system_json['system']['totalSwapSpaceSize'], timestamp)

def domain_metrics(stats, domain, prefix, ts, q=None):
    for stat_name in stats.keys():
        if stat_name in stats and (type(stats[stat_name]) == int or type(stats[stat_name]) == float):
            dispatch_value(domain, '{0}.{1}'.format(prefix,stat_name), stats[stat_name], ts, q)

def topic_metrics(stats, domain, tags, ts, q=None):
    for stat_name in stats.keys():
        if stat_name in stats:
            if 'topic' in tags:
                dispatch_value(domain, '{0}.topics.{1}.{2}'.format(tags['client-id'],escape_topic(tags['topic']),stat_name), stats[stat_name], ts, q)
            elif stat_name.count('.') > 0 and len(stat_name.split('.')) > 1:
                topic_part = escape_topic('.'.join(stat_name.split('.')[:-1]))
                stat_part = stat_name.split('.')[-1]
                dispatch_value(domain, '{0}.topics.{1}.{2}'.format(tags['client-id'],topic_part,stat_part), stats[stat_name], ts, q)
            else:
                dispatch_value(domain, '{0}.{1}'.format(tags['client-id'],stat_name), stats[stat_name], ts, q)

def broker_topic_metrics(stats, domain, tags, ts, q=None):
    for stat_name in stats.keys():
        if stat_name in stats and (type(stats[stat_name]) == int or type(stats[stat_name]) == float):
            if 'topic' in tags:
                dispatch_value(domain, 'topics.{0}.{1}.{2}'.format(escape_topic(tags['topic']),tags['name'],stat_name), stats[stat_name], ts, q)
            else:
                dispatch_value(domain, '{0}.{1}'.format(tags['name'],stat_name), stats[stat_name], ts, q)

domains_content = request_and_response_or_bail('GET', '/jolokia/read/kafka.*:*', 'Error while retrieving domains.')
domains_json = json.loads(domains_content)

domain_names = domains_json['value'].keys()

if len(domain_names) == 0:
    sys.stderr.write('No domains\n')
    sys.exit(1)

for domain_name in domain_names:
    q = list()
    domain = domain_name.split(':')[0]
    tag_list = domain_name.split(':')[1].split(',')
    tags = {}
    for tag in tag_list:
        tags[tag.split('=')[0]] = tag.split('=')[1]
    if 'client-id' in tags and 'type' in tags and tags['type'].endswith(('-node-metrics')):
        domain_metrics(domains_json['value'][domain_name], domain, '{0}.{1}'.format(tags['client-id'],tags['node-id']), timestamp, q)
    elif 'client-id' in tags and 'type' in tags and tags['type'] in ['producer-metrics','consumer-metrics','connect-metrics','consumer-coordinator-metrics','connect-coordinator-metrics']:
        domain_metrics(domains_json['value'][domain_name], domain, tags['client-id'], timestamp, q)
    elif 'client-id' in tags and 'type' in tags and tags['type'] in ['consumer-fetch-manager-metrics','producer-topic-metrics']:
        topic_metrics(domains_json['value'][domain_name], domain, tags, timestamp, q)
    elif 'client-id' in tags and 'type' in tags and tags['type'] == 'kafka-metrics-count':
        dispatch_value(domain, '{0}.kafka-metrics-count'.format(tags['client-id']), domains_json['value'][domain_name]['count'], timestamp, q)
    elif 'type' in tags and tags['type'] in ['Partition','Log']:
        dispatch_value(domain, 'topics.{0}.partition-{1}.{2}'.format(escape_topic(tags['topic']),tags['partition'],tags['name']), domains_json['value'][domain_name]['Value'], timestamp, q)
    elif 'type' in tags and tags['type'] == 'RequestMetrics':
        domain_metrics(domains_json['value'][domain_name], domain, '{0}.{1}'.format(tags['request'],tags['name']), timestamp, q)
    elif 'type' in tags and tags['type'] == 'BrokerTopicMetrics':
        broker_topic_metrics(domains_json['value'][domain_name], domain, tags, timestamp, q)
    elif 'client-id' in tags and 'type' in tags and tags['type'] == 'Fetch':
        domain_metrics(domains_json['value'][domain_name], domain, tags['client-id'], timestamp, q)
    elif 'type' in tags and tags['type'] in ['LogCleanerManager','GroupMetadataManager','KafkaController']:
        dispatch_value(domain, '{0}'.format(tags['name']), domains_json['value'][domain_name]['Value'], timestamp, q)
    elif 'type' in tags and tags['type'] == 'DelayedOperationPurgatory':
        dispatch_value(domain, '{0}.{1}'.format(tags['delayedOperation'],tags['name']), domains_json['value'][domain_name]['Value'], timestamp, q)
    elif 'type' in tags and tags['type'] == 'DelayedFetchMetrics':
        domain_metrics(domains_json['value'][domain_name], domain, '{0}.{1}'.format(tags['fetcherType'],tags['name']), timestamp, q)
    elif 'type' in tags and tags['type'] in ['ControllerStats','SessionExpireListener','KafkaRequestHandlerPool']:
        domain_metrics(domains_json['value'][domain_name], domain, '{0}'.format(tags['name']), timestamp, q)
    elif 'type' in tags and tags['type'] in ['jetty-metrics','jersey-metrics']:
        domain_metrics(domains_json['value'][domain_name], domain, '{0}'.format(tags['type']), timestamp, q)
    #else:
    #    print tags
    #    print domains_json['value'][domain_name].keys()

    for i in q:
        print i

#if __name__ == '__main__':
#    p = Pool(len(domain_names))
#    results = p.map(core_stats, domain_names)
#    for q in results:
#        for i in q:
#            print i
