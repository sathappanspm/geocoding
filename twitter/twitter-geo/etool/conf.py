#!/usr/bin/env python

import sys
import __builtin__
import json
import logging
import os
import os.path
import string
from collections import OrderedDict

log = logging.getLogger(__name__)
conf = {}

# constant to select bind() for attaching the socket
BIND = 1
# constant to select connect() for attaching the socket
CONNECT = 2

def init(args=None):
    # init logger
    # load/get the config
    # eventually this needs a search path for the config
    # should be env(QFU_CONFIG);./embers.conf;/etc/embers/embers.conf;tcp://localhost:3473
    # use 3473 as the global control channel
    global conf

    cf = None
    if args and vars(args).get('embers_conf', None):
        log.debug('trying embers config %s', vars(args)['embers_conf'])
        cf = vars(args)['embers_conf']

    if not (cf and os.path.exists(cf)):
        log.debug('trying embers config %s', os.path.join(os.getcwd(), 'embers.conf'))
        cf = os.path.join(os.getcwd(), 'embers.conf')

    if not (cf and os.path.exists(cf)):
        log.debug('trying embers config %s', os.path.join(os.environ.get('HOME', '.'), 'embers.conf'))
        cf = os.path.join(os.environ.get('HOME', '.'), 'embers.conf')

    if not (cf and os.path.exists(cf)):
        log.debug('trying embers config %s', os.path.join(os.path.dirname(sys.argv[0]), 'embers.conf'))
        cf = os.path.join(os.path.dirname(sys.argv[0]), 'embers.conf')

    if not (cf and os.path.exists(cf)):
        log.warn('Could not find embers.conf, bailing out.')
        return

    try:
        with __builtin__.open(cf, 'r') as f:
            conf = json.load(f)
    except Exception as e:
        log.exception("Could not find or load config file %s", (cf,))

    log.debug('loaded config=%s from "%s"', conf, cf)


def get_all_s3only(prodOnly):

    plst = []

    for data_name in conf["data"]:
        if conf["data"][data_name]["type"] == "s3":
            pfx, include, prodOnlyPref = get_prefix_for_s3(data_name)
            if not include:
               continue
            if not prodOnly:
               plst.append(pfx)
            elif prodOnlyPref:
               plst.append(pfx)

    return plst

def get_all_prefixes():

    plst = []

    for data_name in conf["data"]:

        if conf["data"][data_name]["type"] == "queue":
            pfx, include = get_prefix_for_queue(data_name,withBasename=False)
            if include:
               plst.append(pfx)
            else:
               continue
        elif conf["data"][data_name]["type"] == "s3":
            pfx, include, prodOnly = get_prefix_for_s3(data_name)
            if include:
               plst.append(pfx)
            else:
               continue
            plst.append(pfx)
        else:
            continue

    plst = sorted(list(OrderedDict.fromkeys(plst)))

    return plst


def get_all_prefixpairs(includeS3=True):

    plst = []
    qlst = []

    for qname in conf["data"]:

        pfx = get_prefixpair(qname=qname,includeS3=includeS3)
        if pfx:
           plst.append(pfx)
           qlst.append(qname)

    return plst, qlst

def get_prefixpair(prefix=None,qname=None,includeS3=True,withBasename=True):

    if not (prefix or qname):
         return None

    if qname:	# get prefix from queue
        if conf["data"][qname]["type"] == "queue":
             pfx, include = get_prefix_for_queue(qname,withBasename=withBasename)
             if include:
                   return pfx
        elif includeS3 and conf["data"][qname]["type"] == "s3":
             pfx, include, prodOnly = get_prefix_for_s3(qname)
             if include:
                   return pfx
    else:	# get queue from prefix

        for data_name in conf["data"]:
            if conf["data"][data_name]["type"] == "queue":
                pfx, include = get_prefix_for_queue(data_name,withBasename=withBasename)
                if not include:
                   continue
                if pfx == prefix:
                   return data_name
            elif includeS3 and conf["data"][data_name]["type"] == "s3":
                pfx, include, prodOnly = get_prefix_for_s3(data_name)
                if not include:
                   continue
                if pfx == prefix:
                   return data_name

    return None

def get_prefix_for_s3(s3name):

    s3conf = conf["data"][s3name]
    prefix_str = None
    include = False
    prodOnly = False

    if "index" not in s3conf or not s3conf["index"]:
        return prefix_str, include, prodOnly

    prefix_str = s3conf["path"]

    if not prefix_str:
       return prefix_str, include, prodOnly

    include = True
    if not prefix_str[-1] == "/":
       prefix_str = prefix_str + "/"

    if not "prodonly" in s3conf or not s3conf["prodonly"]:
       prodOnly = False
    else:
       prodOnly = True

    return prefix_str, include, prodOnly

def get_prefix_for_queue(qname,withBasename=False):

    qconf = conf["data"][qname]
    prefix_str = None
    include = False

    if "index" not in qconf or not qconf["index"]:
        return prefix_str, include

    if qconf.get("archive",None) is None:
        return prefix_str, include

    arch = qconf["archive"]
    if arch.get('active', None) is None or not arch["active"]:
        return prefix_str, include

    prefix_str = arch.get('prefix', None)
    if prefix_str:
       include = True
       if not prefix_str[-1] == "/":
          prefix_str = prefix_str + "/"
       if withBasename:
          bstr = arch.get('basename', None)
          if bstr:
             prefix_str = prefix_str + bstr
       return prefix_str, include
    else:
       return prefix_str, include

def get_all_active_archived_queues(ignoreExternal=False):
    # Get list of active queues (see get_all_active_queues) that are also archived
    # This excludes e.g. queues corresponding to internal steps of enrichment
    # Used for e.g. on the fly audit trail indexing
    #

    active_qs = get_all_active_queues(ignoreExternal)

    qs = []
    for q in active_qs:
        try:
            # Ignore queues that are not being archived and do not have "index" set to True
            qconf = conf["data"][q]
            #if not qconf["archive"]["active"] and (not "index" in qconf or not qconf["index"]):
            if not "index" in qconf or not qconf["index"]:
                continue
        except:
            # Ignore queues that don't have the archiving
            # information specified
            continue

        qs.append(q)

    return qs


def get_data(host=None,**kwargs):
    # Get list of names of all queues which gets output by active services (services that are on a hosts list)

    datas = []

    data_cfgs = conf["data"]
    for data_name in data_cfgs.keys():
        try:
            # Process all data entries
            host_name, service_name  = get_host_and_service(data_name,ignoreExternal=False)
            if host_name and service_name:
                if (host is None) or (host==host_name): # if host is set, only return data of this host
                        datas.append(data_name)
        except:
            continue


    # Now filter out by the parameters given in the kwargs
    # This checks if the parameter exists on the top level
    # and in the monitor section.
    datas_filtered = []
    if len(kwargs) <= 0:
        # If no filters have been specified
        datas_filtered = datas
    else:
        # Apply all filters
        for data_name in datas:
            data = conf["data"][data_name]
            accept = False
            for key, value in kwargs.iteritems():

                if value is None:
                    # If the filter argument is None, we accept by default
                    accept = True

                else:
                    # Check if the parameter is on the top level
                    try:
                        if data[key] == value:
                            accept = True
                            continue
                    except:
                        pass
                    # Check if the parameter is in the monitor entry
                    try:
                        if data["monitor"][key] == value:
                            accept = True
                            continue
                    except:
                        pass

            # If the entry passed all filters, add it to the list
            if accept:
                datas_filtered.append(data_name)

    return datas_filtered


def get_all_cached_queues(hostname):
    """
    Returns a list of queues for the hostname provided that have the 'cache:true' option set
    :param hostname: str
    :return: list str
    """
    cached_queues = []
    queues = get_all_active_queues(host=hostname)
    for queue in queues:
        queue_config = conf["data"][queue]
        if 'cache' in queue_config and queue_config['cache']:
            cached_queues.append(queue)

    return cached_queues


def get_all_active_queues(ignoreExternal=False,host=None):
    # Get list of names of all queues which gets output by active services (services that are on a hosts list)
    # (upstart or cron)
    # If ignoreExternal is True, ignore queues that belong to external hosts (like "vt")
    # (default is currently False, to make it backward compatible, although in reality we might always specify it as True)

    qs = []

    queue_cfgs = conf["data"]
    for queue_name in queue_cfgs.keys():
        try:
            # Ignore anything that's not a queue
            if not queue_cfgs[queue_name]["type"] == "queue":
                continue
        except:
            # If any of the parameters is not specified, ignore queue also
            continue
        try:
            # Process all queue entries
            host_name, service_name  = get_host_and_service(queue_name,ignoreExternal)
            if host_name and service_name:
                if (host is None) or (host==host_name): # if host is set, only return queues of this host
                        qs.append(queue_name)
        except:
            continue

    return qs

def get_host_and_service(data_name,ignoreExternal=False):
    # We need to traverse up the embers.conf tree in order
    # to get to the host via the service

    # First look for what service produces the data for
    # the specified queue.
    service_name = None
    for s in conf["services"]:
        for o in conf["services"][s].get("outputs",[]):
            if o == data_name:
                service_name = s
    # throw error if service cannot be found
    if service_name is None:
        return None, None

    # Next look for host that runs the service
    host_name = None
    for host in conf["cluster"]:
        if ignoreExternal and "instance_type" in conf["cluster"][host] and conf["cluster"][host]["instance_type"] == "external":
            continue
        for s in conf["cluster"][host]["services"]:
            if s == service_name:
                host_name = host

    return host_name, service_name


def get_queue_inputs(queue_name):

    services = conf.get('services')
    if not services:
        return None

    for service in services:
	service_info = get_service_config(service)
        if queue_name in service_info["outputs"]:
            inputs = service_info.get('inputs',None)
            if inputs and len(inputs) > 0:
                return inputs
            else:
                return None

    return None

def get_queue_data(queue_name):

    data_entry = None
    try:
        data_entry = conf["data"][queue_name]
    except:
        pass

    return data_entry

def get_queue_info(queue_name):
    """
    Returns host and port for the queue with the specified name.
    """

    # Read the port information from the corresponding data entry
    data_entry = get_queue_data(queue_name)

    if data_entry is None:
        return None, None

    port = data_entry.get("port",None)
    if port is None:
        log.warn("Error in embers.conf. Port was not found in data entry for queue %s." % (queue_name))

    # Grab the host name via the helper function
    host_name, service_name  = get_host_and_service(queue_name)

    return host_name, port

def get_resource_info(resource_name):

    res_info = None
    try:
        res_info = conf["data"][resource_name]
        if not 'type' in res_info or not res_info['type'] == 'resource':
            return None
    except:
        return None

    return res_info

def get_conf_entry(queue_name):
    """
    Return the entire JSON expression for a given qname.
    """

    # Grab the host name via the helper function
    host_name, service_name  = get_host_and_service(queue_name)

    # Now we just stick the host name into the data entry for the
    # specified queue and return the result
    queue_entry = conf["data"][queue_name]
    if queue_entry is None:
        log.warn("Error in embers.conf. Data entry for queue %s not found." % (queue_name))
        return None
    queue_entry["host"] = host_name

    return queue_entry


def get_host_stream_service_list(host_name):
    """
    Return the list of services associated with a host

    Args:
        host_name: Name of the host

    Returns:
        A python list of services on the host
    """
    host = conf["cluster"].get(host_name,None)
    if host:
        return  [service for service in host.get("services",[])+get_allhosts_service_list() if get_service_config(service).get("type") == "stream"]
    else:
        return None
        log.warn("No entry for host %s." % (queue_name))

def get_allhosts_service_list():
    return [s for (s,sinfo) in conf['services'].iteritems() if sinfo.get('allhosts',False)]


def get_service_config(service_name):
    """
    Return the configuration dictionary associated with a service name.

    Args:
        service_name: Name of the service.

    Returns:
        A python dictionary if there is a configuration for a requesting service
        or None otherwise.
    """
    services = conf.get('services')
    if services and service_name in services:
        return services.get(service_name)
    else:
        return None


def get_default_queue_names(service_name, qtype):
    if not service_name:
        service_name = os.environ.get('UPSTART_JOB')
    service_info = get_service_config(service_name)
    assert service_info, 'No service information available for setting queue defaults.'
    if qtype == 'out':
        output_queues = [x for x in service_info.get('outputs') if get_conf_entry(x).get('type') == 'queue']
        assert len(output_queues) == 1, 'Improper input information for service %s' % service_name
        return output_queues[0]
    else:
        input_queues = [x for x in service_info.get('inputs') if get_conf_entry(x).get('type') == 'queue']
        assert input_queues, 'No input queues specified for service %s' %  service_name
        return input_queues


def generate_batch_conf():
    '''
    Generates the json that is needed to run the batch monitor.

    It only includes data entries that are produced by batch
    jobs, which means the scripts are invoked by crontab. The
    data entries are limited to SimpleDB and S3 data. It skips
    any data entries that have monitored:active set to false.
    '''

    # Flip the hosts inside out to be able to query host
    # by service name
    service_to_host = {}
    for host in conf["cluster"]:
        for s in conf["cluster"][host]["services"]:
            service_to_host[s] = host

    # Create a map for finding services that output to specific data
    data_to_service = {}
    for s in conf["services"]:
        service = conf["services"][s]
        if service["type"] == "batch":
            for o in service.get("outputs",[]):
                data_to_service[o] = s

    # Now create the new json
    bconf = {}
    for dname in conf["data"]:

        try:
            data = conf["data"][dname]
            monitor = data.get("monitor",None)
            if monitor:
                if data["monitor"]["active"]:

                    if conf["data"][dname]["type"] in ["s3","sdb"]:
                        service = data_to_service.get(dname,None)
                        host = service_to_host.get(service,None)
                        if service and host:
                            # Copy all the values over
                            bconf[dname] = conf["data"][dname]
                            bconf[dname]["service"] = conf["services"][service]
                            bconf[dname]["host"] = host
        except Exception as e:
            log.error("Error in processing a batch entry: %s" % (e))

    # Done
    return bconf


###############################################
###############################################

def get_q_cat_mon(qname,conf):

	cat = None
	mon = None

	try:
		cat = conf.get('category', None)
	except Exception, ee:
		log.error("Error when getting CATEGORY for '%s': %s" % (qname, str(ee)))
		cat = None

	try:
		if 'active' in conf['monitor'] and conf['monitor']['active'] is False:
			mon = None
		else:
			mon = conf['monitor']['check-interval']
	except Exception, ee:
		#log.error("Error when getting INTERVAL for '%s': %s" % (qname, str(ee)))
		mon = None

	return cat, mon

def is_high(mon):

	ret = False

	try:
		if not mon or mon > 100:
			ret = False
		else:
			ret = True
	except:
		ret = False

	return ret

def get_q_hi_lo_ex_lists():

	qs_hi = []
	qs_lo = []

	qs_hi_set = set()
	qs_lo_set = set()
	qs_skipped = set()

	queue_cfgs = conf["data"]

	for qname in queue_cfgs.keys():

		descr = queue_cfgs.get(qname)
		if not "type" in descr or not descr["type"] == "queue":
			#print "Skipping non-queue %s" % descr
			continue

		cat, mon = get_q_cat_mon(qname,descr)

		#if act == "n" or act == "N" or cat == "internal":
		#
		# IZ 2013-12-03: Hard to figure out the right rules for including/excluding queue
		# so for now let's exclude only internal ones. Monitoring is not uyet set up correctly
		# for all queues so it's not a robust criterion
		#
		#if cat == "internal" or mon is None:
		if not cat or cat == "internal":
			qs_skipped.add(qname)
			continue

		if not mon:
			qs_skipped.add(qname)
			continue


		if is_high(mon):
			qs_hi_set.add(qname)
		else:
			qs_lo_set.add(qname)

	qs_hi = list(qs_hi_set)
	qs_lo = list(qs_lo_set)
	qs_excl = list(qs_skipped)

	return qs_hi, qs_lo, qs_excl


###############################################
###############################################


#initialize the module
#init()

def main():

    import args
    import logs
# preliminary stab at some useful command line functionality (only cluster is currently needed)
    ap = args.get_parser()
    ap.add_argument('-c', '--cluster', action="store_true",
                    help='Switch to output names of cluster nodes.')
    ap.add_argument('--get', metavar='REQUEST', nargs='?', type=str,
                    help='Information to get from embers.conf [service|data|cluster|host|input|output]')
    ap.add_argument('--s3only', action="store_true", help="Write debug messages.")
    ap.add_argument('--s3onlyprod', action="store_true", help="Write debug messages.")
    ap.add_argument('--prefixes', action="store_true", help="Write debug messages.")
    ap.add_argument('--prefixpairs', action="store_true", help="Write debug messages.")
    group = ap.add_mutually_exclusive_group()
    group.add_argument('--host', metavar='HOST', nargs='?', type=str,
                    help='Name of the host to get information about')
    group.add_argument('--data', metavar='DATA', nargs='?', type=str,
                    help='Name of the data (queue, etc.) to get information about')

    arg = ap.parse_args()
    logs.init(arg)
    init(arg)

    if arg.get:
        assert arg.get in ('service','data','cluster','host','inputs','outputs','services'), 'Improper get request'
    try:
        if arg.s3only or arg.s3onlyprod:
            if arg.s3only:
                prodOnly = False
            else:
                prodOnly = True
            plst = get_all_s3only(prodOnly)
            if plst:
                print string.join(plst)
            return

        if arg.prefixpairs:
            plst, qlst = get_all_prefixpairs()
            for i in range(len(plst)):
                print "%s %s" % (qlst[i], plst[i])
            return

        if arg.prefixes:
            plst = get_all_prefixes()
            for p in plst:
                print p
            return

        if arg.host:
            assert arg.host in conf.get('cluster'), 'Host is not listed in embers.conf'
            if arg.get == 'services':
                print conf.get('cluster')[arg.host]['services']
            if arg.get == 'data':
                print [data for service in conf['cluster'][arg.host]['services'] for data in conf['services'][service]['outputs']]
        if arg.data:
            assert arg.data in conf.get('data'), 'Data is not listed in embers.conf'
            (host, service) = get_host_and_service(arg.data)
            if arg.get == 'host':
                print host
            if arg.get == 'service':
                print service
        if arg.service:
            assert arg.service in conf.get('services'), 'Service is not listed in embers.conf'
            if arg.get == 'host':
                print [host for host in conf['cluster'] if arg.service in conf['cluster'][host]['services']]
            if arg.get == 'inputs':
                print conf['services'][arg.service]['inputs']
            if arg.get == 'outputs':
                print conf['services'][arg.service]['outputs']

        if arg.cluster or arg.get == 'cluster':
            print ' '.join(conf.get('cluster',''))
        if arg.get == 'data' and not (arg.service or arg.data or arg.host):
            print ' '.join(conf.get('data',''))
        if arg.get == 'host' and not (arg.service or arg.data or arg.host):
            print ' '.join(conf.get('cluster',''))
        if arg.get == 'service' and not (arg.service or arg.data or arg.host):
            print ' '.join(conf.get('services',''))


    except Exception, e:
        log.error("Requested information not in embers.conf, error %s", e)



if __name__ == '__main__':
    sys.exit(main())
