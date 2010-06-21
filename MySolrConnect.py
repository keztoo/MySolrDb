from datetime import datetime
import httplib
from httplib import HTTPConnection
from pprint import pprint
import simplejson
import sys
import time
import urllib2

DEBUG = True 
#DEBUG = False
DEBUG_WRITES = DEBUG and False
DEBUG_CALLER = DEBUG and True

def solr_facet_request(which_index, req_str):
    start = time.time()
    req_url = solr_indices[which_index] + solr_request_string

    print >> sys.stderr , "\n\nsolr_facet_request %s\nPOST=%s" % (req_url, urllib2.unquote(req_str))

    if DEBUG:
        print >> sys.stderr , "\n\nsolr_facet_request %s\nPOST=%s" % (req_url, urllib2.unquote(req_str))
    try:
        req = urllib2.Request(req_url, req_str)
        response = urllib2.urlopen(req)
        the_page = response.read()
        result = simplejson.loads(the_page)
    except Exception , ex:
        raise Exception, 'Index Error on Facet Request'
    docs = []
    if 'Error' in result:
        raise Exception, 'Result Error on Facet Request'
    docs = result['facet_counts']['facet_fields']
    if DEBUG:
        # /2 because each k is a tuple ('k', v, 'k', v, ...)
        lens = dict([ (k, len(v) / 2) for (k, v) in docs.items() ])
        print >> sys.stderr , "solr_facet_request took", time.time() - start, "found %s docs: %s lens: %s" % (len(docs,), docs, lens)
        if DEBUG_CALLER:
            import traceback
            pprint(traceback.extract_stack())
    return docs

def solr_delete(which_index, req_str):
    start = time.time()
    req_url = solr_indices[which_index] + "/solr/update"
    req_str = "wt=json&stream.body=" + req_str
    if DEBUG:
        print >> sys.stderr , "\n\nsolr_delete %s\nPOST=%s" % (req_url, urllib2.unquote(req_str))
    try:
        req = urllib2.Request(req_url, req_str)
        response = urllib2.urlopen(req)
        the_page = response.read()
        result = simplejson.loads(the_page)
        result = result['responseHeader']
        result = result['status']
    except Exception , ex:
        raise Exception, 'Solr Delete Exception'
    if int(result) == 0:
        solr_commit(which_index)
    if DEBUG:
        print >> sys.stderr , "solr_delete took", time.time() - start, "result:", result
        if DEBUG_CALLER:
            import traceback
            pprint(traceback.extract_stack())
    return result

def solr_request(req_url, req_str):
    # req_url is ip:port
    start = time.time()

    req_url = "http://" + req_url + "/solr/select"
    req_str = "?version=2.2&qt=standard&wt=json&indent=off&" + req_str

    try:
        req = urllib2.Request(req_url, req_str)
    except Exception:
        print "Error on Urllib2.Request()"
        raise Exception, 'Urllib2 Request Error'

    response = urllib2.urlopen(req)
    try:
        response = urllib2.urlopen(req)
    except Exception:
        print "Error on Urllib2.open()"
        raise Exception, 'Urllib2 Open Error'

    try:
        the_page = response.read()
    except Exception:
        print "Error on response.read()"
        raise Exception, 'Response.read() Error'

    try:
        result = simplejson.loads(the_page)
    except Exception , ex:
        raise Exception, 'Error on Json loads() '
    if 'Error' in result:
        raise Exception, 'Result has an Error Code '


    result = result['response']
    docs = result['docs']
    return docs



def solr_add(req_url, DATA):
    # req_url is ip:port
    start = time.time()

    host_name = req_url
    if DEBUG_WRITES:
        print >> sys.stderr , "\nAttempting to Update Host", host_name

    error_flag = False
    try:
        con = HTTPConnection(host_name)
        con.putrequest('POST', '/solr/update/')
        con.putheader('content-length', str(len(DATA)))
        con.putheader('content-type', 'text/xml; charset=UTF-8')
        con.endheaders()
        con.send(DATA)
        r = con.getresponse()
    except Exception , ex:
        error_flag = True
        print >> sys.stderr , "solr_add Error - can not connect to update host", host_name
        if DEBUG_WRITES:
            print >> sys.stderr , "Exception, Host at ", host_name, "is not responding (probably a communication problem or the index is not started). must append to the ", which_index, "index for host", host_name, "the following xml --->", DATA
    else:
        # no httplib exception. we have a response 
        if str(r.status) != '200':
            error_flag = True
            print >> sys.stderr , "solr_add %s unexpected response from %s: %s %s %s" % (datetime.now(), host_name, r.status, r.msg, r.reason)
            if DEBUG_WRITES:
                print >> sys.stderr , "Exception, Host at ", host_name, "is not accepting the update (probably an index schema error). must append to the ", which_index, "index for host", host_name, "the following xml --->", DATA 

    # NOTE: this will only return -1 if the most recent write attempt failed
    if error_flag:
        return -1 
    return 0


def solr_commit(req_url):
    """
    commit changes
    """
    DATA = '<commit/>'
    host_name = req_url

    #print "\nAttempting to Commit to Host", host_name

    try:
        con = HTTPConnection(host_name)
        con.putrequest('POST', '/solr/update/')
        con.putheader('content-length', str(len(DATA)))
        con.putheader('content-type', 'text/xml; charset=UTF-8')
        con.endheaders()
        con.send(DATA)
        r = con.getresponse()
    except Exception , ex:
        print >> sys.stderr , "Error - can not commit to host", host_name, ex

    if str(r.status) == '200':
        return 0
    else:
        return -1


