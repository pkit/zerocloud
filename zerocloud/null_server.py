import os
from swift import gettext_ as _
from contextlib import contextmanager
from eventlet import Timeout

from swift.obj import server

from swift.common.bufferedhttp import http_connect
from swift.common.exceptions import ConnectionTimeout
from swift.common.http import is_success
from swift.common.swob import HTTPNotImplemented, \
    HTTPInternalServerError, Response
from swift.obj.diskfile import DiskFileManager


class DiskFileWriter(object):
    def __init__(self):
        self._upload_size = 0

    def write(self, chunk):
        self._upload_size += len(chunk)
        return self._upload_size

    def put(self, metadata):
        pass


class DiskFile(object):

    def __init__(self):
        pass

    def read_metadata(self):
        return {}

    @contextmanager
    def create(self, size=None):
        try:
            yield DiskFileWriter()
        finally:
            pass

    def delete(self, timestamp):
        pass


class ObjectController(server.ObjectController):
    """
    Implements the WSGI application for the Swift In-Memory Object Server.
    """

    def setup(self, conf):
        pass

    def get_diskfile(self, device, partition, account, container, obj,
                     **kwargs):
        return DiskFile()

    def REPLICATE(self, request):
        """
        Handle REPLICATE requests for the Swift Object Server.  This is used
        by the object replicator to get hashes for directories.
        """
        pass

    # method is not public
    # will return HTTPMethodNotAllowed when called from proxy
    def GET(self, request):
        return HTTPNotImplemented(request=request)

    # method is not public
    # will return HTTPMethodNotAllowed when called from proxy
    def HEAD(self, request):
        return HTTPNotImplemented(request=request)

    # method is not public
    # will return HTTPMethodNotAllowed when called from proxy
    def POST(self, request):
        return HTTPNotImplemented(request=request)

    def async_update(self, op, account, container, obj, host, partition,
                     contdevice, headers_out, objdevice, policy_index):
        # we fail on unsuccessful async update, instead of saving it
        # because we do not want delayed container syncs
        # client should better retry and get another quorum
        headers_out['user-agent'] = 'obj-server %s' % os.getpid()
        full_path = '/%s/%s/%s' % (account, container, obj)
        if all([host, partition, contdevice]):
            try:
                with ConnectionTimeout(self.conn_timeout):
                    ip, port = host.rsplit(':', 1)
                    conn = http_connect(ip, port, contdevice, partition, op,
                                        full_path, headers_out)
                with Timeout(self.node_timeout):
                    response = conn.getresponse()
                    body = response.read()
                    if is_success(response.status):
                        return
                    else:
                        self.logger.error(_(
                            'ERROR Container update failed'
                            ': %(status)d '
                            'response from %(ip)s:%(port)s/%(dev)s'),
                            {'status': response.status, 'ip': ip, 'port': port,
                             'dev': contdevice})
                        error_resp = Response()
                        error_resp.status = \
                            '%d %s' % (response.status, response.reason)
                        error_resp.headers = response.getheaders()
                        error_resp.body = body
                        raise error_resp
            except (Exception, Timeout):
                self.logger.exception(_(
                    'ERROR container update failed with '
                    '%(ip)s:%(port)s/%(dev)s (saving for async update later)'),
                    {'ip': ip, 'port': port, 'dev': contdevice})
        raise HTTPInternalServerError()



def app_factory(global_conf, **local_conf):
    """paste.deploy app factory for creating WSGI object server apps"""
    conf = global_conf.copy()
    conf.update(local_conf)
    return ObjectController(conf)
