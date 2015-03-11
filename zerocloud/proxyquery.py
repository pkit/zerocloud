from copy import deepcopy
import ctypes
from itertools import chain
import re
import struct
import traceback
import time
import datetime
from urllib import unquote
import uuid
from hashlib import md5
from random import randrange, choice
import greenlet
from eventlet import GreenPile
from eventlet import GreenPool
from eventlet import Queue
from eventlet import spawn_n
from eventlet.green import socket
from eventlet.timeout import Timeout
import zlib

from swift.common.storage_policy import POLICIES

from swift.common.request_helpers import get_sys_meta_prefix
from swift.common.wsgi import make_subrequest
from swiftclient.client import quote
from swift.common.http import HTTP_CONTINUE
from swift.common.http import is_success
from swift.common.http import HTTP_INSUFFICIENT_STORAGE
from swift.common.http import is_client_error
from swift.common.http import HTTP_NOT_FOUND
from swift.common.http import HTTP_REQUESTED_RANGE_NOT_SATISFIABLE
from swift.proxy.controllers.base import update_headers
from swift.proxy.controllers.base import delay_denial
from swift.proxy.controllers.base import cors_validation
from swift.proxy.controllers.base import get_info
from swift.proxy.controllers.base import close_swift_conn
from swift.common.utils import split_path
from swift.common.utils import get_logger
from swift.common.utils import TRUE_VALUES
from swift.common.utils import get_remote_client
from swift.common.utils import ContextPool
from swift.common.utils import cache_from_env
from swift.common.utils import normalize_timestamp
from swift.common.utils import GreenthreadSafeIterator
from swift.proxy.server import ObjectController
from swift.proxy.server import ContainerController
from swift.proxy.server import AccountController
from swift.common.bufferedhttp import http_connect
from swift.common.exceptions import ConnectionTimeout
from swift.common.exceptions import ChunkReadTimeout
from swift.common.constraints import check_utf8
from swift.common.constraints import MAX_FILE_SIZE
from swift.common.constraints import MAX_HEADER_SIZE
from swift.common.constraints import MAX_META_NAME_LENGTH
from swift.common.constraints import MAX_META_VALUE_LENGTH
from swift.common.constraints import MAX_META_COUNT
from swift.common.constraints import MAX_META_OVERALL_SIZE
from swift.common.swob import Request
from swift.common.swob import Response
from swift.common.swob import HTTPNotFound
from swift.common.swob import HTTPPreconditionFailed
from swift.common.swob import HTTPRequestTimeout
from swift.common.swob import HTTPRequestEntityTooLarge
from swift.common.swob import HTTPBadRequest
from swift.common.swob import HTTPUnprocessableEntity
from swift.common.swob import HTTPServiceUnavailable
from swift.common.swob import HTTPClientDisconnect
from swift.common.swob import wsgify
from swift.common.swob import HTTPNotImplemented
from swift.common.swob import HeaderKeyDict
from swift.common.swob import HTTPException
from zerocloud import load_server_conf
from zerocloud.common import CLUSTER_CONFIG_FILENAME
from zerocloud.common import NODE_CONFIG_FILENAME
from zerocloud import TAR_MIMES
from zerocloud import POST_TEXT_OBJECT_SYSTEM_MAP
from zerocloud import POST_TEXT_ACCOUNT_SYSTEM_MAP
from zerocloud import merge_headers
from zerocloud import DEFAULT_EXE_SYSTEM_MAP
from zerocloud import STREAM_CACHE_SIZE
from zerocloud.common import parse_location
from zerocloud import can_run_as_daemon
from zerocloud.common import SwiftPath
from zerocloud.common import ImagePath
from zerocloud import TIMEOUT_GRACE
from zerocloud.configparser import ClusterConfigParser
from zerocloud.configparser import ClusterConfigParsingError
from zerocloud.tarstream import StringBuffer
from zerocloud.tarstream import UntarStream
from zerocloud.tarstream import TarStream
from zerocloud.tarstream import REGTYPE
from zerocloud.tarstream import BLOCKSIZE
from zerocloud.tarstream import NUL
from zerocloud.tarstream import ExtractedFile
from zerocloud.tarstream import Path
from zerocloud.tarstream import ReadError
from zerocloud.thread_pool import Zuid


ZEROVM_COMMANDS = ['open', 'api']
ZEROVM_EXECUTE = 'x-zerovm-execute'

try:
    import simplejson as json
except ImportError:
    import json

STRIP_PAX_HEADERS = ['mtime']


# Monkey patching Request to support content_type property properly
def _req_content_type_property():
    """
    Set and retrieve Request.content_type
    Strips off any charset when retrieved
    """
    def getter(self):
        if 'content-type' in self.headers:
            return self.headers.get('content-type').split(';')[0]

    def setter(self, value):
        self.headers['content-type'] = value

    return property(getter, setter,
                    doc="Retrieve and set the request Content-Type header")

Request.content_type = _req_content_type_property()


def check_headers_metadata(new_req, headers, target_type, req, add_all=False):
    prefix = 'x-%s-meta-' % target_type.lower()
    meta_count = 0
    meta_size = 0
    for key, value in headers.iteritems():
        if isinstance(value, basestring) and len(value) > MAX_HEADER_SIZE:
            raise HTTPBadRequest(body='Header value too long: %s' %
                                      key[:MAX_META_NAME_LENGTH],
                                 request=req, content_type='text/plain')
        if not key.lower().startswith(prefix):
            if add_all and key.lower() not in STRIP_PAX_HEADERS and not \
                    key.lower().startswith('x-nexe-'):
                new_req.headers[key] = value
            continue
        new_req.headers[key] = value
        key = key[len(prefix):]
        if not key:
            raise HTTPBadRequest(body='Metadata name cannot be empty',
                                 request=req, content_type='text/plain')
        meta_count += 1
        meta_size += len(key) + len(value)
        if len(key) > MAX_META_NAME_LENGTH:
            raise HTTPBadRequest(
                body='Metadata name too long: %s%s' % (prefix, key),
                request=req, content_type='text/plain')
        elif len(value) > MAX_META_VALUE_LENGTH:
            raise HTTPBadRequest(
                body='Metadata value longer than %d: %s%s' % (
                    MAX_META_VALUE_LENGTH, prefix, key),
                request=req, content_type='text/plain')
        elif meta_count > MAX_META_COUNT:
            raise HTTPBadRequest(
                body='Too many metadata items; max %d' % MAX_META_COUNT,
                request=req, content_type='text/plain')
        elif meta_size > MAX_META_OVERALL_SIZE:
            raise HTTPBadRequest(
                body='Total metadata too large; max %d'
                     % MAX_META_OVERALL_SIZE,
                request=req, content_type='text/plain')


def is_zerocloud_request(version, account, headers):
    return account and (ZEROVM_EXECUTE in headers or version in
                        ZEROVM_COMMANDS)


class GreenPileEx(GreenPile):
    """Pool with iterator semantics. Good for I/O-related tasks."""

    def __init__(self, size_or_pool=1000):
        super(GreenPileEx, self).__init__(size_or_pool)
        self.current = None

    def next(self):
        """Wait for the next result, suspending the current greenthread until it
        is available.  Raises StopIteration when there are no more results."""
        if self.counter == 0 and self.used:
            raise StopIteration()
        try:
            if not self.current:
                self.current = self.waiters.get()
            res = self.current.wait()
            self.current = None
            return res
        finally:
            if not self.current:
                self.counter -= 1


class CachedBody(object):
    """Implements caching and iterative consumption of large bodies.

    Typical (and currently, the only) uses are for managing large tarball or
    script submissions from the user. The reason why we do this is because user
    submitted content is allowed to be any size--so we don't want to hold, for
    example, an entire 5GiB tarball in memory.

    CachedBody is iterable. The ``cache`` parameter contains at all times the
    "head", while the ``read_iter`` contains the "tail".
    """

    def __init__(self, read_iter, cache=None, cache_size=STREAM_CACHE_SIZE,
                 total_size=None):
        """
        :param read_iter:
            A stream iterable.
        :param list cache:
            Defaults to None. If ``cache`` is None, constructing a `CachedBody`
            object will initialize the ``cache`` and read _at least_
            ``cache_size`` bytes from ``read_iter`` and store them in
            ``cache``. In other words, the beginning of a stream.

            If a ``cache`` is specified, this can represent the intermediate
            state of a cached body, where something is already in the cache. In
            other words, "mid-stream".
        :param int cache_size:
            Minimum amount of bytes to cache from ``read_iter``. Note: If the
            size of each chunk from ``read_iter`` is greater than
            ``cache_size``, the actual amount of bytes cached in ``cache`` will
            be the chunk size.
        :param int total_size:
            (In bytes.) If ``total_size`` is set, iterate over the
            ``read_iter`` stream until ``total_size`` counts down to 0.
            Else, just read chunks until ``read_iter`` raises a
            `StopIteration`.
        """
        self.read_iter = read_iter
        self.total_size = total_size
        if cache:
            self.cache = cache
        else:
            self.cache = []
            size = 0
            for chunk in read_iter:
                self.cache.append(chunk)
                size += len(chunk)
                if size >= cache_size:
                    break

    def __iter__(self):
        if self.total_size:
            for chunk in self.cache:
                self.total_size -= len(chunk)
                if self.total_size < 0:
                    yield chunk[:self.total_size]
                    break
                else:
                    yield chunk
            if self.total_size > 0:
                for chunk in self.read_iter:
                    self.total_size -= len(chunk)
                    if self.total_size < 0:
                        yield chunk[:self.total_size]
                        break
                    else:
                        yield chunk
            for _junk in self.read_iter:
                pass
        else:
            for chunk in self.cache:
                yield chunk
            for chunk in self.read_iter:
                yield chunk


class FinalBody(object):

    def __init__(self, resp):
        self.responses = [resp]
        self.app_iters = [resp.app_iter]

    def __iter__(self):
        try:
            for app_iter in self.app_iters:
                for chunk in app_iter:
                    yield chunk
        finally:
            self.close()

    def append(self, resp):
        self.responses.append(resp)
        self.app_iters.append(resp.app_iter)

    def close(self):
        for resp in self.responses:
            if resp.http_response:
                resp.http_response.nuke_from_orbit()
        self.responses = []
        self.app_iters = []


class NameService(object):
    """DNS-like server using a binary protocol.

    This is usable only with ZeroMQ-based networking for ZeroVM, and not
    zbroker.

    DNS resolves names to IPs; this name service resolves IDs to IP+port.
    """

    # INTEGER (4 bytes)
    INT_FMT = '!I'
    # INTEGER (4 bytes) + HOST (2 bytes)
    INPUT_RECORD_FMT = '!IH'
    # 4 bytes of string + HOST (2 bytes)
    OUTPUT_RECORD_FMT = '!4sH'
    INT_SIZE = struct.calcsize(INT_FMT)
    INPUT_RECORD_SIZE = struct.calcsize(INPUT_RECORD_FMT)
    OUTPUT_RECORD_SIZE = struct.calcsize(OUTPUT_RECORD_FMT)

    def __init__(self, peers):
        """
        :param int peers:
            Number of ZeroVM instances that will contact this name server.
        """
        self.port = None
        self.hostaddr = None
        self.peers = peers
        self.sock = None
        self.thread = None
        self.bind_map = {}
        self.conn_map = {}
        self.peer_map = {}
        self.int_pool = GreenPool()

    def start(self, pool):
        """
        :param pool:
            `GreenPool` instance
        """
        self.sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self.sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        # bind to any port, any address
        self.sock.bind(('', 0))
        self.thread = pool.spawn(self._run)
        (self.hostaddr, self.port) = self.sock.getsockname()

    def _run(self):
        while 1:
            try:
                message, peer_address = self.sock.recvfrom(65535)
                offset = 0
                peer_id = struct.unpack_from(NameService.INT_FMT, message,
                                             offset)[0]
                offset += NameService.INT_SIZE
                bind_count = struct.unpack_from(NameService.INT_FMT, message,
                                                offset)[0]
                offset += NameService.INT_SIZE
                connect_count = struct.unpack_from(NameService.INT_FMT,
                                                   message, offset)[0]
                offset += NameService.INT_SIZE
                for i in range(bind_count):
                    connecting_host, port = struct.unpack_from(
                        NameService.INPUT_RECORD_FMT, message, offset)[0:2]
                    offset += NameService.INPUT_RECORD_SIZE
                    self.bind_map.setdefault(peer_id, {})[connecting_host] = \
                        port
                self.conn_map[peer_id] = (connect_count,
                                          offset,
                                          ctypes.create_string_buffer(
                                              message[:]))
                # peer_address[0] == ip
                self.peer_map.setdefault(peer_id, {})[0] = peer_address[0]
                # peer_address[1] == port
                self.peer_map.setdefault(peer_id, {})[1] = peer_address[1]

                if len(self.peer_map) == self.peers:
                    for peer_id in self.peer_map.iterkeys():
                        (connect_count, offset, reply) = self.conn_map[peer_id]
                        for i in range(connect_count):
                            connecting_host = struct.unpack_from(
                                NameService.INT_FMT, reply, offset)[0]
                            port = self.bind_map[connecting_host][peer_id]
                            connect_to = self.peer_map[connecting_host][0]
                            if connect_to == self.peer_map[peer_id][0]:
                                # both on the same host
                                connect_to = '127.0.0.1'
                            struct.pack_into(NameService.OUTPUT_RECORD_FMT,
                                             reply, offset,
                                             socket.inet_pton(socket.AF_INET,
                                                              connect_to),
                                             port)
                            offset += NameService.OUTPUT_RECORD_SIZE
                        self.sock.sendto(reply, (self.peer_map[peer_id][0],
                                                 self.peer_map[peer_id][1]))
            except greenlet.GreenletExit:
                return
            except Exception:
                print traceback.format_exc()
                pass

    def stop(self):
        self.thread.kill()
        self.sock.close()


class ProxyQueryMiddleware(object):

    def list_account(self, account, mask=None, marker=None, request=None):
        new_req = request.copy_get()
        new_req.path_info = '/' + quote(account)
        new_req.query_string = 'format=json'
        if marker:
            new_req.query_string += '&marker=' + marker
        resp = AccountController(self.app, account).GET(new_req)
        if resp.status_int == 204:
            data = resp.body
            return []
        if resp.status_int < 200 or resp.status_int >= 300:
            raise Exception('Error querying object server')
        data = json.loads(resp.body)
        if marker:
            return data
        ret = []
        while data:
            for item in data:
                if not mask or mask.match(item['name']):
                    ret.append(item['name'])
            marker = data[-1]['name']
            data = self.list_account(account, mask=None, marker=marker,
                                     request=request)
        return ret

    def list_container(self, account, container, mask=None, marker=None,
                       request=None):
        new_req = request.copy_get()
        new_req.path_info = '/' + quote(account) + '/' + quote(container)
        new_req.query_string = 'format=json'
        if marker:
            new_req.query_string += '&marker=' + marker

        # We need to remove the authorize function here on this request in
        # order to allow an "other" or "anonymous" user to be to allowed to
        # run this container listing in some job. It is only removed from
        # `new_req`.
        # This should not allow the "other/anonymous" user to list the
        # container directly; this will only be allowed within the context of
        # execution under setuid permission. Any direct container listing from
        # this user will result in a "401 Unauthorized" (from Swift) before we
        # ever get here.
        if 'swift.authorize' in new_req.environ:
            del new_req.environ['swift.authorize']

        resp = ContainerController(self.app, account, container).GET(new_req)
        if resp.status_int == 204:
            data = resp.body
            return []
        if resp.status_int < 200 or resp.status_int >= 300:
            raise Exception('Error querying object server')
        data = json.loads(resp.body)
        if marker:
            return data
        ret = []
        while data:
            for item in data:
                if item['name'][-1] == '/':
                    continue
                if not mask or mask.match(item['name']):
                    ret.append(item['name'])
            marker = data[-1]['name']
            data = self.list_container(account, container,
                                       mask=None, marker=marker,
                                       request=request)
        return ret

    def parse_daemon_config(self, daemon_list):
        result = []
        request = Request.blank('/daemon', environ={'REQUEST_METHOD': 'POST'},
                                headers={'Content-Type': 'application/json'})
        socks = {}
        for sock, conf_file in zip(*[iter(daemon_list)] * 2):
            if socks.get(sock, None):
                self.logger.warning('Duplicate daemon config for uuid %s'
                                    % sock)
                continue
            socks[sock] = 1
            try:
                json_config = json.load(open(conf_file))
            except IOError:
                self.logger.warning('Cannot load daemon config file: %s'
                                    % conf_file)
                continue
            parser = ClusterConfigParser(self.zerovm_sysimage_devices,
                                         self.zerovm_content_type,
                                         self.parser_config,
                                         self.list_account,
                                         self.list_container,
                                         network_type=self.network_type)
            try:
                parser.parse(json_config, False, request=request)
            except ClusterConfigParsingError, e:
                self.logger.warning('Daemon config %s error: %s'
                                    % (conf_file, str(e)))
                continue
            if len(parser.nodes) != 1:
                self.logger.warning('Bad daemon config %s: too many nodes'
                                    % conf_file)
            for node in parser.nodes.itervalues():
                if node.bind or node.connect:
                    self.logger.warning('Bad daemon config %s: '
                                        'network channels are present'
                                        % conf_file)
                    continue
                if not isinstance(node.exe, ImagePath):
                    self.logger.warning('Bad daemon config %s: '
                                        'exe path must be in image file'
                                        % conf_file)
                    continue
                image = None
                for sysimage in parser.sysimage_devices.keys():
                    if node.exe.image == sysimage:
                        image = sysimage
                        break
                if not image:
                    self.logger.warning('Bad daemon config %s: '
                                        'exe is not in sysimage device'
                                        % conf_file)
                    continue
                node.channels = sorted(node.channels, key=lambda ch: ch.device)
                result.append((sock, node))
                self.logger.info('Loaded daemon config %s with UUID %s'
                                 % (conf_file, sock))
        return result

    def __init__(self, app, conf, logger=None,
                 object_ring=None, container_ring=None):
        self.app = app
        if logger:
            self.logger = logger
        else:
            self.logger = get_logger(conf, log_route='proxy-query')
        # let's load appropriate server config sections here
        load_server_conf(conf, ['app:proxy-server'])
        timeout = int(conf.get('zerovm_timeout',
                               conf.get('node_timeout', 10)))
        self.zerovm_timeout = timeout
        self.node_timeout = timeout + (TIMEOUT_GRACE * 2)
        self.immediate_response_timeout = float(conf.get(
            'interactive_timeout', timeout)) + (TIMEOUT_GRACE * 2)
        self.ignore_replication = conf.get(
            'zerovm_ignore_replication', 'f').lower() in TRUE_VALUES
        # network chunk size for all network ops
        self.network_chunk_size = int(conf.get('network_chunk_size',
                                               65536))
        # max time to wait for upload to finish, used in POST requests
        self.max_upload_time = int(conf.get('max_upload_time', 86400))
        self.client_timeout = float(conf.get('client_timeout', 60))
        self.put_queue_depth = int(conf.get('put_queue_depth', 10))
        self.conn_timeout = float(conf.get('conn_timeout', 0.5))
        # execution engine version
        self.zerovm_execute_ver = '1.0'
        # maximum size of a system map file
        self.zerovm_maxconfig = int(conf.get('zerovm_maxconfig', 65536))
        # name server hostname or ip, will be autodetected if not set
        self.zerovm_ns_hostname = conf.get('zerovm_ns_hostname')
        # name server thread pool size
        self.zerovm_ns_maxpool = int(conf.get('zerovm_ns_maxpool', 1000))
        self.zerovm_ns_thrdpool = GreenPool(self.zerovm_ns_maxpool)
        # use newest files when running zerovm executables, default - False
        self.zerovm_uses_newest = conf.get(
            'zerovm_uses_newest', 'f').lower() in TRUE_VALUES
        # use executable validation info, stored on PUT or POST,
        # to shave some time on zerovm startup
        self.zerovm_prevalidate = conf.get(
            'zerovm_prevalidate', 'f').lower() in TRUE_VALUES
        # use CORS workaround to POST execute commands, default - False
        self.zerovm_use_cors = conf.get(
            'zerovm_use_cors', 'f').lower() in TRUE_VALUES
        # Accounting: enable or disabe execution accounting data,
        # default - disabled
        self.zerovm_accounting_enabled = conf.get(
            'zerovm_accounting_enabled', 'f').lower() in TRUE_VALUES
        # Accounting: system account for storing accounting data
        self.cdr_account = conf.get('user_stats_account', 'userstats')
        # Accounting: storage API version
        self.version = 'v1'
        # default content-type for unknown files
        self.zerovm_content_type = conf.get(
            'zerovm_default_content_type', 'application/octet-stream')
        # names of sysimage devices, no sysimage devices exist by default
        devs = [(i.strip(), None)
                for i in conf.get('zerovm_sysimage_devices', '').split()
                if i.strip()]
        self.zerovm_sysimage_devices = dict(devs)
        # GET support: container for content-type association storage
        self.zerovm_registry_path = '.zvm'
        # GET support: cache config files for this amount of seconds
        self.zerovm_cache_config_timeout = 60
        self.parser_config = {
            'limits': {
                # total maximum iops for channel read or write operations
                # per zerovm session
                'reads': int(conf.get('zerovm_maxiops', 1024 * 1048576)),
                'writes': int(conf.get('zerovm_maxiops', 1024 * 1048576)),
                # total maximum bytes for a channel write operations
                # per zerovm session
                'rbytes': int(conf.get('zerovm_maxoutput', 1024 * 1048576)),
                # total maximum bytes for a channel read operations
                # per zerovm session
                'wbytes': int(conf.get('zerovm_maxinput', 1024 * 1048576))
            }
        }
        # storage policies that will be used for random node picking
        policies = [i.strip()
                    for i in conf.get('standalone_policies', '').split()
                    if i.strip()]
        self.standalone_policies = []
        for pol in policies:
            try:
                pol_idx = int(pol)
                policy = POLICIES.get_by_index(pol_idx)
            except ValueError:
                policy = POLICIES.get_by_name(pol)
            if policy:
                self.standalone_policies.append(policy.idx)
            else:
                self.logger.warning('Could not load storage policy: %s'
                                    % pol)
        if not self.standalone_policies:
            self.standalone_policies = [0]
        # use direct tcp connections (tcp) or intermediate broker (opaque)
        self.network_type = conf.get('zerovm_network_type', 'tcp')

        # 'opaque' == 'zbroker'
        # NOTE(larsbutler): for more info about zbroker, see
        # https://github.com/zeromq/zbroker
        if self.network_type == 'opaque':
            # opaque network does not support replication right now
            self.ignore_replication = True
        # list of daemons we need to lazy load
        # (first request will start the daemon)
        daemon_list = [i.strip() for i in
                       conf.get('zerovm_daemons', '').split() if i.strip()]
        self.zerovm_daemons = self.parse_daemon_config(daemon_list)
        self.uid_generator = Zuid()

    @wsgify
    def __call__(self, req):
        try:
            version, account, container, obj = split_path(req.path, 1, 4, True)
        except ValueError:
            return HTTPNotFound(request=req)
        if is_zerocloud_request(version, account, req.headers):
            exec_ver = '%s/%s' % (version, self.zerovm_execute_ver)
            exec_header_ver = req.headers.get(ZEROVM_EXECUTE, exec_ver)
            req.headers[ZEROVM_EXECUTE] = exec_header_ver
            if req.content_length and req.content_length < 0:
                return HTTPBadRequest(request=req,
                                      body='Invalid Content-Length')
            if not check_utf8(req.path_info):
                return HTTPPreconditionFailed(request=req, body='Invalid UTF8')
            controller = self.get_controller(exec_header_ver, account,
                                             container, obj)
            if not controller:
                return HTTPPreconditionFailed(request=req, body='Bad URL')
            if 'swift.trans_id' not in req.environ:
                # if this wasn't set by an earlier middleware, set it now
                trans_id = 'tx' + uuid.uuid4().hex
                req.environ['swift.trans_id'] = trans_id
                self.logger.txn_id = trans_id
            req.headers['x-trans-id'] = req.environ['swift.trans_id']
            controller.trans_id = req.environ['swift.trans_id']
            self.logger.client_ip = get_remote_client(req)
            if version:
                req.path_info_pop()
            try:
                handler = getattr(controller, req.method)
            except AttributeError:
                return HTTPPreconditionFailed(request=req,
                                              body='Bad HTTP method')
            start_time = time.time()
            # each request is assigned a unique k-sorted id
            # it will be used by QoS code to assign slots/priority
            req.headers['x-zerocloud-id'] = self.uid_generator.get()
            req.headers['x-zerovm-timeout'] = self.zerovm_timeout
            try:
                res = handler(req)
            except HTTPException as error_response:
                return error_response
            perf = time.time() - start_time
            if 'x-nexe-cdr-line' in res.headers:
                res.headers['x-nexe-cdr-line'] = \
                    '%.3f, %s' % (perf, res.headers['x-nexe-cdr-line'])
            return res
        return self.app

    def get_controller(self, version, account, container, obj):
        if version == 'open/1.0':
            if container and obj:
                return RestController(self.app, account, container, obj, self,
                                      version)
            return None
        elif version == 'api/1.0':
            if container:
                return ApiController(self.app, account, container, obj, self,
                                     version)
            return None
        return ClusterController(self.app, account, container, obj, self,
                                 version)


def select_random_partition(ring):
    partition_count = ring.partition_count
    part = randrange(0, partition_count)
    return part


class ClusterController(ObjectController):

    header_exclusions = [get_sys_meta_prefix('account'),
                         get_sys_meta_prefix('container'),
                         get_sys_meta_prefix('object'),
                         'x-backend', 'x-auth', 'content-type',
                         'content-length', 'x-storage-token', 'cookie']

    def __init__(self, app, account_name, container_name, obj_name, middleware,
                 command, **kwargs):
        ObjectController.__init__(self, app,
                                  account_name,
                                  container_name or '',
                                  obj_name or '')
        self.middleware = middleware
        self.command = command
        self.parser = ClusterConfigParser(
            {},
            self.middleware.zerovm_content_type,
            self.middleware.parser_config,
            self.middleware.list_account,
            self.middleware.list_container,
            network_type=self.middleware.network_type)
        self.exclusion_test = self.make_exclusion_test()
        self.image_resp = None
        self.cgi_env = None
        self.exe_resp = None
        self.cluster_config = ''

    def create_cgi_env(self, req):
        headers = dict(req.headers)
        keys = filter(self.exclusion_test, headers)
        for key in keys:
            headers.pop(key)
        env = {}
        env.update(('HTTP_' + k.upper().replace('-', '_'), v)
                   for k, v in headers.items())
        env['REQUEST_METHOD'] = req.method
        env['REMOTE_USER'] = req.remote_user
        env['QUERY_STRING'] = req.query_string
        env['PATH_INFO'] = req.path_info
        env['REQUEST_URI'] = req.path_qs
        return env

    def make_exclusion_test(self):
        expr = '|'.join(self.header_exclusions)
        test = re.compile(expr, re.IGNORECASE)
        return test.match

    def get_daemon_socket(self, config):
        for daemon_sock, daemon_conf in self.middleware.zerovm_daemons:
            if can_run_as_daemon(config, daemon_conf):
                return daemon_sock
        return None

    def get_standalone_policy(self):
        policy = choice(self.middleware.standalone_policies)
        ring = self.app.get_object_ring(policy)
        return ring, policy

    def _get_own_address(self):
        if self.middleware.zerovm_ns_hostname:
            addr = self.middleware.zerovm_ns_hostname
        else:
            addr = None
            object_ring = self.app.get_object_ring(0)
            partition_count = object_ring.partition_count
            part = randrange(0, partition_count)
            nodes = object_ring.get_part_nodes(part)
            for n in nodes:
                addr = _get_local_address(n)
                if addr:
                    break
        return addr

    def _make_exec_requests(self, pile, exec_requests):
        """Make execution request connections and start the execution.

        This method calls :meth:`_connect_exec_node` to start the execution.

        :param pile:
            :class:`GreenPileEx` instance.
        :param exec_requests:
            `list` of `swift.common.swob.Request` objects.
        :returns:
            `list` of `swift.common.bufferedhttp.BufferedHTTPConnection`
            objects.
        """
        exec_list = []
        known_locations = {}
        known_salts = {}
        result = []
        logger = self.app.logger.thread_locals
        for exec_request in exec_requests:
            node = exec_request.node
            account, container, obj = (
                # NOTE(larsbutler): `node.path_info` is a path like one of the
                # following:
                # - /account
                # - /account/container
                # - /account/container/object
                split_path(node.path_info, 1, 3, True))
            container_info = self.container_info(account, container,
                                                 exec_request)
            container_partition = container_info['partition']
            # nodes in the cluster which contain a replica of this container:
            container_nodes = container_info['nodes']
            if not container_nodes:
                # We couldn't find the container.
                # Probably the job referenced a container that either doesn't
                # exist or has no replicas at the moment.
                raise HTTPNotFound(request=exec_request,
                                   body='Error while fetching %s'
                                        % node.path_info)
            if obj:
                # The reauest is targetting an object.
                # (A request can be sent to /account, /account/container, or
                # /account/container/object.
                # In this case, we have an object.
                # Try to co-locate with the object:
                policy_index = exec_request.headers.get(
                    'X-Backend-Storage-Policy-Index',
                    container_info['storage_policy'])
                ring = self.app.get_object_ring(policy_index)
                partition = ring.get_part(account, container, obj)
                # ``node_iter`` is all of the candiate object servers
                # for running the job.
                node_iter = GreenthreadSafeIterator(
                    self.iter_nodes_local_first(ring,
                                                partition))
                # If the storage-policy-index was not set, we set it.
                # Why does swift need this to be set?
                # Because the object servers don't know about policies.
                # Object servers use different volumes and names depending on
                # the policy index.
                # You need to send this so the object server knows where to
                # look for files (on a particular drive, for example).
                exec_request.headers['X-Backend-Storage-Policy-Index'] = \
                    str(policy_index)
            elif container:
                # This request is targetting an /account/container.
                # We want to co-locate with the container.
                ring = self.app.container_ring
                partition = ring.get_part(account, container)
                # Same as above: ``node_iter`` is the all of the candidate
                # container servers for running the job.
                node_iter = GreenthreadSafeIterator(
                    self.app.iter_nodes(ring, partition))
                # NOTE: Containers have no storage policies. See the `obj`
                # block above.
            else:
                # The request is just targetting an account; run it anywhere.
                object_ring, policy_index = self.get_standalone_policy()
                # Similar to the `obj` case above, but just select a random
                # server to execute the job.
                partition = select_random_partition(object_ring)
                node_iter = GreenthreadSafeIterator(
                    self.iter_nodes_local_first(
                        object_ring,
                        partition))
                exec_request.headers['X-Backend-Storage-Policy-Index'] = \
                    str(policy_index)
            # Create N sets of headers
            # Usually 1, but can be more for replicates
            # FIXME(larsbutler): `_backend_requests` is a private method of the
            # Swift's ObjectController class. This is HIGHLY internal and we
            # probably shouldn't rely on it.
            exec_headers = self._backend_requests(exec_request,
                                                  node.replicate,
                                                  container_partition,
                                                  container_nodes)
            if node.skip_validation:
                for hdr in exec_headers:
                    hdr['x-zerovm-valid'] = 'true'
            node_list = [node]
            node_list.extend(node.replicas)
            # main nodes and replicas must all run on different servers
            for i, repl_node in enumerate(node_list):
                # co-location:
                location = ('%s-%d' % (node.location, i)
                            if node.location else None)
                # If we are NOT co-locating, just kick off all of the execution
                # jobs in parallel (using a GreenPileEx).
                # If we ARE co-locating, run the first of the execution jobs by
                # itself to determine a location to run, and then run all of
                # the remaining executing jobs on THAT location.
                if location and location not in known_locations:
                    # we are trying to co-locate
                    salt = uuid.uuid4().hex
                    conn = self._connect_exec_node(node_iter,
                                                   partition,
                                                   exec_request,
                                                   logger,
                                                   repl_node,
                                                   exec_headers[i],
                                                   [], salt)
                    # If we get here, we have started to execute
                    # and either recevied a success, or a continue.
                    # It can also fail.

                    # add the swift node (conn.node) to a list of known
                    # locations,
                    known_locations[location] = [conn.node]
                    known_salts[location] = salt
                    result.append(conn)
                else:
                    # If we reach this, we are either
                    # a) not co-locating
                    # OR
                    # b) we already chose a node for execution, and we try to
                    #    locate everything else there

                    # If known_nodes is [], we are not co-locating.
                    # Elif location in known_locations, we have found a
                    # location and will locate everything there.
                    known_nodes = known_locations.get(location, [])
                    exec_list.append((node_iter,
                                      partition,
                                      exec_request,
                                      logger,
                                      repl_node,
                                      exec_headers[i],
                                      known_nodes,
                                      known_salts.get(location, '0')))
        for args in exec_list:
            # spawn executions in parallel
            pile.spawn(self._connect_exec_node, *args)
        result.extend([connection for connection in pile if connection])
        return result

    def _spawn_file_senders(self, conns, pool, req):
        for conn in conns:
            conn.failed = False
            conn.queue = Queue(self.middleware.put_queue_depth)
            conn.tar_stream = TarStream()
            pool.spawn(self._send_file, conn, req.path)

    def _create_request_for_remote_object(self, data_sources, channel,
                                          req, nexe_headers, node):
        """Create a request which fetches remote objects (that is, objects to
        which a job is NOT co-located) from the object server. The request is
        executed later; we only CREATE the request here and pre-authorize it.
        """
        source_resp = None
        # channel.path = zerocloud.common.ObjPath instance
        # channel.path.path = actual string/url of the object
        load_from = channel.path.path

        # It's a swift path, but with no object, thus it only is a path to a
        # container (/account/container).
        # NOTE(larsbutler): Fetching containers as remote objects right now is
        # restricted.
        # TODO(larsbutler): Document why that is the case.
        if isinstance(channel.path, SwiftPath) and not channel.path.obj:
            return HTTPBadRequest(request=req,
                                  body='Cannot use container %s as a remote '
                                       'object reference' % load_from)

        # NOTE(larsbutler): The following is super important for understanding
        # how ZeroCloud jobs are coordinated and remain somewhat efficient.
        #
        # We reuse requests for remote objects so that we don't fetch things a
        # redundant amount of times.
        #
        # Here's about the most concise but detailed way I can state this:
        #
        # If multiple object nodes in a given job require the same _remote_
        # object (that is, an object which does not have a replica on the
        # object node), the proxy node coordinating the job--where this code is
        # running right now--will fetch each object _only once_, iteratively
        # stream the object in chunks so as to not load too much stuff into
        # memory at once, and _push_ copies of the object to each node that
        # needs it. I need to emphasize that; when an object node needs a
        # remote object to run some job, the proxy node PUSHES the object--the
        # object node never pulls. This is an optimization to avoid redundant
        # transfer of object data throughout the cluster.
        #
        # We implement this logic in the next few lines.
        #
        # Linear search for an already-existing response:
        for resp in data_sources:
            # We reuse requests, to avoid doing a bunch of redundant fetches
            # from the object server. That is, there is no need to a fetch one
            # object multiple times.
            if resp.request and load_from == resp.request.path_info:
                source_resp = resp
                break
        # response doesn't already exist
        if not source_resp:
            # copy as GET request
            source_req = req.copy_get()
            source_req.path_info = load_from
            # we don't want to pass query string
            source_req.query_string = None
            if self.middleware.zerovm_uses_newest:
                # object server will try to use the object with the most recent
                # timestamp
                # this is good because we get the latest,
                # this is bad because we have more latency because we ask
                # multiple object servers
                source_req.headers['X-Newest'] = 'true'
            if self.middleware.zerovm_prevalidate \
                    and 'boot' in channel.device:
                # FIXME: request to validate
                # proxy doesn't know it is valid or not, it can only request to
                # validate
                source_req.headers['X-Zerovm-Valid'] = 'true'
            acct, src_container_name, src_obj_name = \
                split_path(load_from, 1, 3, True)
            # We do GET only here, so we check read_acl.
            # We don't do PUTs (writes) until after the job, and this is
            # authorized in `process_server_response`
            self.authorize_job(source_req, acl='read_acl')
            source_resp = (
                # passes a request to different middleware
                ObjectController(self.app,
                                 acct,
                                 src_container_name,
                                 src_obj_name).GET(source_req))
            if source_resp.status_int >= 300:
                update_headers(source_resp, nexe_headers)
                source_resp.body = 'Error %s while fetching %s' \
                                   % (source_resp.status,
                                      source_req.path_info)
                return source_resp
            # everything went well
            source_resp.nodes = []
            # collect the data source into the "master" list
            # so it can be reused
            data_sources.append(source_resp)
        # Data sources are all Response objects: some real, some fake
        node.last_data = source_resp
        # The links between data sources and the nodes which use a given data
        # source are bi-directional.
        # - Each data source has a reference to all nodes which use it
        # - Each node has a reference to all data sources it uses
        source_resp.nodes.append({'node': node, 'dev': channel.device})
        # check if the validation passed
        if (source_resp.headers.get('x-zerovm-valid', None)
                and 'boot' in channel.device):
            # If the data source is valid and the device is the executable, we
            # can skip validation
            node.skip_validation = True
        for repl_node in node.replicas:
            # do the same thing as above for replicated nodes
            repl_node.last_data = source_resp
            source_resp.nodes.append({'node': repl_node,
                                      'dev': channel.device})

    def create_final_response(self, conns, req):
        final_body = None
        final_response = Response(request=req)
        req.cdr_log = []
        for conn in conns:
            resp = conn.resp
            if conn.error:
                conn.nexe_headers['x-nexe-error'] = \
                    conn.error.replace('\n', '')
            # Set the response status to the highest one
            if conn.resp.status_int > final_response.status_int:
                # This collects the most severe error (500 over 400, etc.).
                # NOTE(larsbutler): This might be a little weird, though. For
                # example, that means that if we get a 503 and then a 507, we
                # will return the 507 even though (as far as I can tell) there
                # is no real difference in severity between responses at the
                # same level (400, 500, etc.).
                final_response.status = conn.resp.status
            merge_headers(final_response.headers, conn.nexe_headers,
                          resp.headers)
            self._store_accounting_data(req, conn)
            if (is_success(resp.status_int)
                    and 'x-nexe-status' not in resp.headers):
                # If there's no x-nexe-status, we have hit an object server
                # that doesn't know anything about zerovm (and probably means
                # it's not running the ZeroCloud object_query middleware).
                return HTTPServiceUnavailable(
                    request=req,
                    headers=resp.headers,
                    body='objectquery middleware is not installed '
                         'or not functioning')
            if resp and resp.headers.get('x-zerovm-daemon', None):
                # We don't want to expose the daemon ID to the client, since
                # this is internal only, but we do want to notify them that
                # daemon execution succeeded (so that they can understand
                # significant differences in execution times between some
                # jobs).
                # The client doesn't/can't choose to run with a daemon; this is
                # a internal optimization.
                # The 'x-nexe-cached' indicates that a daemon was used. If this
                # is not the case, this header is omitted from the final
                # response.
                final_response.headers['x-nexe-cached'] = 'true'
            if resp and resp.content_length > 0:
                # We have some "body" to send back to the client.
                if not resp.app_iter:
                    # we might have received either a string or an iter
                    # TODO(larsbutler): it might be good wrap the right hand
                    # side in iter()
                    resp.app_iter = [resp.body]

                # this is an old feature, whereby all channels with a null path
                # just have their output concatenated and returned
                # (any writeable channel with null path, such as stderr,
                # stdout, output, etc.)
                # Typically, this applies to `stdout`, and is most often how
                # this is used.
                if final_body:
                    # this is not the first iteration of the loop
                    final_body.append(resp)
                    final_response.content_length += resp.content_length
                else:
                    # this the first iteration of the loop
                    final_body = FinalBody(resp)
                    # FIXME: `resp` needs to be closed at some point;
                    # it might not get garbage collected, so we need to nuke
                    # from orbit.
                    # NOTE: this can cause jobs to send back 0 length, no
                    # content, and no error
                    final_response.app_iter = final_body
                    final_response.content_length = resp.content_length
                    # NOTE: each output channel may have a different content
                    # type, so the content-type set here may surprise you!
                    # we assign the content type of the first channel of the
                    # first connection that we encounter
                    final_response.content_type = resp.headers['content-type']
        if self.middleware.zerovm_accounting_enabled:
            # NOTE(larsbutler): This doesn't work, and should be disabled since
            # it relies on object append support (which Swift does not have).
            self.middleware.zerovm_ns_thrdpool.spawn_n(
                self._store_accounting_data,
                req)
        if self.middleware.zerovm_use_cors and self.container_name:
            container_info = self.container_info(self.account_name,
                                                 self.container_name, req)
            if container_info.get('cors', None):
                # NOTE(larsbutler): This is a workaround for a Swift bug that
                # should be solved now.
                # Swift defines CORS per container.
                # Probably it should define CORS per account.
                # FIXME(larsbutler): We should probably test this to see if
                # it's still an issue.
                if container_info['cors'].get('allow_origin', None):
                    final_response.headers['access-control-allow-origin'] = \
                        container_info['cors']['allow_origin']
                if container_info['cors'].get('expose_headers', None):
                    final_response.headers['access-control-expose-headers'] = \
                        container_info['cors']['expose_headers']
        # Why is the etag based on the time?
        # Some browsers are very cache-hungry (like Chrome) and so if you
        # submit a job multiple times, your browser might give you a cached
        # result. And that's bad.
        #
        # Same thing for an http proxy between the client and the swift proxy.
        #
        # We cannot base the etag on the results, because we would have to
        # potentially cache GBs of data here on the proxy in order to compute
        # it, and that's crazy.
        etag = md5(str(time.time()))
        final_response.headers['Etag'] = etag.hexdigest()
        return final_response

    def read_system_map(self, read_iter, chunk_size, content_type, req):
        upload_expiration = time.time() + self.middleware.max_upload_time
        try:
            if content_type in ['application/x-gzip']:
                read_iter = gunzip_iter(read_iter, chunk_size)
            path_list = [StringBuffer(CLUSTER_CONFIG_FILENAME),
                         StringBuffer(NODE_CONFIG_FILENAME)]
            untar_stream = UntarStream(read_iter, path_list)
            for chunk in untar_stream:
                req.bytes_transferred += len(chunk)
                if time.time() > upload_expiration:
                    raise HTTPRequestTimeout(request=req)
        except (ReadError, zlib.error):
            raise HTTPUnprocessableEntity(
                request=req,
                body='Error reading %s stream'
                     % content_type)
        for buf in path_list:
            if buf.is_closed:
                self.cluster_config = buf.body
                break

    def _load_input_from_chain(self, req, chunk_size):
        data_resp = None
        if 'chain.input' in req.environ:
            chain_input = req.environ['chain.input']
            bytes_left = int(req.environ['chain.input_size']) - \
                chain_input.bytes_received
            if bytes_left > 0:
                data_resp = Response(
                    app_iter=iter(lambda: chain_input.read(
                        chunk_size), ''),
                    headers={
                        'Content-Length': bytes_left,
                        'Content-Type': req.environ['chain.input_type']})
                data_resp.nodes = []
        return data_resp

    def read_json_job(self, req, req_iter):
        etag = md5()
        upload_expiration = time.time() + self.middleware.max_upload_time
        for chunk in req_iter:
            req.bytes_transferred += len(chunk)
            if time.time() > upload_expiration:
                raise HTTPRequestTimeout(request=req)
            if req.bytes_transferred > \
                    self.middleware.zerovm_maxconfig:
                raise HTTPRequestEntityTooLarge(request=req)
            etag.update(chunk)
            self.cluster_config += chunk
        if 'content-length' in req.headers and \
                int(req.headers['content-length']) != req.bytes_transferred:
            raise HTTPClientDisconnect(request=req,
                                       body='application/json '
                                            'POST unfinished')
        etag = etag.hexdigest()
        if 'etag' in req.headers and req.headers['etag'].lower() != etag:
            raise HTTPUnprocessableEntity(request=req)

    def _tarball_cluster_config(self, req, req_iter):
        # Tarball (presumably with system.map) has been POSTed
        # we must have Content-Length set for tar-based requests
        # as it will be impossible to stream them otherwise
        if 'content-length' not in req.headers:
            raise HTTPBadRequest(request=req,
                                 body='Must specify Content-Length')
        headers = {'Content-Type': req.content_type,
                   'Content-Length': req.content_length}
        if not self.cluster_config:
            # buffer first blocks of tar file
            # and search for the system map
            cached_body = CachedBody(req_iter)
            self.read_system_map(
                cached_body.cache,
                self.middleware.network_chunk_size,
                req.content_type,
                req
            )
            if not self.cluster_config:
                raise HTTPBadRequest(request=req,
                                     body='System boot map was not '
                                          'found in request')
            req_iter = iter(cached_body)
        if not self.image_resp:
            self.image_resp = Response(app_iter=req_iter,
                                       headers=headers)
        self.image_resp.nodes = []
        try:
            cluster_config = json.loads(self.cluster_config)
            return cluster_config
        except Exception:
            raise HTTPUnprocessableEntity(body='Could not parse '
                                               'system map')

    def _system_map_cluster_config(self, req, req_iter):
        # System map was sent as a POST body
        if not self.cluster_config:
            self.read_json_job(req, req_iter)
        try:
            cluster_config = json.loads(self.cluster_config)
            return cluster_config
        except Exception:
            raise HTTPUnprocessableEntity(
                body='Could not parse system map')

    def _script_cluster_config(self, req, req_iter):
        if 'content-length' not in req.headers:
            raise HTTPBadRequest(request=req,
                                 body='Must specify Content-Length')
        cached_body = CachedBody(req_iter)
        # all scripts must start with shebang
        if not cached_body.cache[0].startswith('#!'):
            raise HTTPBadRequest(request=req,
                                 body='Unsupported Content-Type')
        buf = ''
        shebang = None
        for chunk in cached_body.cache:
            i = chunk.find('\n')
            if i > 0:
                shebang = buf + chunk[0:i]
                break
            buf += chunk
        if not shebang:
            raise HTTPBadRequest(request=req,
                                 body='Cannot find '
                                      'shebang (#!) in script')
        command_line = re.split('\s+',
                                re.sub('^#!\s*(.*)', '\\1', shebang), 1)
        sysimage = None
        args = None
        exe_path = command_line[0]
        location = parse_location(exe_path)
        if not location:
            raise HTTPBadRequest(request=req,
                                 body='Bad interpreter %s' % exe_path)
        if isinstance(location, ImagePath):
            if 'image' == location.image:
                raise HTTPBadRequest(request=req,
                                     body='Must supply image name '
                                          'in shebang url %s'
                                          % location.url)
            sysimage = location.image
        if len(command_line) > 1:
            args = command_line[1]
        params = {'exe_path': exe_path}
        if args:
            params['args'] = args.strip() + " "
        if self.container_name and self.object_name:
            template = POST_TEXT_OBJECT_SYSTEM_MAP
            location = SwiftPath.init(self.account_name,
                                      self.container_name,
                                      self.object_name)
            config = _config_from_template(params, template, location.url)
        else:
            template = POST_TEXT_ACCOUNT_SYSTEM_MAP
            config = _config_from_template(params, template, '')

        try:
            cluster_config = json.loads(config)
        except Exception:
            raise HTTPUnprocessableEntity(body='Could not parse '
                                               'system map')
        if sysimage:
            cluster_config[0]['file_list'].append({'device': sysimage})
        string_path = Path(REGTYPE,
                           'script',
                           int(req.headers['content-length']),
                           cached_body)
        stream = TarStream(path_list=[string_path])
        stream_length = stream.get_total_stream_length()
        self.image_resp = Response(app_iter=iter(stream),
                                   headers={
                                       'Content-Length': stream_length})
        self.image_resp.nodes = []

        return cluster_config

    def _get_cluster_config_data_resp(self, req):
        chunk_size = self.middleware.network_chunk_size
        # request body from user:
        req_body = req.environ['wsgi.input']
        req_iter = iter(lambda: req_body.read(chunk_size), '')
        data_resp = None

        # If x-zerovm-source header is specified in the client request,
        # we need to read the system.map from somewhere else other than the
        # request body. (In the case of sending a zapp in the request body, we
        # just read the system.map from from the zapp tarball.)
        source_header = req.headers.get('X-Zerovm-Source')
        if source_header:
            req, req_iter, data_resp = self._process_source_header(
                req, source_header
            )
        # Who will be billed for this job? Who is allowed to execute it?
        # We don't check for read_acl or write_acl; we only check for execution
        # rights.
        # By default, only the owner of the account is allowed to execute
        # things in the account.
        # Other users (including anonymous) can be given permission to others,
        # but the owner will be billed. Permission is given via
        # `X-Container-Meta-Zerovm-Suid`.
        #
        # We don't remove the auth here because we need to check for read/write
        # permissions on various objects/containers later on (in this request
        # or subsequent chained requests).
        self.authorize_job(req, remove_auth=False)

        req.path_info = '/' + self.account_name
        req.bytes_transferred = 0
        if req.content_type in TAR_MIMES:
            # Tarball (presumably with system.map) has been POSTed
            cluster_conf_dict = self._tarball_cluster_config(req, req_iter)
        elif req.content_type in 'application/json':
            cluster_conf_dict = self._system_map_cluster_config(req, req_iter)
        else:
            # assume the posted data is a script and try to execute
            cluster_conf_dict = self._script_cluster_config(req, req_iter)

        try:
            def replica_resolver(account, container):
                if self.middleware.ignore_replication:
                    return 1
                container_info = self.container_info(account, container, req)
                ring = self.app.get_object_ring(
                    container_info['storage_policy'])
                return ring.replica_count

            cluster_config = self.parser.parse(
                cluster_conf_dict,
                self.image_resp is not None,
                self.account_name,
                replica_resolver=replica_resolver,
                request=req
            )
        except ClusterConfigParsingError, e:
            self.app.logger.warn(
                'ERROR Error parsing config: %s', cluster_conf_dict)
            raise HTTPBadRequest(request=req, body=str(e))

        # NOTE(larsbutler): `data_resp` is None if there is no x-zerovm-source
        # header; see above.
        return cluster_config, data_resp

    def _process_source_header(self, req, source_header):
        # The client can execute code on ZeroCloud in 7 different ways:
        # 1) POSTing a script to /version/account; the script contents
        # must have a `#!file://...` header.
        # 2) POSTing a System Map to /version/account.
        # 3) POSTing a zapp (a tarbal with a System Map inside it)
        # 4) GET using the "open" method. That is, a object can be fetched
        #    from Swift/ZeroCloud and processed by a zapp on the fly.
        # 5) REST API using open/1.0 method: Requests are submitted to
        #    /version/account/container/zapp_object, and can include a
        #    query string).
        # 6) REST API using api/1.0 method: Requests are submitted to a
        #    /version/account/container/plus/any/arbitrary/path and query
        #    string. The `container` must exist, but can be empty. Requests
        #    will be handled by the zapp specified in the
        #    `X-Container-Meta-Rest-Endpoint` header set on the
        #    `container`. The value of this header is a swift path:
        #    `swift://account/container/zapp_object` for example.
        # 7) If the user uses methods 1, 2, or 3, and sets the
        #    `X-Zerovm-Source` header, the POST contents will instead be
        #    treated simply as input data to the request, and the request
        #    will be serviced by the zapp specified in the
        #    `X-Zerovm-Source` header.
        #
        # No matter what, all requests which reach `post_job` will be 1 of
        # 3 types:
        #   - A script
        #   - A tarball
        #   - A System Map
        #
        # RestController and ApiController (which process open/1.0 and
        # api/1.0 requests, respectively) will convert REST API requests to
        # the System Map case.
        #
        # The user can explicitly specify `X-Zerovm-Source` in cases 1-3,
        # which will change the request to case 7. Cases 5 and 6 will
        # implicitly set the `X-Zerovm-Source`; the user has no control
        # over this.

        req_body = req.environ['wsgi.input']

        source_loc = parse_location(unquote(source_header))
        if not isinstance(source_loc, SwiftPath):
            return HTTPPreconditionFailed(
                request=req,
                body='X-Zerovm-Source format is '
                     'swift://account/container/object')

        data_resp = None
        if req.content_length:
            data_resp = Response(
                app_iter=iter(
                    lambda: req_body.read(self.middleware.network_chunk_size),
                    ''
                ),
                headers={
                    'Content-Length': req.content_length,
                    'Content-Type': req.content_type})
            data_resp.nodes = []

        source_loc.expand_account(self.account_name)
        source_req = make_subrequest(req.environ, method='GET',
                                     swift_source='zerocloud')
        source_req.path_info = source_loc.path
        source_req.query_string = None
        source_req.headers['x-zerovm-source'] = req.headers['x-zerovm-source']
        # If the `X-Zerovm-Source` is set--and it is in this case--we need
        # to check that submitter of the request EITHER has Read ACL
        # permissions OR Setuid permissions to the zapp specified in
        # `X-Zerovm-Source`.
        #
        # Again, we are only checking authorization on
        # the object specified in `X-Zerovm-Source`.
        #
        # If Read ACL check succeeds, continue executing this request.
        # Else, check if Setuid is allowed.
        # If Setuid is allowed, continue executing this request.
        # Else, raise an HTTP 403 error.
        self.authorize_job(source_req, acl='read_acl', save_env=req.environ)
        sink_req = Request.blank(req.path_info,
                                 environ=req.environ, headers=req.headers)
        source_resp = source_req.get_response(self.app)
        if not is_success(source_resp.status_int):
            return source_resp
        sink_req.content_length = source_resp.content_length
        sink_req.content_type = source_resp.headers['Content-Type']
        sink_req.etag = source_resp.etag
        req_iter = iter(source_resp.app_iter)
        req = sink_req

        return req, req_iter, data_resp

    def _create_sysmap_resp(self, node):
        sysmap = node.dumps()
        return Response(app_iter=iter([sysmap]),
                        headers={'Content-Length': str(len(sysmap))})

    def post_job(self, req):
        chunk_size = self.middleware.network_chunk_size
        if 'content-type' not in req.headers:
            return HTTPBadRequest(request=req,
                                  body='Must specify Content-Type')

        cluster_config, data_resp = self._get_cluster_config_data_resp(req)

        if not self.cgi_env:
            self.cgi_env = self.create_cgi_env(req)

        # List of `swift.common.swob.Request` objects
        data_sources = []
        if self.exe_resp:
            self.exe_resp.nodes = []
            data_sources.append(self.exe_resp)

        # Address of this machine for remote machines to connect to:
        addr = self._get_own_address()
        if not addr:
            return HTTPServiceUnavailable(
                body='Cannot find own address, check zerovm_ns_hostname')
        ns_server = None

        # Start the `NameService`, if necessary.
        # If the network type is 'tcp' (ZeroVM+ZeroMQ networking) and there is
        # more than one node in the cluster config, we need a name service.
        # NOTE(larsbutler): If there's only one node, we don't need networking,
        # and thus, don't need to start the name service.
        if (self.middleware.network_type == 'tcp'
                and cluster_config.total_count > 1):
            ns_server = NameService(cluster_config.total_count)
            if self.middleware.zerovm_ns_thrdpool.free() <= 0:
                return HTTPServiceUnavailable(
                    body='Cluster slot not available',
                    request=req)
            ns_server.start(self.middleware.zerovm_ns_thrdpool)
            if not ns_server.port:
                # no free ports
                return HTTPServiceUnavailable(body='Cannot bind name service')

        # exec_requests: Send these to the appropriate object servers
        exec_requests = []
        # NOTE(larsbutler): if not data_resp and load_data_resp: chain = True
        load_data_resp = True

        # self.parser.node_list defines all of the zerovm instances --
        # including replicates -- that will be launched for this job (or for
        # this part of the chain)
        # NOTE(larsbutler): 'node' == 'object server'
        # FIXME(larsbutler): This loop is too long; we should abstract out some
        # pieces of this.
        for node in cluster_config.nodes.itervalues():
            nexe_headers = HeaderKeyDict({
                'x-nexe-system': node.name,
                'x-nexe-status': 'ZeroVM did not run',
                'x-nexe-retcode': 0,
                'x-nexe-etag': '',
                'x-nexe-validation': 0,
                'x-nexe-cdr-line': '0.0 0.0 0 0 0 0 0 0 0 0',
                'x-nexe-policy': '',
                'x-nexe-colocated': '0'
            })
            path_info = req.path_info
            # Copy the request path, environ, and headers from the client
            # request into the new request.
            # NOTE(larsbutler): The `path_info` is overwritten below. The
            # `Request.blank` method just requires a valid path for the first
            # arg.
            exec_request = Request.blank(path_info,
                                         environ=req.environ,
                                         headers=req.headers)
            # Each node has its own request `path_info`.
            # `node.path_info` can be:
            #   - /account
            #   - /account/container
            #   - /account/container/object
            # If the path is just /account, there no object to co-locate with
            # so it doesn't matter _where_ we execute this request.
            # If the path contains container or container/object, we need to
            # co-locate it with the container or object (whichever the case may
            # be).
            exec_request.path_info = node.path_info
            if 'zerovm.source' in req.environ:
                # `zerovm.source` is needed for authorization and job chaining.
                # It's a "hidden" variable; the client never sees it, neither
                # in the response nor the untrusted code execution environment.
                exec_request.environ['zerovm.source'] = (
                    req.environ['zerovm.source']
                )

            # x-zerovm-access is set to "GET" (read) or "PUT" (write)
            exec_request.headers['x-zerovm-access'] = node.access
            # NOTE(larsbutler): We don't set the etag here because we're
            # iteratively streaming our request contents, and thus, we don't
            # know the entire contents of the stream. (In order to calculate a
            # proper hash/etag, we would have to buffer the entire contents,
            # which could be several GiBs.
            exec_request.etag = None
            # We are sending a tar stream to object server, that's why we set
            # the content-type here.
            exec_request.content_type = TAR_MIMES[0]
            # We need to check for access to the local object.
            # The job is co-located to the file it processes.
            # Object is never fetched; it is just read from disk.
            # We do the authorization check BEFORE the job is sent to the
            # target object server.
            # We can only co-locate with one object; if a node has access to
            # more objects, the "remote" objects must be fetched.
            #
            # Job description can specify the "attach" attribute to explicitly
            # attaches to a specific object/node as the local object.
            # Otherwise, we apply some heuristics to decide where to put the
            # job. `ConfigParser` has this logic. Here the gist of the
            # heuristics:
            # - Read trumps write. If you have read and write, you will be
            #   attached to read.
            # - If you have a "script" channel; although it is "read", you will
            #   never be attached to that because more than likely, the data
            #   you process with the script willl be much bigger than the
            #   script itself.
            # - If you have multiple read channels and no script, behavior is
            #   undefined.
            acl = None
            if node.access == 'GET':
                acl = 'read_acl'
            elif node.access == 'PUT':
                acl = 'write_acl'
            if acl:
                # NOTE(larsbutler): We could probably remove the auth here,
                # since this request is a new one, and won't be used later.
                # The `exec_request` is only used to send a job to the object
                # server.
                self.authorize_job(exec_request, acl=acl, remove_auth=False)

            # chunked encoding handling looks broken in Swift
            # but let's leave it here, maybe somebody will fix it:
            # exec_request.content_length = None
            # exec_request.headers['transfer-encoding'] = 'chunked'

            # FIXME(larsbutler): x-account-name is a deprecated header,
            # probably we can remove this. Also, account-name could be
            # different from request url, since we have the
            # acl/setuid execution feature available to other users.
            exec_request.headers['x-account-name'] = self.account_name
            # Proxy sends timestamp to each object server in advance
            # for each object that the objserver will create.
            # So if this job creates multiple objects, it will use this
            # timestamp.
            # FIXME(larsbutler): Just create ONE timestamp per job (outside of
            # this loop at the beginning of `post_job`).
            exec_request.headers['x-timestamp'] = \
                normalize_timestamp(time.time())

            # NOTE(larsbutler): We explicitly set `x-zerovm-valid` to 'false';
            # the client CANNOT be allowed to dictate this. Validity will be
            # resolved later.
            exec_request.headers['x-zerovm-valid'] = 'false'

            # If there are multiple nodes but they are not connected (no
            # networking), we can use the default pool/scheduling:
            exec_request.headers['x-zerovm-pool'] = 'default'

            # If we are using networking, use the 'cluster' pool.
            # FIXME(larsbutler): If we are using zvm:// URLs to communicate
            # between execution groups (zvm:// means something like "pipes"),
            # we should probably also use the cluster pool. With the current
            # implementation, a job with zvm:// URLs would not use the cluster
            # pool. Probably this needs to be revisited.
            if len(node.connect) > 0 or len(node.bind) > 0:
                # Execution node operation depends on connections to other
                # nodes.
                # We use a special pool for cluster jobs because we want to
                # start all nodes in a cluster job at the same time; we don't
                # want some nodes in the job to be queued and leave others
                # waiting.
                # NOTE(larsbutler): In other words, we run all at once (or not
                # at all, apparently?).
                exec_request.headers['x-zerovm-pool'] = 'cluster'
            if ns_server:
                node.name_service = 'udp:%s:%d' % (addr, ns_server.port)

            if cluster_config.total_count > 1:
                # FIXME(larsbutler): Make `build_connect_string` return a
                # value, and assign instead of mutating `node` inside the
                # method.
                self.parser.build_connect_string(
                    node, req.headers.get('x-trans-id'))

                # Replication directive comes from system.map ("replicate")
                # which is usually, 0, 1 or 3 (since we have by default 3
                # replicas in a typical cluster; depends on the swift config,
                # though).
                if node.replicate > 1:
                    # We need an ADDITIONAL N-1 copies to bring the total
                    # number of copies to N.
                    for i in range(0, node.replicate - 1):
                        node.replicas.append(deepcopy(node))
                        # Generate an ID for the replica.
                        # IDs are generated in the following way:
                        # Say there are 4 nodes in the job, with IDs 1, 2, 3,
                        # and 4 respectively.
                        # We calculate replica IDs such that the first set of
                        # replicas will have sequential IDs start after the
                        # highest ID in the original node set.
                        # The second set of replicas does that same thing,
                        # except the IDs start after the highest ID in the
                        # first set of replicas. For example:
                        #
                        # orig: 1  2  3  4  <-- 4 execution nodes in the job
                        # rep1: 5  6  7  8  <-- first set of replicas
                        # rep2: 9  10 11 12 <-- second set of replicas
                        # rep3: 13 14 15 16 <-- third set
                        # and so on..
                        node.replicas[i].id = \
                            node.id + (i + 1) * len(cluster_config.nodes)
            # each exec requests needs a copy of the cgi env stuff (vars)
            node.copy_cgi_env(request=exec_request, cgi_env=self.cgi_env)

            # we create a fake data source
            # a fake response containing the system.map just now created for
            # this object server:
            resp = self._create_sysmap_resp(node)
            # adds the response to two places:
            # 1) add it to "master" list of data sources ->why? so we don't
            # redundantly fetch data; we cache and reuse
            # 2) add it to node-specific list of data sources
            node.add_data_source(data_sources, resp, 'sysmap')
            for repl_node in node.replicas:
                # repeat the above for each replica:
                repl_node.copy_cgi_env(request=exec_request,
                                       cgi_env=self.cgi_env)
                resp = self._create_sysmap_resp(repl_node)
                repl_node.add_data_source(data_sources, resp, 'sysmap')
            # for each node, we want to know the remote objects it needs to
            # reference
            channels = node.get_list_of_remote_objects()
            for ch in channels:
                # Translate channels into data sources.
                # A data source is just a response from the object server.
                # It's a byte stream with headers (and content-type).
                # Some responses could have no content-type, because we know
                # their content-type beforehand (system.map is json, for
                # example).
                # TODO(larsbutler): raise errors instead of returning them
                error = self._create_request_for_remote_object(data_sources,
                                                               ch,
                                                               req,
                                                               nexe_headers,
                                                               node)
                if error:
                    return error
            # the user sent us a zapp or other tar image
            if self.image_resp:
                # image is user data and must go last
                node.last_data = self.image_resp
                self.image_resp.nodes.append({'node': node,
                                              'dev': 'image'})
                for repl_node in node.replicas:
                    repl_node.last_data = self.image_resp
                    self.image_resp.nodes.append({'node': repl_node,
                                                  'dev': 'image'})

            # NOTE(larsbutler): The following block was added for job chaining.
            if node.data_in:
                # We have "data" in the request body.
                # That is, we used x-zerovm-source and request request body
                # contains "data input" instead of an application tarball or
                # system.map.
                # `data_resp` is None, there was no x-zerovm-source in the
                # client request.
                if not data_resp and load_data_resp:
                    # This can occur at any point in a chained job (either the
                    # first part of the chain or a subsequent part).
                    data_resp = self._load_input_from_chain(req, chunk_size)
                    load_data_resp = False
                if data_resp:
                    node.last_data = data_resp
                    data_resp.nodes.append({'node': node,
                                            'dev': 'stdin'})
                    for repl_node in node.replicas:
                        repl_node.last_data = data_resp
                        data_resp.nodes.append({'node': repl_node,
                                                'dev': 'stdin'})

            exec_request.node = node
            exec_request.resp_headers = nexe_headers
            # If possible, try to submit the job to a daemon.
            # This is an internal optimization.
            sock = self.get_daemon_socket(node)
            if sock:
                exec_request.headers['x-zerovm-daemon'] = str(sock)
            exec_requests.append(exec_request)
        # End `exec_requests` loop

        # We have sent request for each "data source" (object to be used by the
        # execution), and we have a response which includes the headers (but we
        # haven't read the body yet). See `_create_request_for_remote_object`.
        # This is includes fake data sources that we made up on the fly (like
        # for system.maps).

        if self.image_resp and self.image_resp.nodes:
            # if the user sent a tar/zapp, then we will have an image_resp (not
            # None) or image was specified by x-zerovm-source
            data_sources.append(self.image_resp)
        if data_resp and data_resp.nodes:
            # if and only if image resp is set by by x-zerovm-source
            data_sources.append(data_resp)
        tstream = TarStream()
        for data_src in data_sources:
            # this loop calculates the sizes of all of the streams for the
            # nodes
            # NOTE: 1 stream will have multiple files
            # we have one stream channel between the proxy and each object node
            for n in data_src.nodes:
                # Get node size. "Node size" is the size of the stream that
                # will be passed to that node.
                if not getattr(n['node'], 'size', None):
                    # if it's not set, initialize to 0
                    n['node'].size = 0
                n['node'].size += len(tstream.create_tarinfo(
                    ftype=REGTYPE,
                    name=n['dev'],
                    size=data_src.content_length))
                n['node'].size += \
                    TarStream.get_archive_size(data_src.content_length)
        # We have calclated the content_length of the requests

        # Using greenlet pool/pile, start execution of each part of the job on
        # the object nodes.
        pile = GreenPileEx(cluster_config.total_count)
        conns = self._make_exec_requests(pile, exec_requests)
        if len(conns) < cluster_config.total_count:
            self.app.logger.exception(
                'ERROR Cannot find suitable node to execute code on')
            for conn in conns:
                close_swift_conn(getattr(conn, 'resp'))
            return HTTPServiceUnavailable(
                body='Cannot find suitable node to execute code on')

        for conn in conns:
            if hasattr(conn, 'error'):
                if hasattr(conn, 'resp'):
                    close_swift_conn(conn.resp)
                return Response(app_iter=[conn.error],
                                status="%d %s" % (conn.resp.status,
                                                  conn.resp.reason),
                                headers=conn.nexe_headers)

        _attach_connections_to_data_sources(conns, data_sources)

        # chunked encoding handling looks broken in Swift
        # but let's leave it here, maybe somebody will fix it
        # chunked = req.headers.get('transfer-encoding')
        chunked = False
        try:
            with ContextPool(cluster_config.total_count) as pool:
                self._spawn_file_senders(conns, pool, req)
                for data_src in data_sources:
                    # FIXME: don't attach bytes_transferred to this object
                    # kinda ugly
                    data_src.bytes_transferred = 0
                    _send_tar_headers(chunked, data_src)
                    while True:
                        with ChunkReadTimeout(self.middleware.client_timeout):
                            try:
                                data = next(data_src.app_iter)
                            except StopIteration:
                                # TODO: return num of bytes transfered
                                error = _finalize_tar_streams(chunked,
                                                              data_src,
                                                              req)
                                if error:
                                    return error
                                break
                        # TODO: return num of bytes transfered
                        error = _send_data_chunk(chunked, data_src, data, req)
                        if error:
                            return error
                    if data_src.bytes_transferred < data_src.content_length:
                        return HTTPClientDisconnect(
                            request=req,
                            body='data source %s dead' % data_src.__dict__)
                for conn in conns:
                    if conn.queue.unfinished_tasks:
                        # wait for everything to finish
                        conn.queue.join()
                    conn.tar_stream = None
        except ChunkReadTimeout, err:
            self.app.logger.warn(
                'ERROR Client read timeout (%ss)', err.seconds)
            self.app.logger.increment('client_timeouts')
            # FIXME: probably we need to expand on what caused the error
            # it could be ANY data source that timed out
            # this can include the client request; this is ALLLLLL data sources
            # Only above do we begin to read from all sources
            return HTTPRequestTimeout(request=req)
        except (Exception, Timeout):
            print traceback.format_exc()
            self.app.logger.exception(
                'ERROR Exception causing client disconnect')
            return HTTPClientDisconnect(request=req, body='exception')

        # we have successfully started execution and sent all data sources
        for conn in conns:
            # process all of the responses in parallel
            pile.spawn(self._process_response, conn, req)

        # x-zerovm-deferred means, run the job async and close the client
        # connection asap -> results are saved into swift
        do_defer = req.headers.get('x-zerovm-deferred', 'never').lower()
        if do_defer == 'always':
            # 0 means timeout immediately
            defer_timeout = 0
        elif do_defer == 'auto':
            defer_timeout = self.middleware.immediate_response_timeout
        else:
            # None means no timeout
            defer_timeout = None
        conns = []
        try:
            with Timeout(seconds=defer_timeout):
                for conn in pile:
                    if conn:
                        conns.append(conn)
        except Timeout:
            # if timeout is 0, we immediately get an exception (the case where
            # x-zerovm-deferred is specified)
            # if timeout is None, we never get an exception
            # if timeout is > 0, we might get a timeout exception

            def store_deferred_response(deferred_url):
                """
                :param str deferred_url:
                    Path where we will try to store the result object.
                    A swift:// url.
                """
                # GreenPileEx iterator blocks until the next result is ready
                for conn in pile:
                    if conn:
                        conns.append(conn)
                resp = self.create_final_response(conns, req)
                path = SwiftPath(deferred_url)
                container_info = get_info(self.app, req.environ.copy(),
                                          path.account, path.container,
                                          ret_not_found=True)
                if container_info['status'] == HTTP_NOT_FOUND:
                    # container doesn't exist (yet)
                    # try to create the container
                    cont_req = Request(req.environ.copy())
                    cont_req.path_info = '/%s/%s' % (path.account,
                                                     path.container)
                    cont_req.method = 'PUT'
                    cont_resp = \
                        ContainerController(self.app,
                                            path.account,
                                            path.container).PUT(cont_req)
                    if cont_resp.status_int >= 300:
                        self.app.logger.warn(
                            'Failed to create deferred container: %s'
                            % cont_req.url)
                        return
                # this would normally get returned to the client, but since
                # we're deferred, the client has already disconnected
                resp.input_iter = iter(resp.app_iter)

                # subsequent consumption of this response expects an object
                # with a read() method
                def iter_read(chunk_size=None):
                    # if a chunk size is specified, it shouldn't be a big deal
                    # because this will only be read by other parts of
                    # ZeroCloud middleware.
                    if chunk_size is None:
                        return ''.join(resp.input_iter)
                    chunk = next(resp.input_iter)
                    return chunk
                resp.read = iter_read

                # Create the new object to store the results (from `resp`):
                deferred_put = Request(req.environ.copy())
                deferred_put.path_info = path.path
                deferred_put.method = 'PUT'
                deferred_put.environ['wsgi.input'] = resp
                deferred_put.content_length = resp.content_length
                deferred_resp = ObjectController(self.app,
                                                 path.account,
                                                 path.container,
                                                 path.obj).PUT(deferred_put)
                if deferred_resp.status_int >= 300:
                    # TODO(larsbutler): should this be a critical error?
                    self.app.logger.warn(
                        'Failed to create deferred object: %s : %s'
                        % (deferred_put.url, deferred_resp.status))
                    # we don't return here, at least we should try to store the
                    # headers (before we think about returning an error)
                report = self._create_deferred_report(resp.headers)
                resp.input_iter = iter([report])
                deferred_put = Request(req.environ.copy())
                # we not only store the result (in an object),
                # but we also store the headers (in a separate object)
                deferred_put.path_info = path.path + '.headers'
                deferred_put.method = 'PUT'
                deferred_put.environ['wsgi.input'] = resp
                deferred_put.content_length = len(report)
                deferred_resp = \
                    ObjectController(self.app,
                                     path.account,
                                     path.container,
                                     path.obj + '.headers').PUT(deferred_put)
                if deferred_resp.status_int >= 300:
                    self.app.logger.warn(
                        'Failed to create deferred object: %s : %s'
                        % (deferred_put.url, deferred_resp.status))
                # End `store_deferred_response`.

            # request url can be:
            #   /account
            #   /account/container
            #   /account/container/object

            # we will have a container if the request url includes a container
            if self.container_name:
                # either it's the container where the job was running
                container = self.container_name
            else:
                # or it's the configured directory, like `.zvm`
                container = self.middleware.zerovm_registry_path

            # We will have an object name if the request url includes an
            # object.
            if self.object_name:
                obj = self.object_name
            else:
                # Otherwise we just generate an object ID.
                obj = 'job-%s' % uuid.uuid4()
            # TODO(larsbutler): Use `SwiftPath.create_url()` here.
            deferred_path = SwiftPath.init(self.account_name, container, obj)
            resp = Response(request=req,
                            body=deferred_path.url)
            # spawn it with any thread that can handle it
            spawn_n(store_deferred_response, deferred_path.url)
            # FIXME(larsbutler): We might want to stop the name server at the
            # end of store_deferred_response, instead of here.
            if ns_server:
                ns_server.stop()
            # return immediately, our job is likely still running
            return resp
            # end of deferred/timeout case
        if ns_server:
            # If we are running with networking and have a NameService server,
            # stop the server.
            ns_server.stop()
        return self.create_final_response(conns, req)

    def process_server_response(self, conn, request, resp):
        # Process object server response (responses from requests which are
        # co-located with an object).
        conn.resp = resp
        if not is_success(resp.status_int):
            conn.error = resp.body
            return conn
        if resp.content_length == 0:
            return conn
        if 'x-nexe-error' in resp.headers:
            resp.status = 500
        node = conn.cnode
        untar_stream = UntarStream(resp.app_iter)
        bytes_transferred = 0
        while True:
            try:
                data = next(untar_stream.tar_iter)
            except StopIteration:
                break
            untar_stream.update_buffer(data)
            info = untar_stream.get_next_tarinfo()
            while info:
                # We can store arbitrary key-value metadata in the tar; let's
                # grab those.
                headers = info.get_headers()
                chan = node.get_channel(device=info.name)
                if not chan:
                    conn.error = 'Channel name %s not found' % info.name
                    return conn
                # If there is a path, something needs to be saved back into the
                # Swift data store.
                if not chan.path:
                    # If there is no path, send the data back to the client.
                    app_iter = iter(CachedBody(
                        untar_stream.tar_iter,
                        cache=[untar_stream.block[info.offset_data:]],
                        total_size=info.size))
                    resp.app_iter = app_iter
                    resp.content_length = headers['Content-Length']
                    resp.content_type = headers['Content-Type']
                    check_headers_metadata(resp, headers, 'object', request,
                                           add_all=True)
                    if resp.headers.get('status'):
                        resp.status = resp.headers['status']
                        del resp.headers['status']
                    return conn
                dest_req = Request.blank(chan.path.path,
                                         environ=request.environ,
                                         headers=request.headers)
                # NOTE(larsbutler): We have to override the `path_info`, since
                # the `Request.blank` chops any path down to /<account>/auth.
                dest_req.path_info = chan.path.path
                dest_req.query_string = None
                dest_req.method = 'PUT'
                dest_req.headers['content-length'] = headers['Content-Length']
                untar_stream.to_write = info.size
                untar_stream.offset_data = info.offset_data
                dest_req.environ['wsgi.input'] = ExtractedFile(untar_stream)
                dest_req.content_type = headers['Content-Type']
                check_headers_metadata(dest_req, headers, 'object', request)
                try:
                    # Check if the user is authorized to write to the specified
                    # channel/object.
                    # If user has write permissions, continue.
                    # Else, check is user has Setuid.
                    # If user has Setuid, continue.
                    # Else, raise a 403.
                    self.authorize_job(dest_req, acl='write_acl')
                    dest_resp = \
                        ObjectController(self.app,
                                         chan.path.account,
                                         chan.path.container,
                                         chan.path.obj).PUT(dest_req)
                except HTTPException as error_resp:
                    dest_resp = error_resp
                if dest_resp.status_int >= 300:
                    conn.error = 'Status %s when putting %s' \
                                 % (dest_resp.status, chan.path.path)
                    if resp.status_int < dest_resp.status_int:
                        resp.status = dest_resp.status
                    return conn
                info = untar_stream.get_next_tarinfo()
            bytes_transferred += len(data)
        untar_stream = None
        # we should be done reading, but just for sanity, we set the
        # content-length to 0 so we don't try to read anyway
        # TODO(larsbutler): If it's not already 0, we should probably raise an
        # error. We need double check this.
        resp.content_length = 0
        return conn

    def _process_response(self, conn, request):
        conn.error = None
        chunk_size = self.middleware.network_chunk_size
        server_response = conn.resp
        if conn.resp:
            # success
            resp = Response(status='%d %s' %
                                   (server_response.status,
                                    server_response.reason),
                            app_iter=iter(lambda: server_response.read(
                                chunk_size), ''),
                            headers=dict(server_response.getheaders()))
        else:
            # got "continue"
            # no response yet; we need to read it
            try:
                with Timeout(self.middleware.node_timeout):
                    server_response = conn.getresponse()
                    resp = Response(status='%d %s' %
                                           (server_response.status,
                                            server_response.reason),
                                    app_iter=iter(lambda: server_response.read(
                                        chunk_size), ''),
                                    headers=dict(server_response.getheaders()))
            except (Exception, Timeout):
                self.app.exception_occurred(
                    conn.node, 'Object',
                    'Trying to get final status of POST to %s'
                    % request.path_info)
                resp = HTTPRequestTimeout(
                    body='Timeout: trying to get final status of POST '
                         'to %s' % request.path_info)
        resp.http_response = server_response
        return self.process_server_response(conn, request, resp)

    def _connect_exec_node(self, obj_nodes, part, request,
                           logger_thread_locals, cnode, request_headers,
                           known_nodes, salt):
        """Do the actual execution.

        :param obj_nodes:
            Iterator of object node `dict` objects, each with the following
            keys:

            * id
            * ip
            * port
            * zone
            * region
            * device
            * replication_ip
            * replication_port
        :param int part:
            Partition ID.
        :param request:
            `swift.common.swob.Request` instance.
        :param logger_thread_locals:
            2-tuple of ("transaction-id", logging.Logger).
        :param cnode:
            :class:`zerocloud.configparser.ZvmNode` instance.
        :param request_headers:
            Dict of request headers.
        :param list known_nodes:
            See `_make_exec_requests` for usage info.
        :param str salt:
            Generated unique ID for a given server.
        """
        self.app.logger.thread_locals = logger_thread_locals
        conn = None
        for node in chain(known_nodes, obj_nodes):
            # this loop is trying to connect to candidate object servers (for
            # execution) and send the execution request headers

            # if we get an exception, we can keep trying on other nodes
            if ((known_nodes and node in known_nodes)
                    # this is the first node we are trying to use for
                    # co-location
                    or (not known_nodes and cnode.location)):
                request_headers['x-nexe-colocated'] = \
                    '%s:%s:%s' % (salt, node['ip'], node['port'])
            try:
                with ConnectionTimeout(self.middleware.conn_timeout):
                    request_headers['Expect'] = '100-continue'
                    request_headers['Content-Length'] = str(cnode.size)

                    # NOTE(larsbutler): THIS line right here kicks off the
                    # actual execution.
                    conn = http_connect(node['ip'], node['port'],
                                        node['device'], part, request.method,
                                        request.path_info, request_headers)
                # If we get here, it means object started reading our
                # requests, read all headers until the body, processed the
                # headers, and has now issued a read on the body
                # but we haven't sent any data yet
                with Timeout(self.middleware.node_timeout):
                    resp = conn.getexpect()
                # node == the swift object server we are connected to
                conn.node = node
                # cnode == the zerovm node
                conn.cnode = cnode
                conn.nexe_headers = request.resp_headers
                if resp.status == HTTP_CONTINUE:
                    conn.resp = None
                    return conn
                elif is_success(resp.status):
                    conn.resp = resp
                    return conn
                elif resp.status == HTTP_INSUFFICIENT_STORAGE:
                    # increase the error count for this node
                    # to optimize, the proxy server can use this count to limit
                    # the number of requests send to this particular object
                    # node.
                    self.app.error_limit(node,
                                         'ERROR Insufficient Storage')
                    conn.error = 'Insufficient Storage'
                    # the final response is `resp`, which is error
                    # could be disk failed, etc.
                    conn.resp = resp
                    resp.nuke_from_orbit()
                elif is_client_error(resp.status):
                    conn.error = resp.read()
                    conn.resp = resp
                    if resp.status == HTTP_NOT_FOUND:
                        # it could be "not found" because either a) the object
                        # doesn't exist or b) just this server doesn't have a
                        # copy

                        # container or object was either not found, or due to
                        # eventual consistency, it can't be found here right
                        # now
                        # so, we try to continue and look for it elsewhere

                        # the 404 error here doesn't include the url, so we
                        # include it here (so the client so they know which url
                        # has a problem)
                        conn.error = 'Error %d %s while fetching %s' \
                                     % (resp.status, resp.reason,
                                        request.path_info)
                    else:
                        # don't keep trying; this is user error
                        return conn
                else:
                    # unknown error
                    # some 500 error that's not insufficient storage
                    self.app.logger.warn('Obj server failed with: %d %s'
                                         % (resp.status, resp.reason))
                    conn.error = resp.read()
                    conn.resp = resp
                    resp.nuke_from_orbit()
                    # we still keep trying; maybe we'll have better luck on
                    # another replicate (could be a problem with threadpool,
                    # etc.)
            except Exception:
                self.app.exception_occurred(node, 'Object',
                                            'Expect: 100-continue on %s'
                                            % request.path_info)
                if getattr(conn, 'resp'):
                    conn.resp.nuke_from_orbit()
                conn = None
        if conn:
            return conn

    def _store_accounting_data(self, request, connection=None):
        # FIXME(larsbutler): We're not even sure if this still works.
        txn_id = request.environ['swift.trans_id']
        acc_object = datetime.datetime.utcnow().strftime('%Y/%m/%d.log')
        if connection:
            # If connection is not None, only cache accounting data on the
            # input ``request`` object; nothing actually gets saved.
            body = '%s %s %s (%s) [%s]\n' % (
                datetime.datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S'),
                txn_id,
                connection.nexe_headers['x-nexe-system'],
                connection.nexe_headers['x-nexe-cdr-line'],
                connection.nexe_headers['x-nexe-status'])
            request.cdr_log.append(body)
            self.app.logger.info('zerovm-cdr %s %s %s (%s) [%s]'
                                 % (self.account_name,
                                    txn_id,
                                    connection.nexe_headers['x-nexe-system'],
                                    connection.nexe_headers['x-nexe-cdr-line'],
                                    connection.nexe_headers['x-nexe-status']))
        else:
            # Here, something is actually saved
            body = ''.join(request.cdr_log)
            append_req = Request.blank('/%s/%s/%s/%s'
                                       % (self.middleware.version,
                                          self.middleware.cdr_account,
                                          self.account_name,
                                          acc_object),
                                       headers={'X-Append-To': '-1',
                                                'Content-Length': len(body),
                                                'Content-Type': 'text/plain'},
                                       body=body)
            append_req.method = 'POST'
            resp = append_req.get_response(self.app)
            if resp.status_int >= 300:
                self.app.logger.warn(
                    'ERROR Cannot write stats for account %s',
                    self.account_name)

    def _create_deferred_report(self, headers):
        # just dumps headers as a json object for now
        return json.dumps(dict(headers))

    @delay_denial
    @cors_validation
    def GET(self, req):
        return HTTPNotImplemented(request=req)

    @delay_denial
    @cors_validation
    def PUT(self, req):
        return HTTPNotImplemented(request=req)

    @delay_denial
    @cors_validation
    def DELETE(self, req):
        return HTTPNotImplemented(request=req)

    @delay_denial
    @cors_validation
    def HEAD(self, req):
        return HTTPNotImplemented(request=req)

    @delay_denial
    @cors_validation
    def POST(self, req):
        return self.post_job(req)

    def authorize_job(self, req, acl=None, remove_auth=True, save_env=None):
        """
        Authorizes a request using the acl attribute and authorize() function
        from environment

        :param req: `swob.Request` instance that we are authorizing
        :param acl: type of the acl we read from container info
        :param remove_auth: if True will remove authorize() from environment
        :param save_env: if not None will save container info in the
                         provided environment dictionary

        :raises: various HTTPException instances
        """
        container_info = {'meta': {}}
        source_header = req.headers.get('X-Zerovm-Source')
        try:
            if 'swift.authorize' in req.environ:
                version, account, container, obj = \
                    split_path(req.path, 2, 4, True)
                if 'zerovm.source' in req.environ:
                    container_info = req.environ['zerovm.source']
                    source_header = container_info['meta'].get('rest-endpoint')

                if container:
                    container_info = self.container_info(account, container)

                if acl:
                    req.acl = container_info.get(acl)
                aresp = req.environ['swift.authorize'](req)
                if aresp and container_info:
                    setuid_acl = container_info['meta'].get('zerovm-suid')
                    endpoint = container_info['meta'].get('rest-endpoint')
                    if all((source_header, setuid_acl, endpoint)) \
                            and endpoint == source_header:
                        req.acl = setuid_acl
                        aresp = req.environ['swift.authorize'](req)
                if aresp:
                    raise aresp
                if remove_auth:
                    del req.environ['swift.authorize']
        except ValueError:
            raise HTTPNotFound(request=req)
        finally:
            if save_env:
                save_env['zerovm.source'] = container_info
            req.acl = None


class RestController(ClusterController):

    config_path = None

    def _get_content_config(self, req, content_type):
        req.template = None
        cont = self.middleware.zerovm_registry_path
        obj = '%s/config' % content_type
        config_path = '/%s/%s/%s' % (self.account_name, cont, obj)
        memcache_client = cache_from_env(req.environ)
        memcache_key = 'zvmconf' + config_path
        if memcache_client:
            req.template = memcache_client.get(memcache_key)
            if req.template:
                return
        config_req = req.copy_get()
        config_req.path_info = config_path
        config_req.query_string = None
        config_resp = ObjectController(
            self.app, self.account_name, cont, obj).GET(config_req)
        if config_resp.status_int == 200:
            req.template = ''
            for chunk in config_resp.app_iter:
                req.template += chunk
                if self.middleware.zerovm_maxconfig < len(req.template):
                    req.template = None
                    return HTTPRequestEntityTooLarge(
                        request=config_req,
                        body='Config file at %s is too large' % config_path)
        if memcache_client and req.template:
            memcache_client.set(
                memcache_key,
                req.template,
                time=float(self.middleware.zerovm_cache_config_timeout))

    @delay_denial
    @cors_validation
    def GET(self, req):
        resp = self.handle_request(req)
        if resp:
            return resp
        if self.object_name:
            return self.handle_object_open(req)
        return HTTPNotImplemented(request=req)

    @delay_denial
    @cors_validation
    def POST(self, req):
        resp = self.handle_request(req)
        if resp:
            return resp
        return HTTPNotImplemented(request=req)

    @delay_denial
    @cors_validation
    def PUT(self, req):
        resp = self.handle_request(req)
        if resp:
            return resp
        return HTTPNotImplemented(request=req)

    @delay_denial
    @cors_validation
    def DELETE(self, req):
        resp = self.handle_request(req)
        if resp:
            return resp
        return HTTPNotImplemented(request=req)

    @delay_denial
    @cors_validation
    def HEAD(self, req):
        resp = self.handle_request(req)
        if resp:
            return resp
        return HTTPNotImplemented(request=req)

    def load_config(self, req, config_path):
        self.config_path = config_path
        memcache_client = cache_from_env(req.environ)
        memcache_key = 'zvmapp' + config_path.path
        if memcache_client:
            config = memcache_client.get(memcache_key)
            if config:
                self.cluster_config = config
                return None
        config_req = req.copy_get()
        config_req.path_info = config_path.path
        config_req.query_string = None
        buffer_length = self.middleware.zerovm_maxconfig * 2
        config_req.range = 'bytes=0-%d' % (buffer_length - 1)
        config_resp = ObjectController(
            self.app, self.account_name,
            self.container_name, self.object_name).GET(config_req)
        if config_resp.status_int == HTTP_REQUESTED_RANGE_NOT_SATISFIABLE:
            return None
        if not is_success(config_resp.status_int) or \
                config_resp.content_length > buffer_length:
            return config_resp
        if config_resp.content_type in TAR_MIMES:
            chunk_size = self.middleware.network_chunk_size
            config_req.bytes_transferred = 0
            self.read_system_map(config_resp.app_iter, chunk_size,
                                 config_resp.content_type, config_req)
            if memcache_client and self.cluster_config:
                memcache_client.set(
                    memcache_key,
                    self.cluster_config,
                    time=float(self.middleware.zerovm_cache_config_timeout))
        return None

    def handle_request(self, req):
        swift_path = SwiftPath.init(self.account_name, self.container_name,
                                    self.object_name)
        error = self.load_config(req, swift_path)
        if error:
            return error
        # if we successfully got config, we know that we have a zapp in hand
        if self.cluster_config:
            self.cgi_env = self.create_cgi_env(req)
            req.headers['x-zerovm-source'] = self.config_path.url
            req.method = 'POST'
            return self.post_job(req)
        return None

    def handle_object_open(self, req):
        obj_req = req.copy_get()
        obj_req.method = 'HEAD'
        obj_req.query_string = None
        run = False
        if self.object_name[-len('.nexe'):] == '.nexe':
            # let's get a small speedup as it's quite possibly an executable
            obj_req.method = 'GET'
            run = True
        controller = ObjectController(
            self.app,
            self.account_name,
            self.container_name,
            self.object_name)
        handler = getattr(controller, obj_req.method, None)
        obj_resp = handler(obj_req)
        if not is_success(obj_resp.status_int):
            return obj_resp
        content = obj_resp.content_type
        if content == 'application/x-nexe':
            run = True
        elif run:
            # speedup did not succeed...
            # still need to read the whole response
            for _junk in obj_resp.app_iter:
                pass
            obj_req.method = 'HEAD'
            run = False
        template = DEFAULT_EXE_SYSTEM_MAP
        error = self._get_content_config(obj_req, content)
        if error:
            return error
        if obj_req.template:
            template = obj_req.template
        elif not run:
            return HTTPNotFound(request=req,
                                body='No application registered for %s'
                                     % content)
        location = SwiftPath.init(self.account_name,
                                  self.container_name,
                                  self.object_name)
        self.cluster_config = _config_from_template(req.params, template,
                                                    location.url)
        self.cgi_env = self.create_cgi_env(req)
        post_req = Request.blank('/%s' % self.account_name,
                                 environ=req.environ,
                                 headers=req.headers)
        post_req.method = 'POST'
        post_req.content_type = 'application/json'
        post_req.query_string = req.query_string
        if obj_req.method in 'GET':
            self.exe_resp = obj_resp
        return self.post_job(post_req)


class ApiController(RestController):

    def load_config(self, req, config_path):
        memcache_client = cache_from_env(req.environ)
        memcache_key = 'zvmapp' + config_path.path
        if memcache_client:
            config = memcache_client.get(memcache_key)
            if config:
                self.cluster_config, config_path_url = config
                self.config_path = SwiftPath(config_path_url)
                return None
        container_info = self.container_info(config_path.account,
                                             config_path.container)
        # This is the zapp which services the endpoint.
        source = container_info['meta'].get('rest-endpoint')
        if not source:
            raise HTTPNotFound(request=req,
                               body='No API endpoint configured for '
                                    'container %s' % self.container_name)
        # REST endpoint source must be a full URL, with no wildcards.
        # Otherwise, the account would be ambiguous since the executing user is
        # not necessarily the owner of the container.
        self.config_path = parse_location(unquote(source))
        config_req = req.copy_get()
        config_req.path_info = self.config_path.path
        config_req.query_string = None
        buffer_length = self.middleware.zerovm_maxconfig * 2
        config_req.range = 'bytes=0-%d' % (buffer_length - 1)
        # Set x-zerovm-source to the zapp configured for the
        # x-container-meta-rest-endpoint.
        config_req.headers['X-Zerovm-Source'] = self.config_path.url
        # We check for read permissions to the zapp which services this
        # endpoint.
        # If user has read permissions, continue.
        # Else, check if user has Setuid permissions.
        # If user has Setuid permissions, continue.
        # Else, raise and HTTP 403.
        self.authorize_job(config_req, acl='read_acl',
                           save_env=req.environ)
        config_resp = ObjectController(
            self.app,
            self.config_path.account,
            self.config_path.container,
            # `read_acl` is checked above, since we are doing a GET/read (not a
            # PUT/write) to the object server
            self.config_path.obj).GET(config_req)
        if config_resp.status_int == HTTP_REQUESTED_RANGE_NOT_SATISFIABLE:
            return None
        if not is_success(config_resp.status_int) or \
                config_resp.content_length > buffer_length:
            return config_resp
        if config_resp.content_type in TAR_MIMES:
            chunk_size = self.middleware.network_chunk_size
            config_req.bytes_transferred = 0
            self.read_system_map(config_resp.app_iter, chunk_size,
                                 config_resp.content_type, config_req)
        elif config_resp.content_type in ['application/json']:
            config_req.bytes_transferred = 0
            config_req.content_length = config_resp.content_length
            self.read_json_job(config_req, config_resp.app_iter)
        if memcache_client and self.cluster_config:
            memcache_client.set(
                memcache_key,
                (self.cluster_config, self.config_path.url),
                time=float(self.middleware.zerovm_cache_config_timeout))
        return None


def _config_from_template(params, template, url):
    for k, v in params.iteritems():
        if k == 'object_path':
            continue
        ptrn = r'\{\.%s(|=[^\}]+)\}'
        ptrn = ptrn % k
        template = re.sub(ptrn, v, template)
    config = template.replace('{.object_path}', url)
    config = re.sub(r'\{\.[^=\}]+=?([^\}]*)\}', '\\1', config)
    return config


def _attach_connections_to_data_sources(conns, data_sources):
    """
    :param conns:
        `list` of `swift.common.bufferedhttp.BufferedHTTPConnection` objects.
    :param data_sources:
        `list` of `swift.common.swob.Request` objects.
    """
    for data_src in data_sources:
        data_src.conns = []
        for node in data_src.nodes:
            for conn in conns:
                if conn.cnode is node['node']:
                    conn.last_data = node['node'].last_data
                    data_src.conns.append({'conn': conn, 'dev': node['dev']})


def _queue_put(conn, data, chunked):
    conn['conn'].queue.put('%x\r\n%s\r\n'
                           % (len(data), data) if chunked else data)


def _send_tar_headers(chunked, data_src):
    for conn in data_src.conns:
        name = conn['dev']
        if name == 'image' and data_src.content_type == 'application/x-gzip':
            name = 'image.gz'
        info = conn['conn'].tar_stream.create_tarinfo(
            ftype=REGTYPE,
            name=name,
            size=data_src.content_length)
        for chunk in conn['conn'].tar_stream.serve_chunk(info):
            if not conn['conn'].failed:
                _queue_put(conn, chunk, chunked)


def _send_data_chunk(chunked, data_src, data, req):
    data_src.bytes_transferred += len(data)
    if data_src.bytes_transferred > MAX_FILE_SIZE:
        return HTTPRequestEntityTooLarge(request=req)
    for conn in data_src.conns:
        for chunk in conn['conn'].tar_stream.serve_chunk(data):
            if not conn['conn'].failed:
                _queue_put(conn, chunk, chunked)
            else:
                return HTTPServiceUnavailable(request=req)


def _finalize_tar_streams(chunked, data_src, req):
    blocks, remainder = divmod(data_src.bytes_transferred, BLOCKSIZE)
    if remainder > 0:
        nulls = NUL * (BLOCKSIZE - remainder)
        for conn in data_src.conns:
            for chunk in conn['conn'].tar_stream.serve_chunk(nulls):
                if not conn['conn'].failed:
                    _queue_put(conn, chunk, chunked)
                else:
                    return HTTPServiceUnavailable(request=req)
    for conn in data_src.conns:
        if conn['conn'].last_data is data_src:
            if conn['conn'].tar_stream.data:
                data = conn['conn'].tar_stream.data
                if not conn['conn'].failed:
                    _queue_put(conn, data, chunked)
                else:
                    return HTTPServiceUnavailable(request=req)
            if chunked:
                conn['conn'].queue.put('0\r\n\r\n')


def _get_local_address(node):
    s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    s.connect((node['ip'], node['port']))
    result = s.getsockname()[0]
    s.shutdown(socket.SHUT_RDWR)
    s.close()
    return result


def gunzip_iter(data_iter, chunk_size):
    dec = zlib.decompressobj(16 + zlib.MAX_WBITS)
    unc_data = ''
    for chunk in data_iter:
        while dec.unconsumed_tail:
            while len(unc_data) < chunk_size and dec.unconsumed_tail:
                unc_data += dec.decompress(dec.unconsumed_tail,
                                           chunk_size - len(unc_data))
            if len(unc_data) == chunk_size:
                yield unc_data
                unc_data = ''
            if unc_data and dec.unconsumed_tail:
                chunk += dec.unconsumed_tail
                break
        unc_data += dec.decompress(chunk, chunk_size - len(unc_data))
        if len(unc_data) == chunk_size:
            yield unc_data
            unc_data = ''
    if unc_data:
        yield unc_data


def filter_factory(global_conf, **local_conf):
    """
    paste.deploy app factory for creating WSGI proxy apps.
    """
    conf = global_conf.copy()
    conf.update(local_conf)

    def query_filter(app):
        return ProxyQueryMiddleware(app, conf)

    return query_filter
