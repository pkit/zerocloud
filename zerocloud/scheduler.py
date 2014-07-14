from eventlet import Timeout, sleep, spawn_n
from random import randrange, random
from eventlet.green import socket
from swift.common.bufferedhttp import http_connect, http_connect_raw
from swift.common.exceptions import ConnectionTimeout
from swift.common.http import is_success
from swift.common.ring import Ring
from swift.common.swob import Request, Response, \
    HTTPOk, HTTPNotFound, HTTPException, HTTPServerError
from swift.common.utils import get_logger, split_path, json
from swift.common.wsgi import WSGIContext, make_subrequest
from zerocloud.common import load_server_conf
from zerocloud.queue import QueueClient, QUEUE_ENDPOINT


def _guess_own_address(ring):
    part = randrange(0, ring.partition_count)
    nodes = ring.get_part_nodes(part)
    result = (None, None)
    for node in nodes:
        try:
            s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
            s.connect((node['ip'], node['port']))
            result = s.getsockname()
            s.shutdown(socket.SHUT_RDWR)
            s.close()
        except Exception:
            pass
        if result[0]:
            break
    return result


class Scheduler(object):
    def __init__(self, app, conf, logger=None, ring=None, client=None):
        if logger:
            self.logger = logger
        else:
            self.logger = get_logger(conf, log_route='scheduler')
        self.election = Election(conf, ring=ring)
        self.interval = int(conf.get('interval', 5))
        if client:
            self.client = client
        else:
            self.client = QueueClient(app, conf)
        self.watched_queueus = \
            [i.strip()
             for i in conf.get('watched_queues', '').split() if i.strip()]

    def start_task(self, req):
        return HTTPOk()

    def send_to_master(self, account, queue, req):
        return self.election.send_to_master(account, queue, req,
                                            self.election.send_request)

    def server(self):
        sleep(random() * self.interval)  # start with some entropy
        while True:
            try:
                sleep(self.interval)
            except Exception:
                self.logger.exception('Exception occurred while '
                                      'running scheduler')
                sleep(self.interval)
                continue


class Election(object):
    def __init__(self, conf, ring=None):
        self.swift_dir = conf.get('swift_dir', '/etc/swift')
        self.ring = ring or Ring(self.swift_dir,
                                 ring_name='scheduler')
        host_address = conf.get('host_address')
        if host_address:
            ip, port = host_address.split(':', 1)
        else:
            ip, port = _guess_own_address(self.ring)
        verified = False
        for node in self.ring.devs:
            if ip == node['ip'] and port == node['port']:
                verified = True
                break
        if not verified:
            path = self.ring.serialized_path
            raise ValueError('Could not find address: %s:%d '
                             'in ring %s' % (ip, port, path))
        self.addr = (ip, port)
        self.masters = set()
        self.slaves = set()
        self.conn_timeout = float(conf.get('conn_timeout', 0.5))
        self.node_timeout = int(conf.get('node_timeout', 10))

    def part_iter(self, partition):
        nodes = self.ring.get_part_nodes(partition)
        for n in nodes:
            addr = (n['ip'], n['port'])
            if addr != self.addr:
                yield addr

    def move_to_slaves(self, addr):
        self.slaves.add(addr)
        self.masters.discard(addr)

    def move_to_masters(self, addr):
        self.masters.add(addr)
        self.slaves.discard(addr)

    def _return_ok(self, from_addr):
        ok = False
        if from_addr in self.slaves:
            ok = True
        if from_addr > self.addr:
            self.move_to_masters(from_addr)
        else:
            self.move_to_slaves(from_addr)
        return ok

    def response(self, from_addr):
        if self._return_ok(from_addr):
            return HTTPOk()
        return HTTPNotFound()

    def send_ping(self, addr, req):
        path = '/%s/%d' % addr
        try:
            with ConnectionTimeout(self.conn_timeout):
                conn = http_connect(addr[0], addr[1], 'scheduler', '0', 'GET',
                                    path,
                                    headers={'Client-Id': '%s:%d' % self.addr})
                with Timeout(self.node_timeout):
                    response = conn.getresponse()
                    response.read()
                    if is_success(response.status):
                        return HTTPOk(request=req)
        except Timeout:
            pass
        return None

    def send_to_master(self, account, container, req, action):
        partition = self.ring.get_part(account, container)
        for addr in self.part_iter(partition):
            if addr in self.slaves:
                continue
            resp = action(addr, req)
            if resp:
                if self.addr > addr:
                    self.move_to_slaves(addr)
                else:
                    self.move_to_masters(addr)
                return resp
            else:
                self.move_to_slaves(addr)
        return None

    def send_request(self, addr, req):
        try:
            headers = req.headers
            headers['x-scheduler-id'] = '%s:%d' % self.addr
            with ConnectionTimeout(self.conn_timeout):
                conn = http_connect_raw(addr[0], addr[1], req.method,
                                        req.path, headers=headers,
                                        query_string=req.query_string)
                with Timeout(self.node_timeout):
                    response = conn.getresponse()
                    headers = dict(response.getheaders())
                    if 'X-Scheduler-Master' in headers:
                        del headers['X-Scheduler-Master']
                        status = '%d %s' % (response.status, response.reason)
                        body = response.read()
                        resp = Response(status=status, headers=headers,
                                        body=body, request=req)
                        return resp
                    response.read()
        except Timeout:
            pass
        return None


class SchedulerContext(WSGIContext):
    def __init__(self, wsgi_app, scheduler, account, queue):
        super(SchedulerContext, self).__init__(wsgi_app)
        self.scheduler = scheduler
        self.account = account
        self.queue_name = queue

    def handle_message(self, env, start_response):
        resp = self._app_call(env)
        if is_success(self._get_status_int()):
            data = ''
            for chunk in resp:
                data += chunk
            msg = json.loads(data)
            msg_path = msg['claim_id']
            path = '/%s/%s/%s/claim/%s?ttl=%d' % \
                   (QUEUE_ENDPOINT, self.account,
                    self.queue_name, msg_path, self.scheduler.ttl)
            req = make_subrequest(env, method='POST', path=path,
                                  swift_source='scheduler')
            claim_resp = req.get_response(self.app)
            if is_success(claim_resp.status_int):
                msg = json.loads(claim_resp.body)
                spawn_n(self.scheduler.schedule(msg))
                self.scheduler.add_watch(msg)
            resp = [data]
        start_response(self._response_status, self._response_headers,
                       self._response_exc_info)
        return resp


class SchedulerMiddleware(object):
    def __init__(self, app, conf, logger=None, ring=None):
        self.app = app
        self.conf = conf
        if logger:
            self.logger = logger
        else:
            self.logger = get_logger(conf, log_route='scheduler')
        load_server_conf(conf, ['app:object-server'])
        self.account = conf.get('scheduler_account', 'scheduler')
        self.in_queue = conf.get('incoming_queue', 'incoming')
        self.out_queue = conf.get('outgoing_queue', 'outgoing')
        self.client = QueueClient(app, conf)
        self.in_container = '%s%s' % (self.client.queue_prefix, self.in_queue)
        self.election = Election(conf, ring=ring)
        self.watched_queueus = \
            [i.strip()
             for i in conf.get('watched_queues', '').split() if i.strip()]

    def __call__(self, env, start_response):
        req = Request(env)
        try:
            version, account, queue, _rest = split_path(req.path, 1, 4, True)
        except ValueError:
            return self.app(env, start_response)
        try:
            if version == QUEUE_ENDPOINT and account and queue:
                if req.method == 'POST' and _rest == 'message' and \
                        queue in self.watched_queueus:
                    context = SchedulerContext(self.app, self, account, queue)
                    return context.handle_message(env, start_response)
        except HTTPException as error_response:
            return error_response(env, start_response)
        except (Exception, Timeout):
            self.logger.exception('ERROR Unhandled exception in request')
            return HTTPServerError(request=req)(env, start_response)
        return self.app(env, start_response)


def filter_factory(global_conf, **local_conf):
    """
    paste.deploy app factory for creating WSGI proxy apps.
    """
    conf = global_conf.copy()
    conf.update(local_conf)

    def scheduler_filter(app):
        return SchedulerMiddleware(app, conf)

    return scheduler_filter
