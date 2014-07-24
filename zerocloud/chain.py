from time import time
from swift.common.swob import Request, HTTPRequestEntityTooLarge
from swift.common.utils import split_path, get_logger
from swift.common.wsgi import WSGIContext, make_subrequest
from zerocloud.proxyquery import ZEROVM_EXECUTE, ZEROVM_COMMANDS


class ChainContext(WSGIContext):
    def __init__(self, wsgi_app, version, account, middleware):
        super(ChainContext, self).__init__(wsgi_app)
        self.version = version
        self.account = account
        self.middleware = middleware

    def handle_chain(self, env, start_response):
        total_time = 0
        while True:
            start = time()
            resp = self._app_call(env)
            total_time += time() - start
            print total_time
            if not self._response_header_value('x-zerovm-execute') or \
                    self._response_header_value('content-type') != \
                    'application/json' or int(self._response_header_value(
                    'content-length')) > self.middleware.zerovm_maxconfig or \
                    total_time > self.middleware.chain_timeout:
                self._response_headers.append(('X-Chain-Total-Time', '%.3f' %
                                               total_time))
                start_response(self._response_status, self._response_headers,
                               self._response_exc_info)
                return resp
            # we are chaining
            req = Request(env)
            data = ''
            bytes_transferred = 0
            for chunk in resp:
                data += chunk
                bytes_transferred += len(chunk)
                if bytes_transferred > self.middleware.zerovm_maxconfig:
                    return HTTPRequestEntityTooLarge(request=req)
            path = '/v1/%s' % self.account
            headers = {'X-Zerovm-Execute': '1.0',
                       'Content-Type': 'application/json'}
            new_req = make_subrequest(env, method='POST', path=path, body=data,
                                      headers=headers, swift_source='chain')
            env = new_req.environ


class ChainMiddleware(object):
    def __init__(self, app, conf, logger=None):
        self.app = app
        if logger:
            self.logger = logger
        else:
            self.logger = get_logger(conf, log_route='chain')
        self.zerovm_maxconfig = int(conf.get('zerovm_maxconfig', 65536))
        self.chain_timeout = int(conf.get('chain_timeout', 20))

    def __call__(self, env, start_response):
        req = Request(env)
        try:
            version, account, _rest = split_path(req.path, 2, 3, True)
        except ValueError:
            return self.app(env, start_response)
        if account and \
            (ZEROVM_EXECUTE in req.headers
             or version in ZEROVM_COMMANDS):
            context = ChainContext(self.app, version, account, self)
            return context.handle_chain(env, start_response)
        return self.app(env, start_response)


def filter_factory(global_conf, **local_conf):
    """
    paste.deploy app factory for creating WSGI proxy apps.
    """
    conf = global_conf.copy()
    conf.update(local_conf)

    def chain_filter(app):
        return ChainMiddleware(app, conf)

    return chain_filter
