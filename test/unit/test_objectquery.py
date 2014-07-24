from contextlib import contextmanager
from StringIO import StringIO
import logging
from posix import rmdir
import unittest
import os
from time import time
from eventlet import GreenPool
from unittest.case import SkipTest
from hashlib import md5
from tempfile import mkstemp, mkdtemp
from shutil import rmtree
from copy import copy
import math
import tarfile
from eventlet.wsgi import Input

from swift.common import utils
from swift.common.swob import Request
from swift.common.utils import mkdirs, normalize_timestamp, get_logger
from swift.obj.server import ObjectController
from test.unit import FakeLogger, create_random_numbers, get_sorted_numbers, \
    create_tar
from test.unit import trim

from test_proxyquery import ZEROVM_DEFAULT_MOCK
from zerocloud.common import ZvmNode, ACCESS_READABLE, ACCESS_WRITABLE, \
    NodeEncoder, ACCESS_CDR, \
    parse_location, ACCESS_RANDOM, TAR_MIMES
from zerocloud import objectquery
from zerocloud.thread_pool import WaitPool, Zuid

try:
    import simplejson as json
except ImportError:
    import json


def get_headers(self):
    headers = {}
    for key, value in self.pax_headers.items():
        if isinstance(key, unicode):
            key = key.encode('utf-8')
        if isinstance(value, unicode):
            value = value.encode('utf-8')
        headers[key.title()] = value
    return headers

tarfile.TarInfo.get_headers = get_headers


class FakeLoggingHandler(logging.Handler):

    def __init__(self, *args, **kwargs):
        self.reset()
        logging.Handler.__init__(self, *args, **kwargs)

    def emit(self, record):
        self.messages[record.levelname.lower()].append(record.getMessage())

    def reset(self):
        self.messages = {
            'debug': [],
            'info': [],
            'warning': [],
            'error': [],
            'critical': [],
        }


class FakeApp(ObjectController):

    def __init__(self, conf):
        ObjectController.__init__(self, conf)
        self.bytes_per_sync = 1
        self.fault = False

    def __call__(self, env, start_response):
        if self.fault:
            raise Exception
        ObjectController.__call__(self, env, start_response)


class OsMock():

    def __init__(self):
        self.closed = False
        self.unlinked = False
        self.path = os.path
        self.SEEK_SET = os.SEEK_SET

    def close(self, fd):
        self.closed = True
        raise OSError

    def unlink(self, fd):
        self.unlinked = True
        raise OSError

    def write(self, fd, str):
        return os.write(fd, str)

    def read(self, fd, bufsize):
        return os.read(fd, bufsize)

    def lseek(self, fd, pos, how):
        return os.lseek(fd, pos, how)


class TestObjectQuery(unittest.TestCase):

    def setUp(self):
        utils.HASH_PATH_SUFFIX = 'endcap'
        self.testdir = \
            os.path.join(mkdtemp(), 'tmp_test_object_server_ObjectController')
        mkdirs(os.path.join(self.testdir, 'sda1', 'tmp'))
        self.conf = {
            'devices': self.testdir,
            'mount_check': 'false',
            'disable_fallocate': 'true',
            'zerovm_sysimage_devices': ('sysimage1 /opt/zerovm/sysimage1 '
                                        'sysimage2 /opt/zerovm/sysimage2')
        }
        self.obj_controller = FakeApp(self.conf)
        self.app = objectquery.ObjectQueryMiddleware(
            self.obj_controller, self.conf, logger=FakeLogger())
        self.app.zerovm_maxoutput = 1024 * 1024 * 10
        self.zerovm_mock = None
        self.uid_generator = Zuid()

    def tearDown(self):
        """ Tear down for testing swift.object_server.ObjectController """
        rmtree(os.path.dirname(self.testdir))
        if self.zerovm_mock:
            os.unlink(self.zerovm_mock)

    def setup_zerovm_query(self, mock=None):
        # ensure that python executable is used
        zerovm_mock = ZEROVM_DEFAULT_MOCK
        if mock:
            fd, zerovm_mock = mkstemp()
            os.write(fd, mock)
            os.close(fd)
            self.zerovm_mock = zerovm_mock
        self.app.zerovm_exename = ['python', zerovm_mock]
        # do not set it lower than 2 * BLOCKSIZE (2 * 512)
        # it will break tar RPC protocol
        self.app.app.network_chunk_size = 2 * 512
        randomnumbers = create_random_numbers(10)
        self.create_object(randomnumbers)
        self._nexescript = 'return pickle.dumps(sorted(id))'
        self._sortednumbers = get_sorted_numbers()
        self._randomnumbers_etag = md5()
        self._randomnumbers_etag.update(randomnumbers)
        self._randomnumbers_etag = self._randomnumbers_etag.hexdigest()
        self._sortednumbers_etag = md5()
        self._sortednumbers_etag.update(self._sortednumbers)
        self._sortednumbers_etag = self._sortednumbers_etag.hexdigest()
        self._nexescript_etag = md5()
        self._nexescript_etag.update(self._nexescript)
        self._nexescript_etag = self._nexescript_etag.hexdigest()
        self._stderr = '\nfinished\n'
        self._emptyresult = '(l.'
        self._emptyresult_etag = md5()
        self._emptyresult_etag.update(self._emptyresult)
        self._emptyresult_etag = self._emptyresult_etag.hexdigest()

    def create_object(self, body, path='/sda1/p/a/c/o'):
        timestamp = normalize_timestamp(time())
        headers = {'X-Timestamp': timestamp,
                   'Content-Type': 'application/octet-stream'}
        req = Request.blank(path,
                            environ={'REQUEST_METHOD': 'PUT'}, headers=headers)
        req.body = body
        resp = req.get_response(self.app)
        self.assertEquals(resp.status_int, 201)

    def zerovm_object_request(self):
        req = Request.blank('/sda1/p/a/c/o',
                            environ={'REQUEST_METHOD': 'POST'},
                            headers={'Content-Type': 'application/x-gtar',
                                     'x-zerovm-execute': '1.0',
                                     'x-account-name': 'a',
                                     'x-zerovm-access': 'GET'})
        req.headers['x-zerocloud-id'] = self.uid_generator.get()
        return req

    def zerovm_free_request(self):
        req = Request.blank('/sda1/p/a',
                            environ={'REQUEST_METHOD': 'POST'},
                            headers={'Content-Type': 'application/x-gtar',
                                     'x-zerovm-execute': '1.0',
                                     'x-account-name': 'a',
                                     'x-zerovm-access': ''})
        req.headers['x-zerocloud-id'] = self.uid_generator.get()
        return req

    def test_tmpdir_mkstemp_creates_dir(self):
        tmpdir = os.path.join(self.testdir, 'sda1', 'tmp')
        os.rmdir(tmpdir)
        with objectquery.TmpDir(tmpdir, 'sda1').mkstemp():
            self.assert_(os.path.exists(tmpdir))

    def test_QUERY_realzvm(self):
        raise SkipTest
        orig_exe = self.app.zerovm_exename
        orig_sysimages = self.app.zerovm_sysimage_devices
        try:
            self.app.zerovm_sysimage_devices['python-image'] = (
                '/media/40G/zerovm-samples/zshell/zpython2/python.tar'
            )
            self.setup_zerovm_query()
            self.app.zerovm_exename = ['/opt/zerovm/bin/zerovm']
            req = self.zerovm_free_request()
            req.headers['x-zerovm-daemon'] = 'asdf'
            conf = ZvmNode(1, 'python', parse_location(
                'file://python-image:python'), args='hello.py')
            conf.add_new_channel('stdout', ACCESS_WRITABLE)
            conf.add_new_channel(
                'python-image', ACCESS_READABLE | ACCESS_RANDOM)
            conf.add_new_channel('image', ACCESS_CDR, removable='yes')
            # print json.dumps(conf, cls=NodeEncoder, indent=2)
            conf = json.dumps(conf, cls=NodeEncoder)
            sysmap = StringIO(conf)
            image = open('/home/kit/python-script.tar', 'rb')
            with self.create_tar({'sysmap': sysmap, 'image': image}) as tar:
                length = os.path.getsize(tar)
                req.body_file = Input(open(tar, 'rb'), length)
                resp = self.app.zerovm_query(req)
                print ['x-zerovm-daemon', resp.headers.get('x-zerovm-daemon',
                                                           '---')]
                print ['x-nexe-cdr-line', resp.headers['x-nexe-cdr-line']]
                if resp.content_type in TAR_MIMES:
                    fd, name = mkstemp()
                    for chunk in resp.app_iter:
                        os.write(fd, chunk)
                    os.close(fd)
                    tar = tarfile.open(name)
                    names = tar.getnames()
                    members = tar.getmembers()
                    for n, m in zip(names, members):
                        print [n, tar.extractfile(m).read()]
                else:
                    print resp.body
        finally:
            self.app.zerovm_exename = orig_exe
            self.app.zerovm_sysimage_devices = orig_sysimages

    def test_QUERY_sort(self):
        self.setup_zerovm_query()
        req = self.zerovm_object_request()
        nexefile = StringIO(self._nexescript)
        conf = ZvmNode(1, 'sort', parse_location('swift://a/c/exe'))
        conf.add_new_channel(
            'stdin', ACCESS_READABLE, parse_location('swift://a/c/o'))
        conf.add_new_channel('stdout', ACCESS_WRITABLE)
        conf = json.dumps(conf, cls=NodeEncoder)
        sysmap = StringIO(conf)
        with create_tar({'boot': nexefile, 'sysmap': sysmap}) as tar:
            length = os.path.getsize(tar)
            req.body_file = Input(open(tar, 'rb'), length)
            req.content_length = length
            resp = self.app.zerovm_query(req)
            fd, name = mkstemp()
            for chunk in resp.app_iter:
                os.write(fd, chunk)
            os.close(fd)
            self.assertEqual(os.path.getsize(name), resp.content_length)
            tar = tarfile.open(name)
            names = tar.getnames()
            members = tar.getmembers()
            self.assertIn('stdout', names)
            self.assertEqual(names[-1], 'stdout')
            self.assertEqual(members[-1].size, len(self._sortednumbers))
            file = tar.extractfile(members[-1])
            self.assertEqual(file.read(), self._sortednumbers)
            self.assertEqual(resp.headers['x-nexe-retcode'], '0')
            self.assertEqual(resp.headers['x-nexe-status'], 'ok.')
            self.assertEqual(resp.headers['x-nexe-validation'], '0')
            self.assertEqual(resp.headers['x-nexe-system'], 'sort')
            timestamp = normalize_timestamp(time())
            self.assertEqual(math.floor(float(resp.headers['X-Timestamp'])),
                             math.floor(float(timestamp)))
            self.assertEquals(
                resp.headers['content-type'], 'application/x-gtar')
            # self.assertEqual(self.app.logger.log_dict['info'][0][0][0],
            #    'Zerovm CDR: 0 0 0 0 1 46 2 56 0 0 0 0')

    def test_QUERY_sort_textout(self):
        self.setup_zerovm_query()
        req = self.zerovm_object_request()
        nexefile = StringIO('return str(sorted(id))')
        conf = ZvmNode(1, 'sort', parse_location('swift://a/c/exe'))
        conf.add_new_channel(
            'stdin', ACCESS_READABLE, parse_location('swift://a/c/o'))
        conf.add_new_channel('stdout', ACCESS_WRITABLE)
        conf = json.dumps(conf, cls=NodeEncoder)
        sysmap = StringIO(conf)
        with create_tar({'boot': nexefile, 'sysmap': sysmap}) as tar:
            length = os.path.getsize(tar)
            req.body_file = Input(open(tar, 'rb'), length)
            req.content_length = length
            resp = self.app.zerovm_query(req)
            fd, name = mkstemp()
            for chunk in resp.app_iter:
                os.write(fd, chunk)
            os.close(fd)
            self.assertEqual(os.path.getsize(name), resp.content_length)
            tar = tarfile.open(name)
            names = tar.getnames()
            members = tar.getmembers()
            self.assertIn('stdout', names)
            self.assertEqual(names[-1], 'stdout')
            file = tar.extractfile(members[-1])
            self.assertEqual(file.read(), '[0, 1, 2, 3, 4, 5, 6, 7, 8, 9]')
            self.assertEqual(resp.headers['x-nexe-retcode'], '0')
            self.assertEqual(resp.headers['x-nexe-status'], 'ok.')
            self.assertEqual(resp.headers['x-nexe-validation'], '0')
            self.assertEqual(resp.headers['x-nexe-system'], 'sort')
            timestamp = normalize_timestamp(time())
            self.assertEqual(math.floor(float(resp.headers['X-Timestamp'])),
                             math.floor(float(timestamp)))
            self.assertEquals(
                resp.headers['content-type'], 'application/x-gtar')
            # self.assertEqual(self.app.logger.log_dict['info'][0][0][0],
            #    'Zerovm CDR: 0 0 0 0 1 46 2 40 0 0 0 0')

    def test_QUERY_http_message(self):
        self.setup_zerovm_query()
        req = self.zerovm_object_request()
        conf = ZvmNode(1, 'sort', parse_location('swift://a/c/exe'))
        conf.add_new_channel(
            'stdin', ACCESS_READABLE, parse_location('swift://a/c/o'))
        conf.add_new_channel(
            'stdout', ACCESS_WRITABLE, content_type='message/http')
        conf = json.dumps(conf, cls=NodeEncoder)
        sysmap = StringIO(conf)
        nexefile = StringIO(trim(r'''
            resp = '\n'.join([
                'HTTP/1.1 200 OK',
                'Content-Type: application/json',
                'X-Object-Meta-Key1: value1',
                'X-Object-Meta-Key2: value2',
                '', ''
                ])
            out = str(sorted(id))
            return resp + out
            '''))
        with create_tar({'boot': nexefile, 'sysmap': sysmap}) as tar:
            length = os.path.getsize(tar)
            req.body_file = Input(open(tar, 'rb'), length)
            req.content_length = length
            resp = self.app.zerovm_query(req)
            fd, name = mkstemp()
            for chunk in resp.app_iter:
                os.write(fd, chunk)
            os.close(fd)
            self.assertEqual(os.path.getsize(name), resp.content_length)
            tar = tarfile.open(name)
            names = tar.getnames()
            members = tar.getmembers()
            self.assertIn('stdout', names)
            self.assertEqual(names[-1], 'stdout')
            file = tar.extractfile(members[-1])
            self.assertEqual(file.read(), '[0, 1, 2, 3, 4, 5, 6, 7, 8, 9]')
            self.assertEqual(resp.headers['x-nexe-retcode'], '0')
            self.assertEqual(resp.headers['x-nexe-status'], 'ok.')
            self.assertEqual(resp.headers['x-nexe-validation'], '0')
            self.assertEqual(resp.headers['x-nexe-system'], 'sort')
            self.assertEquals(
                resp.headers['content-type'], 'application/x-gtar')
            stdout_headers = members[-1].get_headers()
            self.assertEqual(stdout_headers['Content-Type'],
                             'application/json')
            self.assertEqual(stdout_headers['X-Object-Meta-Key1'],
                             'value1')
            self.assertEqual(stdout_headers['X-Object-Meta-Key2'],
                             'value2')

    def test_QUERY_cgi_message(self):
        self.setup_zerovm_query()
        req = self.zerovm_object_request()
        conf = ZvmNode(1, 'sort', parse_location('swift://a/c/exe'))
        conf.add_new_channel(
            'stdin', ACCESS_READABLE, parse_location('swift://a/c/o'))
        conf.add_new_channel(
            'stdout', ACCESS_WRITABLE, content_type='message/cgi')
        conf = json.dumps(conf, cls=NodeEncoder)
        sysmap = StringIO(conf)
        nexefile = StringIO(trim(r'''
            resp = '\n'.join([
                'Content-Type: application/json',
                'X-Object-Meta-Key1: value1',
                'X-Object-Meta-Key2: value2',
                '', ''
                ])
            out = str(sorted(id))
            return resp + out
            '''))
        with create_tar({'boot': nexefile, 'sysmap': sysmap}) as tar:
            length = os.path.getsize(tar)
            req.body_file = Input(open(tar, 'rb'), length)
            req.content_length = length
            resp = self.app.zerovm_query(req)
            fd, name = mkstemp()
            for chunk in resp.app_iter:
                os.write(fd, chunk)
            os.close(fd)
            self.assertEqual(os.path.getsize(name), resp.content_length)
            tar = tarfile.open(name)
            names = tar.getnames()
            members = tar.getmembers()
            self.assertIn('stdout', names)
            self.assertEqual(names[-1], 'stdout')
            file = tar.extractfile(members[-1])
            self.assertEqual(file.read(), '[0, 1, 2, 3, 4, 5, 6, 7, 8, 9]')
            self.assertEqual(resp.headers['x-nexe-retcode'], '0')
            self.assertEqual(resp.headers['x-nexe-status'], 'ok.')
            self.assertEqual(resp.headers['x-nexe-validation'], '0')
            self.assertEqual(resp.headers['x-nexe-system'], 'sort')
            timestamp = normalize_timestamp(time())
            self.assertEqual(math.floor(float(resp.headers['X-Timestamp'])),
                             math.floor(float(timestamp)))
            self.assertEquals(
                resp.headers['content-type'], 'application/x-gtar')
            stdout_headers = members[-1].get_headers()
            self.assertEqual(stdout_headers['Content-Type'],
                             'application/json')
            self.assertEqual(stdout_headers['X-Object-Meta-Key1'],
                             'value1')
            self.assertEqual(stdout_headers['X-Object-Meta-Key2'],
                             'value2')

    def test_QUERY_invalid_http_message(self):
        self.setup_zerovm_query()
        req = self.zerovm_object_request()
        conf = ZvmNode(1, 'sort', parse_location('swift://a/c/exe'))
        conf.add_new_channel(
            'stdin', ACCESS_READABLE, parse_location('swift://a/c/o'))
        conf.add_new_channel(
            'stdout', ACCESS_WRITABLE, content_type='message/http')
        conf = json.dumps(conf, cls=NodeEncoder)
        sysmap = StringIO(conf)
        nexefile = StringIO(trim('''
            resp = '\\n'.join(['Status: 200 OK',
                               'Content-Type: application/json', '', ''])
            out = str(sorted(id))
            return resp + out
            '''))
        with create_tar({'boot': nexefile, 'sysmap': sysmap}) as tar:
            length = os.path.getsize(tar)
            req.body_file = Input(open(tar, 'rb'), length)
            req.content_length = length
            resp = self.app.zerovm_query(req)
            fd, name = mkstemp()
            for chunk in resp.app_iter:
                os.write(fd, chunk)
            os.close(fd)
            self.assertEqual(os.path.getsize(name), resp.content_length)
            tar = tarfile.open(name)
            names = tar.getnames()
            members = tar.getmembers()
            self.assertIn('stdout', names)
            self.assertEqual(names[-1], 'stdout')
            file = tar.extractfile(members[-1])
            self.assertEqual(file.read(),
                             'Status: 200 OK\n'
                             'Content-Type: application/json\n\n'
                             '[0, 1, 2, 3, 4, 5, 6, 7, 8, 9]')
            self.assertEqual(resp.headers['x-nexe-retcode'], '0')
            self.assertEqual(resp.headers['x-nexe-status'], 'ok.')
            self.assertEqual(resp.headers['x-nexe-validation'], '0')
            self.assertEqual(resp.headers['x-nexe-system'], 'sort')
            timestamp = normalize_timestamp(time())
            self.assertEqual(math.floor(float(resp.headers['X-Timestamp'])),
                             math.floor(float(timestamp)))
            self.assertEqual(
                resp.headers['content-type'], 'application/x-gtar')
            stdout_headers = members[-1].get_headers()
            self.assertEqual(stdout_headers['Content-Type'], 'message/http')

    def test_QUERY_invalid_nexe(self):
        self.setup_zerovm_query()
        req = self.zerovm_object_request()
        nexefile = StringIO('INVALID')
        conf = ZvmNode(1, 'sort', parse_location('swift://a/c/exe'))
        conf.add_new_channel(
            'stdin', ACCESS_READABLE, parse_location('swift://a/c/o'))
        conf.add_new_channel('stdout', ACCESS_WRITABLE)
        conf = json.dumps(conf, cls=NodeEncoder)
        sysmap = StringIO(conf)
        with create_tar({'boot': nexefile, 'sysmap': sysmap}) as tar:
            length = os.path.getsize(tar)
            req.body_file = Input(open(tar, 'rb'), length)
            req.content_length = length
            resp = self.app.zerovm_query(req)
            fd, name = mkstemp()
            for chunk in resp.app_iter:
                os.write(fd, chunk)
            os.close(fd)
            self.assertEqual(os.path.getsize(name), resp.content_length)
            tar = tarfile.open(name)
            names = tar.getnames()
            members = tar.getmembers()
            self.assertIn('stdout', names)
            self.assertEqual(names[-1], 'stdout')
            self.assertEqual(members[-1].size, 0)
            file = tar.extractfile(members[-1])
            self.assertEqual(file.read(), '')
            self.assertEqual(resp.headers['x-nexe-retcode'], '0')
            self.assertEqual(resp.headers['x-nexe-status'], 'nexe is invalid')
            self.assertEqual(resp.headers['x-nexe-validation'], '1')
            self.assertEqual(resp.headers['x-nexe-system'], 'sort')
            timestamp = normalize_timestamp(time())
            self.assertEqual(math.floor(float(resp.headers['X-Timestamp'])),
                             math.floor(float(timestamp)))
            self.assertEqual(
                resp.headers['content-type'], 'application/x-gtar')
            # self.assertEqual(self.app.logger.log_dict['info'][0][0][0],
            #    'Zerovm CDR: 0 0 0 0 0 0 0 0 0 0 0 0')

    def test_QUERY_freenode(self):
        # running code without input file
        self.setup_zerovm_query()
        rmdir(os.path.join(self.testdir, 'sda1', 'tmp'))
        req = self.zerovm_free_request()
        nexefile = StringIO(self._nexescript)
        conf = ZvmNode(1, 'sort', parse_location('swift://a/c/exe'))
        conf.add_new_channel('stdout', ACCESS_WRITABLE)
        conf = json.dumps(conf, cls=NodeEncoder)
        sysmap = StringIO(conf)
        with create_tar({'boot': nexefile, 'sysmap': sysmap}) as tar:
            length = os.path.getsize(tar)
            req.body_file = Input(open(tar, 'rb'), length)
            req.content_length = length
            resp = self.app.zerovm_query(req)
            fd, name = mkstemp()
            for chunk in resp.app_iter:
                os.write(fd, chunk)
            os.close(fd)
            self.assertEqual(os.path.getsize(name), resp.content_length)
            tar = tarfile.open(name)
            names = tar.getnames()
            members = tar.getmembers()
            self.assertIn('stdout', names)
            self.assertEqual(names[-1], 'stdout')
            self.assertEqual(members[-1].size, len(self._emptyresult))
            file = tar.extractfile(members[-1])
            self.assertEqual(file.read(), self._emptyresult)
            self.assertEqual(resp.headers['x-nexe-retcode'], '0')
            self.assertEqual(resp.headers['x-nexe-status'], 'ok.')
            self.assertEqual(resp.headers['x-nexe-validation'], '0')
            self.assertEqual(resp.headers['x-nexe-system'], 'sort')
            timestamp = normalize_timestamp(time())
            self.assertEqual(math.floor(float(resp.headers['X-Timestamp'])),
                             math.floor(float(timestamp)))
            self.assertEqual(
                resp.headers['content-type'], 'application/x-gtar')
            # self.assertEqual(self.app.logger.log_dict['info'][0][0][0],
            #    'Zerovm CDR: 0 0 0 0 1 0 2 13 0 0 0 0')

    def test_QUERY_write_only(self):
        # running the executable creates a new object in-place
        self.setup_zerovm_query()
        nexefile = StringIO(self._nexescript)
        conf = ZvmNode(1, 'sort', parse_location('swift://a/c/exe'))
        meta = {'key1': 'value1',
                'key2': 'value2'}
        content_type = 'application/x-pickle'
        conf.add_new_channel('stdout',
                             ACCESS_WRITABLE,
                             parse_location('swift://a/c/out'),
                             meta_data=meta,
                             content_type=content_type)
        conf = json.dumps(conf, cls=NodeEncoder)
        sysmap = StringIO(conf)
        timestamp = normalize_timestamp(time())
        req = Request.blank('/sda1/p/a/c/out',
                            environ={'REQUEST_METHOD': 'POST'},
                            headers={
                                'Content-Type': 'application/x-gtar',
                                'x-zerovm-execute': '1.0',
                                'x-zerocloud-id': self.uid_generator.get(),
                                'x-timestamp': timestamp,
                                'x-zerovm-access': 'PUT'
                            })
        with create_tar({'boot': nexefile, 'sysmap': sysmap}) as tar:
            length = os.path.getsize(tar)
            req.body_file = Input(open(tar, 'rb'), length)
            req.content_length = length
            resp = self.app.zerovm_query(req)
            self.assertEqual(resp.status_int, 200)
            self.assertEqual(resp.content_length, 0)
            self.assertEqual(resp.headers['x-nexe-retcode'], '0')
            self.assertEqual(resp.headers['x-nexe-status'], 'ok.')
            self.assertEqual(resp.headers['x-nexe-validation'], '0')
            self.assertEqual(resp.headers['x-nexe-system'], 'sort')
            req = Request.blank('/sda1/p/a/c/out')
            resp = self.obj_controller.GET(req)
            self.assertEqual(resp.status_int, 200)
            self.assertEqual(resp.content_length, len(self._emptyresult))
            self.assertEqual(resp.body, self._emptyresult)
            self.assertEqual(math.floor(float(resp.headers['X-Timestamp'])),
                             math.floor(float(timestamp)))
            self.assertEqual(resp.content_type, content_type)
            for k, v in meta.iteritems():
                self.assertEqual(resp.headers['x-object-meta-%s' % k], v)

    def test_QUERY_write_and_report(self):
        # running the executable creates a new object from stdout
        # and sends stderr output to the user
        self.setup_zerovm_query()
        nexefile = StringIO(self._nexescript)
        conf = ZvmNode(1, 'sort', parse_location('swift://a/c/exe'))
        meta = {'key1': 'value1',
                'key2': 'value2'}
        content_type = 'application/x-pickle'
        conf.add_new_channel('stdout',
                             ACCESS_WRITABLE,
                             parse_location('swift://a/c/out'),
                             meta_data=meta,
                             content_type=content_type)
        conf.add_new_channel('stderr', ACCESS_WRITABLE)
        conf = json.dumps(conf, cls=NodeEncoder)
        sysmap = StringIO(conf)
        timestamp = normalize_timestamp(time())
        req = Request.blank('/sda1/p/a/c/out',
                            environ={'REQUEST_METHOD': 'POST'},
                            headers={
                                'Content-Type': 'application/x-gtar',
                                'x-zerovm-execute': '1.0',
                                'x-zerocloud-id': self.uid_generator.get(),
                                'x-timestamp': timestamp,
                                'x-zerovm-access': 'PUT'
                            })
        with create_tar({'boot': nexefile, 'sysmap': sysmap}) as tar:
            length = os.path.getsize(tar)
            req.body_file = Input(open(tar, 'rb'), length)
            req.content_length = length
            resp = self.app.zerovm_query(req)
            self.assertEqual(resp.status_int, 200)
            self.assertEqual(resp.headers['x-nexe-retcode'], '0')
            self.assertEqual(resp.headers['x-nexe-status'], 'ok.')
            self.assertEqual(resp.headers['x-nexe-validation'], '0')
            self.assertEqual(resp.headers['x-nexe-system'], 'sort')
            fd, name = mkstemp()
            for chunk in resp.app_iter:
                os.write(fd, chunk)
            os.close(fd)
            self.assertEqual(os.path.getsize(name), resp.content_length)
            tar = tarfile.open(name)
            names = tar.getnames()
            members = tar.getmembers()
            self.assertIn('stderr', names)
            self.assertEqual(names[-1], 'stderr')
            f = tar.extractfile(members[-1])
            self.assertEqual(f.read(), self._stderr)
            req = Request.blank('/sda1/p/a/c/out')
            resp = self.obj_controller.GET(req)
            self.assertEqual(resp.status_int, 200)
            self.assertEqual(resp.content_length, len(self._emptyresult))
            self.assertEqual(resp.body, self._emptyresult)
            self.assertEqual(math.floor(float(resp.headers['X-Timestamp'])),
                             math.floor(float(timestamp)))
            self.assertEqual(resp.content_type, content_type)
            for k, v in meta.iteritems():
                self.assertEqual(resp.headers['x-object-meta-%s' % k], v)

    def test_QUERY_OsErr(self):
        def mock(*args):
            raise Exception('Mock lseek failed')
        self.app.os_interface = OsMock()
        self.setup_zerovm_query()
        req = self.zerovm_free_request()
        nexefile = StringIO(self._nexescript)
        conf = ZvmNode(1, 'sort', parse_location('swift://a/c/exe'))
        conf.add_new_channel('stdout', ACCESS_WRITABLE)
        conf = json.dumps(conf, cls=NodeEncoder)
        sysmap = StringIO(conf)
        with create_tar({'boot': nexefile, 'sysmap': sysmap}) as tar:
            length = os.path.getsize(tar)
            req.body_file = Input(open(tar, 'rb'), length)
            req.content_length = length
            resp = self.app.zerovm_query(req)
            fd, name = mkstemp()
            for chunk in resp.app_iter:
                os.write(fd, chunk)
            os.close(fd)
            self.assertEqual(os.path.getsize(name), resp.content_length)
            tar = tarfile.open(name)
            names = tar.getnames()
            members = tar.getmembers()
            self.assertIn('stdout', names)
            self.assertEqual(names[-1], 'stdout')
            self.assertEqual(members[-1].size, len(self._emptyresult))
            file = tar.extractfile(members[-1])
            self.assertEqual(file.read(), self._emptyresult)

        del self.app.parser_config['limits']['wbytes']
        self.setup_zerovm_query()
        req = self.zerovm_free_request()
        nexefile = StringIO(self._nexescript)
        conf = ZvmNode(1, 'sort', parse_location('swift://a/c/exe'))
        conf.add_new_channel('stdout', ACCESS_WRITABLE)
        conf = json.dumps(conf, cls=NodeEncoder)
        sysmap = StringIO(conf)
        with create_tar({'boot': nexefile, 'sysmap': sysmap}) as tar:
            length = os.path.getsize(tar)
            req.body_file = Input(open(tar, 'rb'), length)
            req.content_length = length
            raised = False
            try:
                resp = self.app.zerovm_query(req)
            except Exception:
                raised = True
            self.assert_(raised, "Exception not raised")

        self.setup_zerovm_query()
        req = self.zerovm_free_request()
        nexefile = StringIO(self._nexescript)
        conf = ZvmNode(1, 'sort', '/c/exe')
        conf.add_new_channel('stderr', ACCESS_WRITABLE)
        conf = json.dumps(conf, cls=NodeEncoder)
        sysmap = StringIO(conf)
        with create_tar({'boot': nexefile, 'sysmap': sysmap}) as tar:
            length = os.path.getsize(tar)
            req.body_file = Input(open(tar, 'rb'), length)
            req.content_length = length
            raised = False
            try:
                resp = self.app.zerovm_query(req)
            except Exception:
                raised = True
            self.assert_(raised, "Exception not raised")

    def test_QUERY_nexe_environment(self):
        self.setup_zerovm_query()
        req = self.zerovm_object_request()
        nexefile = StringIO(self._nexescript)
        conf = ZvmNode(1, 'sort', parse_location('swift://a/c/exe'))
        conf.add_new_channel(
            'stdin', ACCESS_READABLE, parse_location('swift://a/c/o'))
        conf.add_new_channel('stdout', ACCESS_WRITABLE)
        conf.args = 'aaa bbb'
        conf.env = {'KEY_A': 'value_a', 'KEY_B': 'value_b'}
        conf = json.dumps(conf, cls=NodeEncoder)
        sysmap = StringIO(conf)
        with create_tar({'boot': nexefile, 'sysmap': sysmap}) as tar:
            length = os.path.getsize(tar)
            req.body_file = Input(open(tar, 'rb'), length)
            req.content_length = length
            resp = self.app.zerovm_query(req)
            self.assertEquals(resp.status_int, 200)

    def test_QUERY_multichannel(self):
        self.setup_zerovm_query()
        req = self.zerovm_object_request()
        nexefile = StringIO(self._nexescript)
        conf = ZvmNode(1, 'sort', parse_location('swift://a/c/exe'))
        conf.add_new_channel(
            'input', ACCESS_READABLE, parse_location('swift://a/c/o'))
        conf.add_new_channel(
            'output', ACCESS_WRITABLE, parse_location('swift://a/c/o2'))
        conf = json.dumps(conf, cls=NodeEncoder)
        sysmap = StringIO(conf)
        with create_tar({'boot': nexefile, 'sysmap': sysmap}) as tar:
            length = os.path.getsize(tar)
            req.body_file = Input(open(tar, 'rb'), length)
            req.content_length = length
            resp = self.app.zerovm_query(req)
            self.assertEquals(resp.status_int, 200)

    def test_QUERY_std_list(self):
        self.setup_zerovm_query()
        req = self.zerovm_object_request()
        nexefile = StringIO(self._nexescript)
        conf = ZvmNode(1, 'sort', parse_location('swift://a/c/exe'))
        conf.add_new_channel(
            'stdin', ACCESS_READABLE, parse_location('swift://a/c/o'))
        conf.add_new_channel(
            'stdout', ACCESS_WRITABLE, parse_location('swift://a/c/o2'))
        conf.add_new_channel('stderr', ACCESS_WRITABLE)
        conf = json.dumps(conf, cls=NodeEncoder)
        sysmap = StringIO(conf)
        with create_tar({'boot': nexefile, 'sysmap': sysmap}) as tar:
            length = os.path.getsize(tar)
            req.body_file = Input(open(tar, 'rb'), length)
            req.content_length = length
            resp = self.app.zerovm_query(req)
            self.assertEquals(resp.status_int, 200)
            fd, name = mkstemp()
            for chunk in resp.app_iter:
                os.write(fd, chunk)
            os.close(fd)
            self.assertEqual(os.path.getsize(name), resp.content_length)
            tar = tarfile.open(name)
            names = tar.getnames()
            members = tar.getmembers()
            self.assertIn('stderr', names)
            self.assertEqual(names[-1], 'stderr')
            self.assertEqual(members[-1].size, len(self._stderr))
            file = tar.extractfile(members[-1])
            self.assertEqual(file.read(), self._stderr)
            self.assertIn('stdout', names)
            self.assertEqual(names[0], 'stdout')
            self.assertEqual(members[0].size, len(self._sortednumbers))
            file = tar.extractfile(members[0])
            self.assertEqual(file.read(), self._sortednumbers)

    def test_QUERY_logger(self):
        # check logger assignment
        logger = get_logger({}, log_route='obj-query-test')
        self.app = objectquery.ObjectQueryMiddleware(
            self.obj_controller, self.conf, logger)
        self.assertIs(logger, self.app.logger)

    def test_QUERY_object_not_exists(self):
        # check if querying non existent object
        req = self.zerovm_object_request()
        nexefile = StringIO('SCRIPT')
        conf = ZvmNode(1, 'sort', parse_location('swift://a/c/exe'))
        conf.add_new_channel(
            'stdin', ACCESS_READABLE, parse_location('swift://a/c/o'))
        conf.add_new_channel('stdout', ACCESS_WRITABLE)
        conf = json.dumps(conf, cls=NodeEncoder)
        sysmap = StringIO(conf)
        with create_tar({'boot': nexefile, 'sysmap': sysmap}) as tar:
            length = os.path.getsize(tar)
            req.body_file = Input(open(tar, 'rb'), length)
            req.content_length = length
            resp = self.app.zerovm_query(req)
            self.assertEquals(resp.status_int, 404)

    def test_QUERY_invalid_path(self):
        # check if just querying container fails
        req = Request.blank('/sda1/p/a/c',
                            environ={'REQUEST_METHOD': 'POST'},
                            headers={
                                'x-zerovm-execute': '1.0',
                                'x-zerocloud-id': self.uid_generator.get()
                            })
        resp = req.get_response(self.app)
        self.assertEquals(resp.status_int, 400)

    def test_QUERY_max_upload_time(self):
        class SlowBody():

            def __init__(self, body):
                self.body = body

            def read(self, size=-1):
                return self.body.read(10)

        self.setup_zerovm_query()
        req = self.zerovm_object_request()
        nexefile = StringIO(self._nexescript)
        conf = ZvmNode(1, 'sort', parse_location('swift://a/c/exe'))
        conf.add_new_channel(
            'stdin', ACCESS_READABLE, parse_location('swift://a/c/o'))
        conf.add_new_channel('stdout', ACCESS_WRITABLE)
        conf = json.dumps(conf, cls=NodeEncoder)
        sysmap = StringIO(conf)
        with create_tar({'boot': nexefile, 'sysmap': sysmap}) as tar:
            fp = open(tar, 'rb')
            length = os.path.getsize(tar)
            req.body_file = Input(SlowBody(fp), length)
            req.content_length = length
            resp = req.get_response(self.app)
            fp.close()
            self.assertEquals(resp.status_int, 200)
            fd, name = mkstemp()
            for chunk in resp.app_iter:
                os.write(fd, chunk)
            os.close(fd)
            self.assertEqual(os.path.getsize(name), resp.content_length)
            tar_out = tarfile.open(name)
            names = tar_out.getnames()
            members = tar_out.getmembers()
            self.assertIn('stdout', names)
            self.assertEqual(names[0], 'stdout')
            self.assertEqual(members[0].size, len(self._sortednumbers))
            file = tar_out.extractfile(members[0])
            self.assertEqual(file.read(), self._sortednumbers)
            orig_max_upload_time = self.app.max_upload_time
            self.app.max_upload_time = 0.001
            fp = open(tar, 'rb')
            length = os.path.getsize(tar)
            req.body_file = Input(SlowBody(fp), length)
            req.content_length = length
            resp = req.get_response(self.app)
            fp.close()
            self.app.max_upload_time = orig_max_upload_time
            self.assertEquals(resp.status_int, 408)

    def test_QUERY_no_content_type(self):
        req = self.zerovm_object_request()
        del req.headers['Content-Type']
        req.body = 'SCRIPT'
        resp = req.get_response(self.app)
        self.assertEquals(resp.status_int, 400)
        self.assert_('No content type' in resp.body)

    def test_QUERY_invalid_content_type(self):
        req = self.zerovm_object_request()
        req.headers['Content-Type'] = 'application/blah-blah-blah'
        req.body = 'SCRIPT'
        resp = req.get_response(self.app)
        self.assertEquals(resp.status_int, 400)
        self.assert_('Invalid Content-Type' in resp.body)

    def test_QUERY_invalid_path_encoding(self):
        req = Request.blank('/sda1/p/a/c/o'.encode('utf-16'),
                            environ={'REQUEST_METHOD': 'POST'},
                            headers={'Content-Type': 'application/x-gtar',
                                     'x-zerovm-execute': '1.0',
                                     'x-account-name': 'a'})
        req.body = 'SCRIPT'
        resp = req.get_response(self.app)
        self.assertEquals(resp.status_int, 412)
        self.assert_('Invalid UTF8' in resp.body)

    def test_QUERY_error_upstream(self):
        self.obj_controller.fault = True
        req = Request.blank('/sda1/p/a/c/o',
                            environ={'REQUEST_METHOD': 'GET'},
                            headers={'Content-Type': 'application/x-gtar'})
        resp = req.get_response(self.app)
        self.assertEquals(resp.status_int, 500)
        self.assert_('Traceback' in resp.body)

    def test_QUERY_script_invalid_etag(self):
        # we cannot etag the tar stream because we mangle it while
        # transferring, on the fly
        raise SkipTest
        self.setup_zerovm_query()
        req = self.zerovm_object_request()
        nexefile = StringIO(self._nexescript)
        conf = ZvmNode(1, 'sort', '/c/exe')
        conf.add_new_channel('stdin', ACCESS_READABLE, '/c/o')
        conf.add_new_channel('stdout', ACCESS_WRITABLE)
        conf = json.dumps(conf, cls=NodeEncoder)
        sysmap = StringIO(conf)
        with create_tar({'boot': nexefile, 'sysmap': sysmap}) as tar:
            fp = open(tar, 'rb')
            etag = md5()
            etag.update(fp.read())
            fp.close()
            req.headers['etag'] = etag.hexdigest()
            req.body_file = open(tar, 'rb')
            resp = self.app.zerovm_query(req)
            self.assertEquals(resp.status_int, 200)
            etag = md5()
            etag.update('blah-blah')
            req.headers['etag'] = etag.hexdigest()
            req.body_file = open(tar, 'rb')
            resp = self.app.zerovm_query(req)
            self.assertEquals(resp.status_int, 422)

    def test_QUERY_short_body(self):
        class ShortBody():

            def __init__(self):
                self.sent = False

            def read(self, size=-1):
                if not self.sent:
                    self.sent = True
                    return '   '
                return ''

        self.setup_zerovm_query()
        req = Request.blank('/sda1/p/a/c/o',
                            environ={
                                'REQUEST_METHOD': 'POST',
                                'wsgi.input': Input(ShortBody(), 4)
                            },
                            headers={
                                'X-Timestamp': normalize_timestamp(time()),
                                'x-zerovm-execute': '1.0',
                                'x-zerocloud-id': self.uid_generator.get(),
                                'Content-Length': '4',
                                'Content-Type': 'application/x-gtar'
                            })
        resp = req.get_response(self.app)
        self.assertEquals(resp.status_int, 499)

    def test_QUERY_long_body(self):
        class LongBody():

            def __init__(self):
                self.sent = False

            def read(self, size=-1):
                if not self.sent:
                    self.sent = True
                    return '   '
                return ''

        self.setup_zerovm_query()
        req = Request.blank('/sda1/p/a/c/o',
                            environ={
                                'REQUEST_METHOD': 'POST',
                                'wsgi.input': Input(LongBody(), 2)
                            },
                            headers={
                                'X-Timestamp': normalize_timestamp(time()),
                                'x-zerovm-execute': '1.0',
                                'x-zerocloud-id': self.uid_generator.get(),
                                'Content-Length': '2',
                                'Content-Type': 'application/x-gtar'
                            })
        resp = req.get_response(self.app)
        self.assertEquals(resp.status_int, 499)

    def test_QUERY_zerovm_stderr(self):
        self.setup_zerovm_query(trim(r'''
            import sys
            sys.stderr.write('some shit happened\n')
            '''))
        req = self.zerovm_object_request()

        nexefile = StringIO(self._nexescript)
        conf = ZvmNode(1, 'sort', parse_location('swift://a/c/exe'))
        conf.add_new_channel(
            'stdin', ACCESS_READABLE, parse_location('swift://a/c/o'))
        conf.add_new_channel('stdout', ACCESS_WRITABLE)
        conf = json.dumps(conf, cls=NodeEncoder)
        sysmap = StringIO(conf)
        with create_tar({'boot': nexefile, 'sysmap': sysmap}) as tar:
            length = os.path.getsize(tar)
            req.body_file = Input(open(tar, 'rb'), length)
            req.content_length = length
            resp = self.app.zerovm_query(req)
            self.assertEquals(resp.status_int, 500)
            self.assertIn('ERROR OBJ.QUERY retcode=OK,  '
                          'zerovm_stdout=some shit happened',
                          resp.body)

        self.setup_zerovm_query(trim(r'''
            import sys
            import time
            sys.stdout.write('0\n\nok.\n')
            for i in range(20):
                time.sleep(0.1)
                sys.stderr.write(''.zfill(4096))
            '''))
        nexefile = StringIO(self._nexescript)
        conf = ZvmNode(1, 'sort', parse_location('swift://a/c/exe'))
        conf.add_new_channel(
            'stdin', ACCESS_READABLE, parse_location('swift://a/c/o'))
        conf.add_new_channel('stdout', ACCESS_WRITABLE)
        conf = json.dumps(conf, cls=NodeEncoder)
        sysmap = StringIO(conf)
        with create_tar({'boot': nexefile, 'sysmap': sysmap}) as tar:
            length = os.path.getsize(tar)
            req.body_file = Input(open(tar, 'rb'), length)
            req.content_length = length
            resp = self.app.zerovm_query(req)
            self.assertEqual(resp.status_int, 500)
            self.assertIn('ERROR OBJ.QUERY retcode=Output too long', resp.body)

        self.setup_zerovm_query(trim(r'''
            import sys, time, signal
            signal.signal(signal.SIGTERM, signal.SIG_IGN)
            time.sleep(0.9)
            sys.stdout.write('0\n\nok.\n')
            sys.stderr.write(''.zfill(4096*20))
            '''))
        nexefile = StringIO(self._nexescript)
        conf = ZvmNode(1, 'sort', parse_location('swift://a/c/exe'))
        conf.add_new_channel(
            'stdin', ACCESS_READABLE, parse_location('swift://a/c/o'))
        conf.add_new_channel('stdout', ACCESS_WRITABLE)
        conf = json.dumps(conf, cls=NodeEncoder)
        sysmap = StringIO(conf)
        with create_tar({'boot': nexefile, 'sysmap': sysmap}) as tar:
            length = os.path.getsize(tar)
            req.body_file = Input(open(tar, 'rb'), length)
            req.content_length = length
            req.headers['x-zerovm-timeout'] = 1
            resp = self.app.zerovm_query(req)
            self.assertEqual(resp.status_int, 500)
            self.assertIn(
                'ERROR OBJ.QUERY retcode=Output too long', resp.body)

    def test_QUERY_zerovm_term_timeouts(self):
        self.setup_zerovm_query(trim(r'''
            from time import sleep
            sleep(10)
            '''))
        req = self.zerovm_object_request()
        nexefile = StringIO(self._nexescript)
        conf = ZvmNode(1, 'sort', parse_location('swift://a/c/exe'))
        conf.add_new_channel(
            'stdin', ACCESS_READABLE, parse_location('swift://a/c/o'))
        conf.add_new_channel('stdout', ACCESS_WRITABLE)
        conf = json.dumps(conf, cls=NodeEncoder)
        sysmap = StringIO(conf)
        with create_tar({'boot': nexefile, 'sysmap': sysmap}) as tar:
            length = os.path.getsize(tar)
            req.body_file = Input(open(tar, 'rb'), length)
            req.content_length = length
            req.headers['x-zerovm-timeout'] = 1
            resp = self.app.zerovm_query(req)
            self.assertEquals(resp.status_int, 500)
            self.assertIn('ERROR OBJ.QUERY retcode=Timed out', resp.body)

    def test_QUERY_zerovm_kill_timeouts(self):
        self.setup_zerovm_query(trim(r'''
            import signal, time
            signal.signal(signal.SIGTERM, signal.SIG_IGN)
            time.sleep(10)
            '''))
        req = self.zerovm_object_request()
        nexefile = StringIO(self._nexescript)
        conf = ZvmNode(1, 'sort', parse_location('swift://a/c/exe'))
        conf.add_new_channel(
            'stdin', ACCESS_READABLE, parse_location('swift://a/c/o'))
        conf.add_new_channel('stdout', ACCESS_WRITABLE)
        conf = json.dumps(conf, cls=NodeEncoder)
        sysmap = StringIO(conf)
        with create_tar({'boot': nexefile, 'sysmap': sysmap}) as tar:
            length = os.path.getsize(tar)
            req.body_file = Input(open(tar, 'rb'), length)
            req.content_length = length
            orig_kill_timeout = self.app.zerovm_kill_timeout
            try:
                self.app.zerovm_kill_timeout = 0.1
                req.headers['x-zerovm-timeout'] = 1
                resp = self.app.zerovm_query(req)
                self.assertEquals(resp.status_int, 500)
                self.assertIn('ERROR OBJ.QUERY retcode=Killed', resp.body)
            finally:
                self.app.zerovm_kill_timeout = orig_kill_timeout

    def test_QUERY_simulteneous_running_zerovm_limits(self):
        self.setup_zerovm_query()
        nexefile = StringIO('return sleep(.2)')
        conf = ZvmNode(1, 'sleep', parse_location('swift://a/c/exe'))
        conf = json.dumps(conf, cls=NodeEncoder)
        sysmap = StringIO(conf)
        maxreq_factor = 2
        r = range(0, maxreq_factor * 5)
        req = copy(r)
        with create_tar({'boot': nexefile, 'sysmap': sysmap}) as tar:
            length = os.path.getsize(tar)
            orig_zerovm_threadpools = self.app.zerovm_thread_pools
            try:
                pool = GreenPool()
                t = copy(r)

                def make_requests_storm(queue_factor, pool_factor):
                    for i in r:
                        req[i] = self.zerovm_free_request()
                        req[i].body_file = Input(open(tar, 'rb'), length)
                        req[i].content_length = length
                        req[i].headers['x-zerovm-timeout'] = 5
                    size = int(maxreq_factor * pool_factor * 5)
                    queue = int(maxreq_factor * queue_factor * 5)
                    self.app.zerovm_thread_pools[
                        'default'] = WaitPool(size, queue)
                    spil_over = size + queue
                    for i in r:
                        t[i] = pool.spawn(self.app.zerovm_query, req[i])
                    pool.waitall()
                    resp = copy(r)
                    for i in r[:spil_over]:
                        resp[i] = t[i].wait()
                        # print 'expecting ok #%s: %s' % (i, resp[i])
                        self.assertEqual(resp[i].status_int, 200)
                    for i in r[spil_over:]:
                        resp[i] = t[i].wait()
                        # print 'expecting fail #%s: %s' % (i, resp[i])
                        self.assertEqual(resp[i].status_int, 503)
                        self.assertEqual(resp[i].body, 'Slot not available')

                make_requests_storm(0.2, 0.4)
                make_requests_storm(0, 1)
                make_requests_storm(0.4, 0.6)
                make_requests_storm(0, 0.1)

            finally:
                self.app.zerovm_thread_pools = orig_zerovm_threadpools

    def test_QUERY_max_input_size(self):
        self.setup_zerovm_query()
        orig_maxinput = self.app.parser_config['limits']['rbytes']
        try:
            self.app.parser_config['limits']['rbytes'] = 0
            req = self.zerovm_object_request()
            req.body = 'xxxxxxxxx'
            resp = req.get_response(self.app)
            self.assertEqual(resp.status_int, 413)
            self.assertEqual(resp.body, 'RPC request too large')

            nexefile = StringIO(self._nexescript)
            conf = ZvmNode(1, 'sort', parse_location('swift://a/c/exe'))
            conf.add_new_channel(
                'stdin', ACCESS_READABLE, parse_location('swift://a/c/o'))
            conf.add_new_channel('stdout', ACCESS_WRITABLE)
            conf = json.dumps(conf, cls=NodeEncoder)
            sysmap = StringIO(conf)
            with create_tar({'boot': nexefile, 'sysmap': sysmap}) as tar:
                self.create_object(
                    create_random_numbers(os.path.getsize(tar) + 2))
                self.app.parser_config['limits'][
                    'rbytes'] = os.path.getsize(tar) + 1
                req = self.zerovm_object_request()
                length = os.path.getsize(tar)
                req.body_file = Input(open(tar, 'rb'), length)
                req.content_length = length
                resp = self.app.zerovm_query(req)
                self.assertEqual(resp.status_int, 413)
                self.assertEqual(resp.body, 'Data object too large')
        finally:
            self.create_object(create_random_numbers(10))
            self.app.parser_config['limits']['rbytes'] = orig_maxinput

    def test_QUERY_max_nexe_size(self):
        self.setup_zerovm_query()
        orig_maxnexe = getattr(self.app, 'zerovm_maxnexe')
        try:
            setattr(self.app, 'zerovm_maxnexe', 0)
            req = self.zerovm_object_request()
            nexefile = StringIO(self._nexescript)
            conf = ZvmNode(1, 'sort', parse_location('swift://a/c/exe'))
            conf.add_new_channel(
                'stdin', ACCESS_READABLE, parse_location('swift://a/c/o'))
            conf.add_new_channel('stdout', ACCESS_WRITABLE)
            conf = json.dumps(conf, cls=NodeEncoder)
            sysmap = StringIO(conf)
            with create_tar({'boot': nexefile, 'sysmap': sysmap}) as tar:
                length = os.path.getsize(tar)
                req.body_file = Input(open(tar, 'rb'), length)
                req.content_length = length
                resp = self.app.zerovm_query(req)
                self.assertEquals(resp.status_int, 200)
                self.assertEqual(resp.headers['x-nexe-retcode'], '0')
                self.assertEqual(resp.headers['x-nexe-status'], 'ok.')
                self.assertEqual(resp.headers['x-nexe-validation'], '0')
                self.assertEqual(resp.headers['x-nexe-system'], 'sort')
        finally:
            setattr(self.app, 'zerovm_maxnexe', orig_maxnexe)

    def test_QUERY_bad_system_map(self):
        self.setup_zerovm_query()
        req = self.zerovm_object_request()
        nexefile = StringIO(self._nexescript)
        conf = '{""}'
        sysmap = StringIO(conf)
        with create_tar({'boot': nexefile, 'sysmap': sysmap}) as tar:
            length = os.path.getsize(tar)
            req.body_file = Input(open(tar, 'rb'), length)
            req.content_length = length
            resp = self.app.zerovm_query(req)
            self.assertEqual(resp.status_int, 400)
            self.assertEqual(resp.body, 'Cannot parse system map')
        with create_tar({'boot': nexefile}) as tar:
            length = os.path.getsize(tar)
            req.body_file = Input(open(tar, 'rb'), length)
            req.content_length = length
            resp = self.app.zerovm_query(req)
            self.assertEqual(resp.status_int, 400)
            self.assertEqual(resp.body, 'No system map found in request')

    def test_QUERY_sysimage(self):
        self.setup_zerovm_query()
        req = self.zerovm_free_request()
        for dev, path in self.app.parser.sysimage_devices.items():
            script = 'return mnfst.channels["/dev/%s"]["path"]'\
                     ' + "\\n" + ' \
                     'open(mnfst.channels["/dev/nvram"]["path"]).read()' \
                     % dev
            nexefile = StringIO(script)
            conf = ZvmNode(
                1, 'sysimage-test', parse_location('swift://a/c/exe'))
            conf.add_new_channel(dev, ACCESS_CDR)
            conf.add_new_channel('stdout', ACCESS_WRITABLE)
            conf = json.dumps(conf, cls=NodeEncoder)
            sysmap = StringIO(conf)
            with create_tar({'boot': nexefile, 'sysmap': sysmap}) as tar:
                length = os.path.getsize(tar)
                req.body_file = Input(open(tar, 'rb'), length)
                req.content_length = length
                resp = self.app.zerovm_query(req)
                fd, name = mkstemp()
                self.assertEqual(resp.status_int, 200)
                for chunk in resp.app_iter:
                    os.write(fd, chunk)
                os.close(fd)
                tar = tarfile.open(name)
                names = tar.getnames()
                members = tar.getmembers()
                self.assertIn('stdout', names)
                self.assertEqual(names[-1], 'stdout')
                file = tar.extractfile(members[-1])
                out = (
                    '%s\n'
                    '[fstab]\n'
                    'channel=/dev/%s, mountpoint=/, access=ro, removable=no\n'
                    '[args]\n'
                    'args = sysimage-test\n' % (path, dev)
                )
                self.assertEqual(file.read(), out)
                self.assertEqual(resp.headers['x-nexe-retcode'], '0')
                self.assertEqual(resp.headers['x-nexe-status'], 'ok.')
                self.assertEqual(resp.headers['x-nexe-validation'], '0')
                self.assertEqual(
                    resp.headers['x-nexe-system'], 'sysimage-test')

    def test_QUERY_use_image_file(self):
        self.setup_zerovm_query()
        req = self.zerovm_object_request()
        nexefile = StringIO(self._nexescript)
        conf = ZvmNode(1, 'sort', 'file://usr/bin/sort')
        conf.add_new_channel(
            'stdin', ACCESS_READABLE, parse_location('swift://a/c/o'))
        conf.add_new_channel('stdout', ACCESS_WRITABLE)
        conf.add_new_channel('image', ACCESS_CDR)
        conf = json.dumps(conf, cls=NodeEncoder)
        sysmap = StringIO(conf)
        with create_tar({'usr/bin/sort': nexefile}) as image_tar:
            with create_tar({'image': open(image_tar, 'rb'),
                             'sysmap': sysmap}) as tar:
                length = os.path.getsize(tar)
                req.body_file = Input(open(tar, 'rb'), length)
                req.content_length = length
                resp = self.app.zerovm_query(req)
                fd, name = mkstemp()
                self.assertEqual(resp.status_int, 200)
                for chunk in resp.app_iter:
                    os.write(fd, chunk)
                os.close(fd)
                self.assertEqual(os.path.getsize(name), resp.content_length)
                tar = tarfile.open(name)
                names = tar.getnames()
                members = tar.getmembers()
                self.assertIn('stdout', names)
                self.assertEqual(names[-1], 'stdout')
                self.assertEqual(members[-1].size, len(self._sortednumbers))
                file = tar.extractfile(members[-1])
                self.assertEqual(file.read(), self._sortednumbers)
                self.assertEqual(resp.headers['x-nexe-retcode'], '0')
                self.assertEqual(resp.headers['x-nexe-status'], 'ok.')
                self.assertEqual(resp.headers['x-nexe-validation'], '0')
                self.assertEqual(resp.headers['x-nexe-system'], 'sort')
                timestamp = normalize_timestamp(time())
                self.assertEqual(
                    math.floor(float(resp.headers['X-Timestamp'])),
                    math.floor(float(timestamp))
                )
                self.assertEquals(
                    resp.headers['content-type'], 'application/x-gtar')
                # self.assertEqual(self.app.logger.log_dict['info'][0][0][0],
                #    'Zerovm CDR: 0 0 0 0 1 46 2 56 0 0 0 0')

    def test_QUERY_use_gzipped_image(self):
        self.setup_zerovm_query()
        req = self.zerovm_object_request()
        nexefile = StringIO(self._nexescript)
        conf = ZvmNode(1, 'sort', 'file://usr/bin/sort')
        conf.add_new_channel(
            'stdin', ACCESS_READABLE, parse_location('swift://a/c/o'))
        conf.add_new_channel('stdout', ACCESS_WRITABLE)
        conf.add_new_channel('image', ACCESS_CDR)
        conf = json.dumps(conf, cls=NodeEncoder)
        sysmap = StringIO(conf)
        with create_tar({'usr/bin/sort': nexefile}) as image_tar:
            import gzip
            image_tar_gz = image_tar + '.gz'
            try:
                t = open(image_tar, 'rb')
                gz = gzip.open(image_tar_gz, 'wb')
                gz.writelines(t)
                gz.close()
                t.close()
                with create_tar({'image.gz': open(image_tar_gz, 'rb'),
                                 'sysmap': sysmap}) as tar:
                    length = os.path.getsize(tar)
                    req.body_file = Input(open(tar, 'rb'), length)
                    req.content_length = length
                    resp = self.app.zerovm_query(req)
                    fd, name = mkstemp()
                    self.assertEqual(resp.status_int, 200)
                    for chunk in resp.app_iter:
                        os.write(fd, chunk)
                    os.close(fd)
                    self.assertEqual(
                        os.path.getsize(name), resp.content_length)
                    tar = tarfile.open(name)
                    names = tar.getnames()
                    members = tar.getmembers()
                    self.assertIn('stdout', names)
                    self.assertEqual(names[-1], 'stdout')
                    self.assertEqual(
                        members[-1].size, len(self._sortednumbers))
                    file = tar.extractfile(members[-1])
                    self.assertEqual(file.read(), self._sortednumbers)
                    self.assertEqual(resp.headers['x-nexe-retcode'], '0')
                    self.assertEqual(resp.headers['x-nexe-status'], 'ok.')
                    self.assertEqual(resp.headers['x-nexe-validation'], '0')
                    self.assertEqual(resp.headers['x-nexe-system'], 'sort')
                    timestamp = normalize_timestamp(time())
                    self.assertEqual(
                        math.floor(float(resp.headers['X-Timestamp'])),
                        math.floor(float(timestamp))
                    )
                    self.assertEquals(
                        resp.headers['content-type'], 'application/x-gtar')
            finally:
                try:
                    os.unlink(image_tar_gz)
                except OSError:
                    pass

    def test_QUERY_bypass_image_file(self):
        self.setup_zerovm_query()
        req = self.zerovm_object_request()
        nexefile = StringIO(self._nexescript)
        conf = ZvmNode(1, 'sort', parse_location('swift://a/c/exe'))
        conf.add_new_channel(
            'stdin', ACCESS_READABLE, parse_location('swift://a/c/o'))
        conf.add_new_channel('stdout', ACCESS_WRITABLE)
        conf.add_new_channel('image', ACCESS_CDR)
        conf = json.dumps(conf, cls=NodeEncoder)
        sysmap = StringIO(conf)
        with create_tar({'usr/bin/sort': StringIO('bla-bla')}) as image_tar:
            with create_tar({'image': open(image_tar, 'rb'),
                             'sysmap': sysmap, 'boot': nexefile}) as tar:
                length = os.path.getsize(tar)
                req.body_file = Input(open(tar, 'rb'), length)
                req.content_length = length
                resp = self.app.zerovm_query(req)
                fd, name = mkstemp()
                self.assertEqual(resp.status_int, 200)
                for chunk in resp.app_iter:
                    os.write(fd, chunk)
                os.close(fd)
                self.assertEqual(os.path.getsize(name), resp.content_length)
                tar = tarfile.open(name)
                names = tar.getnames()
                members = tar.getmembers()
                self.assertIn('stdout', names)
                self.assertEqual(names[-1], 'stdout')
                self.assertEqual(members[-1].size, len(self._sortednumbers))
                file = tar.extractfile(members[-1])
                self.assertEqual(file.read(), self._sortednumbers)
                self.assertEqual(resp.headers['x-nexe-retcode'], '0')
                self.assertEqual(resp.headers['x-nexe-status'], 'ok.')
                self.assertEqual(resp.headers['x-nexe-validation'], '0')
                self.assertEqual(resp.headers['x-nexe-system'], 'sort')
                timestamp = normalize_timestamp(time())
                self.assertEqual(
                    math.floor(float(resp.headers['X-Timestamp'])),
                    math.floor(float(timestamp))
                )
                self.assertEquals(
                    resp.headers['content-type'], 'application/x-gtar')
                # self.assertEqual(self.app.logger.log_dict['info'][0][0][0],
                #    'Zerovm CDR: 0 0 0 0 1 46 1 46 0 0 0 0')

    def test_QUERY_bad_channel_path(self):
        self.setup_zerovm_query()
        req = self.zerovm_object_request()
        nexefile = StringIO(self._nexescript)
        conf = ZvmNode(1, 'sort', parse_location('swift://a/c/exe'))
        conf.add_new_channel('stdin', ACCESS_READABLE, 'bla-bla')
        conf = json.dumps(conf, cls=NodeEncoder)
        sysmap = StringIO(conf)
        with create_tar({'sysmap': sysmap, 'boot': nexefile}) as tar:
            length = os.path.getsize(tar)
            req.body_file = Input(open(tar, 'rb'), length)
            req.content_length = length
            resp = self.app.zerovm_query(req)
            fd, name = mkstemp()
            self.assertEqual(resp.status_int, 400)
            self.assertEqual(
                resp.body, 'Could not resolve channel path bla-bla for '
                           'device: stdin')

    def test_QUERY_filter_factory(self):
        app = objectquery.filter_factory(self.conf)(FakeApp(self.conf))
        self.assertIsInstance(app, objectquery.ObjectQueryMiddleware)

    def test_QUERY_prevalidate(self):
        self.setup_zerovm_query()
        req = Request.blank('/sda1/p/a/c/exe',
                            environ={'REQUEST_METHOD': 'PUT'},
                            headers={
                                'X-Timestamp': normalize_timestamp(time()),
                                'x-zerovm-validate': 'true',
                                'Content-Type': 'application/octet-stream'
                            })
        req.body = self._nexescript
        resp = req.get_response(self.app)
        self.assertEquals(resp.status_int, 201)
        self.assertEquals(resp.headers['x-zerovm-valid'], 'true')

        req = Request.blank('/sda1/p/a/c/exe',
                            headers={'x-zerovm-valid': 'true'})
        req.body = self._nexescript
        resp = req.get_response(self.app)
        self.assertEquals(resp.status_int, 200)
        self.assertEquals(resp.headers['x-zerovm-valid'], 'true')

        req = Request.blank('/sda1/p/a/c/exe',
                            environ={'REQUEST_METHOD': 'PUT'},
                            headers={
                                'X-Timestamp': normalize_timestamp(time()),
                                'Content-Type': 'application/octet-stream'
                            })
        req.body = self._nexescript
        resp = req.get_response(self.app)
        self.assertEquals(resp.status_int, 201)
        self.assertNotIn('x-zerovm-valid', resp.headers)

        req = Request.blank('/sda1/p/a/c/exe',
                            headers={'x-zerovm-valid': 'true'})
        req.body = self._nexescript
        resp = req.get_response(self.app)
        self.assertEquals(resp.status_int, 200)
        self.assertNotIn('x-zerovm-valid', resp.headers)

        req = Request.blank('/sda1/p/a/c/exe',
                            environ={'REQUEST_METHOD': 'PUT'},
                            headers={
                                'X-Timestamp': normalize_timestamp(time()),
                                'Content-Type': 'application/x-nexe'
                            })
        req.body = self._nexescript
        resp = req.get_response(self.app)
        self.assertEquals(resp.status_int, 201)
        self.assertEquals(resp.headers['x-zerovm-valid'], 'true')

        req = Request.blank('/sda1/p/a/c/exe',
                            environ={'REQUEST_METHOD': 'PUT'},
                            headers={
                                'X-Timestamp': normalize_timestamp(time()),
                                'Content-Type': 'application/octet-stream'
                            })
        req.body = 'INVALID'
        resp = req.get_response(self.app)
        self.assertEquals(resp.status_int, 201)
        self.assertNotIn('x-zerovm-valid', resp.headers)

        req = Request.blank('/sda1/p/a/c/exe',
                            environ={'REQUEST_METHOD': 'PUT'},
                            headers={
                                'X-Timestamp': normalize_timestamp(time()),
                                'x-zerovm-validate': 'true',
                                'Content-Type': 'application/octet-stream'
                            })
        req.body = 'INVALID'
        resp = req.get_response(self.app)
        self.assertEquals(resp.status_int, 201)
        self.assertNotIn('x-zerovm-valid', resp.headers)

    def test_QUERY_execute_prevalidated(self):
        self.setup_zerovm_query()
        req = self.zerovm_object_request()
        nexefile = StringIO(self._nexescript)
        conf = ZvmNode(1, 'sort', parse_location('swift://a/c/exe'))
        conf.add_new_channel(
            'stdin', ACCESS_READABLE, parse_location('swift://a/c/o'))
        conf.add_new_channel('stdout', ACCESS_WRITABLE)
        conf = json.dumps(conf, cls=NodeEncoder)
        sysmap = StringIO(conf)
        with create_tar({'boot': nexefile, 'sysmap': sysmap}) as tar:
            req.headers['x-zerovm-valid'] = 'true'
            length = os.path.getsize(tar)
            req.body_file = Input(open(tar, 'rb'), length)
            req.content_length = length
            resp = self.app.zerovm_query(req)
            fd, name = mkstemp()
            for chunk in resp.app_iter:
                os.write(fd, chunk)
            os.close(fd)
            self.assertEqual(os.path.getsize(name), resp.content_length)
            tar_result = tarfile.open(name)
            names = tar_result.getnames()
            members = tar_result.getmembers()
            self.assertIn('stdout', names)
            self.assertEqual(names[-1], 'stdout')
            self.assertEqual(members[-1].size, len(self._sortednumbers))
            file = tar_result.extractfile(members[-1])
            self.assertEqual(file.read(), self._sortednumbers)
            self.assertEqual(resp.headers['x-nexe-retcode'], '0')
            self.assertEqual(resp.headers['x-nexe-status'], 'ok.')
            self.assertEqual(resp.headers['x-nexe-validation'], '2')
            self.assertEqual(resp.headers['x-nexe-system'], 'sort')
            timestamp = normalize_timestamp(time())
            self.assertEqual(math.floor(float(resp.headers['X-Timestamp'])),
                             math.floor(float(timestamp)))
            self.assertEquals(
                resp.headers['content-type'], 'application/x-gtar')

            req.headers['x-zerovm-valid'] = 'false'
            length = os.path.getsize(tar)
            req.body_file = Input(open(tar, 'rb'), length)
            req.content_length = length
            resp = self.app.zerovm_query(req)
            fd, name = mkstemp()
            for chunk in resp.app_iter:
                os.write(fd, chunk)
            os.close(fd)
            self.assertEqual(os.path.getsize(name), resp.content_length)
            tar_result = tarfile.open(name)
            names = tar_result.getnames()
            members = tar_result.getmembers()
            self.assertIn('stdout', names)
            self.assertEqual(names[-1], 'stdout')
            self.assertEqual(members[-1].size, len(self._sortednumbers))
            file = tar_result.extractfile(members[-1])
            self.assertEqual(file.read(), self._sortednumbers)
            self.assertEqual(resp.headers['x-nexe-retcode'], '0')
            self.assertEqual(resp.headers['x-nexe-status'], 'ok.')
            self.assertEqual(resp.headers['x-nexe-validation'], '0')
            self.assertEqual(resp.headers['x-nexe-system'], 'sort')
            timestamp = normalize_timestamp(time())
            self.assertEqual(math.floor(float(resp.headers['X-Timestamp'])),
                             math.floor(float(timestamp)))
            self.assertEquals(
                resp.headers['content-type'], 'application/x-gtar')

    def test_zerovm_bad_exit_code(self):

        @contextmanager
        def save_zerovm_exename():
            exename = self.app.zerovm_exename
            try:
                yield True
            finally:
                self.app.zerovm_exename = exename

        self.setup_zerovm_query()
        with save_zerovm_exename():
            (zfd, zerovm) = mkstemp()
            os.write(zfd, trim(r'''
                from sys import exit
                exit(255)
                '''))
            os.close(zfd)
            self.app.zerovm_exename = ['python', zerovm]
            req = self.zerovm_object_request()
            nexefile = StringIO(self._nexescript)
            conf = ZvmNode(1, 'exit', parse_location('swift://a/c/exe'))
            conf = json.dumps(conf, cls=NodeEncoder)
            sysmap = StringIO(conf)
            with create_tar({'boot': nexefile, 'sysmap': sysmap}) as tar:
                length = os.path.getsize(tar)
                req.body_file = Input(open(tar, 'rb'), length)
                req.content_length = length
                resp = self.app.zerovm_query(req)
                self.assertEquals(resp.status_int, 500)
                self.assertIn(
                    'ERROR OBJ.QUERY retcode=Error,  zerovm_stdout=',
                    resp.body
                )
            os.unlink(zerovm)

if __name__ == '__main__':
    unittest.main()
