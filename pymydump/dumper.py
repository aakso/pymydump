from __future__ import print_function, unicode_literals

import abc
import logging
import os
import subprocess
import tempfile
from configparser import ConfigParser
from distutils.spawn import find_executable

from pymydump.errors import PyMyDumpError

LOG = logging.getLogger(__name__)


class Dumper(object):
    __metaclass__ = abc.ABCMeta

    @abc.abstractmethod
    def get_databases():
        pass

    @abc.abstractmethod
    def dump_database(name):
        pass


class MySQLDumper(Dumper):
    DEFAULT_CHUNK_SIZE = 1024**2

    CLIENTCMD = 'mysql'
    DUMPCMD = 'mysqldump'

    SHOW_DBS_ARGS = ['-e', 'show databases', '-B', '-N']

    def __init__(self,
                 host,
                 username,
                 password=None,
                 opts={},
                 chunksize=DEFAULT_CHUNK_SIZE):
        self.host = host
        self.username = username
        self.password = password
        self.opts = opts
        self.chunksize = chunksize

    def get_databases(self):
        resp = list(
            self._invoke_mysqltool(self.CLIENTCMD, *self.SHOW_DBS_ARGS))
        return [x.strip() for x in ''.join(resp).split('\n') if x.strip()]

    def dump_database(self, name):
        return self._invoke_mysqltool(self.DUMPCMD, name)

    def _write_mysqlconf(self, f):
        s = ['[client]']
        if self.host:
            s.append('host="{}"'.format(self.host))
        if self.username:
            s.append('user="{}"'.format(self.username))
        if self.password:
            s.append('password="{}"'.format(self.password))
        if self.opts:
            s.append('[mysqldump]')
            for k, v in self.opts:
                if v:
                    s.append('{}="{}"'.format(k, v))
                else:
                    s.append('{}'.format(k))
        print('\n'.join(s), file=f)

    def _invoke_mysqltool(self, exc, *args):
        exc = find_executable(exc)
        if not exc:
            raise PyMyDumpError('cannot find {} in PATH'.format(exc))

        tfd, fifoname = tempfile.mkstemp()
        os.close(tfd)
        os.unlink(fifoname)
        os.mkfifo(fifoname)

        try:
            cmd = [exc] + ['--defaults-file={}'.format(fifoname)] + list(args)
            LOG.debug("invoke: %s", cmd)
            p = subprocess.Popen(
                cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
            with open(fifoname, 'w') as fifo:
                self._write_mysqlconf(fifo)
            chunk = p.stdout.read(self.chunksize)
            while chunk:
                yield chunk
                chunk = p.stdout.read(self.chunksize)
            if p.wait() != 0:
                raise PyMyDumpError(
                    '{}: failed with retval {} and STDERR: {}'.format(' '.join(
                        cmd), p.wait(), p.stderr.read()))
        finally:
            os.unlink(fifoname)
