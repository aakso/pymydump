from __future__ import print_function, unicode_literals

import abc
import logging
import os

from pymydump.errors import PyMyDumpError

LOG = logging.getLogger(__name__)


class ExpireStrategy(object):
    __metaclass__ = abc.ABCMeta

    @abc.abstractmethod
    def expire(self, name):
        pass


class ExpireDirectoryNumFiles(ExpireStrategy):
    def __init__(self, dirname, keep_files=0):
        self.dirname = dirname
        self.keep_files = keep_files

    def expire(self, pattern):
        if not os.path.isdir(self.dirname):
            raise PyMyDumpError('%s: not a directory'.format(self.dirname))

        occurences = 0
        for (fname, _) in sorted(
                self._list(pattern), key=lambda x: x[1], reverse=True):
            occurences += 1
            if occurences > self.keep_files and self.keep_files > 0:
                LOG.debug("delete: {}".format(fname))
                os.unlink(fname)

    def _list(self, pattern):
        for fname in os.listdir(self.dirname):
            if not os.path.isfile(os.path.join(self.dirname, fname)):
                continue
            if not pattern.search(fname):
                continue
            fname = os.path.join(self.dirname, fname)
            yield (fname, os.path.getmtime(fname))
