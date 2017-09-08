from __future__ import print_function, unicode_literals

import logging
import os
import sys

from pymydump.errors import PyMyDumpError

LOG = logging.getLogger(__name__)


class FileOutput(object):
    def __init__(self, dbstream):
        self.dbstream = dbstream

    def write_to_dir(self, outdir, suffix='.sql'):
        if not os.path.exists(outdir):
            os.makedirs(outdir, mode=0755)
        if not os.path.isdir(outdir):
            raise PyMyDumpError('{}: not a directory'.format(outdir))

        prev = None
        f = None
        for chunk, db in self.dbstream:
            if prev is None or prev != db:
                if f is not None:
                    yield (f.name, prev)
                    f.close()
                name = os.path.join(outdir, db + suffix)
                f = open(name, 'w')
            f.write(chunk)
            prev = db
        yield (f.name, prev)
        f.close()

    def write_to_file(self, name):
        if name == '-':
            f = sys.stdout
        else:
            f = open(name, 'w')

        for chunk, _ in self.dbstream:
            f.write(chunk)

        LOG.debug("wrote: %s", name)
