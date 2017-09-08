from __future__ import print_function, unicode_literals

import bz2


class NoopCompressor(object):
    def compress(self, data):
        return data

    def flush(self):
        return ''


class BZ2Compressor(object):
    def __init__(self):
        self.compressor = bz2.BZ2Compressor()

    def compress(self, data):
        return self.compressor.compress(data)

    def flush(self):
        return self.compressor.flush()
