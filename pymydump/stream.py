from __future__ import print_function, unicode_literals

from pymydump.compress import BZ2Compressor, NoopCompressor
from pymydump.errors import PyMyDumpError


class DBStream(object):
    def __init__(self,
                 dumper,
                 compressor_name='none',
                 pattern=None,
                 single_stream=False):
        self.dumper = dumper
        self.pattern = pattern
        self.single_stream = single_stream
        self.compressor_name = compressor_name
        self.compressor = None
        self._init_compressor()

    def _init_compressor(self):
        if self.compressor_name == 'bz2':
            self.compressor = BZ2Compressor()
        elif self.compressor_name == 'none':
            self.compressor = NoopCompressor()
        else:
            raise PyMyDumpError('invalid compressor: {}'.format(
                self.compressor_name))

    def stream(self):
        if self.single_stream:
            return self._single_stream()
        return self._per_db_stream()

    def _per_db_stream(self):
        for db in self.dumper.get_databases():
            self._init_compressor()
            if not self.pattern.search(db):
                continue
            for chunk in self.dumper.dump_database(db):
                data = self.compressor.compress(chunk)
                if not data:
                    continue
                yield (data, db)
            data = self.compressor.flush()
            if data:
                yield (data, db)

    def _single_stream(self):
        self._init_compressor()
        for db in self.dumper.get_databases():
            if not self.pattern.search(db):
                continue
            for chunk in self.dumper.dump_database(db):
                data = self.compressor.compress(chunk)
                if not data:
                    continue
                yield (data, None)
        data = self.compressor.flush()
        if data:
            yield (data, None)
