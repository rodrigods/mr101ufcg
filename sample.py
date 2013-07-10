#!/usr/bin/env python
# vim:ts=4:sts=4:sw=4:et:wrap:ai:fileencoding=utf-8:


__author__ = "Tiago Alves Macambira < first . last @ chaordicsystems.com>"
__copyright__ = "Copyright (C) 2013 Chaordic Systems S/A"
__license__ = "Public Domain"


from mrjob.job import MRJob


class SampleMrJobApp(MRJob):

    def steps(self):
        return [self.mr(mapper=self.map_identity,
                        reducer=self.reducer_identity),
               ]

    def map_identity(self, key, value):
        yield (key, value)

    def reducer_identity(self, key, values):
        for v in values:
            yield (key, v)


if __name__ == '__main__':
    SampleMrJobApp.run()
