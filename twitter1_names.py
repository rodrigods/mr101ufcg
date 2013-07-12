#!/usr/bin/env python
# vim:ts=4:sts=4:sw=4:et:wrap:ai:fileencoding=utf-8:


__author__ = "Tiago Alves Macambira < first . last @ chaordicsystems.com>"
__copyright__ = "Copyright (C) 2013 Chaordic Systems S/A"
__license__ = "Public Domain"


from mrjob.job import MRJob


class TwitterNumFollowersApp(MRJob):

    def steps(self):
        return [self.mr(mapper=self.map_identity,
                        reducer=self.reducer_identity),
               ]

    def map_counter(self, key, value):
		user = value.split("/t")[0];
		attr = value.split("/t")[1];
		if (type(attr) == 'str'):
			yield (attr, 1)

    def reducer_counter(self, key, values):
        yield (key, sum(values))

	def map_to_same_reducer(self, key, value):
		user = value.split("/t")[0];
		yield (user, 1)

if __name__ == '__main__':
    TwitterNumFollowersApp.run()

