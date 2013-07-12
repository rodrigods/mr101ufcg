#!/usr/bin/env python
# vim:ts=4:sts=4:sw=4:et:wrap:ai:fileencoding=utf-8:


__author__ = "Tiago Alves Macambira < first . last @ chaordicsystems.com>"
__copyright__ = "Copyright (C) 2013 Chaordic Systems S/A"
__license__ = "Public Domain"


from mrjob.job import MRJob
import sys

class TwitterNumFollowersApp(MRJob):

    n = int(sys.argv[2])

    def comparator(self, x, y):
        return x[1] - y[1]

    def steps(self):
        return [self.mr(mapper=self.map_counter,
                        reducer=self.reducer_counter),
                self.mr(mapper=self.map_to_same_reducer,
                        reducer=self.reducer_maximum),]

    def map_counter(self, key, value):
		user = value.split("\t")[0];
        print (user, 1)
		yield (user, 1)

    def reducer_counter(self, key, values):
        print (key, sum(values))
        yield key, sum(values)

	def map_to_same_reducer(self, key, value):
        print (key,value)
		yield "maximum", (key,value)
		
	def reducer_maximum(self, key, values):
        sorted(values, cmp=self.comparator)

        index = 0
        for i in values:
            yield ("maximum_" + str(index), i[0])
            index += 1
            if (index == n):
                break


if __name__ == '__main__':
    TwitterNumFollowersApp.run()

