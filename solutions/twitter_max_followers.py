#!/usr/bin/env python
# vim:ts=4:sts=4:sw=4:et:wrap:ai:fileencoding=utf-8:


__author__ = "Tiago Alves Macambira < first . last @ chaordicsystems.com>"
__copyright__ = "Copyright (C) 2013 Chaordic Systems S/A"
__license__ = "Public Domain"


from mrjob.job import MRJob
import mrjob.protocol


class TwitterMaxFollowers(MRJob):

    # Hey, but our original format can actually be read with JSONProtocol. :)
    INPUT_PROTOCOL = mrjob.protocol.JSONProtocol

    def steps(self):
        return [self.mr(mapper=self.map_parse_inpt,
                        combiner=self.reducer_count_lists,
                        reducer=self.reducer_count_lists),
                self.mr(mapper=self.map_to_single_reducer,
                        combiner=self.reduce_to_max,
                        reducer=self.reduce_to_max),
               ]

    def map_parse_inpt(self, user_id, follower_id):
        yield (user_id, 1)

    def reducer_count_lists(self, user_id, list_of_counters):
        total_followers = 0
        for n in list_of_counters:
            total_followers += n
        yield user_id, total_followers

    def map_to_single_reducer(self, user_id, count):
        yield 'maximum', (user_id, count)

    def reduce_to_max(self, unused_key, user_count_pairs):
        max_count = 0  # Number of followers the most followed user has
        max_count_id = None

        # Hack: None < anything
        for user_id, count in user_count_pairs:
            if count > max_count:
                max_count = count
                max_count_id = user_id
        yield 'maximum', (max_count_id, max_count)


if __name__ == '__main__':
    TwitterMaxFollowers.run()
