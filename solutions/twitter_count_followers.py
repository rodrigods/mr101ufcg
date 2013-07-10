#!/usr/bin/env python
# vim:ts=4:sts=4:sw=4:et:wrap:ai:fileencoding=utf-8:


__author__ = "Tiago Alves Macambira < first . last @ chaordicsystems.com>"
__copyright__ = "Copyright (C) 2013 Chaordic Systems S/A"
__license__ = "Public Domain"


from mrjob.job import MRJob
import mrjob.protocol


class TwitterCountFollowers(MRJob):

    # This means that each line will be provided as-is through
    # mappers value parameter. Key should be ignored.
    INPUT_PROTOCOL = mrjob.protocol.RawValueProtocol

    def steps(self):
        return [self.mr(mapper=self.map_parse_inpt,
                        reducer=self.reducer_count_lists),
               ]

    def map_parse_inpt(self, key, value):
        # our lines consist are int the "user_id\tfollower_id"
        # and each ID is a number. So, let's split those lines
        # and get user_id and follower_id back.
        user_id, follower_id = value.split('\t', 1)
        user_id = int(user_id)
        # We don't really care for the follower id -- we are just counting how
        # many of them one user has:
        # follower_id = int(follower_id)
        yield (user_id, 1)

    def reducer_count_lists(self, user_id, values):
        total_followers = 0
        for v in values:
            total_followers += 1
        yield user_id, total_followers


if __name__ == '__main__':
    TwitterCountFollowers.run()
