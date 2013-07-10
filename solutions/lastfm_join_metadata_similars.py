#!/usr/bin/env python
# vim:ts=4:sts=4:sw=4:et:wrap:ai:fileencoding=utf-8:


__author__ = "Tiago Alves Macambira < first . last @ chaordicsystems.com>"
__copyright__ = "Copyright (C) 2013 Chaordic Systems S/A"
__license__ = "Public Domain"


from mrjob.job import MRJob
import mrjob.protocol


class LastFMJoinMetadataSimilars(MRJob):

    # All LastFM Dataset data is already in JsonProtocol format.
    # Key is the track_id, value is a dict with data
    INPUT_PROTOCOL = mrjob.protocol.JSONProtocol

    def steps(self):
        return [self.mr(mapper=self.map_identity,
                        reducer=self.reducer_join_metadata_similar),
               ]

    def map_identity(self, track_id, value):
        yield (track_id, value)

    def reducer_join_metadata_similar(self, track_id, values):
        merged_track_info = {}
        for v in values:
            merged_track_info.update(v)
        yield track_id, merged_track_info

if __name__ == '__main__':
    LastFMJoinMetadataSimilars.run()
