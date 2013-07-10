#!/usr/bin/env python
# vim:ts=4:sts=4:sw=4:et:wrap:ai:fileencoding=utf-8:


"""Discover the "artist - track name" from the most plain songs.

Here, "plains" means that it is the song most songs are similar to, assuming:
 * a similarity threshold of 0.8,
 * that similarity is not transitive and
 * similarity, as encoded in our data means from current track to other track.

Inputs:
    * metadata files (merged with similarity or not)
    * similarity files (merged with metdata or not)
"""


__author__ = "Tiago Alves Macambira < first . last @ chaordicsystems.com>"
__copyright__ = "Copyright (C) 2013 Chaordic Systems S/A"
__license__ = "Public Domain"


from mrjob.job import MRJob
import mrjob.protocol


MIN_SIMILARITY_THRESHOLD = 0.8


class LastFMMostPlainSong(MRJob):

    # All LastFM Dataset data is already in JsonProtocol format.
    # Key is the track_id, value is a dict with data
    INPUT_PROTOCOL = mrjob.protocol.JSONProtocol

    def steps(self):
        return [self.mr(mapper=self.map_track_similars_and_metadata,
                        reducer=self.reducer_join_votes_and_metadata),
               ]

    def map_track_similars_and_metadata(self, track_id, value):
        if 'similars' in value:
            for sim_track, sim_level in value['similars']:
                if sim_level >= MIN_SIMILARITY_THRESHOLD:
                    # Hey, the current song is really similar this other track,
                    # i.e., to sim_track. So let sim_track know that we are voting
                    # for it to the most plain song
                    yield sim_track, {'votes': 1}
        # Down in the reducer we will need to know how the this song's
        # title and artist so let's forward this information to the reducer step
        if 'title' in value:
            yield track_id, {'title': value['title']}
        if 'artist' in value:
            yield track_id, {'artist': value['artist']}

    def reducer_join_votes_and_metadata(self, track_id, values):
        merged_metadata = {}
        votes = 0
        for v in values:
            if 'votes' in v:
                votes += v['votes']
            if 'title' in v:
                merged_metadata['title'] = v['title']
            if 'artist' in v:
                merged_metadata['artist'] = v['artist']

        merged_metadata['votes'] = votes

        if votes > 100:
            yield track_id, merged_metadata

if __name__ == '__main__':
    LastFMMostPlainSong.run()
