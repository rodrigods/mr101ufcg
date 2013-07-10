#!/usr/bin/env python
# vim:ts=4:sts=4:sw=4:et:wrap:ai:fileencoding=utf-8:


"""Splits original LastFM data into similar and metadata.

Usage::

    python split_lastfm.py --conf=mrjob.conf -r emr --split-output=similars --no-output s3://mr101ufcg/data/lastfm/original/*  --output-dir=s3://mr101ufcg/data/lastfm/similars
    python split_lastfm.py --conf=mrjob.conf -r emr --split-output=metadata --no-output s3://mr101ufcg/data/lastfm/original/*  --output-dir=s3://mr101ufcg/data/lastfm/metadata

"""


from mrjob.job import MRJob
import mrjob.protocol


class SplitLastFMData(MRJob):

    INPUT_PROTOCOL = mrjob.protocol.JSONValueProtocol

    def steps(self):
        return [self.mr(mapper=self.map_split_lastfm_data,
                        reducer=None),  # None here means Identity reducer.
               ]

    def configure_options(self):
        super(SplitLastFMData, self).configure_options()
        self.add_passthrough_option('--split-output', default='metadata',
                                    choices=['metadata', 'similars'])

    def map_split_lastfm_data(self, key, value):
        track_id = value['track_id']

        similars_only = {'similars': value['similars']}

        meta_data_only = value.copy()
        del meta_data_only['similars']

        if self.options.split_output == 'metadata':
            yield track_id, meta_data_only
        elif self.options.split_output == 'similars':
            yield track_id, similars_only
        else:
            assert False, 'WTF, unknown value for split-output opion'


if __name__ == '__main__':
    SplitLastFMData.run()
