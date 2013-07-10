#!/usr/bin/python
# vim:ts=4:sts=4:sw=4:et:wrap:ai:fileencoding=utf-8:

"""Concatenates the contents of files inside a .zip into a single gzip file.

Files are separated from each other by newlines.
"""


import gzip
import zipfile
import sys


def main(zip_filename, gzip_filename):
    with zipfile.ZipFile(zip_filename, 'r') as zip_fs, gzip.open(gzip_filename, 'wb', 9) as outfh:
         # process files in zip container
        for i in zip_fs.infolist():
            # Ignore directories or empty files
            if i.filename.endswith('/') or i.file_size == 0:
                continue
            with zip_fs.open(i.filename, 'r') as in_fh:
                outfh.write(in_fh.read())
                outfh.write('\n')


if __name__ == '__main__':
    zip_filename, gzip_filename = sys.argv[1:]
    main(zip_filename, gzip_filename)
