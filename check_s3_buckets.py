#!/usr/bin/env python
#
# Check specific (or all) buckets for files >options.max_age old.
# To be run from cron.
#
# Use case: you have staging buckets where files are pushed to. Some sort of
# data processing job comes along to consume them (and moves, or removes the files).
# This is a great way to monitor if any files are left behind.
#
"""
Examples:

    * check a bucket named 'my-files' for objects older than 48 hours:
        python check-s3-age.py -b '["my-files"]' -a 48 -d

    * check all buckets for objects older than 1 year:
        python check-s3-age.py -a 8760

    * check all buckets for objects older than 24 hours, if the object name starts with foo:
      # Note: the object name prefix does *not* include the bucket name.
        python check-s3-age.py -p foo -d

Reporting options:
    Everything goes to stdout. This is meant to be run from cron.

    If the --deep (-d) option is used, the script will aggregate results per directory.

    Given these objects in a bucket: a, a/dir/1, a/dir/2, b/zot, b/bzzt, foo/bar/baz/123
    This is the output, if they are all older than max-age:
    { '.': 1,
      'a/dir': 2,
      'b': 2,
      'foo/bar/baz': 1
    }

    If you organize things into a few structured "directories," this will work quite well.
    If --deep is not used, the script will simply report a total number (unless you're
    crazy enough to use --list; then it'll list all objects in s3).


Requires:
    config: .boto
    non-standard packages: python-dateutil, boto
"""

import boto
import datetime
import dateutil.parser
import pprint
from optparse import OptionParser

parser = OptionParser(usage=__doc__)
parser.add_option("-a", "--max-age", help="max age in hours of files, before they are reported", default=24)
parser.add_option("-b", "--buckets", help="LIST of bucket names to check", default=False)
parser.add_option("-d", "--deep", action="store_true", help="report counts by 'directory' structure", default=False)
parser.add_option("-l", "--list-files", action="store_true", help="list found files", default=False)
parser.add_option("-p", "--prefix", help="prefix for s3 API call", default="")
(options, args) = parser.parse_args()

def find_old_objects(bucket):
    """
    Returns all s3 keys (objects) older than max-age hours in the passed bucket object, as a
    list of boto.s3.key.Key objects.
    """
    objects = [x for x in bucket.list(prefix=options.prefix)]

    if len(objects) == 0:
        return None

    past_time_at_max_age = datetime.datetime.now() - datetime.timedelta(hours=options.max_age)

    # timestamp is ISO-8601, i.e. 2013-01-04T17:49:51.000Z. dateutil.parser handles it.
    return [ key for key in objects if dateutil.parser.parse(key.last_modified).replace(tzinfo=None) < past_time_at_max_age ]


if __name__ == '__main__':

    pp = pprint.PrettyPrinter(indent=2)
    buckets = boto.connect_s3().get_all_buckets()


    for bucket in buckets:

        if options.buckets and bucket.name not in options.buckets:
            continue

        old_files = find_old_objects(bucket)

        if not old_files or len(old_files) < 1:
            continue

        print "\nFound %i files more than %s hours old in %s." % (len(old_files), options.max_age, bucket.name)

        if options.list_files:
            print "The files (oh boy, did you ask for it!): "
            pp.pprint([ key.name for key in old_files ])

        if not options.deep:
            continue

        dir_counts = {}
        for path in old_files:
            if '/' in path.name:
                parts = path.name.split('/')
                # if we have 2 slashes, we'll display foo/bar/ as the dir name. Else, just foo/.
                # likewise for deeper nestings.
                if len(parts) > 1:
                    dir = str('/'.join(parts[:len(parts)-1]))
                else:
                    dir = str(parts[0])

                dir_counts[dir] = dir_counts.get(dir, 0) + 1
            else:
                dir_counts['.'] = dir_counts.get('.', 0) + 1

        print "The breakdown: "
        pp.pprint(dir_counts)


