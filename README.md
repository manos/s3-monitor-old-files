S3 file monitor
===============
Check specific (or all) buckets for files >options.max_age old.
To be run from cron.

Use case: you have staging buckets where files are pushed to. Some sort of
data processing job comes along to consume them (and moves, or removes the files).
This is a great way to monitor if any files are left behind.

Examples
--------

* check a bucket named 'my-files' for objects older than 48 hours:

    python check-s3-age.py -b '["my-files"]' -a 48 -d

* check all buckets for objects older than 1 year:

    python check-s3-age.py -a 8760

* check all buckets for objects older than 24 hours, if the object name starts with foo:

 Note: the object name prefix does *not* include the bucket name.
    python check-s3-age.py -p foo -d

Reporting options
-----------------
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

Requires
--------
    config: .boto
    non-standard packages: python-dateutil
