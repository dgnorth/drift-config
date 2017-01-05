# -*- coding: utf-8 -*-
import logging


log = logging.getLogger(__name__)


def diff_table_stores(ts1, ts2):

    report = {}

    try:
        from jsondiff import diff
    except ImportError as e:
        diff = None
        print "Can't import jsondiff' library:", e
        print "To get diffs, run this: pip install jsondiff"

    # Just to keep things fresh, refresh both table stores
    ts1.refresh_metadata()
    ts2.refresh_metadata()

    def timediff_is_older(t1, t2):
        """Returns the time diff sans secs, and if 't1' is older than 't2'."""
        t1 = datetime.strptime(t1, '%Y-%m-%dT%H:%M:%S.%fZ')
        t2 = datetime.strptime(t2, '%Y-%m-%dT%H:%M:%S.%fZ')
        if t1 < t2:
            return str(t2 - t1).split('.', 1)[0], True
        else:
            return str(t1 - t2).split('.', 1)[0], False

    if ts1.meta['last_modified'] == ts2.meta['last_modified']:
        report['last_modified'] = ts1.meta['last_modified']
    else:
        td, is_older = timediff_is_older(ts1.meta['last_modified'], ts2.meta['last_modified'])
        report['last_modified_diff'] = td, is_older, ts1.meta['last_modified'], ts2.meta['last_modified']

    report['tables'] = {}

    for table_name in ts1.tables:
        table_diff = {}
        report['tables'][table_name] = table_diff

        try:
            t1, t2 = ts1.get_table(table_name), ts2.get_table(table_name)
        except KeyError as e:
            print "Can't compare table '{}' as it's missing from origin.".format(table_name)
            continue

        t1_meta, t2_meta = ts1.get_table_metadata(table_name), ts2.get_table_metadata(table_name)
        is_older = False
        if t1_meta['last_modified'] != t2_meta['last_modified']:
            td, is_older = timediff_is_older(t1_meta['last_modified'], t2_meta['last_modified'])
            table_diff['last_modified'] = td, is_older, t1_meta['last_modified'], t2_meta['last_modified']

        if t1_meta['md5'] != t2_meta['md5']:
            diffdump = None
            if diff:
                if is_older:
                    diffdump = diff(t1.find(), t2.find(), syntax='symmetric', marshal=True)
                else:
                    diffdump = diff(t2.find(), t1.find(), syntax='symmetric', marshal=True)

            table_diff['md5'] = diffdump, is_older, t1_meta['md5'], t2_meta['md5']

    return report
