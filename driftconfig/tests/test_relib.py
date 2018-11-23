# -*- coding: utf-8 -*-
import unittest
import json
import tempfile
import shutil

from click import echo
import six

import jsonschema

from driftconfig.relib import TableStore, Table, TableError, ConstraintError, DictBackend
from driftconfig.backends import FileBackend


# TODO:
# - test 'check_only' in Table.add().
def make_store(populate, row_as_file=None):
    """Return a table store with two tables and populate if needed."""
    ts = TableStore()

    # Set up two tables using single field constraints
    table1 = ts.add_table('continents')
    table1.add_primary_key('continent_id')
    table1.add_unique_constraint('name')

    table2 = ts.add_table('countries')
    table2.add_primary_key('country_code')
    table2.add_unique_constraint('name')
    table2.add_foreign_key('continent_id', 'continents')

    if row_as_file:
        table2.set_row_as_file(subfolder_name=table2.name)

    if populate:
        # Not a complete list of continents and its countries.
        table1.add({'continent_id': 1, 'name': 'Africa'})
        table1.add({'continent_id': 2, 'name': 'Asia'})
        table1.add({'continent_id': 3, 'name': 'Europe'})

        table2.add({'country_code': 'sd', 'name': 'Sudan', 'continent_id': 1})
        table2.add({'country_code': 'ke', 'name': 'Kenya', 'continent_id': 1})
        table2.add({'country_code': 'gn', 'name': 'Guynea', 'continent_id': 1})

        table2.add({'country_code': 'jp', 'name': 'Japan', 'continent_id': 2})
        table2.add({'country_code': 'vn', 'name': 'Vietnam', 'continent_id': 2})

        table2.add({'country_code': 'is', 'name': 'Iceland', 'continent_id': 3})

    return ts


class TestRelib(unittest.TestCase):

    def setUp(self):
        pass

    def test_constraints(self):

        ts = TableStore()

        # Test badly formatted table name
        with self.assertRaises(TableError) as context:
            ts.add_table('bad name')

        self.assertIn("didn't match pattern", str(context.exception))

        # Set up two tables using single field constraints
        table1 = ts.add_table('table1')
        table1.add_primary_key('pk_field')
        table1.add_unique_constraint('unique_field')

        # Note, constraints are verified in the order they are added to the table.

        # Test missing primary key field
        with self.assertRaises(ConstraintError) as context:
            table1.add({'bogus_field': 'dummy'})

        # Test successfull row inserts
        row1 = table1.add({'pk_field': 1, 'unique_field': 'iamunique1', 'tag': 'red'})
        row2 = table1.add({'pk_field': 2, 'unique_field': 'iamunique2', 'tag': 'blue'})

        # Test default values and their immutability
        mutable = [{'a_list_item': 1}]
        default_values = {'default_value': 'some_value', 'list': mutable}
        table1.add_default_values(default_values)
        # Mutate it a bit
        del default_values['default_value']
        mutable.append('this should not appear')
        row3 = table1.add({'pk_field': 3, 'unique_field': 'iamunique3', 'tag': 'blue'})
        self.assertEqual(row3.get('default_value'), 'some_value')
        self.assertEqual(row3.get('list'), [{'a_list_item': 1}])

        # Test badly formatted primary key
        with self.assertRaises(ConstraintError) as context:
            table1.add({'pk_field': 'no good', 'unique_field': 'x'})  # Can't have spaces

        self.assertIn("didn't match pattern", str(context.exception))

        # Test primary key violation
        with self.assertRaises(ConstraintError) as context:
            table1.add({'pk_field': 1, 'unique_field': 'somethingelse'})

        self.assertIn("Primary key violation", str(context.exception))

        # Test unique constraint violation
        with self.assertRaises(ConstraintError) as context:
            table1.add({'pk_field': 4, 'unique_field': 'iamunique1'})

        self.assertIn("Unique constraint violation", str(context.exception))

        # Test bad lookup
        with self.assertRaises(TableError) as context:
            table1.get({'not_a_pk_field': 1})

        self.assertIn("can't make primary key", str(context.exception))

        # Test lookup using primary key
        self.assertIs(row1, table1.get({'pk_field': 1}))
        self.assertIs(row2, table1.get({'pk_field': 2}))
        self.assertIs(row3, table1.get({'pk_field': 3}))

        # Test lookup using the row itself
        self.assertIs(row1, table1.get(row1))
        self.assertIs(row2, table1.get(row2))
        self.assertIs(row3, table1.get(row3))

        # Test lookup using search criteria for the whole row.
        self.assertEqual([row1], table1.find(row1))
        self.assertEqual([row2], table1.find(row2))
        self.assertEqual([row3], table1.find(row3))

        # Test lookup using no criteria. Should return all rows.
        six.assertCountEqual(self, [row1, row2, row3], table1.find())

        # Test lookup using unique field as search criteria
        self.assertEqual([row1], table1.find({'unique_field': 'iamunique1'}))
        self.assertEqual([row2], table1.find({'unique_field': 'iamunique2'}))
        self.assertEqual([row3], table1.find({'unique_field': 'iamunique3'}))

        # Test lookup on non-distinct fields
        self.assertEqual([row1], table1.find({'tag': 'red'}))
        six.assertCountEqual(self, [row2, row3], table1.find({'tag': 'blue'}))

        # Test foreign key relationship. Previously inserted rows are not re-checked
        # automatically.
        table2 = ts.add_table('table2')
        table2.add_primary_key('pk_field')
        table1.add_foreign_key('tag', 'table2', 'pk_field')
        blue_row = table2.add({'pk_field': 'blue'})

        # Test adding bogus foreign key relationship.
        with self.assertRaises(ConstraintError) as context:
            table1.add_foreign_key('bogus_field', 'table2')  # Field name should not match
        with self.assertRaises(ConstraintError) as context:
            table1.add_foreign_key('bogus_field', 'table2', 'still_bogus_field')

        # Test adding row with foreign key check
        table1.add({'pk_field': 4, 'unique_field': 'iamunique4', 'tag': 'blue'})

        # Test adding row with foreign key violation
        with self.assertRaises(ConstraintError) as context:
            table1.add({'pk_field': 5, 'unique_field': 'iamunique5', 'tag': 'burgundy'})

        self.assertIn("foreign key record in 'table2' not found", str(context.exception))

        # Get foreign table row. Only 'blue' row is linked. The 'red' one is orphaned.
        self.assertEqual(None, table1.get_foreign_row(row1, 'table2'))
        self.assertEqual(blue_row, table1.get_foreign_row(row2, 'table2'))
        self.assertEqual(blue_row, table1.get_foreign_row(row3, 'table2'))

        # Test bad table name in request for foreign row
        with self.assertRaises(TableError) as context:
            table1.get_foreign_row(row1, 'no_table')

        self.assertIn("No foreign key relationship found", str(context.exception))

        # Test combined keys and single row table.
        table3 = ts.add_table('table3')
        table3.add_primary_key('pk_field1,pk_field2')
        table3.add_unique_constraint('unique_field1,unique_field2')

        # Test foreign key relationship on own table, and with different ordered field names.
        table3.add_foreign_key('foreign_field1,foreign_field2', 'table3', 'unique_field2,unique_field1')

        # Test row inserts
        row1 = table3.add({'pk_field1': 1, 'pk_field2': 'x', 'unique_field1': 'u1', 'unique_field2': 'x'})

        # Test unique fields constraint
        with self.assertRaises(ConstraintError) as context:
            row2 = table3.add({'pk_field1': 2, 'pk_field2': 'x', 'unique_field1': 'u1', 'unique_field2': 'x'})

        # Test foreign key. Link to self.
        row2 = table3.add({
            'pk_field1': 2, 'pk_field2': 'x',
            'unique_field1': 'u2', 'unique_field2': 'x',
            'foreign_field1': 'u1', 'foreign_field2': 'x',
        })

        # Test foreign key violation.
        with self.assertRaises(ConstraintError) as context:
            row2 = table3.add({
                'pk_field1': 3, 'pk_field2': 'x',
                'unique_field1': 'u3', 'unique_field2': 'x',
                'foreign_field1': 'bork', 'foreign_field2': 'x',
            })

        self.assertIn("foreign key record in 'table3' not found", str(context.exception))

        # Test all cases with a single row table
        single = ts.add_table('single', single_row=True)
        # single.add_primary_key('pk_field1,pk_field2')
        single.add_unique_constraint('unique_field1,unique_field2')

        # Test foreign key relationship on own table, and with different ordered field names.
        single.add_foreign_key('foreign_field1,foreign_field2', 'table3', 'unique_field2,unique_field1')

        # Test doc insert and remove
        doc = single.add({'pk_field1': 1, 'pk_field2': 'x', 'unique_field1': 'u1', 'unique_field2': 'x'})
        single.remove(doc)

        # Test foreign key. Link to self.
        doc = single.add({
            'pk_field1': 1, 'pk_field2': 'x',
            'unique_field1': 'u2', 'unique_field2': 'x',
            'foreign_field1': 'u1', 'foreign_field2': 'x',
        })

        # Test foreign key violation.
        with self.assertRaises(ConstraintError) as context:
            doc = single.add({
                'pk_field1': 3, 'pk_field2': 'x',
                'unique_field1': 'u3', 'unique_field2': 'x',
                'foreign_field1': 'bork', 'foreign_field2': 'x',
            })

        self.assertIn("foreign key record in 'table3' not found", str(context.exception))

        # Clear table store
        ts.clear()

    def test_schema(self):
        ts = TableStore()

        table = ts.add_table('test-table')
        table.add_primary_key('id')
        table.add_schema({
            'type': 'object',
            'properties': {
                'a_string': {'type': 'string'},
                'a_pattern': {'pattern': r'^([a-z\d-]){1,25}$'},
            },
            'required': ['a_pattern'],
        })

        # Check 'required' rule.
        with self.assertRaises(jsonschema.ValidationError) as context:
            table.add({'id': 123})

        self.assertIn("'a_pattern' is a required property", str(context.exception))

        # Check 'pattern' rule.
        with self.assertRaises(jsonschema.ValidationError) as context:
            table.add({'id': 123, 'a_pattern': 'not conforming'})

        self.assertIn("Schema check failed", str(context.exception))

        # Define a default value for the required property and make sure we pass.
        table.add_default_values({'a_pattern': 'some-value'})
        table.add({'id': 123})

        # Test schema on document
        single = ts.add_table('single', single_row=True)
        single.add_schema({
            'type': 'object',
            'properties': {
                'a_string': {'type': 'string'},
                'a_pattern': {'pattern': r'^([a-z\d-]){1,25}$'},
            },
            'required': ['a_pattern'],
        })
        single.add({'a_string': 'aaa', 'a_pattern': 'abc'})

        # Check 'pattern' rule.
        with self.assertRaises(jsonschema.ValidationError) as context:
            single.add({'a_pattern': 'not conforming'})

        self.assertIn("Schema check failed", str(context.exception))

    def test_integrity_check(self):
        ts = make_store(populate=True)
        ts.check_integrity()
        ts.get_table('continents').remove({'continent_id': 1})

        # Make sure the the foreign key violation is caught
        with self.assertRaises(ConstraintError) as context:
            ts.check_integrity()
        self.assertIn("foreign key record in 'continents' not found", str(context.exception))

        # Also, make sure the check is run before serializing out.
        with self.assertRaises(ConstraintError) as context:
            DictBackend().save_table_store(ts)
            self.assertIn("foreign key record in 'continents' not found", str(context.exception))

    def test_find_references(self):

        ts = TableStore()

        # Make three tables which all have master-detail relationship and use
        # aliased combined foreign keys for extra points.
        # Master table contains a combinded primary key, master_id2 and master_id2.
        # Middle table has foreign key relationship to master table primary keys but with
        # aliased field names.
        # Detail table has foreign key relationship to middle table using a unique field
        # in the middle table, as opposed to the primary key. It also has a reference to
        # itself.
        ts = make_store(populate=True)
        t1 = ts.add_table('master')
        t1.add_primary_key('master_id1,master_id2')
        t1r1 = t1.add({'master_id1': 1, 'master_id2': 'a'})  # Has two 'middle' rows referencing it.
        t1.add({'master_id1': 2, 'master_id2': 'b'})  # Has one 'middle' row references to it.
        t1.add({'master_id1': 3, 'master_id2': 'c'})  # Has no foreign references to it.

        t2 = ts.add_table('middle')
        t2.add_primary_key('middle_id')
        t2.add_foreign_key('m1,m2', 'master', 'master_id1,master_id2')
        t2.add_unique_constraint('middle_unique_id')
        t2r1 = t2.add({'middle_id': 51, 'm1': 1, 'm2': 'a', 'middle_unique_id': 'unique_51'})  # Has two 'detail' row refs.
        t2r2 = t2.add({'middle_id': 52, 'm1': 1, 'm2': 'a', 'middle_unique_id': 'unique_52'})  # Has two 'detail' row refs.
        t2r3 = t2.add({'middle_id': 53, 'm1': 2, 'm2': 'b', 'middle_unique_id': 'unique_53'})  # Has two 'detail' row refs.

        t3 = ts.add_table('detail')
        t3.add_primary_key('detail_id')
        t3.add_foreign_key('middle_unique_id', 'middle')
        t3.add_foreign_key('other_detail_id', 'detail', 'detail_id')
        # This one references unique field "middle.unique_id=51" as well as itself
        t3r1 = t3.add({'detail_id': 100, 'middle_unique_id': 'unique_51', 'other_detail_id': 100})
        # This one references unique field "middle.unique_id=51" as well as the row above.
        t3r2 = t3.add({'detail_id': 101, 'middle_unique_id': 'unique_51', 'other_detail_id': 100})
        # This one references unique field "middle.unique_id=53" but the row above as well.
        t3r3 = t3.add({'detail_id': 102, 'middle_unique_id': 'unique_53', 'other_detail_id': 101})
        # These ones reference unique field "middle.unique_id=53" and nothing else, and are
        # the only one that survive the cascading delete.
        t3r4 = t3.add({'detail_id': 103, 'middle_unique_id': 'unique_53'})
        t3r5 = t3.add({'detail_id': 104, 'middle_unique_id': 'unique_53'})

        ts.check_integrity()
        result = t1.find_references(t1r1)
        # Only rows from 'middle' and 'detail' tables should be expected
        six.assertCountEqual(self, result.keys(), ['middle', 'detail'])

        # First two of three rows in 'middle' should be expected.
        self.assertEqual(len(result['middle']), 2)
        self.assertIn(t2r1, result['middle'])
        self.assertIn(t2r2, result['middle'])
        self.assertNotIn(t2r3, result['middle'])

        # First three of five rows in 'detail' should be expected.
        self.assertEqual(len(result['detail']), 3)
        self.assertIn(t3r1, result['detail'])
        self.assertIn(t3r2, result['detail'])
        self.assertIn(t3r3, result['detail'])
        self.assertNotIn(t3r4, result['detail'])
        self.assertNotIn(t3r5, result['detail'])

        # Delete top row and expect problems
        t1.remove(t1r1)
        with self.assertRaises(TableError):
            ts.check_integrity()

        # Do cascading delete and expect success.
        for table_name, rows in result.items():
            for row in rows:
                ts.get_table(table_name).remove(row)
        ts.check_integrity()

    def test_serialization_filenames(self):
        table = Table('test-filename')
        table.add_primary_key('pk')

        # Basic filename check if table is written out as a whole
        filename = table.get_filename()
        self.assertEqual(filename, 'test-filename.json')

        # Set table to write out each row separately
        table.set_row_as_file()

        # Must provide 'row' from now on
        with self.assertRaises(TableError) as context:
            filename = table.get_filename()

        self.assertIn("Need 'row' to generate a file", str(context.exception))

        # Do a couple of check now
        filename = table.get_filename({'pk': 'bob'})
        self.assertEqual(filename, 'test-filename.bob.json')
        filename = table.get_filename({'pk': 10055})
        self.assertEqual(filename, 'test-filename.10055.json')

        table = Table('test-filename')
        table.add_primary_key('pk1,pk2')

        # Make sure call fails as no set_row_as_file has been defined yet.
        with self.assertRaises(TableError) as context:
            filename = table.get_filename({'pk1': 'first', 'pk2': 'second'})

        self.assertIn("Can't create filename", str(context.exception))

        # Define row as file and test row filename creation.
        table.set_row_as_file()
        filename = table.get_filename({'pk1': 'first', 'pk2': 'second'})
        self.assertEqual(filename, 'test-filename.first.second.json')

        # Check subfolder option
        table.set_row_as_file(subfolder_name=table.name)
        filename = table.get_filename({'pk1': 'first', 'pk2': 'second'})
        self.assertEqual(filename, 'test-filename/test-filename.first.second.json')

        # Check index file name
        filename = table.get_filename(is_index_file=True)
        self.assertEqual(filename, 'test-filename/#.test-filename.json')

    def test_serialization(self):
        # Test serializing two tables which have master-detail relationship.
        # Do both inline and file-per-row with subfolder option and without it.
        for row_as_file in False, True:
            # Test serializing everything using a makeshift Backend that's just a
            # dict wrapper.

            ts = make_store(populate=True, row_as_file=row_as_file)
            storage = {}
            DictBackend(storage).save_table_store(ts)

            # Do a quick json "leakage" check.
            storage = {k: v.decode("ascii") for k, v in storage.items()}
            storage = json.loads(json.dumps(storage))
            storage = {k: v.encode("ascii") for k, v in storage.items()}

            ts_check = make_store(populate=False, row_as_file=row_as_file)
            ts_check._load_from_backend(DictBackend(storage))

            # The original and the clone should be identical
            for table_name in ts.tables:
                self.assertEqual(ts.get_table(table_name)._rows, ts_check.get_table(table_name)._rows)

            # Serialize a single table and verify
            table_orig = ts.get_table('continents')
            table_check = make_store(populate=False, row_as_file=row_as_file).get_table('continents')
            storage = {}
            DictBackend(storage).save_table_store(ts)
            table_check.load(lambda file_name: storage[file_name])
            self.assertEqual(table_orig._rows, table_check._rows)

    def test_serialization_for_group_by(self):
        # Test row groups per file as well for multiple primary key fields

        def make_multi(populate, group_by):
            ts = TableStore()
            table = ts.add_table('multikey')
            table.add_primary_key('key1,key2,key3')
            table.set_row_as_file(group_by=group_by)
            if populate:
                table.add({'key1': 1, 'key2': 1, 'key3': 1})
                table.add({'key1': 1, 'key2': 1, 'key3': 2})
            return ts

        for group_by in ['key1', 'key1,key2', 'key1,key3', 'key3,key1,key2']:
            ts = make_multi(populate=True, group_by=group_by)
            storage = {}
            DictBackend(storage).save_table_store(ts)
            table_check = make_multi(populate=False, group_by=group_by)
            table_check._load_from_backend(DictBackend(storage))
            self.assertEqual(ts.get_table('multikey')._rows, table_check.get_table('multikey')._rows)

    def test_tablestore_definition(self):
        # Test serializing the definition or meta-data of the table store and tables.
        ts = make_store(populate=False)
        definition = ts.get_definition()
        new_ts = TableStore()
        new_ts.init_from_definition(definition)

        # Note, can't compare the objects wholesale like this because for some reason all dict key fields
        # get type coerced from str to unicode.
        # self.maxDiff = 100000
        # self.assertDictEqual(ts.get_table('continents').__dict__, new_ts.get_table('continents').__dict__)

    def test_single_row_table(self):
        ts = make_store(populate=True)
        srt = ts.add_table('doc-test', single_row=True)
        doc = srt.add({'field1': 'something'})
        doc['field1']

    def run_backend_test(self, backend, show_progress=False):

        def on_progress(msg):
            if show_progress:
                echo(">>> " + msg)

        backend.on_progress = on_progress

        for row_as_file in False, True:
            ts = make_store(populate=True, row_as_file=row_as_file)
            backend.save_table_store(ts)

            # Load it back in
            ts_check = make_store(populate=False, row_as_file=row_as_file)
            ts_check._load_from_backend(backend)

            # The original and the clone should be identical
            for table_name in ts.tables:
                self.assertEqual(ts.get_table(table_name)._rows, ts_check.get_table(table_name)._rows)

            # Load it in clean
            ts_check = backend.load_table_store()
            for table_name in ts.tables:
                self.assertEqual(ts.get_table(table_name)._rows, ts_check.get_table(table_name)._rows)

    @unittest.skip("S3 test is too slow for unit test")
    def test_s3_backend(self):
        from driftconfig.backends import S3Backend
        backend = S3Backend('relib-test', 'first_attempt', 'eu-west-1')
        self.run_backend_test(backend, show_progress=True)

    @unittest.skip("Redis test is really suited for systems test and not unit test")
    def test_redis_backend(self):
        from driftconfig.backends import RedisBackend
        backend = RedisBackend()
        self.run_backend_test(backend)

    def test_file_backend(self):
        tmpdirname = tempfile.mkdtemp()
        try:
            backend = FileBackend(tmpdirname)
            self.run_backend_test(backend)
        finally:
            shutil.rmtree(tmpdirname)


if __name__ == '__main__':
    unittest.main()
