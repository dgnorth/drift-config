# -*- coding: utf-8 -*-
import unittest
import json
import tempfile
import shutil

import jsonschema

from driftconfig.relib import TableStore, Table, TableError, ConstraintError, Backend
from driftconfig.backends import FileBackend


class TestBackend(Backend):
    """Wrap a dict as a Backend for TableStore."""
    def __init__(self, storage):
        self.storage = storage

    def save_data(self, k, v):
        self.storage[k] = v

    def load_data(self, k):
        return self.storage[k]


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
        table2.set_row_as_file(use_subfolder=True)

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

        # Test missing unique field
        with self.assertRaises(ConstraintError) as context:
            table1.add({'pk_field': 123})

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

        self.assertIn("Can't make primary key", str(context.exception))

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
        self.assertItemsEqual([row1, row2, row3], table1.find())

        # Test lookup using unique field as search criteria
        self.assertEqual([row1], table1.find({'unique_field': 'iamunique1'}))
        self.assertEqual([row2], table1.find({'unique_field': 'iamunique2'}))
        self.assertEqual([row3], table1.find({'unique_field': 'iamunique3'}))

        # Test lookup on non-distinct fields
        self.assertEqual([row1], table1.find({'tag': 'red'}))
        self.assertItemsEqual([row2, row3], table1.find({'tag': 'blue'}))

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

        self.assertIn("Foreign key record not found", str(context.exception))

        # Get foreign table row. Only 'blue' row is linked. The 'red' one is orphaned.
        self.assertEqual([], table1.get_foreign_row(row1, 'table2'))
        self.assertEqual([blue_row], table1.get_foreign_row(row2, 'table2'))
        self.assertEqual([blue_row], table1.get_foreign_row(row3, 'table2'))

        # Test bad table name in request for foreign row
        with self.assertRaises(TableError) as context:
            table1.get_foreign_row(row1, 'no_table')

        self.assertIn("No foreign key relationship found", str(context.exception))

        # Test combined keys
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

        self.assertIn("Foreign key record not found", str(context.exception))

        # Clear table store
        ts.clear()

    def test_schema(self):
        ts = TableStore()

        table = ts.add_table('test_table')
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

        self.assertIn("'not conforming' does not match", str(context.exception))

        # Define a default value for the required property and make sure we pass.
        table.add_default_values({'a_pattern': 'some-value'})
        table.add({'id': 123})

    def test_serialization_filenames(self):
        table = Table('test_filename')
        table.add_primary_key('pk')

        # Basic filename check if table is written out as a whole
        filename = table.get_filename()
        self.assertEqual(filename, 'test_filename.json')

        # Set table to write out each row separately
        table.set_row_as_file()

        # Must provide 'row' from now on
        with self.assertRaises(TableError) as context:
            filename = table.get_filename()

        self.assertIn("Need 'row' to generate a file", str(context.exception))

        # Do a couple of check now
        filename = table.get_filename({'pk': 'bob'})
        self.assertEqual(filename, 'test_filename.bob.json')
        filename = table.get_filename({'pk': 10055})
        self.assertEqual(filename, 'test_filename.10055.json')

        table = Table('test_filename')
        table.add_primary_key('pk1,pk2')

        # Make sure call fails as no set_row_as_file has been defined yet.
        with self.assertRaises(TableError) as context:
            filename = table.get_filename({'pk1': 'first', 'pk2': 'second'})

        self.assertIn("Can't create filename", str(context.exception))

        # Define row as file and test row filename creation.
        table.set_row_as_file()
        filename = table.get_filename({'pk1': 'first', 'pk2': 'second'})
        self.assertEqual(filename, 'test_filename.first.second.json')

        # Check subfolder option
        table.set_row_as_file(use_subfolder=True)
        filename = table.get_filename({'pk1': 'first', 'pk2': 'second'})
        self.assertEqual(filename, 'test_filename/test_filename.first.second.json')

        # Check index file name
        filename = table.get_filename(is_index_file=True)
        self.assertEqual(filename, 'test_filename/#.test_filename.json')

    def test_serialization(self):
        # Test serializing two tables which have master-detail relationship.
        # Do both inline and file-per-row with subfolder option and without it.
        for row_as_file in False, True:
            # Test serializing everything using a makeshift Backend that's just a
            # dict wrapper.

            ts = make_store(populate=True, row_as_file=row_as_file)
            storage = {}
            ts.save_to_backend(TestBackend(storage))
            storage = json.loads(json.dumps(storage))  # Do a quick json "leakage" check.
            ts_check = make_store(populate=False, row_as_file=row_as_file)
            ts_check.load_from_backend(TestBackend(storage))

            # The original and the clone should be identical
            for table_name in ts._tables:
                self.assertEqual(ts.get_table(table_name)._rows, ts_check.get_table(table_name)._rows)

            # Serialize a single table and verify
            table_orig = ts.get_table('continents')
            table_check = make_store(populate=False, row_as_file=row_as_file).get_table('continents')
            storage = {}
            ts.save_to_backend(TestBackend(storage))
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
            ts.save_to_backend(TestBackend(storage))
            table_check = make_multi(populate=False, group_by=group_by)
            table_check.load_from_backend(TestBackend(storage))
            self.assertEqual(ts.get_table('multikey')._rows, table_check.get_table('multikey')._rows)

    def test_tablestore_definition(self):
        # Test serializing the definition or meta-data of the table store and tables.
        ts = make_store(populate=False)
        definition = ts.get_definition()
        new_ts = TableStore()
        new_ts.init_from_definition(definition)

        # Note, can't compare the objects wholesale like this because for some reason all dict key fields
        # get type coerced from str to unicode.
        #self.maxDiff = 100000
        #self.assertDictEqual(ts.get_table('continents').__dict__, new_ts.get_table('continents').__dict__)

    def run_backend_test(self, backend, show_progress=False):

        def on_progress(msg):
            if show_progress:
                print ">>>", msg

        backend.on_progress = on_progress

        for row_as_file in False, True:
            ts = make_store(populate=True, row_as_file=row_as_file)
            ts.save_to_backend(backend)

            # Load it back in
            ts_check = make_store(populate=False, row_as_file=row_as_file)
            ts_check.load_from_backend(backend)

            # The original and the clone should be identical
            for table_name in ts._tables:
                self.assertEqual(ts.get_table(table_name)._rows, ts_check.get_table(table_name)._rows)

            # Load it in clean
            ts_check = TableStore(backend)
            for table_name in ts._tables:
                self.assertEqual(ts.get_table(table_name)._rows, ts_check.get_table(table_name)._rows)

    @unittest.skip("S3 test is too slow for unit test")
    def test_s3_backend(self):
        from driftconfig.backends import S3Backend
        backend = S3Backend('relib-test', 'first_attempt', 'eu-west-1')
        self.run_backend_test(backend, show_progress=True)

    #@unittest.skip("Redis test is really suited for systems test and not unit test")
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

