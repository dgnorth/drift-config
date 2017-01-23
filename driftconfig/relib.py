# -*- coding: utf-8 -*-
'''

ReLib - Lightweight Relational Database Library for Json docs.

TODO:
- Attachments: Store auxiliary files as well (.pem, etc..)

'''
import logging
import json
import re
import collections
import copy
from urlparse import urlparse, parse_qs
import hashlib
from datetime import datetime

from schemautil import check_schema

log = logging.getLogger(__name__)


# Global integrity check switches.
# TODO: Add unit tests to check proper functionality of these flags.
CHECK_INTEGRITY = ['pk', 'fk', 'unique', 'schema']


class RelibError(RuntimeError):
    pass


class TableError(RelibError):
    pass


class ConstraintError(TableError):
    pass


class BackendError(RelibError):
    pass


class Table(object):

    TABLENAME_REGEX = re.compile(r"^([a-z\d.-]){1,50}$")
    PK_FIELDNAME_REGEX = re.compile(r"^([\w\d.-]){1,50}$")

    def __init__(self, table_name, table_store=None, from_def=None):

        # Table name must be nicely formatted so we can use it in path names.
        if not table_name.startswith('#') and not self.TABLENAME_REGEX.match(table_name):
            raise TableError("Table name {!r} didn't match pattern '{}'.".format(
                table_name, self.TABLENAME_REGEX.pattern))

        self._table_name = table_name
        self._rows = {}  # Key is a canonical string rep of primary key
        self._schema = {}
        self._default_values = {}
        self._pk_fields = []
        self._constraints = []
        self._table_store = table_store
        self._group_by_fields = None
        self._subfolder = None
        self._is_system_table = False

        if from_def:
            self.__dict__.update(from_def['dict'])

    def __str__(self):
        return "Table('{}')".format(self._table_name)

    @property
    def name(self):
        return self._table_name

    def _canonicalize_key(self, primary_key, use_group_by=False):
        """
        Return a canonical string representation of the primary key 'primary_key' using
        the primary key fields of this table.

        If 'use_group_by'is set, only the fields specified in a call to set_row_as_file()
        are used.

        'primary_key' is a dict containing all the fields that make up the primary key.

        The canonicalized string must conform to PK_FIELDNAME_REGEX pattern so it can be
        used in file names as well.
        """
        fields = self._group_by_fields if use_group_by else self._pk_fields

        if not set(fields).issubset(set(primary_key.keys())):
            raise TableError("For table '{}', can't make primary key. Need {} but got {}.".format(
                self._table_name, fields, primary_key.keys()))

        canonicalized = '.'.join([str(primary_key[k]) for k in fields if k in primary_key])

        if not self.PK_FIELDNAME_REGEX.match(canonicalized):
            raise ConstraintError("Primary key value {!r} didn't match pattern '{}'.".format(
                canonicalized, self.PK_FIELDNAME_REGEX.pattern))

        return canonicalized

    def _check_row(self, row):
        # Make sure 'row' contains primary key and unique key fields and does not violate any
        # constraints thereof.
        # For convenience, the function returns the canonicalized primary key for the row.
        check_pk = 'pk' in CHECK_INTEGRITY
        check_fk = 'fk' in CHECK_INTEGRITY
        check_unique = 'unique' in CHECK_INTEGRITY
        check_schema_ = 'schema' in CHECK_INTEGRITY

        for c in self._constraints:
            if c['type'] in ['primary_key', 'unique'] and not set(c['fields']).issubset(row):
                raise ConstraintError("In table '{}', row violates constraint {}: {}".format(self._table_name, c, row))

            if c['type'] == 'unique' and check_unique:
                # Check for duplicates
                search_criteria = {k: row[k] for k in c['fields']}
                found = self.find(search_criteria)
                if len(found):
                    raise ConstraintError("Unique constraint violation on {} because of {}.".format(search_criteria, found))
            elif c['type'] == 'foreign_key' and check_fk:
                # Verify foreign row reference, if set.
                if set(c['foreign_key_fields']).issubset(row):
                    foreign_row = self.get_foreign_row(None, c['table'], c['foreign_key_fields'], _row=row)
                    if len(foreign_row) < 1:
                        raise ConstraintError("In table '{}', foreign key record in '{}' not found {}.\nRow data:\n{}".format(
                            self.name, c['table'], {k: row[k] for k in c['foreign_key_fields']}, json.dumps(row, indent=4)))

        # Check Json schema format compliance
        if check_schema_:
            check_schema(row, self._schema, "Adding row to {}".format(self))

        # Check primary key violation
        row_key = self._canonicalize_key(row)
        if check_pk and row_key in self._rows:
            raise ConstraintError("Primary key violation in table '{}': {}".format(self._table_name, row_key))

        return row_key

    def find(self, search_criteria=None):
        """
        Find all rows matching 'search_criteria'.
        'search_criteria' is a dict with field=value pairs.
        """
        if search_criteria is None:
            # Special case, return all rows
            return self._rows.values()

        rows = []
        search_criteria = search_criteria or {}
        for row in self._rows.itervalues():
            for k, v in search_criteria.items():
                if k not in row or row[k] != v:
                    break
            else:
                rows.append(row)

        return rows

    def add(self, row, check_only=False):
        """
        Add a row to the table.
        'row' is a dict.
        The 'row' must at least contain the primary key and unique constraint fields.
        Default values are added to 'row' if they are not defined. This is done even
        though this call fails.
        If a schema is defined for the table, 'row' must conform to it as well.

        The 'row' object is returned for convenience.

        Note, a reference to the 'row' instance itself is stored. modifying the 'row'
        object after it's added to the table is acceptable under certain restrictions.
        Primary key fields and unique constraint fields may not be removed or altered
        without compromising relational integrity. Any other modification is fair game though.

        If 'check_only' is True, then the row is only checked for validation but not
        added to the table.
        """
        # Apply default values
        target_row = self._get_default_values()
        target_row.update(row)
        row = target_row

        row_key = self._check_row(row)
        if not check_only:
            self._rows[row_key] = row
        return row

    def get(self, primary_key):
        """
        Get the record pointed to by 'primary_key'.
        'primary_key' is a dict containing all the fields that make up the primary key.
        """
        return self._rows.get(self._canonicalize_key(primary_key))

    def remove(self, primary_key):
        """
        Remove row from table identified by 'primary_key'.
        """
        del self._rows[self._canonicalize_key(primary_key)]

    def add_primary_key(self, primary_key_fields):
        """
        Add primary key constraint.
        'primary_key' is a comma separated list of field names that make up the primary key.

        Note, the order of the field names is not important, but each row in the table will
        maintain the order of these fields when it's written out as Json.
        """
        self._pk_fields = primary_key_fields.split(',')
        c = {'type': 'primary_key', 'fields': sorted(self._pk_fields)}
        if c not in self._constraints:
            self._constraints.append(c)

    def add_foreign_key(self, foreign_key_fields, table_name, alias_key_fields=None):
        """
        Add foreign key relationship.
        'foreign_key_fields' is a comma separated list of field names that make up the foreign key.
        'table_name' is the name of the table to link to.
        If the field names are different between the tables, 'alias_key_fields' must be set to
        identify them.

        The foreign key must be linked to either a primary key or a unique constraint in the
        other table.

        The foreign key can reference its own table.

        Note, the order of the field names is not important.
        """
        alias_key_fields = alias_key_fields or foreign_key_fields
        c = {
            'type': 'foreign_key',
            'foreign_key_fields': sorted(foreign_key_fields.split(',')),
            'table': table_name,
            'alias_key_fields': sorted(alias_key_fields.split(',')),
        }

        # Make sure the fields in the other table exist and are either
        # the primary key or have unique constraints.
        foreign_table = self._table_store.get_table(table_name)
        foreign_keys = set(c['alias_key_fields'])

        for fc in foreign_table._constraints:
            if fc['type'] in ['primary_key', 'unique'] and foreign_keys.issubset(set(fc['fields'])):
                break
        else:
            raise ConstraintError("Can't create foreign key relationship from {} {} to {}.".format(
                self._table_name, alias_key_fields, table_name))

        self._constraints.append(c)

    def add_unique_constraint(self, unique_key_fields):
        """
        Add a unique contraint to ensure no duplicate values in the fields specified.
        'unique_key' is a comma separated list of field names that make up the unique key.

        Note, the order of the field names is not important.
        """
        c = {'type': 'unique', 'fields': sorted(unique_key_fields.split(','))}
        self._constraints.append(c)

    def add_schema(self, schema):
        """Add Json schema for row validation."""
        self._schema = schema

    def add_default_values(self, default_values):
        """
        Define default values for row data.
        'default_values' is a dict.
        """
        self._default_values = copy.deepcopy(default_values)

    def set_subfolder_name(self, subfolder_name):
        """The table file or fileswill be placed in a subfolder called 'subfolder_name'."""
        self._subfolder = subfolder_name

    def set_row_as_file(self, subfolder_name=None, group_by=None):
        """
        When serializing the table, group rows together into separate files.

        Data for each row group will have a unique file name generated using the row's primary key or
        fields from 'fields_group'.

        For Single field primary keys the file name looks like "<table name>.<primary key value>.json".
        For combined primary keys, the value of each key field is joined with a dot:
        "<table_name>.<key value 1>.<key value 2>.json"

        'group_by' is a comma separated list of primarky key field names to group rows by. If not set,
        all the primary key fields are used resulting in one file per row instance.

        If 'subfolder_name' is set, all the row files will be placed in a subfolder with that name.
        The filenames will still include the table name. The default behavior is not to use subfolder.
        """
        if group_by:
            self._group_by_fields = group_by.split(',')
            if not set(self._group_by_fields).issubset(set(self._pk_fields)):
                raise TableError("'group_by' fields {} must be part of primary key fields {}.".format(self._group_by_fields, self._pk_fields))
        else:
            self._group_by_fields = self._pk_fields

        self._subfolder = subfolder_name

    def get_filename(self, row=None, is_index_file=None):
        """
        Return a file name for this table and 'row' for serialization.

        If the table is serialized as a single file, 'row' should be None.

        If 'use_subfolder' was set earlier, the file name is prefixed with a subfolder
        name.

        If 'is_index_file' is True, the file name is for the table index file.
        """
        if self._group_by_fields and (row is None and not is_index_file):
            raise TableError("Need 'row' to generate a file name because rows in table '{}' are "
                " serialized separately.".format(self._table_name)
                )
        if row and self._group_by_fields is None:
            raise TableError("Can't create filename using 'row' fields without a prior call to set_row_as_file().")

        # Prefix index file names with a #.
        if is_index_file:
            file_name = '#.' + self._table_name
        else:
            file_name = self._table_name

        if self._subfolder:
            # When using subfolders, simply prefix file name with the folder name.
            file_name = self._subfolder + '/' + file_name

        if row:
            file_name += '.' + self._canonicalize_key(row, use_group_by=True)

        file_name += '.json'

        return file_name

    def get_foreign_row(self, primary_key, table_name, foreign_key_fields=None, _row=None):
        """
        Fetch foreign row from 'table_name' referenced by 'primary_key'.
        If more than one foreign key relationship exists between the tables, resolve the
        ambiguity by specifying which key to use in 'table_key'.
        '_row' is used internally in the case where the row can't be fetched using 'primary_key'.
        """
        row = _row or self.get(primary_key)

        for c in self._constraints:
            if c['type'] == 'foreign_key' and c['table'] == table_name:
                if foreign_key_fields is None or foreign_key_fields == c['foreign_key_fields']:
                    break
        else:
            raise TableError("No foreign key relationship found between {} and {}".format(self, table_name))

        foreign_table = self._table_store.get_table(table_name)
        search_criteria = {k2: row[k1] for k1, k2 in zip(c['foreign_key_fields'], c['alias_key_fields'])}
        
        # Special case where foreign row is a reference to the 'row' itself, which is in the process
        # of being inserted.
        if self.name == table_name and set(search_criteria.items()).issubset(set(row.items())):
            result = [row]
        else:
            result = foreign_table.find(search_criteria)

        return result

    def find_references(self, ref_row, _refs=None):
        """
        Return a dict of tables and rows that reference 'ref_row' either directly or indirectly.
        {'table name': [row, ...]}
        """
        refs = _refs or []

        for table in self._table_store.tables.values():
            for c in table._constraints:
                if c['type'] == 'foreign_key' and c['table'] == self.name:
                    # 'table' and 'c' is referencing 'self'.
                    search_criteria = {k2: ref_row[k1] for k1, k2 in zip(c['alias_key_fields'], c['foreign_key_fields'])}
                    for row in table.find(search_criteria):
                        refs.append((table.name, row))
                        if table.name != self.name:
                            table.find_references(row, refs)
        
        # Remove duplicates and formalize the result.
        result = {}
        for table_name, row in refs:
            rows = result.setdefault(table_name, [])
            if row not in rows:
                rows.append(row)

        return result

    def save(self, save_data):
        cs = self._save_table_data(save_data)
        if not self._is_system_table:
            table_meta = self._table_store.get_table_metadata(self._table_name)
            if table_meta['md5'] != cs:
                table_meta['md5'] = cs
                table_meta['last_modified'] = datetime.utcnow().isoformat() + 'Z'

    def load(self, fetch_from_storage):
        return self._load_table_data(fetch_from_storage)

    def _save_table_data(self, save_data):
        """
        Save all table data.

        'save_data' is a function accepting a 'file_name' and 'json' parameter where
        'file_name' is a globally unique identifier for the table data or row and can
        be used when writing out the 'json' data to file, db, cloud storage or any other
        device for safe keeping.
        """

        # Save the rows sorted on primary key.
        rows = [self._rows[k] for k in sorted(self._rows)]

        # TODO: Sunset this, as json serialization is now using sort_keys=True to maintain
        # consistent md5 checksums.
        def orderly_row(row):
            # Sort Json row object keys so that primary key fields come first, and in the order
            # they were originally defined.
            d = collections.OrderedDict()
            for pk_field in self._pk_fields:
                d[pk_field] = row[pk_field]
            d.update(row)  # Chuck in the rest
            return d

        # Stub out the save_data() function so we can calculate a checksum.
        checksum = hashlib.sha256()

        def save_data_check(filename, data):
            checksum.update(data)
            return save_data(filename, data)

        if self._group_by_fields:
            row_per_file = self._group_by_fields == self._pk_fields

            if row_per_file:
                for row in rows:
                    save_data_check(self.get_filename(row), json.dumps(orderly_row(row), indent=4, sort_keys=True))
            else:
                # Group one or more rows together for each file.
                group = {}
                for row in rows:
                    key = self._canonicalize_key(row, use_group_by=True)
                    group.setdefault(key, []).append(orderly_row(row))

                for rowset in group.values():
                    save_data_check(self.get_filename(rowset[0]), json.dumps(rowset, indent=4, sort_keys=True))

            # Add index so we can read it back in automatically
            index = [{k: row[k] for k in self._pk_fields} for row in rows]
            save_data_check(self.get_filename(is_index_file=True), json.dumps(index, indent=4, sort_keys=True))

        else:
            # Write out all rows as a list
            rows = [orderly_row(row) for row in rows]
            save_data_check(self.get_filename(), json.dumps(rows, indent=4, sort_keys=True))

        cs = checksum.hexdigest()
        return cs


    def _load_table_data(self, fetch_from_storage):
        """
        Load table data.

        'fetch_from_storage' is an function that accepts 'file_name' as a single argument and
        returns the data pointed to by 'file_name'.
        """
        if not self._group_by_fields:
            data = fetch_from_storage(self.get_filename())
            try:
                rows = json.loads(data)
            except Exception:
                print "Error parsing json file", self.get_filename()
                raise
            for row in rows:
                self.add(row)
        else:
            # Get index
            row_per_file = self._group_by_fields == self._pk_fields
            index_file_name = self.get_filename(is_index_file=True)
            index = fetch_from_storage(index_file_name)
            index = json.loads(index)

            if row_per_file:
                for primary_key in index:
                    file_name = self.get_filename(row=primary_key)
                    data = fetch_from_storage(file_name)
                    row = json.loads(data)
                    self.add(row)
            else:
                # Group one or more rows together for each file.
                key_groups = {}
                for primary_key in index:
                    key = self._canonicalize_key(primary_key, use_group_by=True)
                    key_groups[key] = primary_key

                for group_key in key_groups.values():
                    file_name = self.get_filename(row=group_key)
                    data = fetch_from_storage(file_name)
                    try:
                        rows = json.loads(data)
                    except Exception:
                        print "Error parsing json file", file_name
                        raise
                    for row in rows:
                        self.add(row)

    def _get_default_values(self):
        """
        Return a dict of default values for this table. Dynamic values are calculated.
        """
        # TODO: Move this to a utility

        d = copy.deepcopy(self._default_values)
        for k, v in d.items():
            if isinstance(v, basestring) and v.startswith('@@'):
                if v == '@@utcnow':
                    d[k] = datetime.utcnow().isoformat() + 'Z'
                elif v == '@@identity':
                    if self._rows:
                        d[k] = max([row[k] for row in self._rows.values()]) + 1
                    else:
                        d[k] = 1
                else:
                    log.warning("Unknown dynamic default value '{}' defined in table '{}'".format(k, self._table_name))
        return d


class SingleRowTable(Table):
    """
    A "single row" table, or simply a Json document.

    Just like a table but doesn't have the concept of a primary key, and is serialized
    out with a dict as root object, as opposed to a list, like with the Table object.
    """

    def __init__(self, table_name, table_store=None, from_def=None):
        super(SingleRowTable, self).__init__(table_name, table_store, from_def)
        self.add({})  # A single row table always has one, and only one row.

    def _canonicalize_key(self, primary_key, use_group_by=False):
        return ''

    def get(self):
        if self._rows:
            return self._rows.values()[0]

    def __getitem__(self, key):
        """Convenience operator to access properties of a single row."""
        return self.get()[key]

    def add(self, row, check_only=False):
        # Adding a row to a single row table essentially means overwrite whatever is
        # in there. So let's remove the singleton record before adding this one if needed.
        tmp = self.get()
        self._rows.clear()
        try:
            return super(SingleRowTable, self).add(row, check_only)
        finally:
            if check_only:
                self._rows.add(tmp)

    def set_row_as_file(self, use_subfolder=None, subfolder_name=None, group_by=None):
        raise TableError("Single row table ")

    def add_default_values(self, default_values):
        # As single row table always contains one row, we need to make re-add the
        # default row now.
        # TODO: Make table_add an atomic action. It's messy to do this post processing
        # by hooking into various functions like this.
        super(SingleRowTable, self).add_default_values(default_values)
        self.add({})

    def _save_table_data(self, save_data):
        """
        Save document.
        """
        doc = self.get() or {}
        data = json.dumps(doc, indent=4, sort_keys=True)
        save_data(self.get_filename(), data)

        checksum = hashlib.sha256()
        checksum.update(data)
        return checksum.hexdigest()

    def _load_table_data(self, fetch_from_storage):
        """
        Load document data.
        """
        data = fetch_from_storage(self.get_filename())
        doc = json.loads(data)
        self.add(doc)


class TableStoreEncoder(json.JSONEncoder):
    """
    The TableStore and Table class can be encoded 'verbatim' except that
    we don't want to include any of the actual rows. This encoder will
    simply remove the rows from the table temporarily while the table
    instance is being encoded.
    """
    def default(self, obj):
        if isinstance(obj, TableStore):
            return obj.__dict__
        elif isinstance(obj, Table):
            #
            tmp, obj._rows = obj._rows, {}  # Remove rows temporarily
            tmp2 = obj.__dict__.pop('_table_store')  # Exlude this property from definition
            try:
                return {'class': obj.__class__.__name__, 'dict': obj.__dict__.copy()}
            finally:
                obj._rows = tmp
                obj._table_store = tmp2

        # Let the base class default method raise the TypeError
        return super(TableStoreEncoder, self).default(obj)


class TableStore(object):

    TS_DEF_FILENAME = '#tsdef.json'
    TS_META_TABLENAME = '#tsmeta'

    def __init__(self, backend=None):
        """
        Initialize TableStore. If 'backend' is set, it will load definition and data from
        that backend.
        """
        self._tables = collections.OrderedDict()
        self._tableorder = []  # Table order, because of DAG
        self._origin = 'clean'
        self._add_metatable()
        if backend:
            self.load_from_backend(backend)

    def __str__(self):
        return 'TableStore(Origin: {}. Tables: {})'.format(self._origin, len(self._tables))

    @property
    def meta(self):
        """The 'meta' table."""
        return self.get_table(self.TS_META_TABLENAME)

    @property
    def tables(self):
        """Dict of all tables, excluding system tables."""
        return {tn: table for tn, table in self._tables.items() if not table._is_system_table}

    def add_table(self, table_name, single_row=False):
        if single_row:
            cls = SingleRowTable
        else:
            cls = Table
        table = cls(table_name, self)
        self._tables[table_name] = table
        return table

    def get_table(self, table_name):
        return self._tables[table_name]

    def clear(self):
        for table in self._tables.values():
            table._table_store = None

    def get_definition(self):
        """
        Returns the definition of this table store as well as all its tables as a Json
        doc.
        """
        self._tableorder = self._tables.keys()
        return json.dumps(self, indent=4, cls=TableStoreEncoder, sort_keys=True)

    def init_from_definition(self, definition):
        """
        Initialize this instance using result from a previous call to
        'get_definition'.
        """
        data = json.loads(definition)
        self.__dict__.update(data)

        # TODO: Maintaint proper DAG order of tables during serialization. It can be remedied
        # by serializing it out as tuple list instead of dict, or decorating the key names with
        # ordinals. Until then, this work-around is needed:
        if self._tables.keys() != self._tableorder:
            # Rearranging the tables according to _tableorder.
            tables = self._tables
            self._tables = collections.OrderedDict()
            for table_name in self._tableorder:
                self._tables[table_name] = tables[table_name]

        for table_name, table_data in self._tables.iteritems():
            # TODO: Make this mapping dynamic instead of hardcoded.
            if table_data['class'] == 'Table':
                cls = Table
            elif table_data['class'] == 'SingleRowTable':
                cls = SingleRowTable
            else:
                raise RuntimeError("Unknown table class '{}'".format(table_data['class']))
            self._tables[table_name] = cls(table_name, self, table_data)

    def check_integrity(self):
        """Run constraints and schema integrity check on current table store."""
        if not CHECK_INTEGRITY:  # Do a quick bail-out.
            return

        b = DictBackend()
        self.save_to_backend(b, run_integrity_check=False)
        # Serializing in a table store will in fact run all the integrity checks.
        TableStore(b)  # This will trigger any constraint or schema violations.

    def save_to_backend(self, backend, force=False, run_integrity_check=True):
        """
        Save this table store definition and table data to 'backend'.

        If the table store is only partial (contains only meta table info) or not
        fully intact, it will not save to 'backend' and instead raise an exception.
        Use 'force' = True to override this behavior.

        If 'run_integrity_check' is True, full integrity check on constraints and schema
        will be made prior to saving to backend.
        """
        # Do basic self test
        if len(self._tables) < 2 and not force:
            # Table store is only partially functional.
            raise RuntimeError("Won't save out partially constructed table store.")

        if run_integrity_check:
            self.check_integrity()

        backend.start_saving()
        backend.save_data(self.TS_DEF_FILENAME, self.get_definition())

        user_tables = [table for table in self._tables.values() if not table._is_system_table]
        system_tables = [table for table in self._tables.values() if table._is_system_table]

        for table in user_tables:
            log.debug("Save to backend %s: %s", backend, table)
            table.save(backend.save_data)

        # Calculate checksum for user tables
        checksum = hashlib.sha256()
        for table in user_tables:
            md5 = self.get_table_metadata(table.name)['md5']
            checksum.update(md5)
        self.meta.get()['checksum'] = checksum.hexdigest()

        for table in system_tables:
            log.debug("Save to backend %s: %s", backend, table)
            table.save(backend.save_data)

        backend.done_saving()

    def load_from_backend(self, backend, skip_definition=False):
        """
        Initialize this table store using data from 'backend'.

        If 'skip_definition' is True, the current definition in the
        TableStore object is used, instead of the one stored in the
        backend.
        """
        backend.start_loading()
        if not skip_definition:
            definition = backend.load_data(self.TS_DEF_FILENAME)
            self.init_from_definition(definition)
        self._origin = str(backend)

        for table in self._tables.values():
            log.debug("Load from backend %s: %s", backend, table)
            table.load(backend.load_data)

        backend.done_loading()

    def get_table_metadata(self, table_name):
        for table_meta in self.meta['tables']:
            if table_meta['table_name'] == table_name:
                return table_meta
        table_meta = {
            'table_name': table_name,
            'md5': '',
            'last_modified': '',
        }
        self.meta['tables'].append(table_meta)
        return table_meta

    def refresh_metadata(self):
        """Refreshes local meta data and returns a tuple of old and new metadata."""
        old = copy.deepcopy(self.meta.get())
        backend = create_backend('memory://' + datetime.utcnow().isoformat() + 'Z')
        self.save_to_backend(backend)
        new = self.meta.get()
        if old != new:
            # If something changed, bump the version and timestamp
            new['version'] += 1
            new['last_modified'] = datetime.utcnow().isoformat() + 'Z'

        return old, new

    def _add_metatable(self):
        """Add table to contain TableStore meta info."""
        meta = self.add_table(self.TS_META_TABLENAME, single_row=True)
        meta._is_system_table = True

        meta.add_schema({
            'type': 'object',
            'properties': {
                'created_on': {'format': 'date-time'},
                'last_modified': {'format': 'date-time'},
                'origin': {'type': 'string'},
                'version': {'type': 'integer'},
                'checksum': {'type': 'string'},

                'tables': {'type': 'array', 'items': {
                    'type': 'object',
                    'properties': {
                        'table_name': {'type': 'string'},
                        'md5': {'type': 'string'},
                        'last_modified': {'format': 'date-time'},
                    },
                }},
            },
            #'required': ['domain_name', 'origin'],
        })
        meta.add_default_values({
            'created_on': '@@utcnow',
            'last_modified': '@@utcnow',
            'version': 1,
            'tables': [],
        })


def load_meta_from_backend(backend):
    """Load TableStore from 'backend' that contains only the meta info."""
    ts = TableStore()
    ts.meta.load(backend.load_data)
    return ts


class Backend(object):
    """
    Backend is used to serialize table definition and data.
    """

    schemes = {}  # Backend registry using url scheme as key.

    def start_saving(self):
        pass

    def start_loading(self):
        pass

    def done_saving(self):
        pass

    def done_loading(self):
        pass

    def save_data(self, file_name, data):
        pass

    def load_data(self, file_name):
        pass


class DictBackend(Backend):
    """Wrap a dict as a Backend for TableStore."""
    def __init__(self, storage=None):
        self.storage = {} if storage is None else storage

    def save_data(self, k, v):
        self.storage[k] = v

    def load_data(self, k):
        return self.storage[k]


def create_backend(url):
    parts = urlparse(url)
    query = parse_qs(parts.query)
    if parts.scheme in Backend.schemes:
        return Backend.schemes[parts.scheme].create_from_url_parts(parts, query)
    else:
        raise RuntimeError("No backend class registered to handle '{}'".format(url))


def get_store_from_url(url):
    return TableStore(create_backend(url))


def copy_table_store(table_store):
    """"Returns a stand-alone copy of 'table_store'."""
    backend = create_backend('memory://' + datetime.utcnow().isoformat() + 'Z')
    table_store.save_to_backend(backend)
    return TableStore(backend)


def diff_tables(t1, t2):
    """
    Compare table 't1' to 't2' and report the difference.
    Returns a dict with 'identical' as True or False depending on if the tables are identical,
    and 'new_rows', 'deleted_rows' and 'modified_rows' lists with the diffs accordingly.
    """
    if t1 == t2:
        return {'identical': True}

    diff = {}
    diff['identical'] = False

    # Cheat by using the table._rows dict directly
    pk1, pk2 = set(t1._rows), set(t2._rows)
    diff['new_rows'] = [t1._rows[pk] for pk in pk1 - pk2]
    diff['deleted_rows'] = [t2._rows[pk] for pk in pk2 - pk1]
    diff['modified_rows'] = []
    for common_pk in pk1.intersection(pk2):
        first = t1._rows[common_pk]
        second = t2._rows[common_pk]
        if first != second:
            diff['modified_rows'].append({'first': first, 'second': second})

    return diff


def diff_meta(m1, m2):
    """Return a diff report on two meta tables."""
    if m1 == m2:
        return {'identical': True}


    diff = {}
    diff['identical'] = False
    diff['checksum'] = {'first': m1['checksum'], 'second': m2['checksum']}

    def parse_8601(s):
        return datetime.strptime(s, "%Y-%m-%dT%H:%M:%S.%fZ")

    d1, d2 = parse_8601(m1['last_modified']), parse_8601(m2['last_modified'])
    diff['modified_diff'] = abs(d2 - d1)

    t1 = {t['table_name']: t for t in m1['tables']}
    t2 = {t['table_name']: t for t in m2['tables']}
    diff['new_tables'] = list(set(t1) - set(t2))
    diff['deleted_tables'] = list(set(t2) - set(t1))
    diff['modified_tables'] = []
    for table_name in t2:
        if table_name in t1 and t1[table_name]['md5'] != t2[table_name]['md5']:
            diff['modified_tables'].append(table_name)

    return diff


def register(cls):
    """Decorator to register Backend class for a particular URL scheme."""
    Backend.schemes[cls.__scheme__] = cls
    return cls
