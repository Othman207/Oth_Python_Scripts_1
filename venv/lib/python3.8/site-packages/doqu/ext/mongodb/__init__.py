# -*- coding: utf-8 -*-
#
#    Doqu is a lightweight schema/query framework for document databases.
#    Copyright © 2009—2010  Andrey Mikhaylenko
#
#    This file is part of Doqu.
#
#    Doqu is free software: you can redistribute it and/or modify
#    it under the terms of the GNU Lesser General Public License as published
#    by the Free Software Foundation, either version 3 of the License, or
#    (at your option) any later version.
#
#    Doqu is distributed in the hope that it will be useful,
#    but WITHOUT ANY WARRANTY; without even the implied warranty of
#    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
#    GNU Lesser General Public License for more details.
#
#    You should have received a copy of the GNU Lesser General Public License
#    along with Doqu.  If not, see <http://gnu.org/licenses/>.
"""
MongoDB extension
=================

A storage/query backend for MongoDB.

:status: beta
:database: `MongoDB`_
:dependencies: `pymongo`_
:suitable for: general purpose (mostly server-side)

  .. _MongoDB: http://mongodb.org
  .. _pymongo: http://api.mongodb.org/python

.. warning::

    this module is not intended for production. It contains some hacks and
    should be refactored. However, it is actually used in a real project
    involving complex queries. Patches, improvements, rewrites are welcome.

"""
import pymongo

try:
    from bson.objectid import ObjectId
except ImportError:
    # PyMongo ≤ 2.2
    from pymongo.objectid import ObjectId

from doqu.backend_base import BaseStorageAdapter, BaseQueryAdapter
from doqu.utils.data_structures import CachedIterator

from converters import converter_manager
from lookups import lookup_manager


class QueryAdapter(CachedIterator, BaseQueryAdapter):

    #--------------------+
    #  Magic attributes  |
    #--------------------+

    # ...

    #----------------+
    #  Internal API  |
    #----------------+

    def _do_search(self, **kwargs):
        # TODO: slicing? MongoDB supports it since 1.5.1
        # http://www.mongodb.org/display/DOCS/Advanced+Queries#AdvancedQueries-%24sliceoperator
        spec = self.storage.lookup_manager.combine_conditions(self._conditions)
        if self._ordering:
            kwargs.setdefault('sort',  self._ordering)
            # may be undesirable but prevents "pymongo.errors.OperationFailure:
            # database error: too much data for sort() with no index"
            self.storage.connection.ensure_index(self._ordering)
        cursor = self.storage.connection.find(spec, **kwargs)
        self._cursor = cursor  # used in count()    XXX that's a mess
        return iter(cursor) if cursor is not None else []

    def _init(self, storage, doc_class=dict, conditions=None, ordering=None):
        self.storage = storage
        self.doc_class = doc_class
        self._conditions = conditions or []
        self._ordering = ordering
        #self._query = self.storage.connection.find()
        self._iter = self._do_search()

    # XXX can this be inherited?
#    def _clone(self, inner_query=None):
#        clone = self.__class__(self.storage, self.model)
#        clone._query = self._query.clone() if inner_query is None else inner_query
#        return clone

    def _clone(self, extra_conditions=None, extra_ordering=None):
        return self.__class__(
            self.storage,
            self.doc_class,
            conditions = self._conditions + (extra_conditions or []),
            ordering = extra_ordering or self._ordering,
        )

    def _prepare(self):
        # XXX this seems to be [a bit] wrong; check the CachedIterator workflow
        # (hint: if this meth is empty, query breaks on empty result set
        # because self._iter appears to be None in that case)
        # (Note: same crap in in doqu.ext.shelve_db.QueryAdapter.)

        # also note that we have to ensure that _cache is not empty because
        # otherwise it would be filled over and over again (and not even
        # refilled but appended to).
        # _iter can be None in two cases: a) initial state, and b) the iterable
        # is exhausted, cache filled.
        # but what if the iterable is just empty? _iter=None, _cache=[] and we
        # start over and over.
        # this must be fixed.
        if self._iter is None and not self._cache:   # XXX important for all backends!
            self._iter = self._do_search()

    def _prepare_item(self, raw_data):
        return self.storage._decorate(None, raw_data, self.doc_class)

    def __where(self, lookups, negate=False):
        conditions = list(self._get_native_conditions(lookups, negate))
        return self._clone(extra_conditions=conditions)

    def _where(self, **conditions):
        return self.__where(conditions, negate=False)

    def _where_not(self, **conditions):
        return self.__where(conditions, negate=True)

#        # FIXME PyMongo conditions API propagates; we would like to unify all
#        # APIs but let user specify backend-specific stuff.
#        # TODO:inherit other cursor properties (see pymongo.cursor.Cursor.clone)
#
#        ### HACK: using private properties is nasty
#        old_conds = dict(self._query._Cursor__query_spec())['query']
#
#        combined_conds = dict(old_conds, **conditions)
#        q = self.storage.connection.find(combined_conds)
#        return self._clone(q)

    def _order_by(self, names, reverse=False):
        # TODO: MongoDB supports per-key directions. Support them somehow?
        # E.g.   names=[('foo', True)]   ==   names=['foo'],reverse=True
        direction = pymongo.DESCENDING if reverse else pymongo.ASCENDING
        if isinstance(names, basestring):
            names = [names]
        ordering = [(name, direction) for name in names]
        return self._clone(extra_ordering=ordering)

    #--------------+
    #  Public API  |
    #--------------+

    def count(self):
        """Returns the number of records that match given query. The result of
        `q.count()` is exactly equivalent to the result of `len(q)` but does
        not involve fetching of the records.
        """
        return self._cursor.count()

    def values(self, name):
        """Returns a list of unique values for given field name.

        :param name:
            the field name.

        .. note::

            A set is dynamically build on client side if the query contains
            conditions. If it doesn't, a much more efficient approach is used.
            It is only available within current **connection**, not query.

        """
        # TODO: names like "date_time__year"
        if not self._conditions:
            # this is faster but without filtering by query
            return self.storage.connection.distinct(name)
        values = set()
        for d in self._do_search(fields=[name]):
            values.add(d.get(name))
        return values


class StorageAdapter(BaseStorageAdapter):
    """
    :param host:
    :param port:
    :param database:
    :param collection:
    """
    #supports_nested_data = True

    converter_manager = converter_manager
    lookup_manager = lookup_manager
    query_adapter = QueryAdapter

    #--------------------+
    #  Magic attributes  |
    #--------------------+

    def __contains__(self, key):
        key = self._string_to_object_id(key)
        return bool(self.connection.find({'_id': key}).count())

    def __iter__(self):
        """
        Yields all keys available for this connection.
        """
        return (x['_id'] for x in self.connection.find(spec={}, fields={'_id': 1}))

    def __len__(self):
        return self.connection.count()

    #----------------+
    #  Internal API  |
    #----------------+

    def _decorate(self, key, data, doc_class=dict):
        clean_data = dict(data)
        raw_key = clean_data.pop('_id')
        # this case is for queries where we don't know the PKs in advance;
        # however, we do know them when fetching a certain document by PK
        if key is None:
            key = self._object_id_to_string(raw_key)
        return super(StorageAdapter, self)._decorate(key, clean_data, doc_class)

    def _object_id_to_string(self, key):
        if isinstance(key, ObjectId):
            return u'x-objectid-{0}'.format(key)
        return key

    def _string_to_object_id(self, key):
        # XXX check consistency
        # MongoDB will *not* find items by the str/unicode representation of
        # ObjectId so we must wrap them; however, it *will* find items if their
        # ids were explicitly defined as plain strings. These strings will most
        # likely be not accepted by ObjectId as arguments.
        # Also check StorageAdapter.__contains__, same try/catch there.
        #print 'TESTING GET', model.__name__, primary_key
        assert isinstance(key, basestring)
        if key.startswith('x-objectid-'):
            return ObjectId(key.split('x-objectid-')[1])
        return key

    def _clear(self):
        """
        Clears the whole storage from data.
        """
        self.connection.remove()

    def _connect(self):
        host = self._connection_options.get('host', '127.0.0.1')
        port = self._connection_options.get('port', 27017)
        database_name = self._connection_options.get('database', 'default')
        collection_name = self._connection_options.get('collection', 'default')

        self._mongo_connection = pymongo.Connection(host, port)
        self._mongo_database = self._mongo_connection[database_name]
        self._mongo_collection = self._mongo_database[collection_name]
        self.connection = self._mongo_collection

    def _delete(self, key):
        key = self._string_to_object_id(key)
        self.connection.remove({'_id': key})

    def _disconnect(self):
        self._mongo_connection.disconnect()
        self._mongo_connection = None
        self._mongo_database = None
        self._mongo_collection = None
        self.connection = None

    def _get(self, key):
        obj_id = self._string_to_object_id(key)
        data = self.connection.find_one({'_id': obj_id})
        if data:
            return data
        raise KeyError('collection "{collection}" of database "{database}" '
                       'does not contain key "{key}"'.format(
                           database = self._mongo_database.name,
                           collection = self._mongo_collection.name,
                           key = str(key)
                       ))

    def _get_many(self, keys):
        obj_ids = [self._string_to_object_id(key) for key in keys]
        results = self.connection.find({'_id': {'$in': obj_ids}}) or []
        found_keys = []
        for data in results:
            key = str(self._object_id_to_string(data['_id']))
            found_keys.append(key)
            yield key, data
        if len(found_keys) < len(keys):
            missing_keys = [k for k in keys if k not in found_keys]
            raise KeyError('collection "{collection}" of database "{database}"'
                           ' does not contain keys "{keys}"'.format(
                               database = self._mongo_database.name,
                               collection = self._mongo_collection.name,
                               keys = ', '.join(missing_keys)))
        '''
        assert len(results) <= len(keys), '_id must be unique'
        _get_obj_pk = lambda obj: str(self._object_id_to_string(data['_id']))
        if len(data) == len(keys):
            return ((_get_obj_pk(data), data) for data in results)
#            return [self._decorate(doc_class, _get_obj_pk(obj), data)
#                    for data in results]

        # not all keys were found; raise an informative exception
        _keys = [_get_obj_pk(obj) for obj in results]
        missing_keys = [pk for pk in _keys if pk not in keys]
        raise KeyError('collection "{collection}" of database "{database}" '
                       'does not contain keys "{keys}"'.format(
                           database = self._mongo_database.name,
                           collection = self._mongo_collection.name,
                           keys = ', '.join(missing_keys)))
        '''

    def _prepare_data_for_saving(self, data):
        # the "_id" field should be excluded from the resulting dictionary
        # because a) ObjectId is not handled correctly by converters, and
        # b) the _save() method will anyway add the correct "_id"
        clean = dict((k,v) for k,v in data.iteritems() if k != '_id')
        return super(StorageAdapter, self)._prepare_data_for_saving(clean)

    def _save(self, key, data):
        if key:
            data = dict(data, _id=self._string_to_object_id(key))
        obj_id = self.connection.save(data)
        return self._object_id_to_string(obj_id) or key
