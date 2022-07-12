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
Backend API
===========

Abstract classes for unified storage/query API with various backends.

Derivative classes are expected to be either complete implementations or
wrappers for external libraries. The latter is assumed to be a better solution
as Doqu is only one of the possible layers. It is always a good idea to
provide different levels of abstraction and let others combine them as needed.

The backends do not have to subclass :class:`BaseStorageAdapter` and
:class:`BaseQueryAdapter`. However, they must closely follow their API.
"""

import logging
import warnings

import document_base


__all__ = [
    'BaseStorageAdapter', 'BaseQueryAdapter',
    'ProcessorDoesNotExist',
    'LookupManager', 'LookupProcessorDoesNotExist',
    'ConverterManager', 'DataProcessorDoesNotExist',
]

log = logging.getLogger(__name__)


class BaseStorageAdapter(object):
    """Abstract adapter class for storage backends.

    .. note:: Backends policy

        If a public method `foo()` internally uses a private method `_foo()`,
        then subclasses should only overload only the private attribute. This
        ensures that docstring and signature are always correct. However, if
        the backend introduces some deviations in behaviour or extends the
        signature, the public method can (and should) be overloaded at least to
        provide documentation.

    """
    # these must be defined by the backend subclass
    converter_manager = NotImplemented
    lookup_manager = NotImplemented
    query_adapter = NotImplemented

    #--------------------+
    #  Magic attributes  |
    #--------------------+

    def __contains__(self, key):
        raise NotImplementedError

    def __init__(self, **kw):
        "Typical kwargs: host, port, name, user, password."
        attrs = (x for x in dir(self) if not x.startswith('_'))
        self._assert_implemented(*attrs)

        self._connection_options = kw
        self.connection = None
        self.connect()

    def __iter__(self):
        raise NotImplementedError

    def __len__(self):
        raise NotImplementedError

    def __nonzero__(self):
        return self.connection is not None

    #----------------+
    #  Internal API  |
    #----------------+

    @classmethod
    def _assert_implemented(cls, *attrs):
        for attr in attrs:
            assert getattr(cls, attr) != NotImplemented, (
                'Backend {cls.__module__} must define '
                '{cls.__name__}.{attr}'.format(**locals()))

    def _clear(self):
        raise NotImplementedError # pragma: nocover

    def _connect(self):
        raise NotImplementedError

    def _decorate(self, key, data, doc_class=dict):
        """Populates a document class instance with given data. If the class
        has the method `from_storage(storage, key, data)`, it is used to
        produce the result. If this method is not present, a tuple of key and
        data dictionary is returned.
        """
        if hasattr(doc_class, 'from_storage'):
            return doc_class.from_storage(storage=self, key=key, data=data)
        else:
            return key, doc_class(**data)

    def _delete(self, key):
        raise NotImplementedError

    def _get(self, key):
        "Returns a dictionary representing the record with given primary key."
        raise NotImplementedError # pragma: nocover

    def _get_many(self, keys):
        # return (or yield) key/data pairs (order doesn't matter)
        return ((pk, self._get(pk)) for pk in keys)

    def _disconnect(self):
        # typical implementation:
        #   self.connection.close()
        #   self.connection = None
        raise NotImplementedError # pragma: nocover

    def _prepare_data_for_saving(self, data):
        # some backends (e.g. MongoDB) need to skip certain fields so they
        # would overload this method
        return dict((k, self.value_to_db(v)) for k,v in data.iteritems())

    def _save(self, key, data):
        # NOTE: must return the key (given or a new one if none given)
        # NOTE: `key` can be `None`.
        raise NotImplementedError # pragma: nocover

    def _sync(self):
        # a backend should only overload this if it supports the operation
        raise NotImplementedError # pragma: nocover

    #--------------+
    #  Public API  |
    #--------------+

    def clear(self):
        """Clears the whole storage from data, resets autoincrement counters.
        """
        if self.connection is None:  # pragma: nocover
            raise RuntimeError('Cannot clear storage: no connection.')
        self._clear()

    def connect(self):
        """Connects to the database. Raises RuntimeError if the connection is
        not closed yet. Use :meth:`reconnect` to explicitly close the
        connection and open it again.
        """
        if self.connection is not None:  # pragma: nocover
            raise RuntimeError('already connected')
        self._connect()

    def delete(self, key):
        """Deletes record with given primary key.
        """
        if self.connection is None:
            raise RuntimeError('Cannot delete key: no connection.')
        self._delete(key)

    def disconnect(self):
        """Closes internal store and removes the reference to it. If the
        backend works with a file, then all pending changes are saved now.
        """
        if self.connection is None:
            raise RuntimeError('Cannot disconnect: no connection.')
        self._disconnect()

    def get(self, key, doc_class=dict):
        """Returns document instance for given document class and primary key.
        Raises KeyError if there is no item with given key in the database.

        :param key:
            a numeric or string primary key (as supported by the backend).
        :param doc_class:
            a document class to wrap the data into. Default is `dict`.
        """
        if self.connection is None:  # pragma: nocover
            raise RuntimeError('Cannot fetch item: no connection.')

        if not isinstance(key, (int,basestring)):
            warnings.warn('db.get(doc_class, key) is deprecated; use '
                          'db.get(key, doc_class) instead', DeprecationWarning)
            key, doc_class = doc_class, key

        #log.debug('fetching record "%s"' % key)
        data = self._get(key)
        #return self._decorate(doc_class, primary_key, data)

        return self._decorate(key, data, doc_class)

#        # FIXME HACK this should use some nice simple API (maybe "require_key")
#        return self._decorate(key, data, doc_class)
#        if hasattr(doc_class, 'from_storage'):
#            return result
#        else:
#            return result#[1]  # HACK!!!

    def get_many(self, keys, doc_class=dict):
        """Returns an iterator of documents with primary keys from given list.
        Basically this is just a simple wrapper around
        :meth:`~BaseStorageAdapter.get` but some backends can reimplement the
        method in a much more efficient way.
        """
        if self.connection is None:  # pragma: nocover
            raise RuntimeError('Cannot fetch items: no connection.')

        if not hasattr(keys, '__iter__') or \
           not isinstance(keys[0], (int,basestring)):
            warnings.warn('db.get_many(doc_class, keys) is deprecated; use '
                          'db.get_many(keys, doc_class) instead', DeprecationWarning)
            keys, doc_class = doc_class, keys

        return (self._decorate(key, data, doc_class=doc_class)
                           for key, data in self._get_many(keys))

    def get_or_create(self, doc_class=dict, **conditions):
        """Queries the database for records associated with given document
        class and conforming to given extra conditions. If such records exist,
        picks the first one (the order may be random depending on the
        database). If there are no such records, creates one.

        Returns the document instance and a boolean value "created".
        """
        assert conditions

        if self.connection is None:  # pragma: nocover
            raise RuntimeError('Cannot fetch items: no connection.')

        query = self.find(doc_class).where(**conditions)
        if query.count():
            return query[0], False
        else:
            obj = doc_class(**conditions)
            obj.save(self)
            return obj, True

    def find(self, doc_class=dict, **conditions):
        """Returns instances of given class, optionally filtered by given
        conditions.

        :param doc_class:
            Document class. Default is `dict`. Normally you will want a more
            advanced class, such as :class:`~doqu.document_base.Document` or
            its more concrete subclasses (with explicit structure and
            validators).
        :param conditions:
            key/value pairs, same as in :meth:`~BaseQueryAdapter.where`.

        .. note::

            By default this returns a tuple of ``(key, data_dict)`` per item.
            However, this can be changed if `doc_class` provides the method
            `from_storage()`. For example,
            :class:`~doqu.document_base.Document` has the notion of "saved
            state" so it can store the key within. Thus, only a single
            `Document` object is returned per item.

        """
        if self.connection is None:  # pragma: nocover
            raise RuntimeError('Cannot fetch items: no connection.')

        query = self.query_adapter(storage=self, doc_class=doc_class)
        if hasattr(doc_class, 'contribute_to_query'):
            query = doc_class.contribute_to_query(query)
        return query.where(**conditions)

    def get_query(self, model):
        import warnings
        warnings.warn('StorageAdapter.get_query() is deprecated, use '
                      'StorageAdapter.find() instead.', DeprecationWarning)
        return self.find(doc_class=model)

    def reconnect(self):
        """Gracefully closes current connection (if it's not broken) and
        connects again to the database (e.g. reopens the file).
        """
        self.disconnect()
        self.connect()

    def save(self, key, data): #, doc_class=dict):
        """Saves given data with given primary key into the storage. Returns
        the primary key.

        :param key:

            the primary key for given object; if `None`, will be generated.

        :param data:

            a `dict` containing all properties to be saved.

        Note that you must provide current primary key for a record which is
        already in the database in order to update it instead of copying it.
        """
        if self.connection is None:  # pragma: nocover
            raise RuntimeError('Cannot fetch items: no connection.')

        if key is not None and not isinstance(key, (int,basestring)):
            warnings.warn('db.save(data, key) is deprecated; use '
                          'db.get(key, data) instead',
                          DeprecationWarning)
            key, data = data, key

        outgoing = self._prepare_data_for_saving(data)

        resulting_key = self._save(key, outgoing)
        assert resulting_key, 'Backend-specific _save() must return a key'
        return resulting_key

    def sync(self):
        """Synchronizes the storage to disk immediately if the backend supports
        this operation. Normally the data is synchronized either on
        :meth:`save`, or on timeout, or on :meth:`disconnect`. This is strictly
        backend-specific. If a backend does not support the operation,
        `NotImplementedError` is raised.
        """
        if self.connection is None:  # pragma: nocover
            raise RuntimeError('Cannot fetch items: no connection.')

        self._sync()

    def value_from_db(self, datatype, value):
        return self.converter_manager.from_db(datatype, value)

    def value_to_db(self, value):
        return self.converter_manager.to_db(value, self)


class BaseQueryAdapter(object):
    """
    Query adapter for given backend.
    """

    #--------------------+
    #  Magic attributes  |
    #--------------------+

    def __getitem__(self, key):
        raise NotImplementedError # pragma: nocover

    def __init__(self, storage, doc_class):
        self.storage = storage
        self.doc_class = doc_class
        self._init()

    def __iter__(self):
        raise NotImplementedError # pragma: nocover

    def __len__(self):
        return len(self[:])

    def __nonzero__(self):
        # __len__ would be enough for a simple iterable but it would fetch the
        # whole set of results on `bool(query)`. This method makes sure that if
        # the results are fetched in chunks, only the first chunk is fetched.
        try:
            self[0]
        except IndexError:
            return False
        else:
            return True

    def __or__(self, other):
        raise NotImplementedError # pragma: nocover

    def __repr__(self):
        # we make extra DB hits here because query representation is mostly
        # used for interactive debug sessions or tests, so performance is
        # barely an issue in this case.
        MAX_ITEMS_IN_REPR = 10
        cnt = self.count()
        if MAX_ITEMS_IN_REPR < cnt:
            # assuming the query object supports slicing...
            return (str(list(self[:MAX_ITEMS_IN_REPR]))[:-1] + ' ... (other %d items '
                    'not displayed)]' % (cnt - MAX_ITEMS_IN_REPR))
        else:
            return str(list(self))

    def __sub__(self, other):
        raise NotImplementedError # pragma: nocover

    #----------------+
    #  Internal API  |
    #----------------+

    def _get_native_conditions(self, conditions, negate=False):
        """
        Returns a generator for backend-specific conditions based on a
        dictionary of backend-agnostic ones.
        """
        # TODO: enable other query APIs (Mongo-like, TC-like, etc.)
        for lookup, value in conditions.iteritems():
            if '__' in lookup:
                name, operation = lookup.split('__')    # XXX check if there are 2 parts
            else:
                name, operation = lookup, None
            processor = self.storage.lookup_manager.get_processor(operation)
            # lookup processor may (not) want to convert value to the
            # database-friendly format; we pass the appropriate function along
            # with the intact "pythonized" value
            def preprocessor(x):
                return self.storage.converter_manager.to_db(x, self.storage)
            native = processor(name, value, preprocessor, negate)

            # yield name/value pair(s)
            if hasattr(native, 'next') or isinstance(native, (list, tuple)):
                for x in native:
                    yield x
            else:
                yield native  #(name, value)

    def _init(self):
        pass

    def _delete(self):
        raise NotImplementedError # pragma: nocover

    def _order_by(self, names, reverse=False):
        raise NotImplementedError # pragma: nocover

    def _values(self, **conditions):
        raise NotImplementedError # pragma: nocover

    def _where(self, **conditions):
        raise NotImplementedError # pragma: nocover

    def _where_not(self, **conditions):
        raise NotImplementedError # pragma: nocover

    #--------------+
    #  Public API  |
    #--------------+

    def count(self):
        """Returns the number of records that match given query. The result of
        `q.count()` is exactly equivalent to the result of `len(q)`. The
        implementation details do not differ by default, but it is recommended
        that the backends stick to the following convention:

        - `__len__` executes the query, retrieves all matching records and
          tests the length of the resulting list;
        - `count` executes a special query that only returns a single value:
          the number of matching records.

        Thus, `__len__` is more suitable when you are going to iterate the
        records anyway (and do no extra queries), while `count` is better when
        you just want to check if the records exist, or to only use a part of
        matching records (i.e. a slice).
        """
        return len(self)    # may be inefficient, override if possible

    def delete(self):
        """Deletes all records that match current query.
        """
        self._delete()

    def order_by(self, names, reverse=False):
        """Returns a query object with same conditions but with results sorted
        by given field. By default the direction of sorting is ascending.

        :param names:

            list of strings: names of fields by which results should be sorted.
            Some backends may only support a single field for sorting.

        :param reverse:

            `bool`: if `True`, the direction of sorting is reversed
            and becomes descending. Default is `False`.

        """
        return self._order_by(names, reverse=reverse)

    def values(self, name):
        """Returns a list of unique values for given field name.

        :param name:
            the field name.

        """
        return self._values(name)

    def where(self, **conditions):
        """Returns Query instance filtered by given conditions.
        The conditions are specified by backend's underlying API.
        """
        return self._where(**conditions)

    def where_not(self, **conditions):
        """Returns Query instance. Inverted version of `where()`.
        """
        return self._where_not(**conditions)


#--- PROCESSORS


class ProcessorDoesNotExist(Exception):
    """This exception is raised when given backend does not have a processor
    suitable for given value. Usually you will need to catch a subclass of this
    exception.
    """
    pass


class ProcessorManager(object):
    """Abstract manager of named functions or classes that process data.
    """
    exception_class = ProcessorDoesNotExist

    def __init__(self):
        self.processors = {}
        self.default = None

    def register(self, key, default=False):
        """Registers given processor class with given datatype. Decorator.
        Usage::

            converter_manager = ConverterManager()

            @converter_manager.register(bool)
            class BoolProcessor(object):
                def from_db(self, value):
                    return bool(value)
                ...

        Does not allow registering more than one processor per datatype. You
        must unregister existing processor first.
        """
        def _inner(processor):
            if key in self.processors:
                raise RuntimeError(
                    'Cannot register %s as processor for %s: %s is already '
                    'registered as such.'
                    % (processor, key, self.processors[key]))
            self._validate_processor(processor)
            self.processors[key] = processor
            if default:
                self.default = processor
            return processor
        return _inner

    def unregister(self, key):
        """Unregisters and returns a previously registered processor for given
        value or raises :class:`ProcessorDoesNotExist` is none was registered.
        """
        try:
            processor = self.processors[key]
        except KeyError:
            raise DataProcessorDoesNotExist
        else:
            del self.processors[key]
            return processor

    def get_processor(self, value):
        """Returns processor for given value.

        Raises :class:`DataProcessorDoesNotExist` if no suitable processor is
        defined by the backend.
        """
        key = self._preprocess_key(value)
        try:
            if key:
                return self.processors[key]
            else:
                if self.default:
                    return self.default
                raise KeyError
        except KeyError:
            raise DataProcessorDoesNotExist(
                'Backend does not define a processor for %s.' % repr(key))

    def _validate_processor(self, processor):
        "Returns `True` if given `processor` is acceptable."
        return True

    def _preprocess_key(self, key):
        return key


class LookupProcessorDoesNotExist(ProcessorDoesNotExist):
    """This exception is raised when given backend does not support the
    requested lookup.
    """
    pass


class LookupManager(ProcessorManager):
    """Usage::

        lookup_manager = LookupManager()

        @lookup_manager.register('equals', default=True)  # only one lookup can be default
        def exact_match(name, value):
            '''
            Returns native Tokyo Cabinet lookup triplets for given
            backend-agnostic lookup triplet.
            '''
            if isinstance(value, basestring):
                return (
                    (name, proto.RDBQCSTREQ, value),
                )
            if isinstance(value, (int, float)):
                return (
                    (name, proto.RDBQCNUMEQ, value),
                )
            raise ValueError

    Now if you call ``lookup_manager.resolve('age', 'equals', 99)``, the
    returned value will be ``(('age', proto.RDBCNUMEQ, 99),)``.

    A single generic lookup may yield multiple native lookups because some
    backends do not support certain lookups directly and therefore must
    translate them to a combination of elementary conditions. In most cases
    :meth:`~LookupManager.resolve` will yield a single condition. Its format is
    determined by the query adapter.
    """
    exception_class = LookupProcessorDoesNotExist

    # TODO: yield both abstract and native lookups. Abstract lookups will be
    # then parsed further until a set of all-native lookups is collected.
    # (beware: 1. endless recursion, and 2. possible logic trees)

    def resolve(self, name, operation, value):
        """Returns a set of backend-specific conditions for given
        backend-agnostic triplet, e.g.::

            ('age', 'gt', 90)

        will be translated by the Tokyo Cabinet backend to::

            ('age', 9, '90')

        or by the MongoDB backend to::

            {'age': {'$gt': 90}}

        """
        # TODO: provide example in docstring
        datatype = type(value)
        processor = self.get_processor(operation)
        return processor(name, value)


class DataProcessorDoesNotExist(ProcessorDoesNotExist):
    """This exception is raised when given backend does not have a datatype
    processor suitable for given value.
    """
    pass


class ConverterManager(ProcessorManager):
    """An instance of this class can manage property processors for given
    backend. Processor classes must be registered against Python types or
    classes. The processor manager allows encoding and decoding data between a
    document class instance and a database record. Each backend supports only a
    certain subset of Python datatypes and has its own rules in regard to how
    `None` values are interpreted, how complex data structures are serialized
    and so on. Moreover, there's no way to guess how a custom class should be
    processed. Therefore, each combination of data type + backend has to be
    explicitly defined as a set of processing methods (to and from).
    """
    exception_class = DataProcessorDoesNotExist

    def _preprocess_key(self, value):
        if issubclass(value, document_base.Document):
            return document_base.Document
        if isinstance(value, document_base.OneToManyRelation):
            return document_base.OneToManyRelation
        return value

    def _validate_processor(self, processor):
        if hasattr(processor, 'from_db') and hasattr(processor, 'to_db'):
            return True
        raise AttributeError('Converter class %s must have methods "from_db" '
                             'and "to_db".' % processor)

    def _pick_processor(self, datatype):
        # try datatype; if the backend does not directly support it, try the
        # datatype's bases
        try:
            bases = datatype.mro()
        except AttributeError:
            bases = type(datatype).mro()
        for base in bases:
            try:
                processor = self.get_processor(datatype)
            except DataProcessorDoesNotExist:
                # try an underlying class
                continue
            except TypeError:
                # looks like we should stop trying
                raise DataProcessorDoesNotExist(str(datatype))
            else:
                return processor
        raise DataProcessorDoesNotExist(str(datatype))


    def from_db(self, datatype, value):
        """Converts given value to given Python datatype. The value must be
        correctly pre-encoded by the symmetrical :meth:`PropertyManager.to_db`
        method before saving it to the database.

        Raises :class:`DataProcessorDoesNotExist` if no suitable processor is
        defined by the backend.
        """
        if isinstance(datatype, basestring):
            # probably lazy import path, noop will do, model will take care
            return value

        p = self._pick_processor(datatype)
        return p.from_db(value)

    def to_db(self, value, storage):
        """Prepares given value and returns it in a form ready for storing in
        the database.

        Raises :class:`DataProcessorDoesNotExist` if no suitable processor is
        defined by the backend.
        """

        # XXX references declared with lazy imports?

        datatype = type(value)
        p = self._pick_processor(datatype)
        return p.to_db(value, storage)
