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

from doqu.backend_base import BaseQueryAdapter


class QueryAdapter(BaseQueryAdapter):

    #--------------------+
    #  Magic attributes  |
    #--------------------+

    def __and__(self, other):
        assert isinstance(other, self.__class__)
        q = self._query.intersect(other._query)
        return self._clone(q)

    def __getitem__(self, k):
        result = self._query[k]
        if isinstance(k, slice):
            return [self.storage._decorate(key, data, self.doc_class)
                                       for key, data in result]
        else:
            key, data = result
            return self.storage._decorate(key, data, self.doc_class)

    def __iter__(self):
        for key, data in self._query:
            yield self.storage._decorate(key, data, self.doc_class)

    def __or__(self, other):
        assert isinstance(other, self.__class__)
        q = self._query.union(other._query)
        return self._clone(q)

    def __sub__(self, other):
        assert isinstance(other, self.__class__)
        q = self._query.minus(other._query)
        return self._clone(q)

    #----------------------+
    #  Private attributes  |
    #----------------------+

    def _init(self):
        self._query = self.storage.connection.query
    #    # by default only fetch columns specified in the Model
    #    col_names = self.doc_class._meta.props.keys()
    #    self._query = self.storage.connection.query.columns(*col_names)

    def _clone(self, inner_query=None):
        clone = self.__class__(self.storage, self.doc_class)
        clone._query = self._query if inner_query is None else inner_query
        return clone

    def _where(self, conditions, negate):
        q = self._query
        native_conditions = self._get_native_conditions(conditions)
        for x in native_conditions:
            q = q.exclude(**x) if negate else q.filter(**x)
        return self._clone(q)

    #--------------+
    #  Public API  |
    #--------------+

    def count(self):
        """
        Returns the number of records that match current query. Does not fetch
        the records.
        """
        return self._query.count()

    def _delete(self):
        """
        Deletes all records that match current query.
        """
        self._query.delete()

    def _order_by(self, names, reverse=False):
        if not isinstance(names, basestring):
            raise ValueError('This backend only supports sorting by a single '
                             'field')
        name = names
        # introspect document class and use numeric sorting if appropriate
        # FIXME declare this API somewhere?
        if (hasattr(self.doc_class, 'meta') and
            hasattr(self.doc_class.meta, 'structure')):
            field_type = self.doc_class.meta.structure.get(name)
            numeric = field_type in (int, float)
        else:
            numeric = False

        if reverse:
            name = '-{0}'.format(name)

        q = self._query.order_by(name, numeric)
        return self._clone(q)

    def values(self, name):
        """
        Returns a list of unique values for given column name.
        """
        # note that we get raw values (never through a Document instances) so
        # we need to convert them if possibble
        values = self._query.values(name)
        if (hasattr(self.doc_class, 'meta') and
            hasattr(self.doc_class.meta, 'structure')):
            datatype = self.doc_class.meta.structure.get(name, unicode)
        else:
            datatype = unicode
        # FIXME are these unique? :) nopes.
        return [self.storage.value_from_db(datatype, v) for v in values]

    def where(self, **conditions):
        """
        Returns Query instance filtered by given conditions.
        """
        return self._where(conditions, negate=False)

    def where_not(self, **conditions):
        """
        Returns Query instance. Inverted version of
        :meth:`~doqu.backends.tokyo_tyrant.Query.where`.
        """
        return self._where(conditions, negate=True)
