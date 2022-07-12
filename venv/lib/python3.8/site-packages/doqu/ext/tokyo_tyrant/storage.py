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

from doqu.backend_base import BaseStorageAdapter
from managers import converter_manager, lookup_manager
from query import QueryAdapter

from pyrant import Tyrant


DEFAULT_HOST = '127.0.0.1'
DEFAULT_PORT = 1978


class StorageAdapter(BaseStorageAdapter):
    supports_nested_data = False
    converter_manager = converter_manager
    lookup_manager = lookup_manager
    query_adapter = QueryAdapter

    #--------------------+
    #  Magic attributes  |
    #--------------------+

    def __contains__(self, key):
        return key in self.connection

    def __iter__(self):
        return iter(self.connection)

    def __len__(self):
        return len(self.connection)

    #----------------+
    #  Internal API  |
    #----------------+

    def _clear(self):
        self.connection.clear()

    def _connect(self):
        # TODO: sockets, etc.
        host = self._connection_options.get('host', DEFAULT_HOST)
        port = self._connection_options.get('port', DEFAULT_PORT)
        self.connection = Tyrant(host=host, port=port)

    def _delete(self, key):
        del self.connection[key]

    def _disconnect(self):
        self.connection = None

    def _get(self, primary_key):
        return self.connection[primary_key] or {}

    def _save(self, key, data):
        key = key or self.connection.generate_key()
        self.connection[key] = data
        return key
