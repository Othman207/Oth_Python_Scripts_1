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
Future features
===============

This module contains functions that are considered important but either their
design is likely to change or they lack proper tests. Please use them with care
in test applications and never rely on them in production.
"""
from document_base import Document


class AdvancedDocument(Document):
    """
    Advanced base class for document schemata. Provides more methods than
    :class:`Document` but the API is less stable and behaviour may change.

    Wrapper for a record with predefined metadata.

    Usage::

        >>> from doqu import Document
        >>> from doqu.validators import AnyOf

        >>> class Note(Document):
        ...     structure = {
        ...         'text': unicode,
        ...         'is_note': bool,
        ...     }
        ...     defaults = {
        ...         'is_note': True,
        ...     }
        ...     validators = {
        ...         'is_note': [AnyOf([True])],
        ...     }
        ...
        ...     def __unicode__(self):
        ...         return u'{text}'.format(**self)

    To save model instances and retrieve them you will want a storage::

        >>> from doqu import get_db

        >>> db = get_db(backend='doqu.ext.tokyo_tyrant', port=1983)

        # and another one, just for testing (yep, the real storage is the same)
        >>> other_db = get_db(backend='doqu.ext.tokyo_tyrant', port=1983)

        # let's make sure the storage is empty
        >>> db.clear()

    See documentation on methods for more details.
    """
    #----------------------+
    #  Private attributes  |
    #----------------------+

    def _clone(self, as_schema=None):
        return clone_doc(self, as_schema=as_schema)

    #---------------------+
    #  Public attributes  |
    #---------------------+

    def convert_to(self, other_schema, overrides=None):
        return convert_doc_to(doc, other_schema, overrides=overrides)

    def dump(self, **kwargs):
        return utils.dump_doc(self, **kwargs)

    def is_field_changed(self, name):
        return utils.is_field_changed(self, name)

    def is_valid(self):
        return utils.is_doc_valid(self)

    @classmethod
    def object(cls, storage, pk):
        """
        Returns an instance of given document class associated with a record
        stored with given primary key in given storage. Usage::

            event = Event.object(db, key)

        :param storage:
            a :class:`~doqu.backend_base.BaseStorageAdapter` subclass (see
            :doc:`ext`).
        :param pk:
            the record's primary key (a string).

        """
        return storage.get(cls, pk)

    def save_as(self, key=None, storage=None, **kwargs):
        return future.save_as(doc=self, key=key, storage=storage, **kwargs)

    def validate(self):
        utils.validate_doc(self)

def is_field_changed(doc, name):
    if doc.meta.structure:
        assert name in doc.meta.structure
    if not doc.pk:
        return True

    # FIXME this does *NOT* work for non-native data types such as dates.
    # self._data contains complex types while self._saved_state.data
    # contains only primitive types (supported by given DB backend).
    # We need to coerce them both to one or another form. The problem is
    # that this *may* have side effects such as fetching data from the DB
    # (upwards coercion) or updating/creating records and files (downwards
    # coercion). The solution could be to introduce post-save hooks that
    # would *only* be called when the value is actually saved. Another
    # solution would be introducing a dry run mode and running the
    # converter hooks with this flag.
    if doc.get(name) == doc._saved_state.data.get(name):
        return False
    return True

def clone_doc(doc, as_document=None):
    """
    Returns an exact copy of current instance with regard to model metadata.

    :param as_document:
        class of the new object (must be a :class:`Document` subclass).

    .. note::
        if `as_document` is set, it is not guaranteed that the resulting
        document instance will validate even if the one being cloned is
        valid. The document classes define different rules for validation.

    """
    cls = as_document or type(doc)

    new_obj = cls()

    fields_to_copy = list(new_obj.meta.structure) or list(new_obj._data)
    for name in fields_to_copy:
        if name in doc._data:
            new_obj._data[name] = doc._data[name]

    if doc._saved_state:
        new_obj._saved_state = doc._saved_state.clone()

    return new_obj

def save_doc_as(doc, key=None, storage=None, **kwargs):
    """
    Saves the document under another key (specified as `key` or generated)
    and returns the newly created instance.

    :param key:
        the key by which the document will be identified in the storage.
        Use with care: any existing record with that key will be
        overwritten. Pay additional attention if you are saving the
        document into another storage. Each storage has its own namespace
        for keys (unless the storage objects just provide different ways to
        access a single real storage). If the key is not specified, it is
        generated automatically by the storage.

    See `save()` for details on other params.

    Usage::

        >>> db.clear()
        >>> note = Note(text="hello")   # just create the item

        # WRONG:

        >>> note.save()               # no storage; don't know where to save
        Traceback (most recent call last):
        ...
        AttributeError: cannot save model instance: storage is not defined neither in instance nor as argument for the save() method
        >>> note.save_as()            # same as above
        Traceback (most recent call last):
        ...
        AttributeError: cannot save model instance: storage is not defined neither in instance nor as argument for the save() method

        # CORRECT:

        >>> new_key = note.save(db)                   # storage provided, key generated
        >>> new_key
        u'1'
        >>> new_obj = note.save_as(storage=db)        # same as above
        >>> new_obj
        <Note hello>
        >>> new_obj.pk  # new key
        u'2'
        >>> new_obj.text  # same data
        'hello'
        >>> new_key = note.save()                     # same storage, same key
        >>> new_key
        u'1'
        >>> new_obj = note.save_as()                  # same storage, autogenerated new key
        >>> new_obj.pk
        u'3'
        >>> new_obj = note.save_as('custom_key')      # same storage, key "123"
        >>> new_obj.pk
        'custom_key'

        >>> note.save_as(123, other_db)     # other storage, key "123"
        <Note hello>
        >>> note.save_as(storage=other_db)  # other storage, autogenerated new key
        <Note hello>

    .. warning::

        Current implementation may lead to data corruption if the document
        comes from one database and is being saved to another one, managed
        by a different backend. Use with care.

    """
    # FIXME: this is totally wrong.  We need to completely pythonize all
    # data. The _saved_state *must* be created using the new storage's
    # datatype converters from pythonized data. Currently we just clone the
    # old storage's native record representation. The pythonized data is
    # stored as doc._data while the sort-of-native is at doc._saved_state.data
    new_instance = doc._clone()
    new_instance._saved_state.update(storage=storage)
    new_instance._saved_state.key = key    # reset to None
    new_instance.save(**kwargs)
    return new_instance

        # TODO:
        # param "crop_data" (default: False). Removes all fields that do not
        # correspond to target document class structure (only if it has a
        # structure). Use case: we need to copy a subset of data fields from a
        # large database. Say, that second database is a view for calculations.
        # Example::
        #
        #    for doc in BigDocument(heavy_db):
        #        doc.save_as(TinyDocument, tmp_db)
        #
        # TinyDocument can even do some calculations on save, e.g. extract some
        # datetime data for quick lookups, grouping and aggregate calculation.

def convert_doc_to(doc, other_schema, overrides=None):
    """
    Returns the document as an instance of another model. Copies attributes
    of current instance that can be applied to another model (i.e. only
    overlapping attributes -- ones that matter for both models). All other
    attributes are re-fetched from the database (if we know the key).

    .. note::

        The document key is *preserved*. This means that the new instance
        represents *the same document*, not a new one. Remember that models
        are "views", and to "convert" a document does not mean copying; it
        can however imply *adding* attributes to the existing document.

    Neither current instance nor the returned one are saved automatically.
    You will have to do it yourself.

    Please note that trying to work with the same document via different
    instances of models whose properties overlap can lead to unpredictable
    results: some properties can be overwritten, go out of sync, etc.

    :param other_model:
        the model to which the instance should be converted.
    :param overrides:
        a dictionary with attributes and their values that should be set on
        the newly created model instance. This dictionary will override any
        attributes that the models have in common.

    Usage::

        >>> class Contact(Note):
        ...     structure = {'name': unicode}
        ...     validators = {'name': [required()]}  # merged with Note's
        ...
        ...     def __unicode__(self):
        ...         return u'{name} ({text})'.format(**self)

        >>> note = Note(text='phone: 123-45-67')
        >>> note
        <Note phone: 123-45-67>

        # same document, contact-specific data added
        >>> contact = note.convert_to(Contact, {'name': 'John Doe'})
        >>> contact
        <Contact John Doe (phone: 123-45-67)>
        >>> contact.name
        'John Doe'
        >>> contact.text
        'phone: 123-45-67'

        # same document, contact-specific data ignored
        >>> note2 = contact.convert_to(Note)
        >>> note2
        <Note phone: 123-45-67>
        >>> note2.name
        Traceback (most recent call last):
        ...
        AttributeError: 'Note' object has no attribute 'name'
        >>> note2.text
        'phone: 123-45-67'

    """
    if doc._saved_state.storage and doc._saved_state.key:
        # the record may be invalid for another document class so we are
        # very careful about it
#            try:
        new_instance = doc._saved_state.storage.get(other_schema, doc.pk)
#            except validators.ValidationError:
#                pass
##            new_instance = other_schema()
##            new_instance._saved_state = self._saved_state.clone()
##            for key, value in self.iteritems():
##                try:
##                    new_instance[key] = value
##                except KeyError:
##                    pass
    else:
        new_instance = clone_doc(doc, as_schema=other_schema)

    if overrides:
        for attr, value in overrides.items():
            setattr(new_instance, attr, value)

    return new_instance
