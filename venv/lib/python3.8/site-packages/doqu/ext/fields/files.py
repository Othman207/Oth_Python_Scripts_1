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
File-related Fields
===================
"""
try:
    import Image    # for ImageField
except ImportError:
    Image = None
import os
import shutil

from doqu.utils import cached_property
from .base import Field


__all__ = ['FileField', 'ImageField',
           'FileWrapper', 'ImageWrapper']


class FileWrapper(object):
    """A thin wrapper around the `file` object. In most cases you'll only want
    to know about :attr:`FileWrapper.file`.
    """
    def __init__(self, base_path, stream=None, path=None, saved=False):
        assert stream or path
        self.path = os.path.split(stream.name)[1] if stream else path
        self.saved = saved

        if hasattr(base_path, '__call__'):
            self.base_path = base_path()
        else:
            self.base_path = base_path

        self._stream = stream or open(self.full_path, 'rb')

    def __repr__(self):
        return '<{cls}: {path}>'.format(
            cls=self.__class__.__name__,
            path=self._stream.name.encode('utf-8'))

    def _get_file_ext(self, ext):
        return ext

    def _generate_path(self, base_path, name):
        """Returns a tuple of ``(full_path, new_name)``.
        """
        # ensure uniqueness
        fname = os.path.split(name)[1]
        root_name, ext = os.path.splitext(fname)
        ext = self._get_file_ext(ext)
        while 1:
            new_name = root_name+ext
            path = os.path.join(base_path, new_name)
            if not os.path.exists(path):
                return path, new_name
            root_name += '_'

#    @cached_property
#    def data(self):
#        return self.file.read()

    @cached_property
    def file(self):
        """The actual file-like object.
        """
        # TODO: validate (at least check extension)
        return self._stream

    @property
    def full_path(self):
        """Full path to the file. If the file was not saved yet (e.g. changed
        or just added), returns `None`.
        """
        if not self.saved:
            return None
        return os.path.join(self.base_path, self.path)

    def save(self):
        """If the `saved` flag is present (i.e. if the object was fetched from
        the database and nothing was changed), does nothing. If not, saves the
        stream to a *new* unique path generated with the original path in mind.
        """
        if self.saved:
            return
        assert self._stream

        destination, new_name = self._generate_path(self.base_path, self.path)
        destination = open(destination, 'wb')

        self._stream.seek(0)   # XXX this is required for images; maybe PIL looks for format?
        shutil.copyfileobj(self._stream, destination)
        self.saved = True
        self.path = new_name


class ImageWrapper(FileWrapper):
    """
    A FileWrapper which deals fith files via PIL and provides advanced
    image-related methods (compared to FileWrapper). See :class:`Image` for
    details. The image is available as ``file`` attribute.
    """
    def _get_file_ext(self, ext):
        # force correct extension for current format
        return '.' + self.file.format.lower()

    @cached_property
    def file(self):
        """An `Image` instance that represents current file.
        """
        if Image is None:
            raise ImportError('PIL is not installed.')
        image = Image.open(self._stream)
        image.verify()  # TODO: wrap exception
        # workaround: verify() breaks the image (image.fh becomes None)
        self._stream.seek(0)
        image = Image.open(self._stream)
        return image


class FileField(Field):
    """
    Handles externally stored files.

    .. warning::

        This field saves the file when :meth:`process_outgoing` is triggered
        (see `outgoing_processors` in
        :class:`~doqu.document_base.DocumentMetadata`).

        Outdated (replaced) files are *not* automatically removed.

    Usage::

        class Doc(Document):
            attachment = FileField(base_path=MEDIA_ROOT+'attachments/')

        d = Doc()
        d.attachment = open('foo.txt')
        d.save(db)

        dd = Doc.objects(db)[0]
        print dd.attachment.file.read()

    :param base_path:

        A string or callable: the directory where the files should be stored.

    """
    skip_type_conversion = True
    file_wrapper_class = FileWrapper

    def __init__(self, base_path, **kwargs):
        self.base_path = base_path  # media_root+upload_dir or whatever
        self._data = None

        if 'pickled' in kwargs:
            raise KeyError('Pickling is not allowed for file fields.')

        super(FileField, self).__init__(self.file_wrapper_class, **kwargs)

    def process_set_item(self, value):
        if value is None:
            return
        # value: a stream
        if isinstance(value, self.file_wrapper_class):
            return value
        return self.file_wrapper_class(stream=value, saved=False,
                                       base_path=self.base_path)

    def process_outgoing(self, value):
        # FileWrapper -> path
        value.save()
        return value.path

    def process_incoming(self, value):
        # path -> FileWrapper
        return self.file_wrapper_class(path=value, saved=True,
                                       base_path=self.base_path)


class ImageField(FileField):
    """A :class:`FileField` that provides extended support for images. The
    :attr:`ImageField.file` is an :class:`ImageWrapper` instance.

    Usage::

        class Photo(Document):
            summary = Field(unicode)
            image = ImageField(base_path='photos/')

        p = Photo(summary='Fido', image=open('fido.jpg'))
        p.save(db)

        # playing with image
        print "The photo is {0}×{1}px".format(*p.image.size)
        p.image.rotate(90)
        p.image.save()

    """
    file_wrapper_class = ImageWrapper

