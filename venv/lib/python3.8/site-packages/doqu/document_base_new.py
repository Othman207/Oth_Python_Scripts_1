'''
Schema (StructuredDocument):
    - collect "schema"
    - convert data in/out
        - on save
    - attrs
        - structure (dict)
        -

Validation (ValidatedDocument):
    - __getitem__(name)
    - __setitem__(name, value)
    - contribute_to_query(query)
    - validate()

        * per field: check value type (against structure)
        * per field: call the series of field validators

Relations (RelatedDocument):
    - ?

Saved state (StoredDocument):
    - _state
    - save(storage)
    - pk()

class Document(object):
    __metaclass__ = ComposedDocumentMetaclass
    handlers = [
        StructuredDocument,
        ValidatedDocument,
        RelatedDocument,
        StoredDocument,
    ]
    _on_save = []
'''

#--- Cooperative classes (*must* call super inits)

class DocMixin(object):
    def __init__(self, *args, **kwargs):
        super(DocMixin, self).__init__() #*args, **kwargs) # XXX
        self.handlers = []

class Structured(DocMixin):
#class Structured(object):
    def __init__(self, *args, **kwargs):
        super(Structured, self).__init__(*args, **kwargs)
        self.handlers.append('structure')

    def save(self):
        getattr(super(Structured, self), 'save', lambda:None)()
        print self, 'save()'

class Validated(DocMixin):
#class Validated(object):
    def __init__(self, *args, **kwargs):
        super(Validated, self).__init__(*args, **kwargs)
        self.handlers.append('validation')

class Stored(DocMixin):
#class Stored(object):
    def __init__(self, *args, **kwargs):
        super(Stored, self).__init__(*args, **kwargs)
        self.handlers.append('stored')

    def save(self):
        return '123'

class Related(DocMixin):
#class Related(object):
    def __init__(self, *args, **kwargs):
        super(Related, self).__init__(*args, **kwargs)
        self.handlers.append('related')

class Document(Validated, Structured, Stored, Related):
    def __init__(self, *args, **kwargs):
        super(Document, self).__init__(*args, **kwargs)
        self.handlers.append('composed')


print Document()
print Document().handlers

print Document(foo=123)

print Document().save()

'''
class Document(object):
    handlers = []

    @classmethod
    def register(cls, bundle):
        cls.handlers.append(bundle)

Document.register(StructureMixin)
Document.register(ValidationMixin)
print Document.handlers

class Fox(Document):
    pass

print Fox.handlers
Fox.register(DocMixin)
print Fox.handlers
print Document.handlers
print ValidationMixin().handlers
'''
