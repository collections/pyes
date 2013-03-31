"""Object Document Mapper"""
from models import ElasticSearchModel
from mappings import AbstractField
from exceptions import ElasticModelException


class ModelMeta(type):
    _registered_models = {} # nested dict. [index][type] -> cls

    def __new__(mcs, name, bases, attrs):
        cls = super(ModelMeta, mcs).__new__(mcs, name, bases, attrs)
        cls._fields = {}
        for name, field in ((k, getattr(cls, k)) for k in dir(cls)):
            if isinstance(field, AbstractField):
                field.name = field.name or name
                if field.name in ['_source', 'fields']:
                    raise ElasticModelException('Field name %s is reserved. Invalid %s' % (name, field))
                cls._fields[name] = field

        if cls.Meta.index and cls.Meta.type:
            mcs._registered_models.setdefault(cls.Meta.index, {})
            mcs._registered_models[cls.Meta.index][cls.Meta.type] = cls
            from queryset import QuerySet
            cls.objects = QuerySet(cls)

        return cls

    @classmethod
    def get_registered_model(mcs, index, type):
        mcs._registered_models.get(index, {}).get(type, None)


def model_factory(default_cls):
    def _model_factory(conn=None, data=None):
        if '_source' in data or 'fields' in data:
            # Assume data to be be in Elasticsearch API format if '_source' or 'fields' exists
            attrs = data.pop('_source', {})
            attrs.update(data.pop('fields', {}))
            meta = {k.lstrip('_'): v for k, v in data.iteritems()}
            meta['parent'] = attrs.pop('_parent', None)
        else:
            # Else assume data to be attrs provided by user
            meta = {}
            attrs = data

        cls = ModelMeta.get_registered_model(meta.get('index', None), meta.get('type', None)) or default_cls
        ins = cls(conn, **attrs)
        ins._meta.update(meta)
        return ins
    _model_factory.default_cls = default_cls
    return _model_factory


def register_model_mappings(conn):
    for index_name, index_types in ModelMeta._registered_models.iteritems():
        conn.indices.create_index_if_missing(index_name)
        for type_name, cls in index_types.iteritems():
            mapping = {'properties': {name: field.as_dict() for name, field in cls._fields.iteritems()}}
            conn.indices.put_mapping(type_name, mapping, index_name)


class Model(ElasticSearchModel):
    __metaclass__ = ModelMeta

    objects = None  # Will be set by metaclass to be a queryset
    default_connection = None

    def __init__(self, conn=None, *args, **kwargs):
        super(Model, self).__init__(conn, *args, **kwargs)
        self._meta.connection = self._meta.connection or self.default_connection
        for k, v in self.Meta.__dict__.iteritems():
            self._meta.setdefault(k, v)

    def save(self, *args, **kwargs):
        for field in self._fields.values():
            field.validate(self)
        super(Model, self).save(*args, **kwargs)

    class Meta:
        index = None
        type = None
