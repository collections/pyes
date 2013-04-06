"""Object Document Mapper"""
from models import ElasticSearchModel
from mappings import *
from exceptions import ElasticModelException
from queryset import QuerySet

# Let's prevent people from shooting themselves in the foot as best we can
forbidden_names = ['_uid', '_id', '_type', '_source', '_all', '_analyzer', '_boost',
                   '_parent', '_routing', '_index', '_size', '_timestamp', '_ttl', 'fields']

class ModelMeta(type):
    _registered_models = {} # nested dict. [index][type] -> cls

    def __new__(mcs, name, bases, attrs):
        cls = super(ModelMeta, mcs).__new__(mcs, name, bases, attrs)
        cls._fields = {}
        for clazz in reversed(cls.__mro__):
            for name, field in clazz.__dict__.iteritems():
                if isinstance(field, ModelField):
                    field.name = field.name or name
                    if field.name in forbidden_names:
                        raise ElasticModelException('Field name %s is reserved. Invalid %s' % (name, field))
                    cls._fields[name] = field

        if getattr(cls.Meta, 'index', None) and getattr(cls.Meta, 'type', None):
            # TODO: Error checking here to make sure type and index are both defined on subclass!
            mcs.register_model(cls.Meta.index, cls.Meta.type, cls)

        return cls

    @classmethod
    def register_model(mcs, index, doc_type, model_cls):
        mcs._registered_models.setdefault(index, {})
        mcs._registered_models[index][doc_type] = model_cls
        model_cls.objects = QuerySet(model_cls)

    @classmethod
    def get_registered_model(mcs, index, type):
        return mcs._registered_models.get(index, {}).get(type, None)

def parse_data(data):
    "Return 3 tuple, class, attrs and meta. Useful for custom model factories"
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

    cls = ModelMeta.get_registered_model(meta.get('index', None), meta.get('type', None))
    return cls, attrs, meta

def model_factory(default_cls, force_default=False):
    def _model_factory(conn=None, data=None):
        cls, attrs, meta = parse_data(data)
        cls = default_cls if force_default or not cls else cls
        ins = cls(conn, **attrs)
        ins._meta.update(meta)
        return ins
    return _model_factory


def register_model_mappings(conn):
    for index_name, index_types in ModelMeta._registered_models.iteritems():
        conn.indices.create_index_if_missing(index_name)
        for type_name, cls in index_types.iteritems():
            mapping = {'properties': {name: field.as_dict() for name, field in cls._fields.iteritems()}}
            conn.indices.put_mapping(type_name, mapping, index_name)


class Model(ElasticSearchModel):
    __metaclass__ = ModelMeta

    objects = QuerySet(None)  # For IDE auto-completion. Will be set by metaclass to be a class specific queryset
    default_connection = None

    class Meta:
        index = None
        type = None

    def __init__(self, conn=None, *args, **kwargs):
        super(Model, self).__init__(conn, *args, **kwargs)
        self._meta.connection = self._meta.connection or self.default_connection
        for k, v in self.Meta.__dict__.iteritems():
            self._meta.setdefault(k, v)
        for name, field in self._fields.iteritems():
            if field.default is not None:
                self[name] = field.__get__(self, self.__class__)

    # Reverse attribute getters and setters changed by DotDict
    __setattr__ = dict.__setattr__
    __delattr__ = dict.__delattr__
    def __getattr__(self, attr):
        raise AttributeError

    def save(self, *args, **kwargs):
        for field in self._fields.values():
            field.validate(self)
            field.on_save(self)
        super(Model, self).save(*args, **kwargs)

    @classmethod
    def get_by_id(cls, id):
        return cls.default_connection.get(cls.Meta.index, cls.Meta.type, id)


class ToManyField(StringField):
    def __init__(self, ordered=True, unique=False, **kwargs):
        super(ToManyField, self).__init__(**kwargs)
        self.ordered = ordered
        self.unique = unique
        if self.ordered != True or self.unique != False:
            raise NotImplementedError

    def __get__(self, instance, owner):
        if not hasattr(instance, '_' + self.name):
            conn = instance._meta.connection
            ids = instance.get(self.name, None)
            ids = [tuple(i.split('/', 2)) for i in ids] if ids else None
            setattr(instance, '_' + self.name, conn.mget(ids) if ids else ids)
        return getattr(instance, '_' + self.name)

    def __set__(self, instance, value):
        # We use '/' here because elasticsearch index and type name are not allowed to contain slashes
        ids = ['%s/%s/%s' % (o._meta.index, o._meta.type, o._meta.id) for o in value or []]
        instance[self.name] = ids if value else value
        setattr(instance, '_' + self.name, value)

        # TODO: Rather than returning simple list, should instead return a lazily evaluated list proxy.