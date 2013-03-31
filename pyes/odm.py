"""Object Document Mapper"""
from queryset import QuerySet, DoesNotExist, MultipleObjectsReturned
from models import ElasticSearchModel, DotDict
from mappings import AbstractField


class ModelMeta(type):
    _registered_models = {} # nested dict. [index][type] -> cls

    def __new__(mcs, name, bases, attrs):
        cls = super(ModelMeta, mcs).__new__(mcs, name, bases, attrs)
        cls._fields = {}
        for name, field in ((k, getattr(cls, k)) for k in dir(cls)):
            if isinstance(field, AbstractField):
                field.name = field.name or name
                cls._fields[name] = field

        if cls.Meta.index and cls.Meta.type:
            mcs._registered_models.setdefault(cls.Meta.index, {})
            mcs._registered_models[cls.Meta.index][cls.Meta.type] = cls
            cls.objects = QuerySet(model_factory(cls))
            # Temporary hacks needed to get QuerySet to cooperate
            cls._index = cls.Meta.index
            cls._type = cls.Meta.type

        return cls

    @classmethod
    def get_registered_model(mcs, index, type):
        mcs._registered_models.get(index, {}).get(type, None)


def model_factory(default_cls=ElasticSearchModel):
    def _model_factory(conn=None, data=None):
        data = data or {}
        if '_source' in data or 'fields' in data:
            cls = ModelMeta.get_registered_model(data.get('_index', None), data.get('_type', None)) or default_cls
            ins = cls(conn)
            ins.update(data.pop("_source", DotDict()))
            ins.update(data.pop("fields", {}))
            ins._meta = DotDict([(k.lstrip("_"), v) for k, v in data.items()])
            ins._meta.parent = ins.pop("_parent", None)
        else:
            ins = default_cls(conn)
            ins.update(data)
        return ins
    _model_factory.DoesNotExist = default_cls.DoesNotExist
    _model_factory.MultipleObjectsReturned = default_cls.MultipleObjectsReturned
    return _model_factory


def register_model_mappings(conn):
    for index_name, index_types in ModelMeta._registered_models.iteritems():
        conn.indices.create_index_if_missing(index_name)
        for type_name, cls in index_types.iteritems():
            mapping = {'properties': {name: field.as_dict() for name, field in cls._fields.iteritems()}}
            conn.indices.put_mapping(type_name, mapping, index_name)


class Model(ElasticSearchModel):
    __metaclass__ = ModelMeta

    DoesNotExist = DoesNotExist
    MultipleObjectsReturned = MultipleObjectsReturned
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
