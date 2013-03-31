"""Object Document Mapper"""
from queryset import QuerySet, DoesNotExist, MultipleObjectsReturned
from models import ElasticSearchModel, DotDict
from mappings import AbstractField
from copy import deepcopy
from pyes import logger

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
            cls.objects = QuerySet(cls)
            # Temporary hacks needed to get QuerySet to cooperate
            cls._index = cls.Meta.index
            cls._type = cls.Meta.type

        return cls

    @classmethod
    def get_registered_model(mcs, index, type):
        mcs._registered_models.get(index, {}).get(type, None)


def model_factory(conn, data):
    cls = ModelMeta.get_registered_model(data._index, data._type)
    if not cls:
        return ElasticSearchModel(conn, data)

    ins = cls(conn)
    ins.update(data.pop("_source", DotDict()))
    ins.update(data.pop("fields", {}))
    ins._meta = DotDict([(k.lstrip("_"), v) for k, v in data.items()])
    ins._meta.parent = ins.pop("_parent", None)
    return ins

def put_model_mappings(conn):
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
        self._meta = DotDict(deepcopy(self.Meta.__dict__))
        self._meta.connection = conn or self.default_connection
        self.__initialised = True
        self.update(dict(*args, **kwargs))
        assert self._meta.connection

    def save(self, *args, **kwargs):
        for field in self._fields.values():
            field.validate(self)
        super(Model, self).save(*args, **kwargs)

    class Meta:
        index = None
        type = None
