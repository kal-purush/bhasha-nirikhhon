from .base import BaseResource


class BaseWithSubclasses(BaseResource):
    _class_identifier = None

    @classmethod
    def _get_subclass_by_identifier(cls, identifier):
        if cls._class_identifier == identifier:
            return cls

        for x in cls.__subclasses__():
            subcls = x._get_subclass_by_identifier(identifier)
            if subcls is not None:
                return subcls

        return None

    @classmethod
    def _search_subclass(cls, identifier):
        subcls = cls._get_subclass_by_identifier(identifier)

        if subcls is None:
            raise ValueError('{} is no subclass of {}'.format(identifier, cls.__name__))

        return subcls

    @classmethod
    def _create_instance_from_data(cls, data):
        subcls = cls._search_subclass(data['_cls'])
        return subcls(**data)

    @classmethod
    def _deserialize(cls, data, many=False):
        if many:
            return [cls._deserialize(item) for item in data]

        subcls = cls._search_subclass(data['_cls'])

        return super(BaseWithSubclasses, subcls)._deserialize(data, many)